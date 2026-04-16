"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
from sqlalchemy import func, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.settings import settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API.

    Uses HTTP Basic Auth to authenticate with the autochecker API.
    Returns a list of item dictionaries containing lab, task, title, and type.
    Raises an exception if the API returns a non-200 status code.
    """
    url = f"{settings.autochecker_api_url}/api/items"
    auth = (settings.autochecker_email, settings.autochecker_password)

    async with httpx.AsyncClient() as client:
        response = await client.get(url, auth=auth)
        response.raise_for_status()
        return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API with pagination.

    Supports incremental syncs using the 'since' parameter. Handles pagination
    by checking the 'has_more' flag and using the last log's submitted_at
    timestamp to fetch the next batch.

    Args:
        since: Optional timestamp to fetch logs after (for incremental sync)

    Returns:
        Combined list of all log dicts from all pages
    """
    url = f"{settings.autochecker_api_url}/api/logs"
    auth = (settings.autochecker_email, settings.autochecker_password)
    all_logs = []

    current_since = since

    async with httpx.AsyncClient() as client:
        while True:
            params = {"limit": 500}
            if current_since:
                params["since"] = current_since.isoformat()

            response = await client.get(url, auth=auth, params=params)
            response.raise_for_status()
            data = response.json()

            logs = data.get("logs", [])
            all_logs.extend(logs)

            # Check pagination flag
            if not data.get("has_more", False):
                break

            # Use the last log's submitted_at as the new "since" value
            if logs:
                last_log = logs[-1]
                current_since = datetime.fromisoformat(
                    last_log["submitted_at"].replace("Z", "+00:00")
                )

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database.

    Process labs and tasks from the items catalog, creating ItemRecord entries
    if they don't already exist. Builds a lookup map from lab short IDs to
    database records for use when processing tasks.

    Args:
        items: Raw item dicts from fetch_items()
        session: Database session

    Returns:
        Number of newly created items
    """
    new_count = 0
    lab_lookup = {}

    # Process labs first
    labs = [item for item in items if item["type"] == "lab"]
    for lab_item in labs:
        lab_title = lab_item["title"]
        lab_short_id = lab_item["lab"]

        # Check if lab already exists
        stmt = select(ItemRecord).where(
            (ItemRecord.type == "lab") & (ItemRecord.title == lab_title)
        )
        existing = await session.execute(stmt)
        existing_record = existing.scalars().first()

        if not existing_record:
            # Create new lab
            record = ItemRecord(type="lab", title=lab_title)
            session.add(record)
            await session.flush()  # Flush to get the ID
            new_count += 1
            lab_lookup[lab_short_id] = record
        else:
            lab_lookup[lab_short_id] = existing_record

    # Process tasks
    tasks = [item for item in items if item["type"] == "task"]
    for task_item in tasks:
        task_title = task_item["title"]
        lab_short_id = task_item["lab"]

        # Find parent lab
        parent_lab = lab_lookup.get(lab_short_id)
        if not parent_lab:
            continue  # Skip if parent lab not found

        # Check if task already exists
        stmt = select(ItemRecord).where(
            (ItemRecord.type == "task")
            & (ItemRecord.title == task_title)
            & (ItemRecord.parent_id == parent_lab.id)
        )
        existing = await session.execute(stmt)
        existing_record = existing.scalars().first()

        if not existing_record:
            # Create new task
            record = ItemRecord(
                type="task", title=task_title, parent_id=parent_lab.id
            )
            session.add(record)
            new_count += 1

    await session.commit()
    return new_count


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database.

    Creates learners (by external_id) and inserts interaction logs with
    idempotent upserts (skips logs that already exist by external_id).
    Maps log fields (lab short ID, task short ID) to item titles in the DB.

    Args:
        logs: Raw log dicts from the API
        items_catalog: Raw item dicts from fetch_items()
        session: Database session

    Returns:
        Number of newly created interaction logs
    """
    new_count = 0

    # Build lookup: (lab_short_id, task_short_id) -> item_title
    title_lookup = {}
    for item in items_catalog:
        if item["type"] == "lab":
            # Labs: key is (lab, None)
            key = (item["lab"], None)
            title_lookup[key] = item["title"]
        elif item["type"] == "task":
            # Tasks: key is (lab, task)
            key = (item["lab"], item["task"])
            title_lookup[key] = item["title"]

    for log in logs:
        # 1. Find or create learner
        learner_external_id = str(log["student_id"])
        stmt = select(Learner).where(Learner.external_id == learner_external_id)
        result = await session.execute(stmt)
        learner = result.scalars().first()

        if not learner:
            learner = Learner(
                external_id=learner_external_id, student_group=log.get("group", "")
            )
            session.add(learner)
            await session.flush()

        # 2. Find matching item
        lookup_key = (log["lab"], log.get("task"))
        if lookup_key not in title_lookup:
            # Skip this log if item is not found
            continue

        item_title = title_lookup[lookup_key]
        stmt = select(ItemRecord).where(ItemRecord.title == item_title)
        result = await session.execute(stmt)
        item = result.scalars().first()

        if not item:
            # Skip if item not found in DB
            continue

        # 3. Check if interaction already exists (idempotent upsert)
        interaction_external_id = log["id"]
        stmt = select(InteractionLog).where(
            InteractionLog.external_id == interaction_external_id
        )
        result = await session.execute(stmt)
        existing_interaction = result.scalars().first()

        if existing_interaction:
            # Skip — already exists
            continue

        # 4. Create new interaction log
        created_at = datetime.fromisoformat(
            log["submitted_at"].replace("Z", "+00:00")
        )

        interaction = InteractionLog(
            external_id=interaction_external_id,
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log.get("score"),
            checks_passed=log.get("passed"),
            checks_total=log.get("total"),
            created_at=created_at,
        )
        session.add(interaction)
        new_count += 1

    await session.commit()
    return new_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline.

    Orchestrates the complete data sync:
    1. Fetches and loads items from the API
    2. Determines the last synced timestamp
    3. Fetches and loads logs since that timestamp
    4. Returns a summary of new and total records

    Args:
        session: Database session

    Returns:
        Dict with keys: new_records (new interactions), total_records (total interactions in DB)
    """
    # Step 1: Fetch and load items
    items_catalog = await fetch_items()
    await load_items(items_catalog, session)

    # Step 2: Determine the last synced timestamp
    stmt = select(func.max(InteractionLog.created_at))
    result = await session.execute(stmt)
    last_sync = result.scalars().first()

    # Step 3: Fetch and load logs
    new_interactions = await fetch_logs(since=last_sync)
    new_records = await load_logs(new_interactions, items_catalog, session)

    # Step 4: Get total number of interactions in DB
    stmt = select(func.count(InteractionLog.id))
    result = await session.execute(stmt)
    total_records = result.scalars().first() or 0

    return {"new_records": new_records, "total_records": total_records}
