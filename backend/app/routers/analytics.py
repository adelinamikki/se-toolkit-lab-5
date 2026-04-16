"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, case, distinct, cast, Date
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner

router = APIRouter()


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab.

    Returns score distribution in four buckets: 0-25, 26-50, 51-75, 76-100.
    Always returns all four buckets, even if count is 0.
    """
    # Transform lab parameter (e.g., "lab-04" → "Lab 04")
    lab_title_pattern = f"Lab {lab.split('-')[1]}"

    # Find lab item
    lab_stmt = select(ItemRecord).where(
        (ItemRecord.type == "lab") & (ItemRecord.title.contains(lab_title_pattern))
    )
    lab_result = await session.execute(lab_stmt)
    lab_item = lab_result.scalars().first()

    if not lab_item:
        return []

    # Find task items belonging to this lab
    task_stmt = select(ItemRecord.id).where(
        (ItemRecord.type == "task") & (ItemRecord.parent_id == lab_item.id)
    )
    task_result = await session.execute(task_stmt)
    task_ids = [row[0] for row in task_result.all()]

    if not task_ids:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0},
        ]

    # Query interactions with score bucketing
    bucket_case = case(
        (InteractionLog.score < 26, "0-25"),
        (InteractionLog.score < 51, "26-50"),
        (InteractionLog.score < 76, "51-75"),
        else_="76-100"
    )

    stmt = (
        select(bucket_case.label("bucket"), func.count().label("count"))
        .where(
            (InteractionLog.item_id.in_(task_ids)) &
            (InteractionLog.score.isnot(None))
        )
        .group_by(bucket_case)
    )

    result = await session.execute(stmt)
    bucket_counts = {row.bucket: row.count for row in result.all()}

    # Ensure all buckets are present
    buckets = ["0-25", "26-50", "51-75", "76-100"]
    return [
        {"bucket": bucket, "count": bucket_counts.get(bucket, 0)}
        for bucket in buckets
    ]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab.

    Returns average score and attempt count for each task.
    """
    # Transform lab parameter
    lab_title_pattern = f"Lab {lab.split('-')[1]}"

    # Find lab item
    lab_stmt = select(ItemRecord).where(
        (ItemRecord.type == "lab") & (ItemRecord.title.contains(lab_title_pattern))
    )
    lab_result = await session.execute(lab_stmt)
    lab_item = lab_result.scalars().first()

    if not lab_item:
        return []

    # Find task items and compute statistics
    stmt = (
        select(
            ItemRecord.title.label("task"),
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(InteractionLog.id).label("attempts")
        )
        .join(InteractionLog, ItemRecord.id == InteractionLog.item_id)
        .where(
            (ItemRecord.type == "task") &
            (ItemRecord.parent_id == lab_item.id) &
            (InteractionLog.score.isnot(None))
        )
        .group_by(ItemRecord.id, ItemRecord.title)
        .order_by(ItemRecord.title)
    )

    result = await session.execute(stmt)
    return [
        {
            "task": row.task,
            "avg_score": float(row.avg_score) if row.avg_score else 0.0,
            "attempts": row.attempts
        }
        for row in result.all()
    ]


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab.

    Returns submission counts grouped by date.
    """
    # Transform lab parameter
    lab_title_pattern = f"Lab {lab.split('-')[1]}"

    # Find lab item
    lab_stmt = select(ItemRecord).where(
        (ItemRecord.type == "lab") & (ItemRecord.title.contains(lab_title_pattern))
    )
    lab_result = await session.execute(lab_stmt)
    lab_item = lab_result.scalars().first()

    if not lab_item:
        return []

    # Find task items
    task_stmt = select(ItemRecord.id).where(
        (ItemRecord.type == "task") & (ItemRecord.parent_id == lab_item.id)
    )
    task_result = await session.execute(task_stmt)
    task_ids = [row[0] for row in task_result.all()]

    if not task_ids:
        return []

    # Group interactions by date using strftime
    stmt = (
        select(
            func.strftime('%Y-%m-%d', InteractionLog.created_at).label("date"),
            func.count().label("submissions")
        )
        .where(InteractionLog.item_id.in_(task_ids))
        .group_by(func.strftime('%Y-%m-%d', InteractionLog.created_at))
        .order_by(func.strftime('%Y-%m-%d', InteractionLog.created_at))
    )

    result = await session.execute(stmt)
    return [
        {"date": row.date, "submissions": row.submissions}
        for row in result.all()
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab.

    Returns average score and student count for each group.
    """
    # Transform lab parameter
    lab_title_pattern = f"Lab {lab.split('-')[1]}"

    # Find lab item
    lab_stmt = select(ItemRecord).where(
        (ItemRecord.type == "lab") & (ItemRecord.title.contains(lab_title_pattern))
    )
    lab_result = await session.execute(lab_stmt)
    lab_item = lab_result.scalars().first()

    if not lab_item:
        return []

    # Find task items
    task_stmt = select(ItemRecord.id).where(
        (ItemRecord.type == "task") & (ItemRecord.parent_id == lab_item.id)
    )
    task_result = await session.execute(task_stmt)
    task_ids = [row[0] for row in task_result.all()]

    if not task_ids:
        return []

    # Group by student group with statistics
    stmt = (
        select(
            Learner.student_group.label("group"),
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(distinct(Learner.id)).label("students")
        )
        .join(InteractionLog, Learner.id == InteractionLog.learner_id)
        .where(
            (InteractionLog.item_id.in_(task_ids)) &
            (InteractionLog.score.isnot(None))
        )
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )

    result = await session.execute(stmt)
    return [
        {
            "group": row.group,
            "avg_score": float(row.avg_score) if row.avg_score else 0.0,
            "students": row.students
        }
        for row in result.all()
    ]
