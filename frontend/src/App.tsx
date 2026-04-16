import { useState } from "react"
import Dashboard from "./Dashboard"
import Items from "./Items"

export default function App() {
  const [page, setPage] = useState<"items" | "dashboard">("items")

  return (
    <div>
      <header style={{ display: "flex", gap: 10 }}>
        <button onClick={() => setPage("items")}>Items</button>
        <button onClick={() => setPage("dashboard")}>Dashboard</button>
      </header>

      {page === "items" && <Items />}
      {page === "dashboard" && <Dashboard />}
    </div>
  )
}