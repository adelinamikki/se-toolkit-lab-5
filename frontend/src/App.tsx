import { useState } from "react"
import Dashboard from "./Dashboard"

export default function App() {
  const [page, setPage] = useState<"dashboard">("dashboard")

  return (
    <div>
      <header style={{ display: "flex", gap: 10 }}>
        <button onClick={() => setPage("dashboard")}>Dashboard</button>
      </header>

      {page === "dashboard" && <Dashboard />}
    </div>
  )
}