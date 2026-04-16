import { useEffect, useState } from "react"
import { Bar, Line } from "react-chartjs-2"
import {
    Chart as ChartJS,
    CategoryScale,
    LinearScale,
    BarElement,
    PointElement,
    LineElement,
    Title,
    Tooltip,
    Legend,
} from "chart.js"

ChartJS.register(
    CategoryScale,
    LinearScale,
    BarElement,
    PointElement,
    LineElement,
    Title,
    Tooltip,
    Legend
)

type Score = { bucket: string; count: number }
type Timeline = { date: string; submissions: number }
type PassRate = { task: string; avg_score: number; attempts: number }

export default function Dashboard() {
    const [lab, setLab] = useState("lab-04")

    const [scores, setScores] = useState<Score[]>([])
    const [timeline, setTimeline] = useState<Timeline[]>([])
    const [passRates, setPassRates] = useState<PassRate[]>([])

    const token = localStorage.getItem("api_key")

    useEffect(() => {
        if (!token) return

        fetch(`/analytics/scores?lab=${lab}`, {
            headers: { Authorization: `Bearer ${token}` },
        }).then(r => r.json()).then(setScores)

        fetch(`/analytics/timeline?lab=${lab}`, {
            headers: { Authorization: `Bearer ${token}` },
        }).then(r => r.json()).then(setTimeline)

        fetch(`/analytics/pass-rates?lab=${lab}`, {
            headers: { Authorization: `Bearer ${token}` },
        }).then(r => r.json()).then(setPassRates)
    }, [lab])

    const barData = {
        labels: scores.map(s => s.bucket),
        datasets: [{ label: "Scores", data: scores.map(s => s.count) }],
    }

    const lineData = {
        labels: timeline.map(t => t.date),
        datasets: [{ label: "Submissions", data: timeline.map(t => t.submissions) }],
    }

    return (
        <div>
            <h2>Dashboard</h2>

            <select value={lab} onChange={e => setLab(e.target.value)}>
                <option value="lab-04">lab-04</option>
                <option value="lab-03">lab-03</option>
            </select>

            <h3>Scores</h3>
            <Bar data={barData} />

            <h3>Timeline</h3>
            <Line data={lineData} />

            <h3>Pass rates</h3>
            <table>
                <thead>
                    <tr>
                        <th>Task</th>
                        <th>Avg Score</th>
                        <th>Attempts</th>
                    </tr>
                </thead>
                <tbody>
                    {passRates.map(p => (
                        <tr key={p.task}>
                            <td>{p.task}</td>
                            <td>{p.avg_score}</td>
                            <td>{p.attempts}</td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    )
}