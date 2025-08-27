'use client'
import { Line, Bar } from 'react-chartjs-2'
import {
  Chart as ChartJS, CategoryScale, LinearScale, PointElement, LineElement,
  BarElement, Tooltip, Legend
} from 'chart.js'
ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, BarElement, Tooltip, Legend)

export function PassRateLine({ series }:{ series:{age:number; pass_rate:number}[] }) {
  const data = {
    labels: series.map(s=>`${s.age}y`),
    datasets: [{ label: 'Pass rate', data: series.map(s=>Math.round(s.pass_rate*100)), borderWidth: 2 }]
  }
  const options = { responsive: true, plugins:{legend:{display:false}}, scales:{y:{ticks:{callback:(v)=>v+'%'}}} }
  return <Line data={data} options={options} />
}

export function FailBar({ shares }:{ shares: Record<string, number> }) {
  const labels = Object.keys(shares)
  const data = { labels, datasets: [{ label: 'Share of failures', data: labels.map(k=>Math.round((shares[k]||0)*100)) }] }
  const options = { responsive:true, plugins:{legend:{display:false}}, scales:{y:{ticks:{callback:(v)=>v+'%'}}} }
  return <Bar data={data} options={options} />
}
