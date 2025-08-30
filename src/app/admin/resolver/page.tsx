'use client'
import { useState } from 'react'

export default function Resolver() {
  const [rows, setRows] = useState([{ make:'Ford', model:'Fiesta Zetec', canonical_make:'Ford', canonical_model:'Fiesta' }])
  const csv = 'make,model,canonical_make,canonical_model\n' + rows.map(r=>`${r.make},${r.model},${r.canonical_make},${r.canonical_model}`).join('\n')
  return (
    <section className="space-y-4">
      <h1 className="text-xl font-semibold">Model Alias Resolver</h1>
      <p className="text-sm">Approve merges and export to CSV.</p>
      <textarea className="w-full h-64 border rounded p-2 text-xs" value={csv} readOnly/>
      <a className="inline-block rounded bg-brand-600 text-white px-4 py-2" href={`data:text/csv;charset=utf-8,${encodeURIComponent(csv)}`} download="model_aliases.csv">Download CSV</a>
    </section>
  )
}
