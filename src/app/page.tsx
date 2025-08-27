'use client'
import { useMemo, useState } from 'react'
import Link from 'next/link'
import Fuse from 'fuse.js'

type Node = { make: string; model: string; years: number[]; path: string }

function buildIndex(): Node[] {
  // Scan fixtures we bundle; in production, consider a small /public/index.json
  const nodes: Node[] = [
    { make:'Ford', model:'Fiesta', years:[2013], path:'/car/ford/fiesta/2013' },
    { make:'Volkswagen', model:'Polo', years:[2013], path:'/car/volkswagen/polo/2013' }
  ]
  return nodes
}

export default function Home() {
  const [q, setQ] = useState('')
  const data = useMemo(buildIndex, [])
  const fuse = useMemo(()=> new Fuse(data, { keys:['make','model'], threshold:0.3 }),[data])
  const results = q ? fuse.search(q).map(r=>r.item) : data

  return (
    <section className="space-y-6">
      <h1 className="text-3xl font-semibold">Find reliability by model-year</h1>
      <div className="max-w-xl">
        <input
          value={q} onChange={e=>setQ(e.target.value)}
          placeholder="e.g., Ford Fiesta 2013" className="w-full rounded border px-3 py-2"
          aria-label="Search make and model"
        />
      </div>
      <div className="grid gap-3 sm:grid-cols-2">
        {results.map(n=>(
          <Link key={n.path} href={n.path} className="rounded border p-4 hover:bg-gray-50 dark:hover:bg-zinc-900">
            <div className="text-sm text-gray-600">{n.make}</div>
            <div className="text-lg font-medium">{n.model}</div>
            <div className="text-xs">Years: {n.years.join(', ')}</div>
          </Link>
        ))}
      </div>
      <div className="pt-4">
        <Link href="/compare" className="inline-flex items-center rounded bg-brand-600 px-4 py-2 text-white">Compare two models</Link>
      </div>
    </section>
  )
}
