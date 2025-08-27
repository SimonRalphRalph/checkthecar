'use client'
import { useSearchParams } from 'next/navigation'
import { useEffect, useState } from 'react'

async function fetchCohort(path: string) {
  const res = await fetch(`/data/${path}.json`)
  if (!res.ok) return null
  return res.json()
}

export default function Compare() {
  const sp = useSearchParams()
  const left = sp.get('left') ?? 'ford/fiesta/2013'
  const right = sp.get('right') ?? 'volkswagen/polo/2013'
  const [a,setA] = useState<any>(null)
  const [b,setB] = useState<any>(null)
  useEffect(()=>{
    fetchCohort(left).then(setA)
    fetchCohort(right).then(setB)
  },[left,right])

  if (!a || !b) return <p>Loading…</p>
  return (
    <section className="space-y-6">
      <h1 className="text-2xl font-semibold">Compare</h1>
      <div className="grid gap-4 md:grid-cols-2">
        {[a,b].map((c: any, i)=>(
          <div key={i} className="rounded border p-4">
            <h2 className="font-medium">{c.firstRegYear} {c.make} {c.model}</h2>
            <p className="text-sm">Latest pass rate: <b>{Math.round(c.pass_rate_by_age.at(-1)?.pass_rate*100)}%</b></p>
            <p className="text-sm">Median mileage @ age {c.mileage_percentiles_by_age.at(-1)?.age}: <b>{c.mileage_percentiles_by_age.at(-1)?.p50.toLocaleString()} km</b></p>
          </div>
        ))}
      </div>
    </section>
  )
}
