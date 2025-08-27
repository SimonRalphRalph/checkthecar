import fs from 'node:fs'
import path from 'node:path'
import { notFound } from 'next/navigation'
import { PassRateLine, FailBar } from '@/components/Charts'
import { Metadata } from 'next'
import clsx from 'clsx'

type Cohort = {
  make: string; model: string; firstRegYear: number;
  pass_rate_by_age: {age:number; pass_rate:number}[];
  mileage_percentiles_by_age: {age:number; p50:number; p75:number; p90:number}[];
  failure_shares: Record<string,number>;
  recalls_timeline: {year:number; count:number}[];
  official?: { co2_g_km?: number; mpg_combined?: number; cycle?: string }
}

export async function generateStaticParams() {
  // scan fixtures for SSG
  const roots = ['ford/fiesta/2013','volkswagen/polo/2013']
  return roots.map(p => {
    const [make, model, year] = p.split('/')
    return { make, model, year }
  })
}

export async function generateMetadata({ params }:{ params:{ make:string; model:string; year:string } }): Promise<Metadata> {
  const title = `${decodeURIComponent(params.year)} ${params.make} ${params.model} MOT pass rate & common failures – CheckTheCar`
  return { title }
}

function loadCohort(make:string, model:string, year:string): Cohort | null {
  const p = path.join(process.cwd(), 'public', 'data', make, model, `${year}.json`)
  if (!fs.existsSync(p)) return null
  return JSON.parse(fs.readFileSync(p, 'utf-8'))
}

export default function Page({ params }:{ params:{ make:string; model:string; year:string } }) {
  const data = loadCohort(params.make, params.model, params.year)
  if (!data) return notFound()
  const badge = data.pass_rate_by_age.at(-1)?.pass_rate ?? 0.7
  const badgeClass = badge >= 0.78 ? 'bg-green-600' : badge >= 0.7 ? 'bg-amber-500' : 'bg-red-600'

  return (
    <article className="space-y-8">
      <header className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold">{data.firstRegYear} {data.make} {data.model}</h1>
          <p className="text-sm text-gray-600">Age-adjusted MOT reliability and running costs</p>
        </div>
        <div className={clsx('rounded px-3 py-2 text-white text-sm', badgeClass)} aria-label="Reliability badge">
          {badge >= 0.78 ? 'Green' : badge >= 0.7 ? 'Amber' : 'Red'}
        </div>
      </header>

      <section className="grid gap-6 md:grid-cols-2">
        <div className="rounded border p-4">
          <h2 className="font-medium mb-2">Pass rate vs age</h2>
          {/* @ts-expect-error Server Components -> Client */}
          <PassRateLine series={data.pass_rate_by_age} />
          <p className="mt-2 text-xs text-gray-600">
            “Is this typical?” At {data.pass_rate_by_age.at(-1)?.age} years, this cohort’s pass rate is {Math.round((data.pass_rate_by_age.at(-1)?.pass_rate||0)*100)}%.
          </p>
        </div>
        <div className="rounded border p-4">
          <h2 className="font-medium mb-2">Top failure categories</h2>
          {/* @ts-expect-error */}
          <FailBar shares={data.failure_shares||{}} />
          <p className="mt-2 text-xs">
            Categories reflect DVSA groupings (brakes, suspension, tyres, emissions). “Dangerous” fails require immediate repair; “Major” fails mean the vehicle did not meet legal standards.
          </p>
        </div>
      </section>

      <section className="rounded border p-4">
        <h2 className="font-medium mb-2">Mileage distribution (percentiles)</h2>
        <ul className="text-sm grid gap-2 sm:grid-cols-2">
          {data.mileage_percentiles_by_age.map(m=>(
            <li key={m.age} className="flex justify-between border rounded px-3 py-2">
              <span>Age {m.age}y</span><span>P50 {m.p50.toLocaleString()} km · P75 {m.p75.toLocaleString()} · P90 {m.p90.toLocaleString()}</span>
            </li>
          ))}
        </ul>
        <p className="mt-2 text-xs text-gray-600">Percentiles approximate typical vs high-use cars for this model-year.</p>
      </section>

      <section className="grid gap-6 md:grid-cols-2">
        <div className="rounded border p-4">
          <h2 className="font-medium mb-2">Recalls timeline</h2>
          <ul className="text-sm space-y-1">
            {data.recalls_timeline.sort((a,b)=>a.year-b.year).map(r=>(
              <li key={r.year} className="flex justify-between"><span>{r.year}</span><span>{r.count} recall(s)</span></li>
            ))}
          </ul>
          <p className="mt-2 text-xs"><a className="underline" href="https://www.check-vehicle-recalls.service.gov.uk/">Check recall details on GOV.UK</a>.</p>
        </div>
        <div className="rounded border p-4">
          <h2 className="font-medium mb-2">Official CO₂ / MPG & VED</h2>
          <p className="text-sm">
            {data.official?.co2_g_km ? <>CO₂: <b>{Math.round(data.official.co2_g_km)}</b> g/km ·</> : null}
            {data.official?.mpg_combined ? <> MPG (combined): <b>{Math.round(data.official.mpg_combined)}</b> ·</> : null}
            {data.official?.cycle ? <> Cycle: {data.official.cycle}</> : null}
          </p>
          <p className="mt-2 text-xs text-gray-600">Official figures (WLTP/NEDC) are for comparison only and may differ from real world. WLTP replaced NEDC in 2017/2018. <a className="underline" href="https://www.vehicle-certification-agency.gov.uk/fuel-consumption-co2/the-worldwide-harmonised-light-vehicle-test-procedure/">Learn more</a>.</p>
        </div>
      </section>

      <aside className="rounded border p-4 text-sm">
        <h3 className="font-medium mb-2">“Is this typical?”</h3>
        <p>At {data.mileage_percentiles_by_age.at(-1)?.age} years, this model’s median mileage is <b>{data.mileage_percentiles_by_age.at(-1)?.p50.toLocaleString()} km</b>. Compare your car’s mileage to see if it is above or below typical for its age.</p>
        <p className="mt-2"><a className="underline" href="/methodology">Methodology & limitations</a></p>
      </aside>
    </article>
  )
}
