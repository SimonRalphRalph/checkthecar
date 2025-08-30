// src/app/car/[make]/[model]/[year]/page.tsx
import fs from "node:fs";
import path from "node:path";
import { notFound } from "next/navigation";
import clsx from "clsx";
import Co2VedPanel from "@/components/Co2VedPanel";
// If these charts are client components, keep the ts-expect-error bridging comments you had:
import { PassRateLine, FailBar } from "@/components/Charts";
import type { Metadata } from "next";

/** ---- Types that match the ETL JSON shape ---- */
type MotCurveRow = {
  age: number;
  tests: number;
  pass_rate: number;  // 0..1
  mileage: { p50: number | null; p75: number | null; p90: number | null };
  // optional from ETL; may be all zeros if RFR not available
  fail_mix?: { bucket: string; share: number }[];
};

type Co2PanelRow = {
  fuel: string;
  co2_gkm: number | null;
  mpg: number | null;
  test_type: string; // WLTP/NEDC
  ved_band: string | null;
  ved_annual: number | null;
  ved_first_year?: number | null;
  ved_supplement?: {
    expensive_car?: { threshold_gbp: number; annual: number; years_applies: number[] } | null;
  } | null;
};

type CohortJson = {
  make: string;
  model: string;
  make_slug: string;
  model_slug: string;
  first_reg_year: number;
  fuels: string[];
  co2_panel: Co2PanelRow[];
  recalls: { year: number; count: number }[];
  mot_curve: MotCurveRow[];
  meta: { source: string; version: string };
};

/** ---- SSG params & metadata ---- */
export async function generateStaticParams() {
  // Optional: seed a couple of pages so `next build` has something to render
  // Replace with a small disk scan if you prefer
  const roots = ["ford/fiesta/2013", "volkswagen/polo/2013"];
  return roots.map((p) => {
    const [make, model, year] = p.split("/");
    return { make, model, year };
  });
}

export async function generateMetadata({
  params,
}: {
  params: { make: string; model: string; year: string };
}): Promise<Metadata> {
  const title = `${decodeURIComponent(params.year)} ${params.make} ${params.model} MOT pass rate & common failures – CheckTheCar`;
  return { title };
}

/** ---- Local loader from /public/data ---- */
function loadCohort(make: string, model: string, year: string): CohortJson | null {
  const p = path.join(process.cwd(), "public", "data", make, model, `${year}.json`);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf-8")) as CohortJson;
}

/** ---- Small transforms so your existing charts keep working ---- */
function toPassRateSeries(mot: MotCurveRow[]) {
  // Your <PassRateLine /> wanted {age, pass_rate}
  return mot
    .slice()
    .sort((a, b) => a.age - b.age)
    .map((r) => ({ age: r.age, pass_rate: r.pass_rate }));
}

function toMileagePercentiles(mot: MotCurveRow[]) {
  // Your page displayed a list of percentiles by age
  return mot
    .slice()
    .sort((a, b) => a.age - b.age)
    .map((r) => ({ age: r.age, p50: r.mileage.p50 ?? 0, p75: r.mileage.p75 ?? 0, p90: r.mileage.p90 ?? 0 }));
}

function failureSharesAtLatestAge(mot: MotCurveRow[]): Record<string, number> {
  // Pick the latest age row (typical chart choice) and convert fail_mix list → map
  const last = mot.slice().sort((a, b) => a.age - b.age).at(-1);
  if (!last || !last.fail_mix) return {};
  const obj: Record<string, number> = {};
  for (const item of last.fail_mix) obj[item.bucket] = item.share;
  return obj;
}

/** ---- Page ---- */
export default function Page({
  params,
}: {
  params: { make: string; model: string; year: string };
}) {
  const data = loadCohort(params.make, params.model, params.year);
  if (!data) return notFound();

  const passRateSeries = toPassRateSeries(data.mot_curve);
  const mileagePercentiles = toMileagePercentiles(data.mot_curve);
  const failureShares = failureSharesAtLatestAge(data.mot_curve);

  const latest = passRateSeries.at(-1);
  const badge = latest?.pass_rate ?? 0.7;
  const badgeClass = badge >= 0.78 ? "bg-green-600" : badge >= 0.7 ? "bg-amber-500" : "bg-red-600";

  return (
    <article className="space-y-8">
      <header className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold">
            {data.first_reg_year} {data.make} {data.model}
          </h1>
          <p className="text-sm text-gray-600">Age-adjusted MOT reliability and running costs</p>
        </div>
        <div className={clsx("rounded px-3 py-2 text-white text-sm", badgeClass)} aria-label="Reliability badge">
          {badge >= 0.78 ? "Green" : badge >= 0.7 ? "Amber" : "Red"}
        </div>
      </header>

      {/* Pass rate & Failures */}
      <section className="grid gap-6 md:grid-cols-2">
        <div className="rounded border p-4">
          <h2 className="font-medium mb-2">Pass rate vs age</h2>
          {/* @ts-expect-error Server → Client bridge */}
          <PassRateLine series={passRateSeries} />
          <p className="mt-2 text-xs text-gray-600">
            At {latest?.age ?? "—"} years, this cohort’s pass rate is {Math.round((latest?.pass_rate ?? 0) * 100)}%.
          </p>
        </div>
        <div className="rounded border p-4">
          <h2 className="font-medium mb-2">Top failure categories</h2>
          {/* @ts-expect-error Server → Client bridge */}
          <FailBar shares={failureShares} />
          <p className="mt-2 text-xs">
            Categories reflect DVSA groupings (brakes, suspension/tyres, emissions, lights). “Dangerous” fails require
            immediate repair; “Major” fails mean the vehicle did not meet legal standards.
          </p>
        </div>
      </section>

      {/* Mileage */}
      <section className="rounded border p-4">
        <h2 className="font-medium mb-2">Mileage distribution (percentiles)</h2>
        <ul className="text-sm grid gap-2 sm:grid-cols-2">
          {mileagePercentiles.map((m) => (
            <li key={m.age} className="flex justify-between border rounded px-3 py-2">
              <span>Age {m.age}y</span>
              <span>
                P50 {m.p50.toLocaleString()} · P75 {m.p75.toLocaleString()} · P90 {m.p90.toLocaleString()}
              </span>
            </li>
          ))}
        </ul>
        <p className="mt-2 text-xs text-gray-600">
          Percentiles approximate typical vs high-use cars for this model-year.
        </p>
      </section>

      {/* Recalls & VED/CO2 */}
      <section className="grid gap-6 md:grid-cols-2">
        <div className="rounded border p-4">
          <h2 className="font-medium mb-2">Recalls timeline</h2>
          <ul className="text-sm space-y-1">
            {data.recalls
              .slice()
              .sort((a, b) => a.year - b.year)
              .map((r) => (
                <li key={r.year} className="flex justify-between">
                  <span>{r.year}</span>
                  <span>{r.count} recall(s)</span>
                </li>
              ))}
          </ul>
          <p className="mt-2 text-xs">
            <a className="underline" href="https://www.check-vehicle-recalls.service.gov.uk/">
              Check recall details on GOV.UK
            </a>
            .
          </p>
        </div>

        <div className="rounded border p-4">
          <h2 className="font-medium mb-2">Official CO₂ / MPG & VED</h2>
          {/* Drop-in VED/CO2 card(s) */}
          <Co2VedPanel panels={data.co2_panel} />
        </div>
      </section>

      <aside className="rounded border p-4 text-sm">
        <h3 className="font-medium mb-2">“Is this typical?”</h3>
        <p>
          At {latest?.age ?? "—"} years, this model’s median mileage is{" "}
          <b>
            {
              // find the matching mot_curve row and show p50
              data.mot_curve.find((r) => r.age === (latest?.age ?? -1))?.mileage.p50 ?? "—"
            }
          </b>
          .
          Compare your car against this to judge typical vs high use.
        </p>
        <p className="mt-2">
          <a className="underline" href="/methodology">
            Methodology & limitations
          </a>
        </p>
      </aside>
    </article>
  );
}
