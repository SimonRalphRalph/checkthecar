// src/components/Co2VedPanel.tsx
import { formatVed, VedPanel } from "@/lib/ved";

export default function Co2VedPanel({ panels }: { panels: VedPanel[] }) {
  return (
    <div className="grid gap-3">
      {panels.map((p, i) => {
        const f = formatVed(p);
        return (
          <div key={i} className="rounded-2xl border p-4 shadow-sm bg-white">
            <div className="flex items-baseline justify-between">
              <h3 className="font-semibold capitalize">{p.fuel}</h3>
              <span className="text-sm text-gray-500">{p.test_type}</span>
            </div>
            <div className="mt-1 text-sm text-gray-700">
              CO₂: {p.co2_gkm ?? "—"} g/km · MPG: {p.mpg ?? "—"}
            </div>
            <div className="mt-2 text-lg font-medium">{f.headline}</div>
            {f.sub && <div className="text-sm text-gray-600 mt-1">{f.sub}</div>}
            <div className="text-xs text-gray-500 mt-1">{f.tooltip}</div>
          </div>
        );
      })}
    </div>
  );
}
