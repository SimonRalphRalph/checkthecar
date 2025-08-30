// lib/ved.ts
export type VedPanel = {
  fuel: string;
  co2_gkm: number | null;
  mpg: number | null;
  test_type: string; // WLTP/NEDC
  ved_band: string | null;
  ved_annual: number | null;
  ved_first_year?: number | null;
  ved_supplement?: {
    expensive_car?: {
      threshold_gbp: number;
      annual: number;
      years_applies: number[];
    } | null;
  } | null;
};

export function formatVed(panel: VedPanel): {
  headline: string;
  sub: string | null;
  tooltip: string;
} {
  if (panel.ved_band) {
    // 2001–2017
    return {
      headline: `VED band ${panel.ved_band}: £${panel.ved_annual ?? "—"}/year`,
      sub: null,
      tooltip:
        "Cars first registered 1 Mar 2001–31 Mar 2017 pay CO₂-based annual VED (see GOV.UK for details).",
    };
  }
  // Post-2017 (flat standard rate)
  const sup = panel.ved_supplement?.expensive_car;
  const supText =
    sup
      ? `Cars with a list price over £${sup.threshold_gbp.toLocaleString()} pay a £${sup.annual}/year supplement in years ${sup.years_applies.join(", ")}.`
      : "";
  const firstYear = panel.ved_first_year ? `First-year (showroom): £${panel.ved_first_year}. ` : "";
  return {
    headline: `VED (standard rate): £${panel.ved_annual ?? "—"}/year`,
    sub: firstYear + (supText || null),
    tooltip:
      "For cars first registered from 1 Apr 2017. Standard annual rate shown; first-year depends on CO₂. Expensive-car supplement may apply.",
  };
}
