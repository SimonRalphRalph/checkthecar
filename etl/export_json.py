# etl/export_json.py
import json, os
from pathlib import Path
from typing import Dict, Any

def write_cohort_json(root: Path, aggregates: Dict[str, Any], recalls_df, vca_df):
    root.mkdir(parents=True, exist_ok=True)
    for make, models in aggregates.items():
        for model, years in models.items():
            for year, metrics in years.items():
                # attach recall timeline
                rec = (
                    recalls_df[(recalls_df["make"].str.lower()==make.lower()) &
                               (recalls_df["model"].str.contains(model, case=False, na=False))]
                    .groupby("year")["count"].sum().reset_index().to_dict(orient="records")
                )
                # CO2 & MPG (take median across variants)
                vc = vca_df[(vca_df["make"].str.lower()==make.lower()) &
                            (vca_df["model_family"].str.contains(model, case=False, na=False))]
                co2 = float(vc["co2"].median()) if not vc.empty else None
                mpg = float(vc["mpg_combined"].median()) if not vc.empty else None
                cycle = vc["test_cycle"].mode()[0] if not vc.empty else None

                payload = {
                    "make": make, "model": model, "firstRegYear": year,
                    **metrics,
                    "recalls_timeline": [{"year": int(r["year"]), "count": int(r["count"])} for r in rec],
                    "official": {"co2_g_km": co2, "mpg_combined": mpg, "cycle": cycle},
                }

                out_dir = root / make.lower().replace(" ","-") / model.lower().replace(" ","-")
                out_dir.mkdir(parents=True, exist_ok=True)
                with open(out_dir / f"{year}.json", "w", encoding="utf-8") as f:
                    json.dump(payload, f, ensure_ascii=False, separators=(",",":"))
