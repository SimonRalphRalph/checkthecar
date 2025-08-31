import json
from pathlib import Path
import pandas as pd
from .paths import MOT_AGG_PARQUET, RECALLS_PARQUET, VCA_PARQUET, PUB, VED_JSON
from .ved import load_ved_bands, ved_for_vehicle

def _compact_float(x, ndigits=2):
    try:
        if x is None or pd.isna(x): return None
    except Exception:
        pass
    return round(float(x), ndigits)

def _top_failure_buckets(row: pd.Series) -> list[dict]:
    buckets = {
        "brakes": row.get("brakes", 0.0),
        "steering": row.get("steering", 0.0),
        "axles_wheels_tyres_suspension": row.get("axles_wheels_tyres_suspension", 0.0),
        "emissions": row.get("emissions", 0.0),
        "lights": row.get("lights", 0.0),
        "visibility": row.get("visibility", 0.0),
        "body_structure": row.get("body_structure", 0.0),
        "other_equipment": row.get("other_equipment", 0.0),
        "other": row.get("other", 0.0),
    }
    items = sorted(buckets.items(), key=lambda kv: kv[1], reverse=True)[:5]
    return [{"bucket": k, "share": _compact_float(v, 3)} for k, v in items if v and v > 0]

def _recall_timeline(rec_df: pd.DataFrame, mk: str, md: str) -> list[dict]:
    if rec_df is None or rec_df.empty: return []
    sub = rec_df[(rec_df["norm_make"]==mk) & (rec_df["norm_model"]==md)]
    if sub.empty: return []
    sub = sub.groupby("year", dropna=True)["recalls"].sum().reset_index().sort_values("year")
    return [{"year": int(r.year), "count": int(r.recalls)} for r in sub.itertuples(index=False)]

def _fuel_group(df: pd.DataFrame) -> list[str]:
    fuels = df["fuel_type"].astype(str).str.lower().str.strip().replace("", pd.NA).dropna().unique().tolist()
    return sorted(fuels)

def build_and_publish():
    print("Reading Parquet…")
    mot = pd.read_parquet(MOT_AGG_PARQUET)
    rec = pd.read_parquet(RECALLS_PARQUET) if Path(RECALLS_PARQUET).exists() else pd.DataFrame(columns=["norm_make","norm_model","year","recalls"])
    vca = pd.read_parquet(VCA_PARQUET) if Path(VCA_PARQUET).exists() else pd.DataFrame(columns=["norm_make","norm_model","first_use_year","fuel_type","co2_gkm","mpg_combined","test_type"])

    mot = mot[(mot["age_at_test"] >= 0) & (mot["age_at_test"] <= 15)]
    cohort_keys = ["norm_make","norm_model","make_slug","model_slug","first_use_year"]

    vca_simple = (
        vca.groupby(["norm_make","norm_model","first_use_year","fuel_type"], dropna=False)
           .agg(co2_gkm=("co2_gkm","median"),
                mpg=("mpg_combined","median"),
                test_type=("test_type", lambda s: s.mode().iloc[0] if len(s.dropna()) else ""))
           .reset_index()
    )

    ved_bands = load_ved_bands(str(VED_JSON)) if Path(VED_JSON).exists() else {"eras":{}}

    cohorts = mot.groupby(cohort_keys, dropna=False).size().reset_index().drop(columns=0)

    out_count = 0
    for r in cohorts.itertuples(index=False):
        mk, md, mk_slug, md_slug, year = r
        msub = mot[(mot["norm_make"]==mk) & (mot["norm_model"]==md) & (mot["first_use_year"]==year)].sort_values("age_at_test")
        if msub.empty: continue

        fuels = _fuel_group(msub)

        curve = []
        for rr in msub.itertuples(index=False):
            curve.append({
                "age": int(rr.age_at_test),
                "tests": int(rr.tests),
                "pass_rate": _compact_float(rr.pass_rate, 3),
                "mileage": {
                    "p50": _compact_float(rr.median_mileage, 0),
                    "p75": _compact_float(rr.p75_mileage, 0),
                    "p90": _compact_float(rr.p90_mileage, 0),
                },
                "fail_mix": _top_failure_buckets(pd.Series(rr._asdict()))
            })

        vsub = vca_simple[(vca_simple["norm_make"]==mk) & (vca_simple["norm_model"]==md) & (vca_simple["first_use_year"]==year)].copy()
        co2_panel = []
        for vs in vsub.itertuples(index=False):
            ved = ved_for_vehicle(ved_bands, vs.co2_gkm, int(year), vs.fuel_type)
            co2_panel.append({
                "fuel": vs.fuel_type,
                "co2_gkm": _compact_float(vs.co2_gkm, 0),
                "mpg": _compact_float(vs.mpg, 0),
                "test_type": vs.test_type,
                "ved_band": ved["band"],
                "ved_annual": ved["annual"],
                "ved_first_year": ved.get("first_year"),
                "ved_supplement": ved.get("supplement"),
            })

        recalls = _recall_timeline(rec, mk, md)

        doc = {
            "make": mk, "model": md,
            "make_slug": mk_slug, "model_slug": md_slug,
            "first_reg_year": int(year),
            "fuels": fuels,
            "co2_panel": co2_panel,
            "recalls": recalls,
            "mot_curve": curve,
            "meta": {
                "source": "DVSA anonymised MOT results & failure items (OGL v3.0); DVSA Recalls; VCA CO₂/MPG; GOV.UK VED",
                "version": "weekly",
            }
        }

        out_dir = PUB / mk_slug / md_slug
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{int(year)}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False, separators=(",",":"))
        out_count += 1

    print(f"Published {out_count} cohort JSON files to {PUB}")
    return out_count

if __name__ == "__main__":
    build_and_publish()
