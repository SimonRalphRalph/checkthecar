# etl/join_publish.py
from __future__ import annotations
import os, json, sys, hashlib
from pathlib import Path
from typing import Dict, List
import pandas as pd

from .paths import MOT_AGG_PARQUET, RECALLS_PARQUET, VCA_PARQUET, PUB, VED_JSON, INT
from .ved import load_ved_bands, ved_for_vehicle

# --- norm/slug helpers ---
try:
    from .resolver import norm as _norm, slug as _slug
except Exception:
    import re, unicodedata
    def _norm(s: str) -> str:
        s = "" if s is None else str(s)
        s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
        s = re.sub(r"[^a-z0-9]+"," ", s.lower()).strip()
        return re.sub(r"\s+"," ", s)
    def _slug(s: str) -> str:
        s = "" if s is None else str(s)
        s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
        return re.sub(r"[^a-z0-9]+","-", s.lower()).strip("-")

def _compact_float(x, nd=2):
    try:
        if x is None or pd.isna(x): return None
    except Exception:
        pass
    return round(float(x), nd)

def _read_opt(path: Path) -> pd.DataFrame | None:
    p = Path(path)
    if not p.exists(): return None
    return pd.read_parquet(p)

def _failure_share_lookup() -> Dict[tuple, Dict[str,float]]:
    fp = INT / "failure_shares.parquet"
    if not fp.exists(): return {}
    df = pd.read_parquet(fp)
    need = {"make","model","firstRegYear","category","share"}
    if not need.issubset(df.columns): return {}
    df = df.copy()
    df["norm_make"] = df["make"].map(_norm)
    df["norm_model"] = df["model"].map(_norm)
    m: Dict[tuple, Dict[str,float]] = {}
    for r in df.itertuples(index=False):
        key = (r.norm_make, r.norm_model, int(r.firstRegYear))
        m.setdefault(key, {})[str(r.category)] = float(r.share)
    return m

def _top_buckets(share_map: Dict[str,float]) -> List[dict]:
    if not share_map: return []
    items = sorted(share_map.items(), key=lambda kv: kv[1], reverse=True)[:5]
    return [{"bucket": k, "share": _compact_float(v, 3)} for k,v in items if v and v>0]

def _recall_timeline(rec: pd.DataFrame | None, mk_norm: str, md_norm: str) -> list[dict]:
    if rec is None or rec.empty: return []
    c = {x.lower(): x for x in rec.columns}
    mk = c.get("norm_make") or c.get("make"); md = c.get("norm_model") or c.get("model")
    yr = c.get("year") or c.get("recall_year"); cnt = c.get("recalls") or c.get("count")
    if not mk or not md or not yr: return []
    sub = rec[(rec[mk].map(_norm)==mk_norm) & (rec[md].map(_norm)==md_norm)]
    if sub.empty: return []
    if cnt not in sub.columns:
        sub = sub.assign(_one=1); cnt = "_one"
    g = sub.groupby(yr, dropna=True)[cnt].sum().reset_index().sort_values(yr)
    return [{"year": int(r[yr]), "count": int(r[cnt])} for _,r in g.iterrows()]

def _vca_panel(vca: pd.DataFrame | None, mk_norm: str, md_norm: str, first_year: int, ved_bands: dict) -> list[dict]:
    if vca is None or vca.empty: return []
    df = vca.copy(); df.columns = [c.lower() for c in df.columns]
    mk = "norm_make" if "norm_make" in df.columns else "make"
    md = "norm_model" if "norm_model" in df.columns else "model"
    yr = "first_use_year" if "first_use_year" in df.columns else ("firstregyear" if "firstregyear" in df.columns else None)
    fuel = "fuel_type" if "fuel_type" in df.columns else ("fuel" if "fuel" in df.columns else None)
    co2 = "co2_gkm" if "co2_gkm" in df.columns else ("co2" if "co2" in df.columns else None)
    mpg = "mpg_combined" if "mpg_combined" in df.columns else ("mpg" if "mpg" in df.columns else None)
    test = "test_type" if "test_type" in df.columns else ("cycle" if "cycle" in df.columns else None)
    if not yr or not fuel or not co2: return []
    sub = df[(df[mk].map(_norm)==mk_norm) & (df[md].map(_norm)==md_norm) & (df[yr].astype("Int64")==int(first_year))]
    if sub.empty: return []
    panels=[]
    for _,r in sub.iterrows():
        ved = ved_for_vehicle(ved_bands, r[co2], int(first_year), str(r[fuel]))
        panels.append({
            "fuel": str(r[fuel]),
            "co2_gkm": _compact_float(r[co2], 0),
            "mpg": _compact_float(r[mpg], 0) if mpg and mpg in r else None,
            "test_type": str(r[test]) if test and test in r else "",
            "ved_band": ved.get("band"),
            "ved_annual": ved.get("annual"),
            "ved_first_year": ved.get("first_year"),
            "ved_supplement": ved.get("supplement"),
        })
    return panels

def _cohort_hash(mk_norm: str, md_norm: str) -> int:
    # Stable small int hash for sharding
    h = hashlib.sha1(f"{mk_norm}::{md_norm}".encode("utf-8")).hexdigest()
    return int(h[:8], 16)

def build_and_publish() -> int:
    sys.stdout.reconfigure(line_buffering=True)  # flush prints immediately

    print("Reading Parquet…")
    mot = pd.read_parquet(MOT_AGG_PARQUET)
    need = {"make","model","firstRegYear","age_at_test","pass_rate"}
    missing = need - set(mot.columns)
    if missing:
        raise KeyError(f"Aggregate parquet missing columns: {missing}")

    mot = mot.copy()
    mot["norm_make"]  = mot["make"].map(_norm)
    mot["norm_model"] = mot["model"].map(_norm)
    mot["make_slug"]  = mot["make"].map(_slug)
    mot["model_slug"] = mot["model"].map(_slug)

    rec  = _read_opt(RECALLS_PARQUET)
    vca  = _read_opt(VCA_PARQUET)
    ved  = load_ved_bands(str(VED_JSON)) if Path(VED_JSON).exists() else {"eras":{}}
    fail = _failure_share_lookup()

    # Filters / caps
    cap = int(os.environ.get("ETL_MAX_COHORTS", "0")) or None
    f_make = os.environ.get("ETL_MAKE_FILTER")
    f_model = os.environ.get("ETL_MODEL_FILTER")
    y_min = os.environ.get("ETL_YEAR_MIN")
    y_max = os.environ.get("ETL_YEAR_MAX")
    shard_idx = int(os.environ.get("ETL_SHARD", "0") or 0)
    shard_cnt = int(os.environ.get("ETL_SHARDS", "1") or 1)
    y_min = int(y_min) if y_min else None
    y_max = int(y_max) if y_max else None

    cohorts = (
        mot[["make","model","norm_make","norm_model","make_slug","model_slug","firstRegYear"]]
        .drop_duplicates()
        .sort_values(["norm_make","norm_model","firstRegYear"])
    )
    if f_make:
        cohorts = cohorts[cohorts["norm_make"]==_norm(f_make)]
    if f_model:
        cohorts = cohorts[cohorts["norm_model"]==_norm(f_model)]
    if y_min is not None:
        cohorts = cohorts[cohorts["firstRegYear"] >= y_min]
    if y_max is not None:
        cohorts = cohorts[cohorts["firstRegYear"] <= y_max]

    # Shard by (make, model)
    if shard_cnt > 1:
        mask = [
            (_cohort_hash(r.norm_make, r.norm_model) % shard_cnt) == shard_idx
            for r in cohorts.itertuples(index=False)
        ]
        cohorts = cohorts[mask]

    if cap:
        cohorts = cohorts.head(cap)

    total = len(cohorts)
    print(f"Cohorts to publish in this shard: {total} (shard {shard_idx+1}/{shard_cnt})")

    def safe_slug(val: str, fallback: str) -> str:
        s = _slug(val or "")
        if not s:
            s = _slug(_norm(val or "")) or fallback
        return s

    out_count = 0
    skipped = 0

    for i, r in enumerate(cohorts.itertuples(index=False), start=1):
        try:
            make, model, mk_norm, md_norm, mk_slug, md_slug, year = r
            # guard slugs (some odd strings can end up empty)
            mk_slug = mk_slug if mk_slug else safe_slug(make, "make")
            md_slug = md_slug if md_slug else safe_slug(model, "model")

            year = int(year) if pd.notna(year) else None
            if year is None:
                raise ValueError("missing year")

            # rows for this cohort
            msub = mot[
                (mot["norm_make"] == mk_norm)
                & (mot["norm_model"] == md_norm)
                & (mot["firstRegYear"] == year)
            ].sort_values("age_at_test")

            if msub.empty:
                raise ValueError("empty cohort slice")

            fail_top = _top_buckets(fail.get((mk_norm, md_norm, int(year)), {}))

            curve = []
            for rr in msub.itertuples(index=False):
                curve.append({
                    "age": int(rr.age_at_test) if pd.notna(rr.age_at_test) else None,
                    "tests": None,
                    "pass_rate": _compact_float(rr.pass_rate, 3),
                    "mileage": {
                        "p50": _compact_float(getattr(rr, "p50", None), 0),
                        "p75": _compact_float(getattr(rr, "p75", None), 0),
                        "p90": _compact_float(getattr(rr, "p90", None), 0),
                    },
                    "fail_mix": fail_top,
                })

            co2_panel = _vca_panel(vca, mk_norm, md_norm, int(year), ved)
            recalls   = _recall_timeline(rec, mk_norm, md_norm)

            doc = {
                "make": make,
                "model": model,
                "make_slug": mk_slug,
                "model_slug": md_slug,
                "first_reg_year": int(year),
                "fuels": sorted({(p.get("fuel") or "").lower() for p in co2_panel if p.get("fuel")}) if co2_panel else [],
                "co2_panel": co2_panel,
                "recalls": recalls,
                "mot_curve": curve,
                "meta": {
                    "source": "DVSA anonymised MOT results & failure items (OGL v3.0); DVSA Recalls; VCA CO₂/MPG; GOV.UK VED",
                    "version": "weekly",
                },
            }

            out_dir = PUB / mk_slug / md_slug
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"{int(year)}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(doc, f, ensure_ascii=False, separators=(",", ":"))

            out_count += 1
            if i % 200 == 0 or i == total:
                print(f"...{i}/{total} cohorts processed (written={out_count}, skipped={skipped}) in shard {shard_idx+1}/{shard_cnt}")

        except Exception as e:
            skipped += 1
            # log enough to find the offender next time
            try:
                print(f"[WARN] skipped cohort #{i} shard {shard_idx+1}/{shard_cnt} "
                      f"({r.norm_make}/{r.norm_model}/{int(r.firstRegYear) if pd.notna(r.firstRegYear) else 'NA'}): {e}")
            except Exception:
                print(f"[WARN] skipped cohort #{i} shard {shard_idx+1}/{shard_cnt}: {e}")

    print(f"Published {out_count} cohort JSON files to {PUB} (skipped={skipped})")
    return out_count

if __name__ == "__main__":
    build_and_publish()