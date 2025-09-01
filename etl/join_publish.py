# etl/join_publish.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

import pandas as pd

from .paths import MOT_AGG_PARQUET, RECALLS_PARQUET, VCA_PARQUET, PUB, VED_JSON, INT
from .ved import load_ved_bands, ved_for_vehicle

# We import helpers from resolver; if not available, use lightweight fallbacks.
try:
    from .resolver import _norm as norm, _slug as slug  # older file
except Exception:
    try:
        from .resolver import norm, slug  # newer file that exports these
    except Exception:
        import re, unicodedata

        def norm(s: str) -> str:
            s = "" if s is None else str(s)
            s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
            s = re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()
            return re.sub(r"\s+", " ", s)

        def slug(s: str) -> str:
            s = "" if s is None else str(s)
            s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
            return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def _compact_float(x, ndigits=2):
    try:
        if x is None or pd.isna(x):
            return None
    except Exception:
        pass
    return round(float(x), ndigits)


def _read_optional_parquet(path: Path) -> pd.DataFrame | None:
    p = Path(path)
    if not p.exists():
        return None
    return pd.read_parquet(p)


def _failure_share_lookup() -> Dict[tuple, Dict[str, float]]:
    """
    Build a mapping: (norm_make, norm_model, firstRegYear) -> {category: share}
    from INT/failure_shares.parquet written by aggregate_mot (optional).
    """
    fp = INT / "failure_shares.parquet"
    if not fp.exists():
        return {}
    df = pd.read_parquet(fp)
    need = {"make", "model", "firstRegYear", "category", "share"}
    if not need.issubset(df.columns):
        return {}
    df = df.copy()
    df["norm_make"] = df["make"].map(norm)
    df["norm_model"] = df["model"].map(norm)
    m: Dict[tuple, Dict[str, float]] = {}
    for r in df.itertuples(index=False):
        key = (r.norm_make, r.norm_model, int(r.firstRegYear))
        m.setdefault(key, {})
        m[key][str(r.category)] = float(r.share)
    return m


def _top_failure_buckets(share_map: Dict[str, float]) -> List[dict]:
    if not share_map:
        return []
    items = sorted(share_map.items(), key=lambda kv: kv[1], reverse=True)[:5]
    return [{"bucket": k, "share": _compact_float(v, 3)} for k, v in items if v and v > 0]


def _recall_timeline(rec_df: pd.DataFrame | None, norm_make: str, norm_model: str) -> list[dict]:
    if rec_df is None or rec_df.empty:
        return []
    # Be tolerant about column names
    cols = {c.lower(): c for c in rec_df.columns}
    mk = cols.get("norm_make") or cols.get("make") or "norm_make"
    md = cols.get("norm_model") or cols.get("model") or "norm_model"
    yr = cols.get("year") or cols.get("recall_year") or "year"
    cnt = cols.get("recalls") or cols.get("count") or "recalls"
    if mk not in rec_df.columns or md not in rec_df.columns:
        return []
    sub = rec_df[(rec_df[mk] == norm_make) & (rec_df[md] == norm_model)]
    if sub.empty or yr not in sub.columns:
        return []
    if cnt not in sub.columns:
        # make a count if only individual rows exist
        sub = sub.assign(_one=1)
        cnt = "_one"
    g = sub.groupby(yr, dropna=True)[cnt].sum().reset_index().sort_values(yr)
    return [{"year": int(r[yr]), "count": int(r[cnt])} for _, r in g.iterrows()]


def _vca_panel(vca_df: pd.DataFrame | None, norm_make: str, norm_model: str, first_year: int, ved_bands: dict) -> list[dict]:
    if vca_df is None or vca_df.empty:
        return []
    df = vca_df.copy()
    df.columns = [c.lower() for c in df.columns]

    # Normalise likely columns
    mk = "norm_make" if "norm_make" in df.columns else "make"
    md = "norm_model" if "norm_model" in df.columns else "model"
    yr = "first_use_year" if "first_use_year" in df.columns else ("firstregyear" if "firstregyear" in df.columns else None)
    fuel = "fuel_type" if "fuel_type" in df.columns else ("fuel" if "fuel" in df.columns else None)
    co2 = "co2_gkm" if "co2_gkm" in df.columns else ("co2" if "co2" in df.columns else None)
    mpg = "mpg_combined" if "mpg_combined" in df.columns else ("mpg" if "mpg" in df.columns else None)
    test_type = "test_type" if "test_type" in df.columns else ("cycle" if "cycle" in df.columns else None)

    if yr is None or fuel is None or co2 is None:
        return []

    sub = df[(df[mk].map(norm) == norm_make) & (df[md].map(norm) == norm_model) & (df[yr].astype("Int64") == int(first_year))]
    if sub.empty:
        return []

    panels = []
    for _, r in sub.iterrows():
        ved = ved_for_vehicle(ved_bands, r[co2], int(first_year), str(r[fuel]))
        panels.append(
            {
                "fuel": str(r[fuel]),
                "co2_gkm": _compact_float(r[co2], 0),
                "mpg": _compact_float(r[mpg], 0) if mpg and mpg in r else None,
                "test_type": str(r[test_type]) if test_type and test_type in r else "",
                "ved_band": ved.get("band"),
                "ved_annual": ved.get("annual"),
                "ved_first_year": ved.get("first_year"),
                "ved_supplement": ved.get("supplement"),
            }
        )
    return panels


def build_and_publish():
    print("Reading Parquet…")

    # --- Read MOT aggregates ---
    mot = pd.read_parquet(MOT_AGG_PARQUET)
    # Expect columns: make, model, firstRegYear, age_at_test, pass_rate, p50, p75, p90
    required = {"make", "model", "firstRegYear", "age_at_test", "pass_rate"}
    missing = required - set(mot.columns)
    if missing:
        raise KeyError(f"Aggregate parquet missing columns: {missing}")

    mot["norm_make"] = mot["make"].map(norm)
    mot["norm_model"] = mot["model"].map(norm)
    mot["make_slug"] = mot["make"].map(slug)
    mot["model_slug"] = mot["model"].map(slug)

    # optional inputs
    rec = _read_optional_parquet(RECALLS_PARQUET)
    vca = _read_optional_parquet(VCA_PARQUET)
    ved_bands = load_ved_bands(str(VED_JSON)) if Path(VED_JSON).exists() else {"eras": {}}
    fail_share_map = _failure_share_lookup()

    # Unique cohorts
    cohorts = (
        mot[["norm_make", "norm_model", "make_slug", "model_slug", "firstRegYear"]]
        .drop_duplicates()
        .sort_values(["norm_make", "norm_model", "firstRegYear"])
    )

    out_count = 0
    for row in cohorts.itertuples(index=False):
        mk_norm, md_norm, mk_slug, md_slug, year = row
        msub = mot[(mot["norm_make"] == mk_norm) & (mot["norm_model"] == md_norm) & (mot["firstRegYear"] == year)].sort_values("age_at_test")
        if msub.empty:
            continue

        # Build curve (tests unknown here; keep None)
        curve = []
        share_key = (mk_norm, md_norm, int(year))
        share_buckets = fail_share_map.get(share_key, {})
        fail_top = _top_failure_buckets(share_buckets)

        for rr in msub.itertuples(index=False):
            curve.append(
                {
                    "age": int(rr.age_at_test) if pd.notna(rr.age_at_test) else None,
                    "tests": None,
                    "pass_rate": _compact_float(rr.pass_rate, 3),
                    "mileage": {
                        "p50": _compact_float(getattr(rr, "p50", None), 0),
                        "p75": _compact_float(getattr(rr, "p75", None), 0),
                        "p90": _compact_float(getattr(rr, "p90", None), 0),
                    },
                    "fail_mix": fail_top,  # cohort-level shares (not by age) for now
                }
            )

        # CO2/MPG/VED panel from VCA (optional)
        co2_panel = _vca_panel(vca, mk_norm, md_norm, int(year), ved_bands)

        # Recalls timeline (optional)
        recalls = _recall_timeline(rec, mk_norm, md_norm)

        doc = {
            "make": msub["make"].iloc[0],
            "model": msub["model"].iloc[0],
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

    print(f"Published {out_count} cohort JSON files to {PUB}")
    return out_count


if __name__ == "__main__":
    build_and_publish()
