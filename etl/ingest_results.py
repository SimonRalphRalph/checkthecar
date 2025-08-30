# etl/ingest_results.py
from __future__ import annotations
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from .paths import RAW, INT
from .resolver import normalise_df
from .lookups import load_lookup_tables, build_result_map, build_fuel_map

def _find_results_csv(root: Path) -> Path:
    cand = list((root / "results").rglob("*.csv"))
    if not cand:
        raise FileNotFoundError("No results CSV under data_raw/results")
    # choose the biggest (main data file)
    return max(cand, key=lambda p: p.stat().st_size)

def ingest_results():
    res_csv = _find_results_csv(RAW)
    # Read only necessary columns; names vary slightly across years
    use = None  # read all, then pick columns robustly
    df = pd.read_csv(res_csv, dtype=str, low_memory=False)

    # Lookups
    look = load_lookup_tables(RAW / "lookups")
    res_map = build_result_map(look.get("result")) if "result" in look else {}
    fuel_map = build_fuel_map(look.get("fuel")) if "fuel" in look else {}

    # Flexible column detection
    def pick(*alts: str) -> str:
        for a in alts:
            if a in df.columns:
                return a
        # case-insensitive
        for a in alts:
            for c in df.columns:
                if c.lower() == a.lower():
                    return c
        raise KeyError(f"None of columns {alts} found in results CSV")

    make_col = pick("make")
    model_col = pick("model")
    test_date_col = pick("test_date","testdate","completed_date")
    odo_col = pick("odometer","test_mileage","odometer_value")
    fuel_code_col = pick("fuel_type_code","fuel_code","fueltypecode")
    res_code_col = pick("result_code","test_result","result")

    # Build clean frame
    out = pd.DataFrame({
        "make": df[make_col].astype(str),
        "model": df[model_col].astype(str),
        "test_date": pd.to_datetime(df[test_date_col], errors="coerce", infer_datetime_format=True),
        "odometer": pd.to_numeric(df[odo_col], errors="coerce"),
        "fuel_type": df[fuel_code_col].astype(str).map(lambda c: fuel_map.get(c, c)).str.lower(),
        "result": df[res_code_col].astype(str).map(lambda c: res_map.get(c, c)).str.upper(),
    })

    # derive first_use_year if present (optional)
    if "first_use_date" in df.columns:
        fy = pd.to_datetime(df["first_use_date"], errors="coerce")
        out["first_use_year"] = fy.dt.year.astype("Int16")
    else:
        out["first_use_year"] = out["test_date"].dt.year.astype("Int16")  # fallback

    # age at test (years)
    if "first_use_date" in df.columns:
        fud = pd.to_datetime(df["first_use_date"], errors="coerce")
        age = (out["test_date"] - fud).dt.days / 365.25
    else:
        age = 0
    out["age_at_test"] = pd.to_numeric(age, errors="coerce").round().clip(lower=0, upper=35).astype("Int16")

    # clean odometer & miles→thousands usable later
    out.loc[(out["odometer"] <= 0) | (out["odometer"] > 1_000_000), "odometer"] = pd.NA
    out["odometer"] = out["odometer"].astype("Int32")

    # Normalise & slug
    out = normalise_df(out, "make", "model")

    # Partitioned Parquet by first_use_year
    out_dir = (INT / "mot")
    out_dir.mkdir(parents=True, exist_ok=True)
    pq.write_to_dataset(pa.Table.from_pandas(out), root_path=str(out_dir), partition_cols=["first_use_year"], existing_data_behavior="overwrite_or_ignore")
    print(f"[ingest_results] wrote Parquet dataset -> {out_dir}")

if __name__ == "__main__":
    ingest_results()