from __future__ import annotations
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from .paths import RAW, INT
from .resolver import normalise_df
from .lookups import load_lookup_tables, build_result_map, build_fuel_map

def _find_results_csv():
    cand = list((RAW / "results").rglob("*.csv"))
    if not cand:
        raise FileNotFoundError("No results CSV under data_raw/results")
    return max(cand, key=lambda p: p.stat().st_size)

def ingest_results():
    res_csv = _find_results_csv()
    df = pd.read_csv(res_csv, dtype=str, low_memory=False)

    look = load_lookup_tables(RAW / "lookups")
    res_map  = build_result_map(look.get("result")) if "result" in look else {}
    fuel_map = build_fuel_map(look.get("fuel"))     if "fuel"   in look else {}

    def pick(*alts: str) -> str:
        for a in alts:
            if a in df.columns: return a
        for a in alts:
            for c in df.columns:
                if c.lower() == a.lower(): return c
        raise KeyError(f"None of columns {alts} found in results CSV")

    make_col  = pick("make")
    model_col = pick("model")
    test_date_col = pick("test_date","testdate","completed_date")
    odo_col   = pick("odometer","test_mileage","odometer_value")
    fuel_code_col = pick("fuel_type_code","fuel_code","fueltypecode")
    res_code_col  = pick("result_code","test_result","result")

    out = pd.DataFrame({
        "make": df[make_col].astype(str),
        "model": df[model_col].astype(str),
        "test_date": pd.to_datetime(df[test_date_col], errors="coerce", infer_datetime_format=True),
        "odometer": pd.to_numeric(df[odo_col], errors="coerce"),
        "fuel_type": df[fuel_code_col].astype(str).map(lambda c: fuel_map.get(c, c)).str.lower(),
        "result": df[res_code_col].astype(str).map(lambda c: res_map.get(c, c)).str.upper(),
    })

    # Optional columns if present
    if "first_use_date" in df.columns:
        fud = pd.to_datetime(df["first_use_date"], errors="coerce")
        out["first_use_year"] = fud.dt.year.astype("Int16")
        out["age_at_test"] = ((out["test_date"] - fud).dt.days / 365.25).round().clip(lower=0, upper=35).astype("Int16")
    else:
        out["first_use_year"] = out["test_date"].dt.year.astype("Int16")
        out["age_at_test"] = 0

    # If test_id exists, keep it so failures can join exactly
    for id_col in ("test_id","testnumber","test_no"):
        if id_col in df.columns:
            out["test_id"] = df[id_col].astype(str)
            break

    out.loc[(out["odometer"] <= 0) | (out["odometer"] > 1_000_000), "odometer"] = pd.NA
    out["odometer"] = out["odometer"].astype("Int32")

    out = normalise_df(out, "make", "model")

    out_dir = (INT / "mot")
    out_dir.mkdir(parents=True, exist_ok=True)
    pq.write_to_dataset(pa.Table.from_pandas(out), root_path=str(out_dir),
                        partition_cols=["first_use_year"], existing_data_behavior="overwrite_or_ignore")
    print(f"[ingest_results] wrote Parquet -> {out_dir}")

if __name__ == "__main__":
    ingest_results()
