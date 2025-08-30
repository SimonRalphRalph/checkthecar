# etl/download_mot.py
"""
Reads the real CSV schema (from the screenshot) and writes a partitioned Parquet dataset:
data_intermediate/mot/ (Hive partitioning by first_use_year)

Columns produced:
- make, model, first_use_year, test_date (date), age_at_test (int),
  fuel_type (normalized 'petrol'/'diesel'/other), odometer (int, miles),
  result ('PASS'/'FAIL'/'PRS'), test_class_id, test_type, postcode_area, cylinder_cc
"""

from pathlib import Path
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from .paths import RAW, INT
from .resolver import normalise_df

OUT_DIR = INT / "mot"

def _normalize_fuel(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip().str.upper()
    mapping = {
        "PE": "petrol",
        "DI": "diesel",
        "EL": "electric",
        "HEV": "hybrid",
        "PHEV": "phev",
        "PETROL": "petrol",
        "DIESEL": "diesel",
    }
    return s.map(mapping).fillna(s.str.lower())

def _normalize_result(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip().str.upper()
    m = {"P": "PASS", "F": "FAIL", "PRS": "PRS"}
    return s.map(m).fillna(s)

def _parse_date(col: pd.Series) -> pd.Series:
    # Your CSV looks like m/d/yy; allow flexibility
    return pd.to_datetime(col, errors="coerce", dayfirst=False, infer_datetime_format=True)

def ingest_csv_to_parquet(csv_path: str, out_dir: Path = OUT_DIR):
    out_dir.mkdir(parents=True, exist_ok=True)

    usecols = [
        "test_id","vehicle_id","test_date","test_class_id","test_type","test_result",
        "test_mileage","postcode_area","make","model","colour","fuel_type",
        "cylinder_capacity","first_use_date","completed_date",
    ]
    df = pd.read_csv(csv_path, usecols=usecols, dtype={"postcode_area":"string"})
    # dates
    df["test_date"] = _parse_date(df["test_date"])
    df["first_use_date"] = _parse_date(df["first_use_date"])
    df["completed_date"] = _parse_date(df["completed_date"])

    # derived
    df["first_use_year"] = df["first_use_date"].dt.year.astype("Int16")
    df["age_at_test"] = (
        (df["test_date"] - df["first_use_date"]).dt.days / 365.25
    ).round().clip(lower=0, upper=35).astype("Int16")

    df["odometer"] = pd.to_numeric(df["test_mileage"], errors="coerce").fillna(0).astype("Int32")
    df["fuel_type"] = _normalize_fuel(df["fuel_type"])
    df["result"] = _normalize_result(df["test_result"])
    df["cylinder_cc"] = pd.to_numeric(df["cylinder_capacity"], errors="coerce").astype("Int32")

    # Keep only the columns we actually use downstream
    keep = [
        "make","model","first_use_year","test_date","age_at_test",
        "fuel_type","odometer","result","postcode_area","test_class_id","test_type","cylinder_cc"
        # NOTE: no rfr_and_comments_code in this source
    ]
    df = df[keep]
    # Normalise make/model + slugs (for join & paths)
    df = normalise_df(df, "make", "model")

    # Write partitioned Parquet by first_use_year
    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(
        table,
        root_path=str(out_dir),
        partition_cols=["first_use_year"],
        existing_data_behavior="overwrite_or_ignore",
    )
    print(f"Wrote partitioned Parquet to {out_dir}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python -m etl.download_mot <path/to/source.csv>")
    ingest_csv_to_parquet(sys.argv[1])
