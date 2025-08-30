# etl/download_mot.py
from __future__ import annotations
import io
import zipfile
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

from .paths import MOT_PARQUET  # directory for the dataset, e.g. INT / "mot"

# Optional: keep the landing page as documentation for future scraping
DATASET_INDEX = (
    "https://www.data.gov.uk/dataset/c63fca52-ae4c-4b75-bab5-8b4735e1a4c9/"
    "anonymised-mot-tests-and-results"
)

# Map DVSA fuel codes to readable values
FUEL_MAP = {
    "PE": "petrol",
    "DI": "diesel",
    "EL": "electric",
    "HE": "hybrid",
    # add others if you see them in the data
}

RESULT_MAP = {
    "P": "PASS", "PASS": "PASS",
    "F": "FAIL", "FAIL": "FAIL",
    "PRS": "PRS",
}

SOURCE_USECOLS = [
    # the columns visible in your sample; add more if you later need them
    "make",
    "model",
    "first_use_date",
    "test_date",
    "test_mileage",
    "test_result",
    "fuel_type",
    # If/when you locate failure codes, add the source name here and to RENAME below
]

RENAME = {
    # source -> internal
    "make": "make",
    "model": "model",
    "first_use_date": "first_use_date",
    "test_date": "test_date",
    "test_mileage": "odometer",
    "test_result": "result",
    "fuel_type": "fuel_type",
    # "rfr_source_name": "rfr_and_comments_code",  # uncomment when you know it
}

KEEP_ORDER = [
    "make", "model",
    "first_use_date", "first_use_year",
    "test_date", "test_year",
    "age_at_test",
    "odometer",
    "result",
    "fuel_type",
    "rfr_and_comments_code",
]


def _is_url(s: str) -> bool:
    try:
        return urlparse(s).scheme in {"http", "https"}
    except Exception:
        return False


def _read_csv_or_zip(path_or_url: str, usecols=None) -> pd.DataFrame:
    """
    Reads either a CSV directly or the first CSV inside a .zip.
    Works with local paths and HTTP(S) URLs.
    """
    if _is_url(path_or_url):
        import requests  # lazy import
        resp = requests.get(path_or_url, stream=True, timeout=180)
        resp.raise_for_status()
        content = io.BytesIO(resp.content)
        # try zip first
        try:
            with zipfile.ZipFile(content) as z:
                csv_name = next(n for n in z.namelist() if n.lower().endswith(".csv"))
                with z.open(csv_name) as f:
                    return pd.read_csv(f, usecols=usecols, low_memory=True)
        except zipfile.BadZipFile:
            content.seek(0)
            return pd.read_csv(content, usecols=usecols, low_memory=True)
    else:
        p = Path(path_or_url)
        if p.suffix.lower() == ".zip":
            with zipfile.ZipFile(p) as z:
                csv_name = next(n for n in z.namelist() if n.lower().endswith(".csv"))
                with z.open(csv_name) as f:
                    return pd.read_csv(f, usecols=usecols, low_memory=True)
        return pd.read_csv(p, usecols=usecols, low_memory=True)


def _norm(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
         .str.strip()
         .str.lower()
         .str.replace(r"\s+", " ", regex=True)
    )


def _transform(df: pd.DataFrame) -> pd.DataFrame:
    # rename to internal names
    df = df.rename(columns={k: v for k, v in RENAME.items() if k in df.columns})

    # parse dates (your sample looks like mm/dd/yy; infer handles both)
    for col in ("first_use_date", "test_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", infer_datetime_format=True)

    # derived fields
    df["first_use_year"] = df["first_use_date"].dt.year
    df["test_year"] = df["test_date"].dt.year

    # age at test in years (approx, rounded)
    df["age_at_test"] = ((df["test_date"] - df["first_use_date"]).dt.days / 365.25).round().astype("Int64")

    # normalize make/model (used later by resolver/aggregates)
    df["norm_make"] = _norm(df["make"])
    df["norm_model"] = _norm(df["model"])

    # standardize result
    df["result"] = (
        df["result"].astype(str).str.upper().map(RESULT_MAP).fillna("FAIL")
    )

    # humanize fuel type codes
    df["fuel_type"] = (
        df["fuel_type"].astype(str).str.strip().str.upper().map(FUEL_MAP).fillna("unknown")
    )

    # ensure rfr present (empty for now if not supplied)
    if "rfr_and_comments_code" not in df.columns:
        df["rfr_and_comments_code"] = ""

    # basic type hygiene
    df["odometer"] = pd.to_numeric(df["odometer"], errors="coerce")  # miles in your feed
    # keep only columns we need (plus norm_* which aggregate may derive again)
    keep = [c for c in KEEP_ORDER if c in df.columns]
    # include normalized keys in case you want to partition more later
    keep += [c for c in ("norm_make", "norm_model") if c in df.columns]
    return df[keep]


def write_hive_dataset(df: pd.DataFrame, base_dir: Path, partition_cols=("first_use_year",)) -> None:
    base_dir.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    ds.write_dataset(
        data=table,
        base_dir=str(base_dir),
        format="parquet",
        partitioning=partition_cols,          # Hive partitions: first_use_year=2013/...
        existing_data_behavior="overwrite_or_ignore",
        basename_template="part-{i}.parquet",
    )


def build_from_source(path_or_url: str) -> None:
    print(f"Reading source: {path_or_url}")
    raw = _read_csv_or_zip(path_or_url, usecols=SOURCE_USECOLS)
    df = _transform(raw)
    write_hive_dataset(df, MOT_PARQUET, partition_cols=("first_use_year",))
    print(f"Wrote Hive dataset to {MOT_PARQUET}")


def main():
    print("Usage:")
    print("  from etl.download_mot import build_from_source")
    print("  build_from_source('path/or/url/to/file.zip')")


if __name__ == "__main__":
    main()
    
