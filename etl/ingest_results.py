# etl/ingest_results.py
"""
Ingest DVSA 'MOT testing data results' CSV(s) into a tidy Parquet dataset.

Handles both legacy and 2024 column layouts:
- fuel:  either 'fuel_type_code' OR 'fuel_type' (e.g. PE/DI)
- result: either 'result'/'result_code' OR 'test_result' (P/F)
- date:   prefer 'completed_date' (ISO) else 'test_date'
"""

from __future__ import annotations
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime

from .paths import RAW, INT, MOT_PARQUET

pd.options.mode.chained_assignment = None  # quieten SettingWithCopy warnings


def _find_biggest_csv_under(folder: Path) -> Path:
    cands = list(folder.rglob("*.csv"))
    if not cands:
        raise FileNotFoundError(f"No CSV files found under {folder}")
    return max(cands, key=lambda p: p.stat().st_size)


def _pick(df: pd.DataFrame, *alts: str) -> str:
    """Pick a column from alternatives, case/spacing-insensitive."""
    cols = list(df.columns)
    lower = {c.lower(): c for c in cols}
    normalised = {c.lower().replace(" ", "").replace("_", ""): c for c in cols}

    for a in alts:
        if a in cols:
            return a
        if a.lower() in lower:
            return lower[a.lower()]
        key = a.lower().replace(" ", "").replace("_", "")
        if key in normalised:
            return normalised[key]
    raise KeyError(f"None of columns {alts} found in results CSV")


def _parse_date(series: pd.Series) -> pd.Series:
    # Try ISO first, then day/month/2-digit-year with ambiguity handling
    s = pd.to_datetime(series, errors="coerce", utc=True)
    if s.notna().any():
        return s
    # Fallback patterns commonly seen in test_date like 12/13/24
    # Prefer month/day/year because DVSA exports often follow US-style in this field.
    s = pd.to_datetime(series, errors="coerce", format="%m/%d/%y", utc=True)
    # If still NaT, try UK-style just in case
    s2 = pd.to_datetime(series, errors="coerce", dayfirst=True, utc=True)
    return s.where(s.notna(), s2)


def _maybe_load_fuel_lookup() -> dict[str, str]:
    """Try to read a fuel lookup table from RAW/lookups (optional)."""
    look_dir = RAW / "lookups"
    if not look_dir.exists():
        return {}
    out = {}
    for p in look_dir.rglob("*.csv"):
        try:
            df = pd.read_csv(p, dtype=str, low_memory=False).rename(columns=str.lower)
        except Exception:
            continue
        # Heuristics for a simple code->name mapping
        # Accept any frame that looks like: ['fuel_type_code', 'fuel_type'] or similar
        candidates = [
            ("fuel_type_code", "fuel_type"),
            ("fuelcode", "fueltype"),
            ("code", "fuel_type"),
            ("fuel_type_code", "description"),
            ("code", "description"),
        ]
        for code_col, name_col in candidates:
            if code_col in df.columns and name_col in df.columns:
                mapping = (
                    df[[code_col, name_col]]
                    .dropna()
                    .drop_duplicates()
                    .set_index(code_col)[name_col]
                    .to_dict()
                )
                # Keep first mapping we find; later files won't override existing keys
                for k, v in mapping.items():
                    out.setdefault(str(k).strip(), str(v).strip())
                break
    # Add tiny safety net for the common 2024 short codes
    out.setdefault("PE", "Petrol")
    out.setdefault("DI", "Diesel")
    out.setdefault("EL", "Electric")
    out.setdefault("HE", "Hybrid")
    return out


def ingest_results() -> None:
    src_root = RAW / "results"
    if not src_root.exists():
        raise FileNotFoundError("Expected data under data_raw/results (did you run the download step?)")

    csv_path = _find_biggest_csv_under(src_root)
    print(f"[ingest_results] reading {csv_path}")

    # Read in chunks if you want to reduce memory; for now load whole file
    df = pd.read_csv(csv_path, dtype=str, low_memory=False)
    df.columns = [c.strip() for c in df.columns]

    # Resolve required columns with flexibility
    make_col = _pick(df, "make")
    model_col = _pick(df, "model")

    # dates: prefer completed_date (ISO) else test_date
    date_col = None
    for alt in ("completed_date", "completeddate", "test_date", "testdate"):
        try:
            date_col = _pick(df, alt)
            break
        except KeyError:
            continue
    if date_col is None:
        raise KeyError("No date column found (expected 'completed_date' or 'test_date').")

    # odometer / mileage
    mileage_col = _pick(df, "test_mileage", "odometer", "odometer_value", "mileage")

    # result: allow result_code/result/test_result
    try:
        result_col = _pick(df, "result_code", "result", "test_result")
    except KeyError:
        # Some exports use 'testresult'
        result_col = _pick(df, "testresult")

    # fuel: allow code or already-short code column
    try:
        fuel_code_col = _pick(df, "fuel_type_code", "fuel_code", "fueltypecode")
        fuel_val = df[fuel_code_col].astype(str).str.strip()
    except KeyError:
        # Fall back to 'fuel_type' (e.g., PE, DI)
        fuel_col = _pick(df, "fuel_type", "fueltype")
        fuel_val = df[fuel_col].astype(str).str.strip()

    # Optional first-use date to compute age
    try:
        first_use_col = _pick(df, "first_use_date", "firstusedate", "first_use", "firstregistrationdate")
        first_use = _parse_date(df[first_use_col])
    except KeyError:
        first_use = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns, UTC]")

    # Build tidy frame
    test_dt = _parse_date(df[date_col])
    odometer = (
        pd.to_numeric(df[mileage_col].str.replace(",", "", regex=False), errors="coerce")
        .astype("Int64")
    )

    # Normalise result to 'P' / 'F'
    result_raw = df[result_col].astype(str).str.upper().str.strip()
    # Common variants
    result = (
        result_raw.replace(
            {
                "PASS": "P",
                "PASSED": "P",
                "P": "P",
                "FAIL": "F",
                "FAILED": "F",
                "F": "F",
            }
        )
    )

    # Map fuel codes if we can
    fuel_lookup = _maybe_load_fuel_lookup()
    fuel_name = fuel_val.map(lambda x: fuel_lookup.get(str(x).strip(), str(x).strip()))

    tidy = pd.DataFrame(
        {
            "make": df[make_col].astype(str).str.strip(),
            "model": df[model_col].astype(str).str.strip(),
            "test_date": test_dt,
            "odometer": odometer,
            "result": result,
            "fuel_type": fuel_name,   # friendly if lookup available; else original code
        }
    )

    # Age at test (years, floored) if first_use available
    if first_use.notna().any():
        age_years = ((test_dt - first_use).dt.days / 365.25).astype(float)
        tidy["age_at_test"] = np.floor(age_years).astype("Int64")
        tidy["first_use_date"] = first_use
    else:
        tidy["age_at_test"] = pd.Series(pd.NA, dtype="Int64")

    # Drop rows with no date or make/model
    tidy = tidy.dropna(subset=["test_date"]).reset_index(drop=True)

    # Write a partitioned dataset (by year for convenience)
    MOT_PARQUET.mkdir(parents=True, exist_ok=True)
    tidy["test_year"] = tidy["test_date"].dt.year.astype("Int64")
    # Use pyarrow via pandas to_parquet (no engine arg needed with pyarrow installed)
    out_path = INT / "mot"  # alias of MOT_PARQUET root
    # Partitioned write: we’ll write per-year files to keep things manageable
    for year, g in tidy.groupby("test_year", dropna=True):
        part = out_path / f"test_year={int(year)}"
        part.mkdir(parents=True, exist_ok=True)
        g.drop(columns=["test_year"]).to_parquet(part / "part.parquet", index=False)
    print(f"[ingest_results] wrote Parquet -> {out_path} ({len(tidy):,} rows)")


if __name__ == "__main__":
    ingest_results()
