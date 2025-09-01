# etl/aggregate_mot.py
"""
Compute model/year aggregates used by the frontend:
- pass_rate_by_age
- mileage percentiles by age (p50/p75/p90)
- failure category shares (if failures parquet present)

Works with 2024+ DVSA layout (ingested by ingest_results.py):
required columns present in Parquet dataset:
  make, model, test_date (datetime64[ns, UTC]), odometer (Int64),
  result ('P'/'F'), fuel_type (string), age_at_test (Int64, optional),
  first_use_date (datetime64[ns, UTC], optional)
"""

from __future__ import annotations
from pathlib import Path
import json
import numpy as np
import pandas as pd
import pyarrow.dataset as ds

from .paths import INT, MOT_PARQUET, MOT_AGG_PARQUET

def _read_results() -> pd.DataFrame:
    dataset = ds.dataset(MOT_PARQUET, format="parquet", partitioning="hive")
    tbl = dataset.to_table()  # select all; columns are modest after ingest
    df = tbl.to_pandas()
    # Ensure expected columns exist
    for c in ("make","model","test_date","odometer","result","fuel_type"):
        if c not in df.columns:
            raise KeyError(f"Missing required column '{c}' in results Parquet")
    if "age_at_test" not in df.columns:
        df["age_at_test"] = pd.Series(pd.NA, index=df.index, dtype="Int64")
    if "first_use_date" not in df.columns:
        df["first_use_date"] = pd.NaT
    return df

def _cohort_first_reg_year(df: pd.DataFrame) -> pd.Series:
    test_year = pd.to_datetime(df["test_date"], utc=True).dt.year.astype("Int64")
    first_from_date = pd.to_datetime(df["first_use_date"], utc=True, errors="coerce").dt.year.astype("Int64")
    # infer from age if available
    age = df["age_at_test"].astype("Int64")
    inferred = test_year - age
    out = first_from_date.copy()
    out = out.where(out.notna(), inferred.where(age.notna()))
    out = out.where(out.notna(), test_year)  # last fallback
    return out.astype("Int64")

def _percentiles(x: pd.Series) -> tuple[float,float,float]:
    arr = pd.to_numeric(x, errors="coerce").dropna().to_numpy()
    if arr.size == 0:
        return (np.nan, np.nan, np.nan)
    return (
        float(np.nanpercentile(arr, 50)),
        float(np.nanpercentile(arr, 75)),
        float(np.nanpercentile(arr, 90)),
    )

def _compute_failure_shares() -> pd.DataFrame | None:
    """Optional: read failures parquet if present.
    Expect columns like: make, model, firstRegYear, age_at_test, category, count
    If not present, return None and the join step will skip failures.
    """
    p = INT / "failures_bucketed.parquet"
    if not p.exists():
        return None
    df = pd.read_parquet(p)
    # Minimal sanity
    needed = {"make","model","firstRegYear","category","count"}
    if not needed.issubset(df.columns):
        return None
    # shares per cohort (make,model,firstRegYear)
    grp = df.groupby(["make","model","firstRegYear","category"], dropna=False)["count"].sum().reset_index()
    totals = grp.groupby(["make","model","firstRegYear"], dropna=False)["count"].sum().rename("total")
    out = grp.merge(totals, left_on=["make","model","firstRegYear"], right_index=True)
    out["share"] = out["count"] / out["total"]
    return out[["make","model","firstRegYear","category","share"]]

def compute_aggregates() -> pd.DataFrame:
    df = _read_results().copy()

    # Compute cohort year (firstRegYear)
    df["firstRegYear"] = _cohort_first_reg_year(df)

    # Ensure age buckets (drop rows with unknown age for age-based metrics)
    # If age_at_test is NA, we can still contribute to cohort size but not to curves.
    age_known = df["age_at_test"].notna()
    df_age = df[age_known].copy()

    # ---------- Pass rate by age ----------
    df_age["is_pass"] = (df_age["result"].astype(str) == "P").astype(int)
    pass_rate = (
        df_age.groupby(["make","model","firstRegYear","age_at_test"], dropna=False)["is_pass"]
        .mean()
        .rename("pass_rate")
        .reset_index()
    )

    # ---------- Mileage percentiles by age ----------
    miles_pct = (
        df_age.groupby(["make","model","firstRegYear","age_at_test"], dropna=False)["odometer"]
        .apply(_percentiles)
        .reset_index()
        .rename(columns={"odometer":"pct"})
    )
    # split tuple column into p50/p75/p90
    miles_pct[["p50","p75","p90"]] = pd.DataFrame(miles_pct["pct"].tolist(), index=miles_pct.index)
    miles_pct = miles_pct.drop(columns=["pct"])

    # ---------- Failure shares (optional) ----------
    fail_shares = _compute_failure_shares()  # None if not available

    # ---------- Assemble a single tidy table ----------
    # We’ll write a single Parquet where each row represents a (make,model,firstRegYear,age) with
    # pass_rate + mileage percentiles; and we’ll also write a separate table for failure shares if present.
    out = pass_rate.merge(
        miles_pct,
        on=["make","model","firstRegYear","age_at_test"],
        how="outer",
        validate="one_to_one",
    )

    # Save primary aggregates
    MOT_AGG_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(MOT_AGG_PARQUET, index=False)
    print(f"[aggregate_mot] wrote {len(out):,} rows -> {MOT_AGG_PARQUET}")

    # Save failure shares next to it if we have them
    if fail_shares is not None:
        p = INT / "failure_shares.parquet"
        fail_shares.to_parquet(p, index=False)
        print(f"[aggregate_mot] wrote failure shares -> {p} ({len(fail_shares):,} rows)")
    else:
        print("[aggregate_mot] no failures parquet found; skipping failure shares")

    return out

if __name__ == "__main__":
    compute_aggregates()
