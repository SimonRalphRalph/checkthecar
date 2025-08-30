# etl/aggregate_mot.py
from __future__ import annotations
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.compute as pc
from .paths import MOT_PARQUET, MOT_AGG_PARQUET, INT
from .resolver import normalise_df

def _read_results(columns=None, filter_=None) -> pa.Table:
    dataset = ds.dataset(MOT_PARQUET, format="parquet", partitioning="hive")
    return dataset.to_table(columns=columns, filter=filter_)

def compute_aggregates() -> pd.DataFrame:
    cols = ["make","model","first_use_year","age_at_test","fuel_type","odometer","result"]
    tbl = _read_results(columns=cols)
    # cast numerics
    for name, typ in (("first_use_year", pa.int16()), ("age_at_test", pa.int16()), ("odometer", pa.int32())):
        if name in tbl.schema.names:
            tbl = tbl.set_column(tbl.schema.get_field_index(name), name, pc.cast(tbl[name], typ))
    df = tbl.to_pandas(types_mapper=pd.ArrowDtype)
    df = normalise_df(df, "make", "model")

    # Basic metrics
    df["is_pass"] = (df["result"].astype(str).str.upper() == "PASS").astype("int8")
    df["odo_k"] = pd.to_numeric(df["odometer"], errors="coerce") / 1000.0
    df.loc[(df["odo_k"] < 1) | (df["odo_k"] > 600), "odo_k"] = pd.NA

    grp = ["norm_make","norm_model","make_slug","model_slug","first_use_year","age_at_test","fuel_type"]
    base = (
        df.groupby(grp, dropna=False)
          .agg(
              tests=("is_pass","size"),
              passes=("is_pass","sum"),
              median_mileage=("odo_k","median"),
              p75_mileage=("odo_k", lambda s: s.quantile(0.75)),
              p90_mileage=("odo_k", lambda s: s.quantile(0.90)),
          ).reset_index()
    )
    base["pass_rate"] = (base["passes"] / base["tests"]).astype("float32")

    # Failure mix from failures.parquet (lookup-based buckets)
    fail_path = INT / "failures.parquet"
    if fail_path.exists():
        fdf = pd.read_parquet(fail_path)  # columns: test_id (optional), rfr_code, fail_bucket, deficiency?
        # We don’t have test_id↔results linkage in aggregated space; instead,
        # approximate failure mix by grouping buckets within same cohort using result FAIL rows.
        # If you do have test_id joins, you can link via a shared id; many public dumps omit it.
        # Approach: join failures back on (norm_make,norm_model,first_use_year,age_at_test,fuel_type)
        # by first computing cohort keys at test-level – but results parquet we built does not retain row-level ids.
        # So we fallback to bucket shares from failure items alone (global by cohort is not directly computable).
        # Pragmatic alternative: compute failure shares relative to all failures within (make, model, first_use_year, age_at_test) *from results sample window*.
        # Since we lack the id join, compute a generic bucket distribution across all failures overall and then apply it per cohort as a heuristic.

        # Global bucket distribution
        bucket_share = (
            fdf.groupby("fail_bucket").size().rename("n").reset_index()
        )
        bucket_share["share"] = (bucket_share["n"] / bucket_share["n"].sum()).astype("float32")
        # broadcast to each cohort row
        for b in ["brakes","steering","visibility","lights","axles_wheels_tyres_suspension","body_structure","other_equipment","emissions","other"]:
            val = bucket_share.loc[bucket_share["fail_bucket"]==b, "share"]
            base[b] = float(val.iloc[0]) if len(val) else 0.0
    else:
        # default zeros
        for b in ["brakes","steering","visibility","lights","axles_wheels_tyres_suspension","body_structure","other_equipment","emissions","other"]:
            base[b] = 0.0

    # compact dtypes
    for c in ("pass_rate","median_mileage","p75_mileage","p90_mileage",
              "brakes","steering","visibility","lights","axles_wheels_tyres_suspension","body_structure","other_equipment","emissions","other"):
        base[c] = base[c].astype("float32")

    base.to_parquet(MOT_AGG_PARQUET, index=False)
    print(f"[aggregate_mot] wrote {MOT_AGG_PARQUET} rows={len(base):,}")
    return base

if __name__ == "__main__":
    compute_aggregates()