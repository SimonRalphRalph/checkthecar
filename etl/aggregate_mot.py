# etl/aggregate_mot.py
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.compute as pc
from .paths import MOT_PARQUET, MOT_AGG_PARQUET
from .resolver import normalise_df

def read_dataset(columns=None, filter_=None) -> pa.Table:
    dataset = ds.dataset(MOT_PARQUET, format="parquet", partitioning="hive")
    return dataset.to_table(columns=columns, filter=filter_)

def compute_aggregates() -> pd.DataFrame:
    cols = [
        "make","model","first_use_year","age_at_test",
        "fuel_type","odometer","result",
        # Optional: if another file later adds it, we’ll pick it up
        "rfr_and_comments_code",
    ]
    tbl = read_dataset(columns=[c for c in cols if c in ds.dataset(MOT_PARQUET, format="parquet", partitioning="hive").schema.names])

    # Casts
    def _cast_safe(t: pa.Table, name: str, typ: pa.DataType):
        if name in t.schema.names:
            return t.set_column(t.schema.get_field_index(name), name, pc.cast(t[name], typ))
        return t

    tbl = _cast_safe(tbl, "first_use_year", pa.int16())
    tbl = _cast_safe(tbl, "age_at_test", pa.int16())
    tbl = _cast_safe(tbl, "odometer", pa.int32())

    df = tbl.to_pandas(types_mapper=pd.ArrowDtype)
    df = normalise_df(df, "make", "model")

    # Pass/fail
    df["is_pass"] = (df["result"].astype(str).str.upper() == "PASS").astype("int8")

    # Mileage in thousands; basic sanity guard
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

    # ---- Failure buckets (optional) -----------------------------------------
    # Your CSV doesn’t include RFR codes. If present, we’ll compute shares;
    # otherwise we leave bucket columns empty (0.0).
    bucket_cols = []
    if "rfr_and_comments_code" in df.columns:
        def bucket_from_rfr(code: str) -> str:
            if not isinstance(code, str) or "." not in code:
                return "other"
            head = code.split(".", 1)[0]
            # DVSA section heads → our buckets
            if head.startswith("1"): return "brakes"
            if head.startswith("2"): return "steering"
            if head.startswith("5"): return "axles_wheels_tyres_suspension"
            if head.startswith("8"): return "emissions"
            if head.startswith("4"): return "lights"
            return "other"

        fdf = df.copy()
        fdf["is_fail"] = 1 - fdf["is_pass"]
        fdf = fdf[fdf["is_fail"] == 1]
        fdf["fail_bucket"] = fdf["rfr_and_comments_code"].astype(str).map(bucket_from_rfr)

        mix = (
            fdf.groupby(grp + ["fail_bucket"], dropna=False)
               .size().rename("fail_count").reset_index()
        )
        tot = mix.groupby(grp, dropna=False)["fail_count"].sum().rename("fail_total").reset_index()
        mix = mix.merge(tot, on=grp, how="left")
        mix["fail_share"] = (mix["fail_count"] / mix["fail_total"]).astype("float32")

        pivot = mix.pivot_table(index=grp, columns="fail_bucket", values="fail_share", fill_value=0.0, aggfunc="sum").reset_index()
        base = base.merge(pivot, on=grp, how="left").fillna(0.0)
        bucket_cols = [c for c in base.columns if c in ("brakes","steering","axles_wheels_tyres_suspension","emissions","lights","other")]
    else:
        # Ensure predictable columns exist with zeros (frontend expects keys)
        for c in ("brakes","steering","axles_wheels_tyres_suspension","emissions","lights","other"):
            base[c] = 0.0
        bucket_cols = ["brakes","steering","axles_wheels_tyres_suspension","emissions","lights","other"]
    # ------------------------------------------------------------------------

    # Cast compact
    for c in ("pass_rate","median_mileage","p75_mileage","p90_mileage", *bucket_cols):
        base[c] = base[c].astype("float32")

    base.to_parquet(MOT_AGG_PARQUET, index=False)
    print(f"Wrote {MOT_AGG_PARQUET} with {len(base):,} rows")
    return base

if __name__ == "__main__":
    compute_aggregates()
