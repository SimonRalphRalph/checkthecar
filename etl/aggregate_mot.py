from __future__ import annotations
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.compute as pc
from .paths import MOT_PARQUET, MOT_AGG_PARQUET, INT
from .resolver import normalise_df

BUCKET_COLS = ["brakes","steering","visibility","lights","axles_wheels_tyres_suspension","body_structure","other_equipment","emissions","other"]

def _read_results(columns=None) -> pa.Table:
    dataset = ds.dataset(MOT_PARQUET, format="parquet", partitioning="hive")
    cols = columns or dataset.schema.names
    return dataset.to_table(columns=[c for c in cols if c in dataset.schema.names])

def compute_aggregates() -> pd.DataFrame:
    cols = ["test_id","make","model","first_use_year","age_at_test","fuel_type","odometer","result"]
    tbl = _read_results(columns=cols)

    for name, typ in (("first_use_year", pa.int16()), ("age_at_test", pa.int16()), ("odometer", pa.int32())):
        if name in tbl.schema.names:
            tbl = tbl.set_column(tbl.schema.get_field_index(name), name, pc.cast(tbl[name], typ))

    df = tbl.to_pandas(types_mapper=pd.ArrowDtype)
    df = normalise_df(df, "make", "model")

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

    # Failure mix
    fail_path = INT / "failures.parquet"
    for c in BUCKET_COLS: base[c] = 0.0
    if fail_path.exists():
        fdf = pd.read_parquet(fail_path)[["fail_bucket","test_id"] if "test_id" in pd.read_parquet(fail_path).columns else ["fail_bucket"]]

        if "test_id" in fdf.columns and "test_id" in df.columns:
            # TRUE per-cohort shares via join on test_id (best)
            j = df[df["is_pass"] == 0][["test_id", *grp]].merge(fdf, on="test_id", how="inner")
            mix = (
                j.groupby([*grp,"fail_bucket"]).size().rename("n").reset_index()
            )
            tot = mix.groupby(grp)["n"].sum().rename("tot").reset_index()
            mix = mix.merge(tot, on=grp, how="left")
            mix["share"] = (mix["n"] / mix["tot"]).astype("float32")
            pivot = mix.pivot_table(index=grp, columns="fail_bucket", values="share", fill_value=0.0, aggfunc="sum").reset_index()
            base = base.merge(pivot, on=grp, how="left").fillna(0.0)
        else:
            # Heuristic: global bucket shares broadcast per cohort
            g = fdf.groupby("fail_bucket").size().rename("n").reset_index()
            g["share"] = (g["n"] / g["n"].sum()).astype("float32")
            for b in BUCKET_COLS:
                val = g.loc[g["fail_bucket"]==b, "share"]
                base[b] = float(val.iloc[0]) if len(val) else 0.0

    for c in ("pass_rate","median_mileage","p75_mileage","p90_mileage", *BUCKET_COLS):
        base[c] = base[c].astype("float32")

    base.to_parquet(MOT_AGG_PARQUET, index=False)
    print(f"[aggregate_mot] wrote {MOT_AGG_PARQUET} rows={len(base):,}")
    return base

if __name__ == "__main__":
    compute_aggregates()
