# etl/vca_co2.py
import pandas as pd
from .paths import VCA_PARQUET
from .resolver import normalise_df

def build_vca_parquet(csv_path: str) -> pd.DataFrame:
    # VCA CSV varies by vintage; keep robust columns
    usecols_guess = [
        "Manufacturer","Model","YearFrom","YearTo","FuelType",
        "CO2 (g/km)","Combined MPG","Test Type"
    ]
    df = pd.read_csv(csv_path)
    # map columns flexibly
    colmap = {}
    for col in df.columns:
        lc = col.lower()
        if "manufact" in lc: colmap[col] = "Manufacturer"
        elif lc == "model": colmap[col] = "Model"
        elif lc in ("yearfrom","from","startyear"): colmap[col] = "YearFrom"
        elif lc in ("yearto","to","endyear"): colmap[col] = "YearTo"
        elif "fuel" in lc: colmap[col] = "FuelType"
        elif "co2" in lc: colmap[col] = "CO2 (g/km)"
        elif "mpg" in lc and "combined" in lc: colmap[col] = "Combined MPG"
        elif "wltp" in lc or "nedc" in lc or "test" in lc: colmap[col] = "Test Type"
    df = df.rename(columns=colmap)
    df["YearFrom"] = pd.to_numeric(df.get("YearFrom"), errors="coerce").fillna(0).astype("int16")
    df["FuelType"] = df.get("FuelType", "").astype(str)
    df = normalise_df(df, "Manufacturer", "Model")

    # Expand to a row per first_reg_year (best-effort banding)
    df["YearTo"] = pd.to_numeric(df.get("YearTo"), errors="coerce")
    df["YearTo"] = df["YearTo"].fillna(df["YearFrom"]).astype("int16")
    records = []
    for _, r in df.iterrows():
        for y in range(int(r["YearFrom"]), int(r["YearTo"])+1):
            records.append({
                "norm_make": r["norm_make"],
                "norm_model": r["norm_model"],
                "make_slug": r["make_slug"],
                "model_slug": r["model_slug"],
                "first_use_year": int(y),
                "fuel_type": str(r["FuelType"]).lower().strip(),
                "co2_gkm": pd.to_numeric(r.get("CO2 (g/km)"), errors="coerce"),
                "mpg_combined": pd.to_numeric(r.get("Combined MPG"), errors="coerce"),
                "test_type": str(r.get("Test Type") or "").upper().strip()  # WLTP/NEDC
            })
    out = pd.DataFrame.from_records(records)
    # collapse duplicates (take median)
    out = (
        out.groupby(["norm_make","norm_model","make_slug","model_slug","first_use_year","fuel_type"])
           .agg(co2_gkm=("co2_gkm","median"), mpg_combined=("mpg_combined","median"),
                test_type=("test_type", lambda s: s.mode().iloc[0] if len(s.dropna()) else ""))
           .reset_index()
    )
    out.to_parquet(VCA_PARQUET, index=False)
    return out

if __name__ == "__main__":
    import sys
    build_vca_parquet(sys.argv[1])
    print(f"Wrote {VCA_PARQUET}")
