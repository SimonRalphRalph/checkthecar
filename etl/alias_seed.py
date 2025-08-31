from pathlib import Path
import pandas as pd
import pyarrow.dataset as ds
from .paths import MOT_PARQUET, CONF
from .resolver import norm

OUT = CONF / "model_aliases.csv"

def main():
    dataset = ds.dataset(MOT_PARQUET, format="parquet", partitioning="hive")
    cols = [c for c in ("make","model") if c in dataset.schema.names]
    if len(cols) < 2:
        print("Parquet missing make/model columns; skipping.")
        return
    table = dataset.to_table(columns=cols)
    df = table.to_pandas().dropna(subset=["make","model"])
    raw_pairs = (
        df.groupby(["make","model"]).size().reset_index().drop(columns=0)
        .rename(columns={"make":"make_raw","model":"model_raw"})
    )
    existing = pd.read_csv(OUT) if OUT.exists() else pd.DataFrame(columns=["make_raw","model_raw","canonical_make","canonical_model"])
    def keyify(s): return norm(str(s))
    existing["_key"] = existing["make_raw"].map(keyify) + "||" + existing["model_raw"].map(keyify)
    raw_pairs["_key"] = raw_pairs["make_raw"].map(keyify) + "||" + raw_pairs["model_raw"].map(keyify)
    missing = raw_pairs[~raw_pairs["_key"].isin(existing["_key"])].drop(columns=["_key"])
    if not len(missing):
        print(f"No missing pairs. Alias file already covers {len(existing)} rows.")
        return
    missing["canonical_make"]  = missing["make_raw"].map(lambda s: norm(str(s)).title())
    missing["canonical_model"] = missing["model_raw"].map(lambda s: norm(str(s)).title())
    out = pd.concat([existing.drop(columns=[c for c in existing.columns if c == "_key"]), missing], ignore_index=True)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)
    print(f"Appended {len(missing)} new rows → {OUT}")

if __name__ == "__main__":
    main()
