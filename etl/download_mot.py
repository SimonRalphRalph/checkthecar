# etl/download_mot.py
import os, io, zipfile, requests, pandas as pd
from typing import Iterable
from datetime import datetime
from tqdm import tqdm

DATASET_INDEX = "https://www.data.gov.uk/dataset/c63fca52-ae4c-4b75-bab5-8b4735e1a4c9/anonymised-mot-tests-and-results"
# Above dataset: all MOT tests and outcomes since 2005 (GB).  [oai_citation:14‡Data.gov.uk](https://www.data.gov.uk/dataset/c63fca52-ae4c-4b75-bab5-8b4735e1a4c9/anonymised-mot-tests-and-results?utm_source=chatgpt.com)

def stream_zip_to_parquet(url: str, out_parquet: str, usecols=None, chunksize=1_000_000):
    r = requests.get(url, stream=True, timeout=120)
    r.raise_for_status()
    z = zipfile.ZipFile(io.BytesIO(r.content))
    csv_name = [n for n in z.namelist() if n.endswith(".csv")][0]
    with z.open(csv_name) as f:
        it = pd.read_csv(f, usecols=usecols, chunksize=chunksize, low_memory=True)
        os.makedirs(os.path.dirname(out_parquet), exist_ok=True)
        for i, df in enumerate(tqdm(it, desc="csv->parquet chunks")):
            mode = "wb" if i == 0 else "ab"
            df.to_parquet(out_parquet, engine="pyarrow", index=False, compression="zstd", append=mode=="ab")

def main():
    # In practice you’d scrape the dataset page for latest year file URLs.
    # For MVP/dev, we expect small fixtures (see /data/public_fixtures).
    print("Use fixtures for local dev; run full download in CI as needed.")

if __name__ == "__main__":
    main()
