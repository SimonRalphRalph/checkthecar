# etl/recalls.py
import pandas as pd, requests, io
from datetime import datetime

DVSA_RECALLS = "https://www.check-vehicle-recalls.service.gov.uk/documents/RecallsFile.csv"  #  [oai_citation:15‡check-vehicle-recalls.service.gov.uk](https://www.check-vehicle-recalls.service.gov.uk/documents/RecallsFile.csv?utm_source=chatgpt.com)

def load_recalls() -> pd.DataFrame:
    r = requests.get(DVSA_RECALLS, timeout=60)
    r.raise_for_status()
    df = pd.read_csv(io.BytesIO(r.content))
    # Normalise columns
    df.rename(columns={"Make":"make","Recalls Model Information":"model","Launch Date":"launch_date"}, inplace=True)
    df["year"] = pd.to_datetime(df["launch_date"], errors="coerce").dt.year
    df["make"] = df["make"].str.strip().str.title()
    df["model"] = df["model"].str.replace(r"\s+", " ", regex=True).str.strip()
    return df[["make","model","year"]]

def aggregate_recalls(df: pd.DataFrame) -> pd.DataFrame:
    g = df.groupby(["make","model","year"]).size().reset_index(name="count")
    return g
