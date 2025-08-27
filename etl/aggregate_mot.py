# etl/aggregate_mot.py
import pandas as pd, numpy as np
from typing import Dict, Any
from pathlib import Path

SCHEMA_COLS = {
    "make": "string",
    "model": "string",
    "firstUseDate": "string",   # yyyy-mm-dd
    "testDate": "string",
    "odometerReading": "Int64",
    "odometerReadingUnits": "string",
    "testResult": "string",     # PASS/FAIL
    "rfrAndComments": "string", # DVSA reason codes
    "fuelType": "string",
}

FAIL_GROUPS = {
    "brakes": ["RBT", "BGT", "BRS", "HDB"],
    "suspension": ["SUS"],
    "tyres": ["TRY", "WHL"],
    "emissions": ["EMI"],
    "lights": ["LGT"],
}

def _age_year(test_date: pd.Series, first_use: pd.Series) -> pd.Series:
    td = pd.to_datetime(test_date, errors="coerce")
    fu = pd.to_datetime(first_use, errors="coerce")
    return ((td - fu).dt.days / 365.25).fillna(0).astype(int).clip(lower=0, upper=25)

def _km(odo: pd.Series, units: pd.Series) -> pd.Series:
    miles = odo.where(units.str.lower().str.contains("miles"), other=np.nan)
    km = odo.where(units.str.lower().str.contains("kilomet"), other=np.nan)
    out = pd.Series(np.where(~miles.isna(), miles*1.60934, km), index=odo.index).astype(float)
    return out

def compute_aggregates(df: pd.DataFrame) -> Dict[str, Any]:
    df = df.copy()
    df["make"] = df["make"].str.strip()
    df["model"] = df["model"].str.strip()
    df["firstRegYear"] = pd.to_datetime(df["firstUseDate"], errors="coerce").dt.year
    df["ageAtTest"] = _age_year(df["testDate"], df["firstUseDate"])
    df["odo_km"] = _km(df["odometerReading"], df["odometerReadingUnits"])
    df["isPass"] = (df["testResult"].str.upper() == "PASS")

    # Mileage percentiles by age
    q = df.groupby(["make","model","firstRegYear","ageAtTest"])["odo_km"].quantile([0.5,0.75,0.9]).unstack()
    q.columns = ["p50_km","p75_km","p90_km"]

    # Pass rate by age
    g = df.groupby(["make","model","firstRegYear","ageAtTest"])["isPass"].mean().to_frame("pass_rate")

    # Failure categories shares
    df["rfrCode"] = df["rfrAndComments"].str.extract(r"^([A-Z]{3})", expand=False)
    cat = pd.DataFrame()
    for name, prefixes in FAIL_GROUPS.items():
        mask = df["rfrCode"].isin(prefixes) & ~df["isPass"]
        tmp = mask.groupby([df["make"],df["model"],df["firstRegYear"]]).mean().rename(f"fail_{name}")
        cat = tmp.to_frame() if cat.empty else cat.join(tmp, how="outer")
    cat = cat.fillna(0.0)

    out = g.join(q, how="left").reset_index()
    # pack per cohort-year structure
    results: Dict[str, Any] = {}
    for (mk,md,yr), grp in out.groupby(["make","model","firstRegYear"]):
        results.setdefault(mk, {}).setdefault(md, {})[int(yr)] = {
            "pass_rate_by_age": [
                {"age": int(r.ageAtTest), "pass_rate": round(float(r.pass_rate), 3)}
                for r in grp.itertuples()
            ],
            "mileage_percentiles_by_age": [
                {"age": int(r.ageAtTest), "p50": round(float(r.p50_km or 0)),
                 "p75": round(float(r.p75_km or 0)), "p90": round(float(r.p90_km or 0))}
                for r in grp.itertuples()
            ],
        }
    # attach failure shares
    for (mk,md,yr), row in cat.reset_index().itertuples(index=False):
        c = results.get(mk, {}).get(md, {}).get(int(yr))
        if c is not None:
            c["failure_shares"] = {
                "brakes": round(float(row.fail_brakes or 0),3),
                "suspension": round(float(row.fail_suspension or 0),3),
                "tyres": round(float(row.fail_tyres or 0),3),
                "emissions": round(float(row.fail_emissions or 0),3),
            }
    return results
