# etl/ved.py
import json
from pathlib import Path
import pandas as pd

def load_ved_bands(json_path: str) -> dict:
    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)

def ved_for_vehicle(ved_bands: dict, co2_gkm: float, first_use_year: int, fuel_type: str) -> dict:
    """
    Simplified: uses current post-2017 graduated bands by CO2 for cars registered
    after 2001 with caveats omitted for brevity. You can expand this to full rules.
    Returns: {"band": "H", "annual": 180}
    """
    if pd.isna(co2_gkm) or co2_gkm <= 0:
        return {"band": None, "annual": None}
    # choose a table key by registration era
    era = "post2017" if first_use_year >= 2017 else "2001to2017"
    table = ved_bands.get(era, [])
    for row in table:
        lo = row["co2_lo"]; hi = row["co2_hi"]
        if (co2_gkm >= lo) and (co2_gkm <= hi):
            return {"band": row["band"], "annual": row["annual"]}
    # fallback highest
    if table:
        row = table[-1]
        return {"band": row["band"], "annual": row["annual"]}
    return {"band": None, "annual": None}
