import json
import pandas as pd
from typing import Optional, Dict, Any, List

def load_ved_bands(json_path: str) -> dict:
    with open(json_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    if "eras" not in cfg:
        raise ValueError("ved_bands.json missing 'eras'")
    return cfg

def _find_band_2001to2017(bands: List[Dict[str, Any]], co2: float) -> Optional[Dict[str, Any]]:
    for row in bands:
        if row["co2_lo"] <= co2 <= row["co2_hi"]:
            return row
    return None

def first_year_rate_post2017(ved_cfg: dict, co2_gkm: float) -> Optional[int]:
    era = ved_cfg["eras"].get("post2017", {})
    fy = era.get("first_year", [])
    for row in fy:
        if row["co2_lo"] <= co2_gkm <= row["co2_hi"]:
            return int(row["rate"])
    return None

def ved_for_vehicle(ved_cfg: dict, co2_gkm: float, first_use_year: int, fuel_type: str) -> dict:
    if pd.isna(co2_gkm) or co2_gkm <= 0:
        return {"band": None, "annual": None, "first_year": None, "supplement": None}

    eras = ved_cfg.get("eras", {})
    if first_use_year >= 2017:
        post = eras.get("post2017", {})
        annual = post.get("standard_rate")
        first_year = first_year_rate_post2017(ved_cfg, float(co2_gkm))
        supp = post.get("expensive_car_supplement", None)
        return {"band": None, "annual": int(annual) if annual is not None else None, "first_year": first_year, "supplement": {"expensive_car": supp}}
    else:
        pre = eras.get("2001to2017", {})
        band_row = _find_band_2001to2017(pre.get("bands", []), float(co2_gkm))
        if not band_row:
            return {"band": None, "annual": None, "first_year": None, "supplement": None}
        return {"band": band_row["band"], "annual": int(band_row["annual"]), "first_year": None, "supplement": None}
