# etl/ved.py
import json
import pandas as pd

def load_ved_bands(json_path: str) -> dict:
    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)

def _find_band(bands, co2):
    for row in bands:
        if row["co2_lo"] <= co2 <= row["co2_hi"]:
            return row
    return None

def ved_for_vehicle(ved_cfg: dict, co2_gkm: float, first_use_year: int, fuel_type: str) -> dict:
    """
    Returns: {"band": <str|None>, "annual": <int|None>, "supplement": {"expensive_car": {...}}}
    Note: first-year rates are not shown in cohort panel (we show ongoing cost). If you want them,
    add a flag and lookup ved_cfg["eras"]["post2017"]["first_year"] similarly.
    """
    if pd.isna(co2_gkm) or co2_gkm <= 0:
        return {"band": None, "annual": None, "supplement": None}

    eras = ved_cfg.get("eras", {})
    if first_use_year >= 2017:
        std = eras.get("post2017", {}).get("standard_rate", {})
        # normalise fuel buckets
        ft = (fuel_type or "").lower()
        if ft in ("petrol","diesel"):
            annual = int(std.get("petrol_diesel")) if std else None
        else:
            # treat hybrids/alt-fuel as alternative fuel
            base = std.get("petrol_diesel")
            disc = std.get("alternative_fuel_discount", 0)
            annual = int(base - disc) if base is not None else None
        supp = eras.get("post2017", {}).get("expensive_car_supplement", None)
        return {"band": None, "annual": annual, "supplement": {"expensive_car": supp}}
    else:
        # 2001-2017 CO2 bands
        bands = eras.get("2001to2017", {}).get("bands", [])
        row = _find_band(bands, float(co2_gkm))
        if not row:
            return {"band": None, "annual": None, "supplement": None}
        return {"band": row["band"], "annual": int(row["annual"]), "supplement": None}
