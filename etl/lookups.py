from __future__ import annotations
import pandas as pd
from pathlib import Path
from typing import Dict, Optional

def _read_any_csv(root: Path, names_like: tuple[str, ...]) -> Optional[pd.DataFrame]:
    if not root.exists():
        return None
    for p in root.rglob("*.csv"):
        lc = p.name.lower()
        if any(k in lc for k in names_like):
            try:
                return pd.read_csv(p)
            except Exception:
                return pd.read_csv(p, encoding="latin-1")
    return None

def load_lookup_tables(lookup_dir: Path) -> dict:
    lookups: Dict[str, pd.DataFrame] = {}
    res = _read_any_csv(lookup_dir, ("result", "test_result"))
    fuel = _read_any_csv(lookup_dir, ("fuel", "fuel_type"))
    rfr = _read_any_csv(lookup_dir, ("rfr", "defect", "item", "failure"))
    if res is not None:  lookups["result"] = res
    if fuel is not None: lookups["fuel"] = fuel
    if rfr is not None:  lookups["rfr"] = rfr
    return lookups

def build_result_map(df: pd.DataFrame) -> Dict[str, str]:
    if df is None or df.empty: return {}
    code_col = next((c for c in df.columns if "code" in c.lower()), None)
    txt_col  = next((c for c in df.columns if any(k in c.lower() for k in ("desc","text","name"))), None)
    out = {}
    for _, r in df.iterrows():
        code = str(r.get(code_col, "")).strip()
        label = str(r.get(txt_col, code)).strip().upper()
        if not code: continue
        if label.startswith("PASS"): out[code] = "PASS"
        elif label.startswith("FAIL"): out[code] = "FAIL"
        elif "PRS" in label or "PASS AFTER" in label: out[code] = "PRS"
        else: out[code] = label
    return out

def build_fuel_map(df: pd.DataFrame) -> Dict[str, str]:
    if df is None or df.empty: return {}
    code_col = next((c for c in df.columns if "code" in c.lower()), None)
    txt_col  = next((c for c in df.columns if any(k in c.lower() for k in ("desc","text","name"))), None)
    out = {}
    for _, r in df.iterrows():
        code = str(r.get(code_col, "")).strip()
        label = str(r.get(txt_col, code)).strip().lower()
        if not code: continue
        if label.startswith("petrol") or label == "pe": label = "petrol"
        elif label.startswith("diesel") or label == "di": label = "diesel"
        elif "hybrid" in label: label = "hybrid"
        elif "electric" in label or label == "ev": label = "electric"
        out[code] = label
    return out

FAIL_BUCKETS = {
    "1": "brakes",
    "2": "steering",
    "3": "visibility",
    "4": "lights",
    "5": "axles_wheels_tyres_suspension",
    "6": "body_structure",
    "7": "other_equipment",
    "8": "emissions",
}

def rfr_section_from_lookup_row(row: pd.Series) -> str:
    for c in row.index:
        lc = c.lower()
        if "section" in lc or "item" in lc:
            val = str(row[c])
            if any(ch.isdigit() for ch in val) and "." in val:
                return val
    return ""

def build_rfr_bucket_map(rfr_lookup: pd.DataFrame) -> Dict[str, str]:
    if rfr_lookup is None or rfr_lookup.empty:
        return {}
    key_col = next((c for c in rfr_lookup.columns if c.lower() in ("code","rfrcode","rfr_code","item_id","rfr_id","defect_id")), None)
    out: Dict[str, str] = {}
    for _, r in rfr_lookup.iterrows():
        key = str(r.get(key_col, "")).strip()
        if not key: continue
        section = rfr_section_from_lookup_row(r)
        head = section.split(".", 1)[0].strip()
        out[key] = FAIL_BUCKETS.get(head, "other")
    return out
