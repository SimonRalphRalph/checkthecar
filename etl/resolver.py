# etl/resolver.py
import re
import unicodedata
from typing import Optional, Tuple
import pandas as pd
from pathlib import Path
from .paths import CONF

# Where we expect the mapping file to live
ALIASES_CSV = CONF / "model_aliases.csv"

# -------------------------
# String helpers
# -------------------------
def _slug(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")
    return s

def _norm(s: str) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    s2 = str(s).lower()
    # remove common body-style / transmission descriptors
    s2 = re.sub(r"\b(hatchback|saloon|estate|coupe|convertible|manual|automatic|auto)\b", "", s2)
    # remove common engine/marketing suffixes; keep these minimal to avoid over-normalising
    s2 = re.sub(r"\b(tdi|tsi|vvt|gdi|tdci|ecoboost|hdi|dci|mhev|phev|hev)\b", "", s2)
    s2 = re.sub(r"[^a-z0-9]+", " ", s2).strip()
    return re.sub(r"\s+", " ", s2)

# -------------------------
# Alias map loading
# -------------------------
def _load_alias_raw() -> pd.DataFrame:
    """Load the raw CSV if present; return empty DF with expected columns otherwise."""
    if not ALIASES_CSV.exists():
        return pd.DataFrame(columns=["make_raw","model_raw"])
    df = pd.read_csv(ALIASES_CSV)
    # normalise column names (case-insensitive)
    df.columns = [c.strip() for c in df.columns]
    return df

def _resolve_alias_columns(df: pd.DataFrame) -> Tuple[str, str]:
    """
    Decide which columns represent the canonical targets.
    Supported shapes:
      - make_raw, model_raw, alias_make, alias_model
      - make_raw, model_raw, canonical_make, canonical_model
    Returns (target_make_col, target_model_col) or ("","") if none.
    """
    candidates = [
        ("canonical_make", "canonical_model"),
        ("alias_make", "alias_model"),
        ("target_make", "target_model"),
    ]
    for mk, md in candidates:
        if mk in df.columns and md in df.columns:
            return mk, md
    return "", ""

def load_alias_map() -> pd.DataFrame:
    """
    Returns a frame keyed by (norm_make,norm_model) → (norm_make_target,norm_model_target)
    with columns: norm_make, norm_model, norm_make_target, norm_model_target
    """
    raw = _load_alias_raw()
    if raw.empty:
        return pd.DataFrame(columns=["norm_make","norm_model","norm_make_target","norm_model_target"])

    # Fill the minimal columns
    for col in ("make_raw","model_raw"):
        if col not in raw:
            raw[col] = ""

    raw["norm_make"] = raw["make_raw"].map(_norm)
    raw["norm_model"] = raw["model_raw"].map(_norm)

    tgt_mk_col, tgt_md_col = _resolve_alias_columns(raw)
    if tgt_mk_col and tgt_md_col:
        raw["norm_make_target"] = raw[tgt_mk_col].map(_norm)
        raw["norm_model_target"] = raw[tgt_md_col].map(_norm)
    else:
        # If no explicit targets provided, assume the raw values *are* canonical
        raw["norm_make_target"] = raw["norm_make"]
        raw["norm_model_target"] = raw["norm_model"]

    # Drop duplicates last so latest rows in the CSV win
    raw = (
        raw[["norm_make","norm_model","norm_make_target","norm_model_target"]]
        .dropna()
        .drop_duplicates(keep="last")
        .reset_index(drop=True)
    )
    return raw

# -------------------------
# Public helpers used by ETL
# -------------------------
def normalise_df(df: pd.DataFrame, make_col: str, model_col: str) -> pd.DataFrame:
    """
    Add 'norm_make','norm_model','make_slug','model_slug' and apply alias mapping.
    """
    out = df.copy()
    out["norm_make"] = out[make_col].map(_norm)
    out["norm_model"] = out[model_col].map(_norm)

    alias = load_alias_map()
    if not alias.empty:
        out = out.merge(alias, how="left", on=["norm_make","norm_model"])
        # If a mapping exists, use the targets; else keep originals
        out["norm_make"] = out["norm_make_target"].fillna(out["norm_make"])
        out["norm_model"] = out["norm_model_target"].fillna(out["norm_model"])
        out = out.drop(columns=["norm_make_target","norm_model_target"], errors="ignore")

    out["make_slug"] = out["norm_make"].map(_slug)
    out["model_slug"] = out["norm_model"].map(_slug)
    return out

def slugify(s: str) -> str:
    return _slug(s)

def norm(s: str) -> str:
    return _norm(s)

# -------------------------
# Tiny CLI: `python -m etl.resolver "Volkswagen" "Golf"`
# -------------------------
def _demo(make: Optional[str], model: Optional[str]):
    mk = norm(make or "")
    md = norm(model or "")
    alias = load_alias_map()
    if not alias.empty:
        row = alias[(alias["norm_make"]==mk) & (alias["norm_model"]==md)]
        if len(row):
            mk = row.iloc[0]["norm_make_target"]
            md = row.iloc[0]["norm_model_target"]
    print("norm_make:", mk, " norm_model:", md, " slugs:", slugify(mk), slugify(md))

if __name__ == "__main__":
    import sys
    args = sys.argv[1:]
    m = args[0] if len(args) >= 1 else ""
    d = args[1] if len(args) >= 2 else ""
    _demo(m, d)