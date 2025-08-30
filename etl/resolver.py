# etl/resolver.py
import re
import unicodedata
import pandas as pd
from pathlib import Path
from .paths import CONF

ALIASES_CSV = CONF / "model_aliases.csv"

def _slug(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")
    return s

def _norm(s: str) -> str:
    if pd.isna(s): return ""
    s2 = s.lower()
    # collapse marketing fluff
    s2 = re.sub(r"\b(hatchback|saloon|estate|coupe|automatic|manual)\b", "", s2)
    s2 = re.sub(r"\b(tdi|tsi|vvt|gdi|tdci|ecoboost|hdi|dci)\b", "", s2)
    s2 = re.sub(r"[^a-z0-9]+", " ", s2).strip()
    return re.sub(r"\s+", " ", s2)

def load_alias_map() -> pd.DataFrame:
    if not ALIASES_CSV.exists():
        # minimal frame
        return pd.DataFrame(columns=["make_raw","model_raw","norm_make","norm_model"])
    df = pd.read_csv(ALIASES_CSV)
    for col in ("make_raw","model_raw"):
        if col not in df:
            df[col] = ""
    if "norm_make" not in df:
        df["norm_make"] = df["make_raw"].map(_norm)
    if "norm_model" not in df:
        df["norm_model"] = df["model_raw"].map(_norm)
    return df.drop_duplicates()

def normalise_df(df: pd.DataFrame, make_col: str, model_col: str) -> pd.DataFrame:
    df = df.copy()
    df["norm_make"] = df[make_col].astype(str).map(_norm)
    df["norm_model"] = df[model_col].astype(str).map(_norm)
    # apply aliases
    alias = load_alias_map()
    if len(alias):
        df = df.merge(alias, how="left", on=["norm_make","norm_model"], suffixes=("","_alias"))
        # prefer explicit alias targets if present (alias_target_make/model)
        for col in ("norm_make","norm_model"):
            alias_col = f"{col}_alias"
            if alias_col in df and df[alias_col].notna().any():
                df[col] = df[alias_col].fillna(df[col])
        drop_cols = [c for c in df.columns if c.endswith("_alias") or c in ("make_raw","model_raw")]
        df = df.drop(columns=drop_cols, errors="ignore")
    df["make_slug"] = df["norm_make"].map(_slug)
    df["model_slug"] = df["norm_model"].map(_slug)
    return df

def slugify(s: str) -> str:
    return _slug(s)

def norm(s: str) -> str:
    return _norm(s)
