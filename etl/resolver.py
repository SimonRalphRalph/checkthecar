import re
import unicodedata
from typing import Optional, Tuple
import pandas as pd
from pathlib import Path
from .paths import CONF

ALIASES_CSV = CONF / "model_aliases.csv"

def _slug(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")
    return s

def _norm(s: str) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    s2 = str(s).lower()
    s2 = re.sub(r"\b(hatchback|saloon|estate|coupe|convertible|manual|automatic|auto)\b", "", s2)
    s2 = re.sub(r"\b(tdi|tsi|vvt|gdi|tdci|ecoboost|hdi|dci|mhev|phev|hev)\b", "", s2)
    s2 = re.sub(r"[^a-z0-9]+", " ", s2).strip()
    return re.sub(r"\s+", " ", s2)

def _load_alias_raw() -> pd.DataFrame:
    if not ALIASES_CSV.exists():
        return pd.DataFrame(columns=["make_raw","model_raw"])
    df = pd.read_csv(ALIASES_CSV)
    df.columns = [c.strip() for c in df.columns]
    return df

def _resolve_alias_columns(df: pd.DataFrame) -> Tuple[str, str]:
    for mk, md in (("canonical_make","canonical_model"), ("alias_make","alias_model"), ("target_make","target_model")):
        if mk in df.columns and md in df.columns:
            return mk, md
    return "", ""

def load_alias_map() -> pd.DataFrame:
    raw = _load_alias_raw()
    if raw.empty:
        return pd.DataFrame(columns=["norm_make","norm_model","norm_make_target","norm_model_target"])
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
        raw["norm_make_target"] = raw["norm_make"]
        raw["norm_model_target"] = raw["norm_model"]
    raw = raw[["norm_make","norm_model","norm_make_target","norm_model_target"]].drop_duplicates(keep="last")
    return raw

def normalise_df(df: pd.DataFrame, make_col: str, model_col: str) -> pd.DataFrame:
    out = df.copy()
    out["norm_make"] = out[make_col].map(_norm)
    out["norm_model"] = out[model_col].map(_norm)
    alias = load_alias_map()
    if not alias.empty:
        out = out.merge(alias, how="left", on=["norm_make","norm_model"])
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
