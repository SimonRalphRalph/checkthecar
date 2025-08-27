# etl/resolver.py
from dataclasses import dataclass
from typing import Dict, Tuple
import csv
import re
from rapidfuzz import process, fuzz

@dataclass(frozen=True)
class ModelKey:
    make: str
    model: str

CANONICAL: Dict[ModelKey, ModelKey] = {}

def _norm(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    # Rules: drop trim/engine tags
    s = re.sub(r"\b(16v|20v|tdci|tdi|tsi|vvt|sport|gt(i|d)?|se|zetec|style|trend|sline|mhd|blue(motion|drive)|ecoboost)\b", "", s)
    return re.sub(r"\s+", " ", s).strip()

def load_aliases(csv_path: str):
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            src = ModelKey(_norm(row["make"]), _norm(row["model"]))
            dst = ModelKey(_norm(row["canonical_make"]), _norm(row["canonical_model"]))
            CANONICAL[src] = dst

def canonicalize(make: str, model: str) -> Tuple[str, str]:
    src = ModelKey(_norm(make), _norm(model))
    if src in CANONICAL:
        dst = CANONICAL[src]
        return (dst.make, dst.model)
    # fuzzy match by make, then model within make
    candidates = [k for k in CANONICAL.keys() if k.make == src.make]
    if candidates:
        best, score, _ = process.extractOne(
            src.model, [c.model for c in candidates], scorer=fuzz.token_set_ratio
        )
        if score >= 92:
            for c in candidates:
                if c.model == best:
                    dst = CANONICAL[c]
                    return (dst.make, dst.model)
    return (src.make, src.model)
