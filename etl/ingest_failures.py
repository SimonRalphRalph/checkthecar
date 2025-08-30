# etl/ingest_failures.py
from __future__ import annotations
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path
from .paths import RAW, INT
from .lookups import load_lookup_tables, build_rfr_bucket_map

def _find_failures_csv(root: Path) -> Path:
    cand = list((root / "failures").rglob("*.csv"))
    if not cand:
        raise FileNotFoundError("No failure items CSV under data_raw/failures")
    return max(cand, key=lambda p: p.stat().st_size)

def ingest_failures():
    fail_csv = _find_failures_csv(RAW)
    df = pd.read_csv(fail_csv, dtype=str, low_memory=False)

    # Common columns (names vary): test_id, rfr_code/id, result_code/defect_result
    # We'll keep a flexible pick similar to results
    def pick(*alts: str) -> str:
        for a in alts:
            if a in df.columns:
                return a
        for a in alts:
            for c in df.columns:
                if c.lower() == a.lower():
                    return c
        raise KeyError(f"None of columns {alts} found in failures CSV")

    test_id_col = pick("test_id","testnumber","test_no")  # not used for join to results here (we aggregate by cohort), but we keep it
    rfr_code_col = pick("rfr_code","rfrid","rfr_id","item_id","defect_id","rfrCode")
    # DVSA provides 'deficiency_category' and/or 'dangerous/major/minor' flags; if present we can keep them.
    deficiency_col = next((c for c in df.columns if "deficiency" in c.lower()), None)

    # Load lookups to produce bucket map
    look = load_lookup_tables(RAW / "lookups")
    rfr_bucket_map = build_rfr_bucket_map(look.get("rfr")) if "rfr" in look else {}

    out = pd.DataFrame({
        "test_id": df[test_id_col].astype(str),
        "rfr_code": df[rfr_code_col].astype(str),
        "fail_bucket": df[rfr_code_col].astype(str).map(lambda c: rfr_bucket_map.get(c, "other")),
    })
    if deficiency_col:
        out["deficiency"] = df[deficiency_col].astype(str).str.lower()  # 'dangerous'/'major'/'minor' etc.

    # Persist compact parquet
    out_path = INT / "failures.parquet"
    pq.write_table(pa.Table.from_pandas(out), out_path)
    print(f"[ingest_failures] wrote {out_path}")

if __name__ == "__main__":
    ingest_failures()