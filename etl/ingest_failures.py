from __future__ import annotations
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from .paths import RAW, INT
from .lookups import load_lookup_tables, build_rfr_bucket_map

def _find_failures_csv():
    cand = list((RAW / "failures").rglob("*.csv"))
    if not cand:
        raise FileNotFoundError("No failure items CSV under data_raw/failures")
    return max(cand, key=lambda p: p.stat().st_size)

def ingest_failures():
    fail_csv = _find_failures_csv()
    df = pd.read_csv(fail_csv, dtype=str, low_memory=False)

    def pick(*alts: str) -> str:
        for a in alts:
            if a in df.columns: return a
        for a in alts:
            for c in df.columns:
                if c.lower() == a.lower(): return c
        raise KeyError(f"None of columns {alts} found in failures CSV")

    test_id_col = None
    for a in ("test_id","testnumber","test_no"):
        if a in df.columns: test_id_col = a; break
        for c in df.columns:
            if c.lower() == a.lower(): test_id_col = c; break

    rfr_code_col = pick("rfr_code","rfrid","rfr_id","item_id","defect_id","rfrCode")
    deficiency_col = next((c for c in df.columns if "deficiency" in c.lower()), None)

    look = load_lookup_tables(RAW / "lookups")
    rfr_bucket_map = build_rfr_bucket_map(look.get("rfr")) if "rfr" in look else {}

    out = pd.DataFrame({
        "rfr_code": df[rfr_code_col].astype(str),
        "fail_bucket": df[rfr_code_col].astype(str).map(lambda c: rfr_bucket_map.get(c, "other")),
    })
    if deficiency_col:
        out["deficiency"] = df[deficiency_col].astype(str).str.lower()
    if test_id_col:
        out["test_id"] = df[test_id_col].astype(str)

    out_path = INT / "failures.parquet"
    pq.write_table(pa.Table.from_pandas(out), out_path)
    print(f"[ingest_failures] wrote {out_path}")

if __name__ == "__main__":
    ingest_failures()
