# etl/paths.py
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data_raw"
INT = ROOT / "data_intermediate"
PUB = ROOT / "public" / "data"
CONF = ROOT / "data"

# Parquet/CSV intermediates produced by earlier scripts
MOT_PARQUET = INT / "mot"                # partitioned parquet dataset root
MOT_AGG_PARQUET = INT / "mot_agg.parquet"
RECALLS_PARQUET = INT / "recalls.parquet"
VCA_PARQUET = INT / "vca.parquet"
VED_JSON = INT / "ved_bands.json"

PUB.mkdir(parents=True, exist_ok=True)
INT.mkdir(parents=True, exist_ok=True)
RAW.mkdir(parents=True, exist_ok=True)
