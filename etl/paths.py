from pathlib import Path
import os

ROOT = Path(__file__).resolve().parents[1]
RAW = Path(os.getenv("ETL_RAW_DIR", ROOT / "data_raw"))
INT = Path(os.getenv("ETL_INT_DIR", ROOT / "data_intermediate"))
PUB = Path(os.getenv("ETL_PUB_DIR", ROOT / "public" / "data"))
CONF = ROOT / "data"  # model_aliases.csv lives here

MOT_PARQUET = INT / "mot"                # partitioned parquet dataset root
MOT_AGG_PARQUET = INT / "mot_agg.parquet"
RECALLS_PARQUET = INT / "recalls.parquet"
VCA_PARQUET = INT / "vca.parquet"
VED_JSON = INT / "ved_bands.json"

PUB.mkdir(parents=True, exist_ok=True)
INT.mkdir(parents=True, exist_ok=True)
RAW.mkdir(parents=True, exist_ok=True)

if __name__ == "__main__":
    print("ROOT:", ROOT)
    print("RAW:", RAW)
    print("INT:", INT)
    print("PUB:", PUB)
    print("MOT_PARQUET:", MOT_PARQUET)
    print("VED_JSON:", VED_JSON)
