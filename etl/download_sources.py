# etl/download_sources.py
from __future__ import annotations
import io, zipfile, sys
from pathlib import Path
import requests
from .paths import RAW

# These are the landing URLs; the actual ZIP file URLs are linked from there.
# For a stable build, allow overriding via env vars or pass explicit URLs.
RESULTS_ZIP_URL = None  # set to direct .zip if you have it
FAILURES_ZIP_URL = None
LOOKUPS_ZIP_URL = None

def _download(url: str) -> bytes:
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    return r.content

def _save_zip(content: bytes, out_dir: Path, name: str) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{name}.zip"
    out.write_bytes(content)
    with zipfile.ZipFile(io.BytesIO(content)) as z:
        z.extractall(out_dir / name)
    return out

def download_all(results_url: str, failures_url: str, lookups_url: str):
    assert results_url and failures_url and lookups_url, "Provide direct .zip URLs for results, failures, lookups"
    print("Downloading results…")
    _save_zip(_download(results_url), RAW, "results")
    print("Downloading failures…")
    _save_zip(_download(failures_url), RAW, "failures")
    print("Downloading lookups…")
    _save_zip(_download(lookups_url), RAW, "lookups")
    print("Saved under", RAW)

if __name__ == "__main__":
    # Usage:
    # python -m etl.download_sources <results_zip_url> <failures_zip_url> <lookups_zip_url>
    if len(sys.argv) != 4:
        raise SystemExit("Usage: python -m etl.download_sources <results_zip_url> <failures_zip_url> <lookups_zip_url>")
    download_all(sys.argv[1], sys.argv[2], sys.argv[3])