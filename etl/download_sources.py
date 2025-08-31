from __future__ import annotations
import io, zipfile, sys
from pathlib import Path
import requests
from .paths import RAW

def _download(url: str) -> bytes:
    r = requests.get(url, timeout=180)
    r.raise_for_status()
    return r.content

def _save_zip(content: bytes, out_dir: Path, name: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / f"{name}.zip").write_bytes(content)
    with zipfile.ZipFile(io.BytesIO(content)) as z:
        z.extractall(out_dir / name)

def download_all(results_zip_url: str, failures_zip_url: str, lookups_zip_url: str):
    print("Downloading results…");  _save_zip(_download(results_zip_url), RAW, "results")
    print("Downloading failures…"); _save_zip(_download(failures_zip_url), RAW, "failures")
    print("Downloading lookups…");  _save_zip(_download(lookups_zip_url),  RAW, "lookups")
    print("Saved under", RAW)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        raise SystemExit("Usage: python -m etl.download_sources <results.zip> <failures.zip> <lookups.zip>")
    download_all(sys.argv[1], sys.argv[2], sys.argv[3])

