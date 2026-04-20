"""Download raw datasets from NYC Open Data + Zillow into data/raw/.

Datasets:
  - NYPD Complaint Data (Current YTD)           5uac-w243
  - DOHMH Restaurant Inspection Results          43nn-pn8j
  - 311 Service Requests (food-safety subset)    erm2-nwe9
  - Zillow ZORI ZIP-level (filtered to NYC)      zillow.com
"""
from __future__ import annotations

import io
import sys
import time
from pathlib import Path

import requests

from .paths import (
    RAW_COMPLAINTS311,
    RAW_CRIME,
    RAW_RENT,
    RAW_RESTAURANTS,
    ensure_dirs,
)

NYC_SOCRATA = "https://data.cityofnewyork.us/resource/{dataset}.csv"
NYC_DOWNLOAD = "https://data.cityofnewyork.us/api/views/{dataset}/rows.csv?accessType=DOWNLOAD"

# Zillow publishes ZORI as a public CSV. URL last verified 2026-04.
ZILLOW_ZORI_URL = (
    "https://files.zillowstatic.com/research/public_csvs/zori/"
    "Zip_zori_uc_sfrcondomfr_sm_month.csv"
)

# 311 food-safety complaint types
FOOD_COMPLAINT_TYPES = [
    "Food Poisoning",
    "Food Establishment",
    "Rodent",
    "Unsanitary Animal Pvt Property",
]


def _stream_to_file(url: str, dest: Path, chunk: int = 1 << 20) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"  -> {url}")
    with requests.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length") or 0)
        written = 0
        t0 = time.time()
        with dest.open("wb") as f:
            for part in r.iter_content(chunk):
                if not part:
                    continue
                f.write(part)
                written += len(part)
                if total:
                    pct = 100 * written / total
                    print(f"\r     {written/1e6:7.1f} / {total/1e6:7.1f} MB  ({pct:5.1f}%)",
                          end="", flush=True)
                else:
                    print(f"\r     {written/1e6:7.1f} MB", end="", flush=True)
        print(f"  done in {time.time()-t0:.1f}s")


def download_crime() -> None:
    if RAW_CRIME.exists():
        print(f"[skip] {RAW_CRIME} already present")
        return
    print("[crime]")
    _stream_to_file(NYC_DOWNLOAD.format(dataset="5uac-w243"), RAW_CRIME)


def download_restaurants() -> None:
    if RAW_RESTAURANTS.exists():
        print(f"[skip] {RAW_RESTAURANTS} already present")
        return
    print("[restaurants]")
    _stream_to_file(NYC_DOWNLOAD.format(dataset="43nn-pn8j"), RAW_RESTAURANTS)


def download_311_food() -> None:
    """Use the Socrata API with a SoQL filter to pull only food-safety types.

    The full 311 dataset is ~10GB+; we only need the food-safety subset.
    """
    if RAW_COMPLAINTS311.exists():
        print(f"[skip] {RAW_COMPLAINTS311} already present")
        return
    print("[311 food]")
    quoted = ",".join(f"'{t}'" for t in FOOD_COMPLAINT_TYPES)
    where = f"complaint_type in({quoted})"
    # Socrata supports $limit up to 50000 per page; paginate.
    base = NYC_SOCRATA.format(dataset="erm2-nwe9")
    page_size = 50000
    offset = 0
    RAW_COMPLAINTS311.parent.mkdir(parents=True, exist_ok=True)
    with RAW_COMPLAINTS311.open("w") as out:
        header_written = False
        while True:
            params = {
                "$where": where,
                "$limit": page_size,
                "$offset": offset,
                "$order": "unique_key",
            }
            print(f"  -> offset={offset}", end="", flush=True)
            r = requests.get(base, params=params, timeout=300)
            r.raise_for_status()
            text = r.text
            if not text.strip():
                print("  (empty)")
                break
            lines = text.splitlines(keepends=True)
            if not header_written:
                out.writelines(lines)
                header_written = True
                n = len(lines) - 1
            else:
                out.writelines(lines[1:])
                n = len(lines) - 1
            print(f"  +{n} rows")
            if n < page_size:
                break
            offset += page_size


def download_rent() -> None:
    if RAW_RENT.exists():
        print(f"[skip] {RAW_RENT} already present")
        return
    print("[rent - Zillow ZORI]")
    _stream_to_file(ZILLOW_ZORI_URL, RAW_RENT)


def main() -> None:
    ensure_dirs()
    download_crime()
    download_restaurants()
    download_311_food()
    download_rent()
    print("\nDownload complete.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\ninterrupted", file=sys.stderr)
        sys.exit(130)
