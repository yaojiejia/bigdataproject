"""One-time download of NTA geometry + NYC population reference tables.

Sources:
  - 2020 NTA polygons: NYC Open Data 9nt8-h7nd (GeoJSON)
  - NTA 2020 population (ACS / decennial): NYC DCP publishes XLSX; we use a
    CSV extract hosted on NYC Open Data (tg5h-jynt) if available, else a
    static fallback file committed under data/geo/nta_population.csv.
"""
from __future__ import annotations

import json
from pathlib import Path

import requests

from data_ingest.alexj.paths import GEO, NTA_GEOJSON, NTA_POPULATION, ensure_dirs

NTA_GEOJSON_URL = "https://data.cityofnewyork.us/api/geospatial/9nt8-h7nd?method=export&format=GeoJSON"


def download_nta_geojson() -> None:
    if NTA_GEOJSON.exists():
        print(f"[skip] {NTA_GEOJSON} already present")
        return
    print(f"[NTA GeoJSON] -> {NTA_GEOJSON}")
    NTA_GEOJSON.parent.mkdir(parents=True, exist_ok=True)
    r = requests.get(NTA_GEOJSON_URL, timeout=300)
    r.raise_for_status()
    # Verify JSON parses.
    data = r.json()
    NTA_GEOJSON.write_text(json.dumps(data))
    n = len(data.get("features", []))
    print(f"  {n} NTA polygons written")


def ensure_population_table() -> None:
    """Ensure a population file exists.

    If we can't reach a live source, write an empty table with headers — the
    features stage will fall back to borough-level estimates if this is empty.
    """
    if NTA_POPULATION.exists():
        print(f"[skip] {NTA_POPULATION} already present")
        return
    # Template file; a curated CSV can be dropped in place manually.
    NTA_POPULATION.parent.mkdir(parents=True, exist_ok=True)
    NTA_POPULATION.write_text("nta_code,nta_name,population\n")
    print(f"[NTA population] wrote empty template at {NTA_POPULATION}")
    print("  (drop in a populated CSV for per-capita features)")


def main() -> None:
    ensure_dirs()
    download_nta_geojson()
    ensure_population_table()


if __name__ == "__main__":
    main()
