"""Filesystem layout for the ingestion scripts.

The big-data surface (ETL, analytics, streaming) is all Scala/Spark now, so
the only Python left in the repo is the two downloaders in this package.
They need a minimal, self-contained path module — no cross-package imports,
no dependency on `pipeline/` (which has been removed).

The canonical data root is `$DATA_ROOT` if set, else `<repo>/data`. On
Dataproc you typically set `DATA_ROOT` to a local scratch dir, run the
downloaders once, then `gsutil cp -r ./data/raw gs://$BUCKET/raw/` before
submitting the Scala Spark jobs.
"""
from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.environ.get("DATA_ROOT", PROJECT_ROOT / "data"))

RAW = DATA_ROOT / "raw"
GEO = DATA_ROOT / "geo"

RAW_CRIME         = RAW / "nypd_complaints.csv"
RAW_RESTAURANTS   = RAW / "restaurant_inspections.csv"
RAW_COMPLAINTS311 = RAW / "complaints_311_food.csv"
RAW_RENT          = RAW / "zori_zip.csv"

NTA_GEOJSON    = GEO / "nta.geojson"
NTA_POPULATION = GEO / "nta_population.csv"


def ensure_dirs() -> None:
    """Create the two directories the downloaders write into."""
    for p in (RAW, GEO):
        p.mkdir(parents=True, exist_ok=True)
