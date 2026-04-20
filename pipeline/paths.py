"""Shared filesystem paths for all pipeline stages."""
from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT = Path(os.environ.get("DATA_ROOT", PROJECT_ROOT / "data"))

RAW = DATA_ROOT / "raw"
CLEANED = DATA_ROOT / "cleaned"
ENRICHED = DATA_ROOT / "enriched"
SCORES = DATA_ROOT / "scores"
STREAM = DATA_ROOT / "stream"
GEO = DATA_ROOT / "geo"

RAW_CRIME = RAW / "nypd_complaints.csv"
RAW_RESTAURANTS = RAW / "restaurant_inspections.csv"
RAW_COMPLAINTS311 = RAW / "complaints_311_food.csv"
RAW_RENT = RAW / "zori_zip.csv"

CLEAN_CRIME = CLEANED / "crime"
CLEAN_RESTAURANTS = CLEANED / "restaurants"
CLEAN_COMPLAINTS311 = CLEANED / "complaints311"
CLEAN_RENT = CLEANED / "rent"

ENR_CRIME = ENRICHED / "crime"
ENR_RESTAURANTS = ENRICHED / "restaurants"
ENR_COMPLAINTS311 = ENRICHED / "complaints311"
ENR_RENT = ENRICHED / "rent"

NTA_GEOJSON = GEO / "nta.geojson"
ZIP_TO_NTA = GEO / "zip_to_nta.csv"
NTA_POPULATION = GEO / "nta_population.csv"

NEIGHBORHOOD_FEATURES = SCORES / "neighborhood_features.parquet"
NEWCOMER_SCORE = SCORES / "newcomer_score.parquet"

STREAM_WINDOWED = STREAM / "windowed"
STREAM_LATEST = STREAM / "latest.parquet"
STREAM_LOGS = STREAM / "logs"


def ensure_dirs() -> None:
    for p in (RAW, CLEANED, ENRICHED, SCORES, STREAM, GEO,
              STREAM_WINDOWED, STREAM_LOGS):
        p.mkdir(parents=True, exist_ok=True)
