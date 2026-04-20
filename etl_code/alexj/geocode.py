"""Attach nta_code + nta_name to each cleaned dataset.

Point datasets (crime, restaurants, 311) are point-in-polygon joined to NTA.
Rent is ZIP-level; we spatially join ZIP-code polygons-as-points (centroids)
to NTAs. For simplicity we use ZIP centroids derived from the restaurant
dataset's lat/long values (one centroid per ZIP), which gives us a usable
ZIP -> NTA table without a separate ZCTA download.
"""
from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

from pipeline.paths import (
    CLEAN_COMPLAINTS311,
    CLEAN_CRIME,
    CLEAN_RENT,
    CLEAN_RESTAURANTS,
    ENR_COMPLAINTS311,
    ENR_CRIME,
    ENR_RENT,
    ENR_RESTAURANTS,
    NTA_GEOJSON,
    ZIP_TO_NTA,
    ensure_dirs,
)
from data_ingest.alexj import geo_download

NTA_NAME_FIELDS = ("ntaname", "NTAName", "nta_name", "NTA_NAME")
NTA_CODE_FIELDS = ("ntacode", "NTACode", "nta2020", "NTA2020", "nta_code")


def _load_nta_polygons() -> gpd.GeoDataFrame:
    if not NTA_GEOJSON.exists():
        geo_download.download_nta_geojson()
    raw = json.loads(NTA_GEOJSON.read_text())
    gdf = gpd.GeoDataFrame.from_features(raw["features"], crs="EPSG:4326")
    # Normalize the code/name columns.
    code_col = next((c for c in NTA_CODE_FIELDS if c in gdf.columns), None)
    name_col = next((c for c in NTA_NAME_FIELDS if c in gdf.columns), None)
    if not code_col or not name_col:
        raise RuntimeError(
            f"NTA GeoJSON is missing expected fields; got columns {list(gdf.columns)}"
        )
    gdf = gdf[[code_col, name_col, "geometry"]].rename(
        columns={code_col: "nta_code", name_col: "nta_name"}
    )
    return gdf


def _sjoin_points(df: pd.DataFrame, lon_col: str, lat_col: str,
                  ntas: gpd.GeoDataFrame) -> pd.DataFrame:
    points = gpd.GeoDataFrame(
        df.copy(),
        geometry=gpd.points_from_xy(df[lon_col], df[lat_col]),
        crs="EPSG:4326",
    )
    joined = gpd.sjoin(points, ntas, how="left", predicate="within")
    joined = joined.drop(columns=["geometry", "index_right"], errors="ignore")
    return pd.DataFrame(joined)


def _report(name: str, before: int, after_joined: int) -> None:
    hit = 100 * after_joined / max(before, 1)
    print(f"[{name}] {before} rows, {after_joined} joined to an NTA ({hit:.1f}%)")


def geocode_points(name: str, in_path: Path, out_path: Path,
                   ntas: gpd.GeoDataFrame, lon_col="Longitude", lat_col="Latitude") -> pd.DataFrame:
    df = pd.read_parquet(in_path)
    before = len(df)
    df = df.dropna(subset=[lon_col, lat_col])
    joined = _sjoin_points(df, lon_col, lat_col, ntas)
    _report(name, before, joined["nta_code"].notna().sum())
    out_path.mkdir(parents=True, exist_ok=True)
    joined.to_parquet(out_path / "part-00000.parquet", index=False)
    return joined


def build_zip_to_nta(restaurants_enriched: pd.DataFrame) -> pd.DataFrame:
    """Derive a ZIP -> NTA mapping from restaurants joined to NTAs.

    For each ZIP, pick the modal NTA (the one most restaurants fall into).
    This is a reasonable proxy without a full ZCTA shapefile.
    """
    df = restaurants_enriched[["ZIPCODE", "nta_code", "nta_name"]].dropna()
    df["ZIPCODE"] = df["ZIPCODE"].astype(str).str.extract(r"(\d{5})", expand=False)
    df = df.dropna(subset=["ZIPCODE"])
    counts = (
        df.groupby(["ZIPCODE", "nta_code", "nta_name"])
        .size().reset_index(name="n")
    )
    counts["rank"] = counts.groupby("ZIPCODE")["n"].rank(method="first", ascending=False)
    modal = counts[counts["rank"] == 1].drop(columns=["rank", "n"])
    ZIP_TO_NTA.parent.mkdir(parents=True, exist_ok=True)
    modal.to_csv(ZIP_TO_NTA, index=False)
    print(f"[zip->nta] {len(modal)} ZIPs mapped; wrote {ZIP_TO_NTA}")
    return modal


def geocode_rent(zip_to_nta: pd.DataFrame) -> None:
    df = pd.read_parquet(CLEAN_RENT)
    df["ZIPCODE"] = df["ZIPCODE"].astype(str).str.extract(r"(\d{5})", expand=False)
    merged = df.merge(zip_to_nta, on="ZIPCODE", how="left")
    _report("rent", len(df), merged["nta_code"].notna().sum())
    ENR_RENT.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(ENR_RENT / "part-00000.parquet", index=False)


def main() -> None:
    ensure_dirs()
    ntas = _load_nta_polygons()
    print(f"[nta] loaded {len(ntas)} polygons")

    rest = geocode_points("restaurants", CLEAN_RESTAURANTS, ENR_RESTAURANTS, ntas)
    geocode_points("crime", CLEAN_CRIME, ENR_CRIME, ntas)
    geocode_points("311 food", CLEAN_COMPLAINTS311, ENR_COMPLAINTS311, ntas)

    zip_to_nta = build_zip_to_nta(rest)
    geocode_rent(zip_to_nta)

    print("\nGeocoding complete.")


if __name__ == "__main__":
    main()
