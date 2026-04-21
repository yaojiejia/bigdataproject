# data_ingest/alexj

HTTP downloaders for the four raw datasets and the NTA GeoJSON.

## Files

| File | Role |
|---|---|
| `download.py` | Fetches the four raw CSVs (crime, restaurants, 311-food, Zillow rent) into `data/raw/`. 311 is pulled via the Socrata `$where` API so only the food-safety subset lands on disk. |
| `geo_download.py` | Fetches the 2020 NTA GeoJSON into `data/geo/nta.geojson` and writes a `nta_population.csv` template (one row per NTA, `population` blank). |

## Design choices

- **Streaming downloads with `requests` + `iter_content`** so large CSVs (the ~400 MB crime file) don't blow up memory.
- **Idempotent** — if the target file already exists the script skips it. Delete a file in `data/raw/` to force a re-download.
- **No API key required** for the volumes we use; Socrata throttles unauthenticated calls but the full dataset fits well under the cap.

## Imports

Both modules import `data_ingest.alexj.paths` for canonical filesystem
locations. `paths.py` is fully self-contained — no cross-package
imports — so the downloader can run without the Scala / Spark side of
the repo being installed.
