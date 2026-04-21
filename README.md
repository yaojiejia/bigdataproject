# NYC Neighborhood Insights

An end-to-end big-data pipeline and interactive map that helps newcomers evaluate New York City neighborhoods across **safety**, **food quality**, **quality of life (311)**, and **affordability (rent)**.

All heavy lifting — ETL, geocoding, scoring, analytics — is **Scala + Apache Spark** and runs on GCP Dataproc. Python survives only as HTTP glue in `data_ingest/`. The UI is a **static bundle** served as plain files: `hyparquet` reads the Spark-written parquet in the browser and Plotly.js / Leaflet render it.

## What it does

1. **Ingests** four public datasets: NYPD crime complaints, DOHMH restaurant inspections, 311 food-safety complaints, and Zillow ZORI rent. *(Python, `requests` only.)*
2. **Cleans and enriches** them with **Scala + Apache Spark** (upper/trim, drop-nulls, `IS_FELONY`, `IS_CRITICAL`, rent wide-to-long melt).
3. **Normalizes geography** by spatially joining each record to a 2020 NYC Neighborhood Tabulation Area (NTA) using **JTS point-in-polygon** inside a broadcast Spark UDF.
4. **Aggregates features** per neighborhood: crime totals / rates, restaurant inspection stats, complaint counts, median rent.
5. **Combines them** into a unified **newcomer score** (z-scored, weighted, 0–100).
6. **Pre-computes every dashboard aggregate** — summary stats, histograms, top/bottom-10, borough box-plot summaries, a 6×6 correlation matrix, and rent-vs-predictor bins/OLS/points — into small single-file parquet tables under `data/analytics/`.
7. **Renders** everything in a **backend-free static site**: Leaflet map (`index.html`) and Plotly dashboard (`dashboard.html`) fetch parquet files directly with [hyparquet](https://github.com/hyparam/hyparquet).

## Architecture

```
┌──────────────────────────────── GCP Dataproc (Scala / Spark) ────────────────────────────────┐
                                                                                               
   Python ingestion                                                                            
   data_ingest/alexj/*.py   →   data/raw/                                                      
                                    │  etl_code/alexj/Clean.scala                              
                                    ▼                                                          
                                data/cleaned/                                                  
                                    │  etl_code/alexj/Geocode.scala   (JTS + broadcast UDF)    
                                    ▼                                                          
                                data/enriched/                                                 
                                    │  etl_code/alexj/Features.scala                           
                                    ▼                                                          
                                data/scores/neighborhood_features.parquet                      
                                    │  etl_code/alexj/Score.scala                              
                                    ▼                                                          
                                data/scores/newcomer_score.parquet                             
                                    │  etl_code/alexj/Analytics.scala                          
                                    ▼                                                          
                                data/analytics/*.parquet                                       
└───────────────────────────────────────────────────────────────────────────────────────────────┘
                                               │
                                               │ gsutil rsync (or publish web/ + data/ to GCS)
                                               ▼
                     ┌──────────────── Static frontend (no backend) ────────────────┐
                     │  web/index.html        – Leaflet map, reads newcomer_score    │
                     │  web/dashboard.html    – Plotly dashboard, reads data/analytics│
                     │  web/lib/parquet.js    – hyparquet loader wrapper              │
                     │  web/lib/figures.js    – Plotly figure builders                │
                     └───────────────────────────────────────────────────────────────┘
```

**Language split:** Scala/Spark for every big-data step; Python only for `data_ingest/` HTTP downloads; plain JS + Plotly + Leaflet for the display layer.

## Project layout

Matches the assignment rubric: `/data_ingest`, `/etl_code`, `/profiling_code`, each with a per-member subdirectory.

```
bigdataproject/
  data_ingest/alexj/             # Python – HTTP ingestion only
    download.py                  # 4 raw CSVs from NYC Open Data + Zillow
    geo_download.py              # NTA GeoJSON + population template
    paths.py                     # tiny, self-contained path module

  etl_code/alexj/                # Scala + Spark – everything else
    Clean.scala                  # raw CSV     -> data/cleaned/*.parquet
    Geocode.scala                # cleaned     -> data/enriched/*.parquet  (JTS PIP)
    Features.scala               # enriched    -> data/scores/neighborhood_features.parquet
    Score.scala                  # features    -> data/scores/newcomer_score.parquet
    Analytics.scala              # score       -> data/analytics/*.parquet (dashboard tables)

  profiling_code/alexj/          # Scala + Spark – exploratory profiling
    FirstCode.scala              # schemas + mean/median/mode + RDD map/reduce
    CountRecs.scala              # row counts + distinct-value surveys

  web/                           # Static frontend – no backend server
    index.html  map.js           # Leaflet choropleth of newcomer score + raw features
    dashboard.html  dashboard.js # Plotly analytics dashboard
    lib/parquet.js               # hyparquet wrapper
    lib/figures.js               # Plotly figure builders (port of old analytics.py)
    style.css  dashboard.css

  ops/submit_dataproc.sh         # wraps `gcloud dataproc jobs submit spark` per stage
  Makefile                       # one target per stage; `make help`

  data/                          # all Spark outputs land here
    raw/  cleaned/  enriched/  scores/  analytics/  geo/
```

## Prerequisites

- **Java 11** — required by Spark.
- **Apache Spark 3.5** — the Scala jobs run via `spark-shell -i`. Install the standalone distribution from <https://spark.apache.org/downloads.html> (or `brew install apache-spark` / `apt install spark`). First run will pull `org.locationtech.jts:jts-core:1.19.0` (for geocoding) via ivy into `~/.ivy2`.
- **Python 3.11+** — only for `data_ingest/alexj/*.py`; the one dependency is `requests` (see `requirements.txt`).
- **GCP SDK (`gcloud`)** — only if you want to run the pipeline on Dataproc via `ops/submit_dataproc.sh`.

## Running the full stack, end-to-end (local)

### One-time setup

```bash
make setup               # pip install requests
make download            # ~570 MB of CSVs into data/raw/
make geo-download        # NTA GeoJSON + population template into data/geo/
```

### Batch pipeline (one command)

```bash
make pipeline            # Clean.scala -> Geocode.scala -> Features.scala -> Score.scala -> Analytics.scala
```

Produces:

- `data/cleaned/{crime,restaurants,complaints311,rent}/*.parquet`
- `data/enriched/{crime,restaurants,complaints311,rent}/*.parquet`
- `data/scores/neighborhood_features.parquet/` (Spark directory)
- `data/scores/newcomer_score.parquet` (single file — what the frontend reads)
- `data/analytics/*.parquet` (nine single-file tables feeding the dashboard)

Wall time on an 8-core laptop: ~1 min for `clean`, ~45 s for `geocode` (JTS PIP on ~500 k points), ~12 s for `features`, ~5 s for `score`, ~10 s for `analytics`.

### Serve the static site

The dashboard + map read parquet directly from disk via `../data/...`, so any static file server works. The Makefile target uses Python's stdlib `http.server` as a convenience only — Python plays **no** runtime role:

```bash
make web                 # http://localhost:5173
# equivalent: npx http-server . -p 5173
#             caddy file-server --root . --listen :5173
```

Open <http://localhost:5173/web/>. The map loads `data/scores/newcomer_score.parquet` directly, builds a GeoJSON against `data/geo/nta.geojson`, and re-styles in-place when you switch metric. Click **Open analytics dashboard** (or go to <http://localhost:5173/web/dashboard.html>) to see the five charts: distribution histogram, top/bottom 10, by-borough box plots, correlation heatmap, and rent-vs-feature trend with an OLS prediction. All figures are built in the browser from the nine parquet aggregates `Analytics.scala` emits.

## Running on Dataproc

1. Upload the raw CSVs + geojson to GCS:

   ```bash
   gsutil -m rsync -r data/raw gs://$BUCKET/data/raw
   gsutil -m rsync -r data/geo gs://$BUCKET/data/geo
   ```

2. Create a cluster (any 2-worker n2-standard-4 will do):

   ```bash
   gcloud dataproc clusters create nyc-insights \
     --region=us-central1 \
     --image-version=2.2-debian12 \
     --num-workers=2 \
     --worker-machine-type=n2-standard-4
   ```

3. Submit each Scala stage — the helper script wraps `gcloud dataproc jobs submit spark`:

   ```bash
   export DATAPROC_CLUSTER=nyc-insights
   export DATAPROC_REGION=us-central1
   export GCS_DATA_ROOT=gs://$BUCKET/data

   ops/submit_dataproc.sh pipeline              # clean → geocode → features → score → analytics
   ```

4. Sync the generated parquet back for local dashboarding, or publish `web/` + `data/` to a GCS static site:

   ```bash
   gsutil -m rsync -r gs://$BUCKET/data ./data
   make web
   ```

   Or for hosted serving: `gsutil -m cp -r web data gs://$STATIC_BUCKET/` and configure the bucket as a website.

## Data sources

| Dataset | Source | ID |
|---|---|---|
| NYPD Complaint (YTD) | NYC Open Data | `5uac-w243` |
| Restaurant Inspection | NYC Open Data | `43nn-pn8j` |
| 311 Service Requests (food subset) | NYC Open Data | `erm2-nwe9` |
| NTA 2020 polygons | NYC Open Data | `9nt8-h7nd` |
| ZORI rent index (ZIP) | Zillow Research | `Zip_zori_uc_sfrcondomfr_sm_month.csv` |

## Notes

- **Population is optional.** Drop a populated CSV at `data/geo/nta_population.csv` (columns `nta_code,nta_name,population`) and `Features.scala` will compute per-capita rates. Otherwise raw totals are used.
- **ZIP → NTA mapping** is derived inside `Geocode.scala` from the restaurant dataset (each ZIP assigned to the NTA containing the plurality of its restaurants). Replace with a proper ZCTA shapefile if you have one.
- **Spark tuning** (shuffle partitions, AQE, broadcast threshold, driver memory) is applied via the Makefile's `SPARK_TUNE` variable. See `SCALABILITY.md`.
- **No Python at serve time.** The dashboard and map are pure static files. The only Python runtime is `requests` in the two download scripts.
