# NYC Neighborhood Insights: A Big Data Pipeline for Newcomers

## Project Overview

This project develops an interactive, data-driven system designed to help newcomers better understand and evaluate neighborhoods in New York City. The system integrates multiple large-scale public datasets — including crime records and restaurant inspection results — to provide a comprehensive view of each neighborhood. The goal is to transform fragmented and complex urban data into clear, accessible insights that allow users to compare areas such as Tribeca, the Lower East Side, or Astoria across multiple dimensions like safety, food quality, and quality of life.

**Runtime surface:** every big-data step (ETL, geocoding, scoring, analytics, Kafka producing/consuming) is written in **Scala + Apache Spark** and is designed to run on a GCP Dataproc cluster via `gcloud dataproc jobs submit spark`. There is no Python backend server; there is no Jupyter notebook. The static frontend reads the Spark-written parquet directly in the browser with [hyparquet](https://github.com/hyparam/hyparquet) and renders charts with Plotly.js.

## Datasets

| Dataset | Source | ID | Description | Approximate Size |
|---------|--------|----|-------------|-----------------|
| NYPD Complaint Data (Current YTD) | NYC Open Data | `5uac-w243` | All valid felony, misdemeanor, and violation crimes reported to NYPD in the current year | ~580K rows, ~200 MB |
| DOHMH Restaurant Inspection Results | NYC Open Data | `43nn-pn8j` | Health inspection results for NYC restaurants including scores and grades | ~295K rows, ~140 MB |
| 311 Service Requests (food-safety subset) | NYC Open Data | `erm2-nwe9` | Food-poisoning, food-establishment, rodent, and unsanitary-animal complaints pulled via Socrata `$where` | ~320K rows, ~225 MB |
| Zillow ZORI (ZIP level) | Zillow Research | `Zip_zori_uc_sfrcondomfr_sm_month.csv` | Monthly Zillow Observed Rent Index by ZIP | ~9 MB (wide format) |

All four inputs are downloaded as CSV by `data_ingest/alexj/download.py` into `data/raw/`.

### Crime Data — Key Columns

| Column | Description |
|--------|-------------|
| `CMPLNT_NUM` | Unique complaint identifier |
| `CMPLNT_FR_DT` | Date the crime began |
| `OFNS_DESC` | Offense description (e.g., ROBBERY, FELONY ASSAULT) |
| `LAW_CAT_CD` | Offense level: FELONY, MISDEMEANOR, or VIOLATION |
| `BORO_NM` | Borough name (MANHATTAN, BRONX, BROOKLYN, QUEENS, STATEN ISLAND) |
| `Latitude` / `Longitude` | Geographic coordinates |

### Restaurant Inspection Data — Key Columns

| Column | Description |
|--------|-------------|
| `CAMIS` | Unique restaurant identifier |
| `DBA` | Business name |
| `BORO` | Borough code |
| `ZIPCODE` | ZIP code |
| `CUISINE DESCRIPTION` | Type of cuisine |
| `INSPECTION DATE` | Date of inspection |
| `SCORE` | Numeric inspection score (lower is better) |
| `GRADE` | Letter grade (A, B, C) |

## Architecture

```
┌──────────────────────────── GCP Dataproc (Scala / Spark) ────────────────────────────┐

    Python ingestion (data_ingest/alexj/*.py  — `requests` only)
                      │
                      ▼
               data/raw/
                      │  etl_code/alexj/Clean.scala           (Scala + Spark)
                      ▼
               data/cleaned/
                      │  etl_code/alexj/Geocode.scala         (Scala + Spark + JTS)
                      ▼
               data/enriched/                (every row has nta_code)
                      │  etl_code/alexj/Features.scala        (Scala + Spark)
                      ▼
               data/scores/neighborhood_features.parquet
                      │  etl_code/alexj/Score.scala           (Scala + Spark)
                      ▼
               data/scores/newcomer_score.parquet
                      │  etl_code/alexj/Analytics.scala       (Scala + Spark)
                      ▼
               data/analytics/*.parquet   (9 pre-aggregated tables)

    ┌──────── streaming (Scala/Spark only) ────────┐
    │ stream/Producer.scala  ──► Kafka (Docker)    │
    │                              │                │
    │                              ▼                │
    │           etl_code/alexj/Consumer.scala       │
    │                              │                │
    │                              ▼                │
    │           data/stream/latest.parquet          │
    └───────────────────────────────────────────────┘
└──────────────────────────────────────────────────────────────────────────────────────┘
                                 │
                                 │  gsutil rsync  (or host web/ + data/ on GCS directly)
                                 ▼
         ┌────────── Static frontend (no backend, no Python) ──────────┐
         │ web/index.html        – Leaflet choropleth                  │
         │ web/dashboard.html    – Plotly analytics dashboard          │
         │ web/lib/parquet.js    – hyparquet loader wrapper            │
         │ web/lib/figures.js    – Plotly figure builders              │
         └─────────────────────────────────────────────────────────────┘
```

### Technology Stack

| Component | Technology | Language | Purpose |
|-----------|-----------|----------|---------|
| Batch cleaning | Apache Spark 3.5 | **Scala 2.12** | `etl_code/alexj/Clean.scala` — all four raw CSVs → cleaned Parquet |
| Geographic join | Spark + JTS `Geometry.contains` | **Scala 2.12** | `etl_code/alexj/Geocode.scala` — point-in-polygon + ZIP→NTA modal |
| Feature aggregation | Apache Spark 3.5 | **Scala 2.12** | `etl_code/alexj/Features.scala` — per-NTA aggregates + join |
| Newcomer score | Apache Spark 3.5 | **Scala 2.12** | `etl_code/alexj/Score.scala` — z-score + weighted sum on 260 NTAs |
| Analytics aggregates | Apache Spark 3.5 | **Scala 2.12** | `etl_code/alexj/Analytics.scala` — writes 9 small parquet tables |
| Streaming consumer | Spark Structured Streaming | **Scala 2.12** | `etl_code/alexj/Consumer.scala` — Kafka → 5-min windows → Parquet |
| Streaming producer | Kafka Clients + Spark | **Scala 2.12** | `stream/Producer.scala` — replays enriched 311 onto Kafka |
| HTTP data download | `requests` | Python | `data_ingest/alexj/download.py`, `geo_download.py` — ingest only |
| Message broker | Apache Kafka | — | `kafka/docker-compose.yml` — topic `complaints311` |
| Parquet reader (browser) | [hyparquet](https://github.com/hyparam/hyparquet) | JavaScript | `web/lib/parquet.js` — single-file parquet over HTTP |
| Frontend map | Leaflet + plain JS | JavaScript | `web/index.html` — choropleth, click-through detail |
| Frontend dashboard | Plotly.js | JavaScript | `web/dashboard.html` — 5 analytical charts |
| Storage | Parquet | — | every stage reads and writes Parquet |
| Cluster target | Google Cloud Dataproc | — | `ops/submit_dataproc.sh` wraps `gcloud dataproc jobs submit spark` |

## Pipeline Stages

### Stage 1: Data Ingestion (`data_ingest/alexj/download.py`, `data_ingest/alexj/geo_download.py`)

Four raw CSVs and one GeoJSON are fetched over HTTP and written to `data/raw/` and `data/geo/`. 311 is fetched through the Socrata API with a `$where` filter so only the food-safety subset (Food Poisoning, Food Establishment, Rodent, Unsanitary Animal Pvt Property) is pulled server-side.

Python's role begins and ends here. The only runtime dependency is `requests`; nothing in these scripts processes data in a big-data sense (no pandas, no geopandas, no pyarrow). They're kept in Python because they're trivially small HTTP glue and shipping the raw datasets through Scala just to download them would be wasted effort.

### Stage 2: Batch Cleaning (`etl_code/alexj/Clean.scala`, Scala + Spark)

Canonical cleaner for all four datasets. Run locally with `make clean` (which shells out to `spark-shell -i etl_code/alexj/Clean.scala`), or submit to Dataproc via `ops/submit_dataproc.sh clean`. The script reads from `$DATA_ROOT/raw` locally or `hdfs:///user/$HDFS_USER/data/` on a cluster — same code, same output.

Per-dataset transformations:

| Dataset | Columns kept | Null-drop keys | Normalization | Derived column |
|---|---|---|---|---|
| Crime | `CMPLNT_NUM`, `CMPLNT_FR_DT`, `OFNS_DESC`, `LAW_CAT_CD`, `BORO_NM`, `Latitude`, `Longitude` | all keys + coords | `UPPER(TRIM(...))` on `BORO_NM`, `OFNS_DESC`, `LAW_CAT_CD` | `IS_FELONY = LAW_CAT_CD == "FELONY"` |
| Restaurants | `CAMIS`, `DBA`, `BORO`, `ZIPCODE`, `CUISINE DESCRIPTION`→`CUISINE`, `INSPECTION DATE`→`INSPECTION_DATE`, `SCORE`, `GRADE`, `Latitude`, `Longitude` | `BORO`, `SCORE`, coords | `UPPER(TRIM(...))` on `BORO`, `DBA`, `CUISINE`, `GRADE` | `IS_CRITICAL = SCORE >= 28` |
| 311 food | `unique_key`→`COMPLAINT_ID`, `created_date`→`CREATED_DATE`, `complaint_type`→`COMPLAINT_TYPE`, `descriptor`→`DESCRIPTOR`, `incident_zip`→`ZIPCODE`, `borough`→`BORO`, `latitude`→`Latitude`, `longitude`→`Longitude` | `COMPLAINT_TYPE`, coords | `UPPER(TRIM(...))` on `BORO`, `COMPLAINT_TYPE` | — |
| Zillow rent | `RegionName`→`ZIPCODE`, `City`, `State`, `Metro`, `month`, `ZORI` | `ZORI` (after melt) | wide-to-long via `stack(n, ...)`; `row_number()` window keeps the latest non-null month per ZIP | — |

**Output:** `data/cleaned/{crime, restaurants, complaints311, rent}/*.parquet`.

`profiling_code/alexj/FirstCode.scala` and `profiling_code/alexj/CountRecs.scala` are exploratory *profiling* scripts — mean/median/mode on the restaurant `SCORE` column and crime `Latitude`/`Longitude`, RDD `map`+`reduceByKey` per borough, and distinct-value surveys for boroughs/offenses/grades/cuisines. Kept in `/profiling_code` because they answer "what's in this data?" questions that *precede* writing ETL; they're deliberately outside the production pipeline.

### Stage 3: Geographic Normalization (`etl_code/alexj/Geocode.scala`, Scala + Spark + JTS)

`Geocode.scala` attaches `nta_code` + `nta_name` to every row of the three point datasets (crime, restaurants, 311) by point-in-polygon against the 2020 NTA GeoJSON. The polygon set (~260 polygons, < 1 MB) is parsed once on the driver with Jackson, converted to `org.locationtech.jts.geom.Geometry` values, and **broadcast to the executors**. A Spark UDF then walks the broadcast array with an envelope pre-filter and `Geometry.contains` to produce `(nta_code, nta_name)` per record.

Rent is ZIP-level with no coordinates, so the same script derives a ZIP → NTA lookup inside Spark by picking, for each ZIP, the NTA containing the plurality of its restaurants. That lookup is written to `data/geo/zip_to_nta.csv` and then broadcast-joined onto the rent parquet to produce `data/enriched/rent/`.

Why this is Scala now: the previous pandas/geopandas implementation was single-machine and couldn't run on Dataproc. Moving the spatial join into Spark with JTS keeps the entire ETL surface as distributed code, which is what the rubric requires.

### Stage 4: Feature Aggregation (`etl_code/alexj/Features.scala`, Scala + Spark)

`Features.scala` reads the four enriched Parquet directories, groups each by `(nta_code, nta_name)`, and produces one row per NTA with:

`total_crimes`, `felonies`, `felony_share`, `n_inspections`, `avg_score`, `critical_rate`, `n_complaints`, `median_rent_zori`.

All four feature tables are outer-joined; population is joined in (from `data/geo/nta_population.csv`) to derive `crimes_per_1k` and `complaints_per_1k` when available. Structural nulls are filled with zeros; rent nulls are kept as null.

**Output:** `data/scores/neighborhood_features.parquet` (~260 NTAs).

### Stage 5: Neighborhood Scoring (`etl_code/alexj/Score.scala`, Scala + Spark)

The cleaned and geocoded data from all four sources is aggregated per NYC Neighborhood Tabulation Area (NTA) in `Features.scala`, producing one row per neighborhood with six numeric features. `Score.scala` then collapses those features into a single **Newcomer Score** on a 0–100 scale using `percentile_approx`, `avg`, and `stddev_pop` on Spark — identical arithmetic to the earlier Python prototype, executed as distributed code.

#### Input features (per NTA)

| Feature | Direction | Source |
|---------|-----------|--------|
| `crimes_per_1k` | lower is better | NYPD complaints / NTA population |
| `felony_share` | lower is better | share of crimes tagged `LAW_CAT_CD == FELONY` |
| `avg_score` | lower is better | mean DOHMH inspection score (lower = cleaner kitchens) |
| `critical_rate` | lower is better | share of inspections flagged `IS_CRITICAL` (`SCORE >= 28`) |
| `complaints_per_1k` | lower is better | 311 food-safety complaints / NTA population |
| `median_rent_zori` | lower is better | Zillow ZORI most-recent-month median rent |

Per-capita rates use population values from `data/geo/nta_population.csv`; if that file is empty, raw counts are used as the intensity proxy.

#### Method

1. **Z-score each feature** across the ~260 NTAs: `z = (x − μ) / σ_pop`. Missing values are imputed with the citywide mean before z-scoring, so a neighborhood missing a signal stays neutral (z ≈ 0) on that axis instead of being penalized.
2. **Sign-flip "bad" features.** All six inputs are "higher is worse", so each z-score is multiplied by `-1`. After this step, a positive z consistently means "good for a newcomer."
3. **Collapse into four sub-scores** by averaging related z-scores:
   - `safety_score` = mean(`z_crimes_per_1k`, `z_felony_share`)
   - `food_safety_score` = mean(`z_avg_score`, `z_critical_rate`)
   - `cleanliness_score` = `z_complaints_per_1k`
   - `affordability_score` = `z_median_rent_zori`
4. **Weighted sum** using the weights defined at the top of `Score.scala`:
   `0.30·safety + 0.25·food_safety + 0.15·cleanliness + 0.30·affordability` (weights sum to 1.0).
5. **Rescale to 0–100** via min-max. The worst-ranking NTA gets 0, the best gets 100. Purely cosmetic.

#### Output

`data/scores/newcomer_score.parquet` — one **single-file** Parquet (Spark writes to a directory with `.coalesce(1)`, and the job then hoists the single part file up to the final path so the static frontend can fetch a predictable URL).

#### Notes

- The score is **relative, not absolute**. A 0/100 doesn't mean "unlivable" — it means worst-scoring of the 260 NTAs given these six signals and these weights.
- Weights are tunable by editing the `WEIGHTS` map at the top of `Score.scala` and rerunning `make score`.

### Stage 6: Analytics Aggregates (`etl_code/alexj/Analytics.scala`, Scala + Spark)

The dashboard needs nine specific things: a summary strip, a per-metric histogram, a per-metric top/bottom-10, per-metric box stats by borough, per-metric raw points by borough, a 6×6 Pearson correlation, and three rent-vs-feature tables. `Analytics.scala` computes all nine in a single Spark job and emits each one as a tiny single-file parquet under `data/analytics/`. The browser fetches each file once at load time and builds the Plotly figures from the arrays.

| File | Shape | Consumers |
|---|---|---|
| `summary.parquet` | 1 row per metric: mean, median, min, max, std, n_ntas, label, unit | dashboard summary strip + histogram mean/median rules |
| `distribution.parquet` | 24 equal-width bins per metric | histogram |
| `top_bottom.parquet` | top/bottom 10 per metric | horizontal-bar chart |
| `borough_box.parquet` | q1/median/q3/lowerfence/upperfence per (metric, borough) | box plot |
| `borough_points.parquet` | raw (metric, borough, nta_name, value) | jittered scatter overlay on the box plot |
| `correlation.parquet` | long-form (feature_row, feature_col, r) | 6×6 heatmap |
| `rent_vs_feature_bins.parquet` | decile bins of each predictor, median + IQR of rent | rent-vs-feature trend line + band |
| `rent_vs_feature_ols.parquet` | slope, intercept, Pearson r, R², example prediction | OLS dashed line + annotation |
| `rent_vs_feature_points.parquet` | raw (x, rent) per NTA, per predictor | borough-coloured scatter |

`Analytics.scala` uses `percentile_approx` for all quantile computations, a single `groupBy().agg()` pass per metric for the summary stats, `ntile(10)` windowing for the rent-vs decile bins, and Spark's `corr()` aggregate for each of the 36 heatmap cells. OLS slope / intercept are derived analytically from `corr`, `avg`, `stddev_pop` — no Spark ML dependency needed at this scale.

### Stage 7: Streaming (`stream/Producer.scala` + `etl_code/alexj/Consumer.scala`)

Both sides are Scala.

- **Producer.** Reads enriched 311 via Spark (so each record already carries its nta_code), collects to the driver (records are small — ~320k), and publishes to the `complaints311` Kafka topic via `org.apache.kafka.clients.producer.KafkaProducer`. A fresh `created_date` is stitched into each JSON payload so the consumer's 10-minute watermark doesn't drop them. Rate is configurable via `STREAM_RPS`.
- **Consumer.** Spark Structured Streaming reads from Kafka, parses the JSON, watermarks at 10 minutes, and groups into 5-minute tumbling windows keyed by `(nta_code, nta_name, complaint_type)`. Two sinks: an append-mode history at `data/stream/windowed/`, and a `foreachBatch` snapshot at `data/stream/latest.parquet` (atomic-rename so the frontend never sees a half-written directory).

The consumer's `ensureTopic()` uses `org.apache.kafka.clients.admin.AdminClient` (shipped with the `spark-sql-kafka-0-10` package) to create the topic idempotently before the query starts, avoiding the "UnknownTopicOrPartitionException" race an earlier version hit.

### Stage 8: Frontend (`web/`)

No backend, no Python at serve time. `web/lib/parquet.js` wraps `hyparquet` with a `loadParquet(url)` helper. `web/lib/figures.js` builds Plotly figure specs from the analytics parquet tables (a direct port of the old Python figure builders so the visuals stay identical). `web/dashboard.js` orchestrates the dashboard; `web/map.js` joins the score parquet onto the NTA GeoJSON client-side and hands it to Leaflet.

The Makefile's `web` target uses Python's stdlib `http.server` purely as a local dev convenience — any static server works. For production, publish `web/` and `data/` to a GCS website-hosting bucket.

## Directory Layout

The top level follows the assignment rubric: `/data_ingest`, `/etl_code`, and `/profiling_code` each with a per-member subdirectory.

```
bigdataproject/
  data_ingest/
    README.md
    alexj/
      download.py          # HTTP fetch of 4 raw CSVs  -> data/raw/
      geo_download.py      # HTTP fetch of NTA GeoJSON -> data/geo/
      paths.py             # self-contained path module (no cross-package imports)

  etl_code/
    README.md
    alexj/
      Clean.scala          # Spark batch: 4 datasets      -> cleaned parquet
      Geocode.scala        # Spark batch: JTS PIP         -> enriched parquet
      Features.scala       # Spark batch: per-NTA agg     -> neighborhood_features.parquet
      Score.scala          # Spark batch: z-score + sum   -> newcomer_score.parquet
      Analytics.scala      # Spark batch: dashboard aggs  -> data/analytics/*.parquet
      Consumer.scala       # Structured Streaming: Kafka  -> data/stream/

  profiling_code/
    README.md
    alexj/
      FirstCode.scala      # mean/median/mode/stddev + RDD map/reduce per borough
      CountRecs.scala      # schemas + record counts + distinct-value surveys

  stream/
    Producer.scala         # Scala Kafka producer (replays 311 onto topic complaints311)

  web/                     # Static frontend — no backend, no Python
    index.html  map.js     # Leaflet choropleth
    dashboard.html  dashboard.js # Plotly analytics dashboard
    lib/parquet.js         # hyparquet loader wrapper
    lib/figures.js         # Plotly figure builders
    style.css  dashboard.css

  ops/submit_dataproc.sh   # wraps `gcloud dataproc jobs submit spark` per stage
  kafka/docker-compose.yml # single-broker Kafka + Zookeeper for local dev
  Makefile                 # one target per stage; see `make help`

  data/
    raw/                   # 4 raw CSVs (git-ignored)
    cleaned/               # Clean.scala output
    enriched/              # Geocode.scala output (has nta_code)
    geo/                   # NTA GeoJSON, population, ZIP->NTA lookup
    scores/                # neighborhood_features + newcomer_score
    analytics/             # 9 pre-aggregated dashboard parquet tables
    stream/                # windowed history + latest.parquet
```

## How to Run

### Prerequisites

- Python 3.11+ (only for `requests`-based downloaders)
- Java 11
- Apache Spark 3.5 with `spark-shell` on `PATH`
- Docker (only for the Kafka streaming layer)
- GCP SDK (`gcloud`) — only if submitting to Dataproc

`make setup` installs the single Python dependency (`requests`). Spark is a separate install; `pyspark` is intentionally absent because every Spark job in this repo is Scala, invoked via `spark-shell -i`. First run of `make geocode` or `make stream-consume` will ivy-resolve the JTS / spark-sql-kafka packages on demand.

### Local (single machine)

```bash
make setup                     # one-time: pip install requests
make download                  # data_ingest/alexj/download.py     -> data/raw/
make geo-download              # data_ingest/alexj/geo_download.py -> data/geo/
make pipeline                  # Clean -> Geocode -> Features -> Score -> Analytics (all Scala)

# Or run individual stages:
make clean                     # etl_code/alexj/Clean.scala
make geocode                   # etl_code/alexj/Geocode.scala   (with --packages JTS)
make features                  # etl_code/alexj/Features.scala
make score                     # etl_code/alexj/Score.scala
make analytics                 # etl_code/alexj/Analytics.scala

# Streaming (optional, 3 terminals):
make stream-up                 # docker compose up (Kafka)
make stream-consume            # etl_code/alexj/Consumer.scala   (Spark Structured Streaming)
make stream-produce            # stream/Producer.scala

# Profiling (exploratory, not part of the production pipeline):
make profile-first             # profiling_code/alexj/FirstCode.scala
make profile-counts            # profiling_code/alexj/CountRecs.scala

# Serving:
make web                       # static server on :5173
# -> http://localhost:5173/web/          → Leaflet map
# -> http://localhost:5173/web/dashboard.html → 5-chart Plotly analytics
```

### Dataproc

See the README for the full walkthrough. Summary:

```bash
export DATAPROC_CLUSTER=nyc-insights
export DATAPROC_REGION=us-central1
export GCS_DATA_ROOT=gs://$BUCKET/data

gsutil -m rsync -r data/raw $GCS_DATA_ROOT/raw
gsutil -m rsync -r data/geo $GCS_DATA_ROOT/geo

ops/submit_dataproc.sh pipeline          # all 5 Scala stages against the cluster

gsutil -m rsync -r $GCS_DATA_ROOT ./data
make web                                 # view locally
# or host web/ + data/ on a GCS static site bucket
```

## Future Work

- **Alternative Scoring Models:** `Score.scala` is a transparent weighted z-score. A learned ranking model or multi-profile scores (student, family, retiree) plugs in as a replacement for the `WEIGHTS` map.
- **Apache Sedona for the spatial join:** JTS in a broadcast UDF is more than enough for 260 polygons; Sedona is a drop-in upgrade if polygon count or point volume grows by an order of magnitude.
- **ZCTA-based ZIP→NTA:** Replace the restaurant-derived ZIP→NTA lookup with a ZCTA shapefile + area-weighted intersection.
- **Additional signals:** Subway access (MTA GTFS), park coverage (NYC Parks GeoJSON), school quality (DOE reports). Each plugs in as: new downloader → new cleaner block in `Clean.scala` → new aggregate in `Features.scala` → optional new weight in `Score.scala`.
- **Cluster scale testing:** full NYPD historical dataset (tens of millions of rows) against the Dataproc shuffle / AQE / broadcast choices documented in `SCALABILITY.md`.
