# NYC Neighborhood Insights: A Big Data Pipeline for Newcomers

## Project Overview

This project develops an interactive, data-driven system designed to help newcomers better understand and evaluate neighborhoods in New York City. The system integrates multiple large-scale public datasets — including crime records and restaurant inspection results — to provide a comprehensive view of each neighborhood. The goal is to transform fragmented and complex urban data into clear, accessible insights that allow users to compare areas such as Tribeca, the Lower East Side, or Astoria across multiple dimensions like safety, food quality, and quality of life.

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
                                       ┌───────────────────┐
       NYC Open Data + Zillow          │  NTA GeoJSON      │
       data_ingest/alexj/download.py   │  (NYC Open Data)  │
               │                       └─────────┬─────────┘
               ▼                                 │
         data/raw/                               │
               │                                 │
               │  etl_code/alexj/Clean.scala     │
               │  (Scala + Spark, canonical)
               ▼                                 │
         data/cleaned/                           │
               │                                 │
               │  etl_code/alexj/geocode.py ◄────┘
               │  (geopandas sjoin)
               ▼
         data/enriched/   (every row has nta_code)
               │
               │  etl_code/alexj/Features.scala
               │  (Scala + Spark, canonical)
               ▼
         data/scores/neighborhood_features.parquet
               │
               │  etl_code/alexj/score.py
               │  (pandas z-score + weighted sum)
               ▼                                  ┌─────────────────────┐
         data/scores/newcomer_score.parquet       │ stream/producer.py  │
               │                                  │ (Kafka producer)    │
               │                                  └──────────┬──────────┘
               │                                             ▼
               │                                  ┌─────────────────────┐
               │                                  │ Kafka (Docker)      │
               │                                  │ topic: complaints311│
               │                                  └──────────┬──────────┘
               │                                             ▼
               │                                  ┌────────────────────────────┐
               │                                  │ etl_code/alexj/Consumer.scala
               │                                  │ (Scala + Spark Structured  │
               │                                  │  Streaming)                │
               │                                  └──────────┬─────────────────┘
               │                                             ▼
               │                                  data/stream/latest.parquet
               ▼                                             │
         ┌─────────────────────────────────────────┐◄────────┘
         │ api/main.py (FastAPI, Python, port 8765)│
         └─────────────────────┬───────────────────┘
                               ▼
                     web/ (Leaflet, static, :5173)
```

### Technology Stack

| Component | Technology | Language | Purpose |
|-----------|-----------|----------|---------|
| Batch cleaning | Apache Spark 3.5 | **Scala 2.12** | `etl_code/alexj/Clean.scala` — all four raw CSVs → cleaned Parquet |
| Feature aggregation | Apache Spark 3.5 | **Scala 2.12** | `etl_code/alexj/Features.scala` — per-NTA aggregates + join |
| Streaming consumer | Spark Structured Streaming | **Scala 2.12** | `etl_code/alexj/Consumer.scala` — Kafka → 5-min windows → Parquet |
| Geographic join | GeoPandas | Python | `etl_code/alexj/geocode.py` — point-in-polygon vs. NTA polygons |
| Newcomer score | Pandas | Python | `etl_code/alexj/score.py` — z-score + weighted sum on 260 NTAs |
| Streaming producer | kafka-python | Python | `stream/producer.py` — replays 311 records to Kafka |
| Message broker | Apache Kafka | — | `kafka/docker-compose.yml` — topic `complaints311` |
| API | FastAPI + uvicorn | Python | `api/main.py` on port 8765 |
| Frontend map | Leaflet + plain JS | JavaScript | `web/index.html` — choropleth, click-through detail |
| Frontend dashboard | Plotly.js | JavaScript | `web/dashboard.html` — 4 analytical charts |
| Analytical figures | Plotly (Python) | Python | `pipeline/analytics.py` — shared by API **and** notebook |
| Narrative notebook | Jupyter | Python | `notebooks/analysis.ipynb` — same 4 charts + deeper analysis |
| Storage | Parquet | — | every stage reads and writes Parquet |
| Cluster target | Google Cloud Dataproc | — | Scala scripts auto-swap to `hdfs:///user/$HDFS_USER/...` paths when that env var is set |

## Pipeline Stages

### Stage 1: Data Ingestion (`data_ingest/alexj/download.py`, `data_ingest/alexj/geo_download.py`)

Four raw CSVs and one GeoJSON are fetched over HTTP and written to `data/raw/` and `data/geo/`. 311 is fetched through the Socrata API with a `$where` filter so only the food-safety subset (Food Poisoning, Food Establishment, Rodent, Unsanitary Animal Pvt Property) is pulled server-side. Not a Spark stage — just Python `requests`.

### Stage 2: Batch Cleaning (`etl_code/alexj/Clean.scala`, Scala + Spark)

`Clean.scala` is the canonical cleaner for all four datasets. Run with `make clean` (which shells out to `spark-shell -i etl_code/alexj/Clean.scala`) or, on Dataproc, `HDFS_USER=<you> spark-shell --master yarn --deploy-mode client -i etl_code/alexj/Clean.scala`. The script reads from `$DATA_ROOT/raw` locally or `hdfs:///user/$HDFS_USER/data/` on a cluster — same code, same output.

Per-dataset transformations:

| Dataset | Columns kept | Null-drop keys | Normalization | Derived column |
|---|---|---|---|---|
| Crime | `CMPLNT_NUM`, `CMPLNT_FR_DT`, `OFNS_DESC`, `LAW_CAT_CD`, `BORO_NM`, `Latitude`, `Longitude` | all keys + coords | `UPPER(TRIM(...))` on `BORO_NM`, `OFNS_DESC`, `LAW_CAT_CD` | `IS_FELONY = LAW_CAT_CD == "FELONY"` |
| Restaurants | `CAMIS`, `DBA`, `BORO`, `ZIPCODE`, `CUISINE DESCRIPTION`→`CUISINE`, `INSPECTION DATE`→`INSPECTION_DATE`, `SCORE`, `GRADE`, `Latitude`, `Longitude` | `BORO`, `SCORE`, coords | `UPPER(TRIM(...))` on `BORO`, `DBA`, `CUISINE`, `GRADE` | `IS_CRITICAL = SCORE >= 28` |
| 311 food | `unique_key`→`COMPLAINT_ID`, `created_date`→`CREATED_DATE`, `complaint_type`→`COMPLAINT_TYPE`, `descriptor`→`DESCRIPTOR`, `incident_zip`→`ZIPCODE`, `borough`→`BORO`, `latitude`→`Latitude`, `longitude`→`Longitude` | `COMPLAINT_TYPE`, coords | `UPPER(TRIM(...))` on `BORO`, `COMPLAINT_TYPE` | — |
| Zillow rent | `RegionName`→`ZIPCODE`, `City`, `State`, `Metro`, `month`, `ZORI` | `ZORI` (after melt) | wide-to-long via `stack(n, ...)`; `row_number()` window keeps the latest non-null month per ZIP | — |

**Output:** `data/cleaned/{crime, restaurants, complaints311, rent}/*.parquet`. Schema parity with the earlier PySpark prototype was verified before that implementation was removed; see `CHANGELOG.md`.

`profiling_code/alexj/FirstCode.scala` and `profiling_code/alexj/CountRecs.scala` are earlier Scala *profiling* scripts — mean/median/mode on the restaurant `SCORE` column and on crime `Latitude`/`Longitude`, RDD `map`+`reduceByKey` per borough, and distinct-value surveys for boroughs/offenses/grades/cuisines. They're kept in the `/profiling_code` tree because they answer the "what's in this data?" questions that *precede* writing ETL, and they're deliberately outside the production pipeline so re-running `make pipeline` doesn't re-run them.

### Stage 3: Geographic Normalization (`etl_code/alexj/geocode.py`)

`geopandas.sjoin` attaches `nta_code` + `nta_name` to every row of the three point datasets (crime, restaurants, 311) by point-in-polygon against the 2020 NTA GeoJSON. Rent doesn't have coordinates, so a ZIP→NTA lookup is derived by picking, for each ZIP, the NTA containing the most restaurants (written to `data/geo/zip_to_nta.csv`).

Kept in Python deliberately: spatial joins are where `geopandas`/`shapely` are at their best, and adding Sedona to the stack for one operation would be disproportionate. The enriched Parquet written here is the handoff back to Spark, so the Python detour is invisible to downstream stages.

### Stage 4: Feature Aggregation (`etl_code/alexj/Features.scala`, Scala + Spark)

`Features.scala` reads the four enriched Parquet directories, groups each by `(nta_code, nta_name)`, and produces one row per NTA with:

`total_crimes`, `felonies`, `felony_share`, `n_inspections`, `avg_score`, `critical_rate`, `n_complaints`, `median_rent_zori`.

All four feature tables are outer-joined; population is joined in (from `data/geo/nta_population.csv`) to derive `crimes_per_1k` and `complaints_per_1k` when available. Structural nulls are filled with zeros; rent nulls are kept as null.

**Output:** `data/scores/neighborhood_features.parquet` (256 NTAs).

### Stage 5: Neighborhood Scoring (`etl_code/alexj/score.py`)

The cleaned and geocoded data from all four sources is aggregated per NYC Neighborhood Tabulation Area (NTA) in `Features.scala`, producing one row per neighborhood with six numeric features. `etl_code/alexj/score.py` then collapses those features into a single **Newcomer Score** on a 0–100 scale.

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

1. **Z-score each feature** across the ~260 NTAs:

   \[ z_i = \frac{x_i - \mu}{\sigma} \]

   Missing values are imputed with the citywide mean before z-scoring, so a neighborhood missing a signal stays neutral (z ≈ 0) on that axis instead of being penalized.

2. **Sign-flip "bad" features.** All six inputs are "higher is worse", so each z-score is multiplied by `-1`. After this step, a positive z consistently means "good for a newcomer."

3. **Collapse into four sub-scores** by averaging related z-scores:

   - `safety_score` = mean(`z_crimes_per_1k`, `z_felony_share`)
   - `food_safety_score` = mean(`z_avg_score`, `z_critical_rate`)
   - `cleanliness_score` = `z_complaints_per_1k`
   - `affordability_score` = `z_median_rent_zori`

4. **Weighted sum** using the weights defined at the top of `etl_code/alexj/score.py`:

   \[ \text{score} = 0.30 \cdot \text{safety} + 0.25 \cdot \text{food\_safety} + 0.15 \cdot \text{cleanliness} + 0.30 \cdot \text{affordability} \]

   Weights sum to 1.0 and are tunable by editing the `WEIGHTS` dict.

5. **Rescale to 0–100** via min-max for display:

   \[ \text{newcomer\_score\_100} = 100 \cdot \frac{s - s_{\min}}{s_{\max} - s_{\min}} \]

   The worst-ranking NTA gets 0, the best gets 100. This is purely cosmetic — it preserves the ranking of the raw weighted score.

#### Output

`data/scores/newcomer_score.parquet` — one row per NTA containing:

- The six raw features (unchanged)
- Their signed z-scores (`z_*`)
- The four sub-scores (`safety_score`, `food_safety_score`, `cleanliness_score`, `affordability_score`)
- `newcomer_score` (raw weighted sum) and `newcomer_score_100` (0–100 display value)

#### Notes

- The score is **relative, not absolute**. A 0/100 doesn't mean "unlivable" — it means worst-scoring of the 260 NTAs given these six signals and these weights. Tribeca and SoHo typically land near 0 because their rent is far above the mean and affordability has a 0.30 weight.
- The methodology is deliberately transparent and retunable: no learned model, no hidden coefficients. Re-weight by editing `WEIGHTS` and rerunning `make score`.

### Stage 6: Analytics Dashboard (`pipeline/analytics.py`, served by API + notebook)

The raw map answers *"where is this metric high or low?"*. The dashboard answers the analytical questions behind it: *how is the metric distributed, which neighborhoods are outliers, does the pattern hold across boroughs, and which inputs correlate?*

`pipeline/analytics.py` is a single module that builds four Plotly figures from `newcomer_score.parquet`:

1. **Distribution** — histogram of the selected metric across all 256 NTAs, annotated with mean and median.
2. **Top vs. bottom 10** — horizontal bar charts of the best- and worst-ranked NTAs; "best" flips direction based on whether the metric is higher-is-better (score) or lower-is-better (crime, rent, 311, critical rate).
3. **By borough** — box plot per borough with every NTA as a jittered point, ordered by median.
4. **Input correlation** — 6×6 Pearson heatmap of the inputs that feed the Newcomer Score.

Both consumers of the module render the exact same figures:

- `api/main.py` exposes `/analytics/distribution`, `/analytics/top-bottom`, `/analytics/by-borough`, `/analytics/correlation`. Each endpoint returns the Plotly figure JSON (with binary array blocks decoded back to plain arrays for maximum compatibility). The web dashboard at `web/dashboard.html` + `dashboard.js` renders them via Plotly.js 2.35.
- `notebooks/analysis.ipynb` imports `pipeline.analytics` directly and renders the same figures with `fig.show()`, then layers in notebook-only analysis (sub-score decomposition waterfall, safety-vs-affordability scatter, outlier personas).

Because both paths run the same Python function, the four shared charts are guaranteed to match between the dashboard and the notebook — same aggregation, same layout, same colors.

## Directory Layout

The top level follows the assignment rubric: `/data_ingest`, `/etl_code`, and `/profiling_code` each with a per-member subdirectory. `pipeline/` holds shared infrastructure (filesystem paths, analytics figure builders) that doesn't belong in any one stage. Everything else is serving-layer glue.

```
bigdataproject/
  data_ingest/
    README.md              # what lives here + run recipes
    alexj/
      download.py          # HTTP fetch of 4 raw CSVs -> data/raw/
      geo_download.py      # HTTP fetch of NTA GeoJSON + population template

  etl_code/
    README.md              # layer map: raw -> cleaned -> enriched -> features -> score
    alexj/
      Clean.scala          # Scala + Spark: canonical batch cleaner (all 4 datasets)
      Features.scala       # Scala + Spark: per-NTA aggregation + join
      Consumer.scala       # Scala + Structured Streaming: Kafka -> 5-min windows
      geocode.py           # geopandas point-in-polygon; also ZIP->NTA lookup
      score.py             # pandas z-score + weighted sum -> 0-100 newcomer score

  profiling_code/
    README.md              # why profiling lives outside the production pipeline
    alexj/
      FirstCode.scala      # mean/median/mode/stddev + RDD map/reduce per borough
      CountRecs.scala      # schemas + record counts + distinct-value surveys

  pipeline/                # shared infrastructure & serving layer
    paths.py               # canonical filesystem paths (imported everywhere)
    analytics.py           # Plotly figure builders shared by API + notebook

  stream/
    producer.py            # Kafka producer (replays 311 onto topic complaints311)

  notebooks/
    analysis.ipynb         # renders pipeline.analytics figures + deeper narrative

  api/main.py              # FastAPI on :8765 (map + /analytics/*)
  web/                     # Leaflet map + Plotly dashboard (:5173)
  kafka/docker-compose.yml # Zookeeper + Kafka for the streaming layer
  Makefile                 # one target per stage; see `make help`

  data/
    raw/                   # 4 raw CSVs (ignored by git)
    cleaned/               # Parquet, produced by Clean.scala
    enriched/              # Parquet with nta_code, produced by geocode.py
    geo/                   # NTA GeoJSON, population, ZIP->NTA lookup
    scores/                # neighborhood_features + newcomer_score
    stream/                # windowed history + latest.parquet (from Consumer.scala)
```

## How to Run

### Prerequisites
- Python 3.11+, Java 11, Apache Spark 3.5 with `spark-shell` on `PATH`, Docker (for Kafka).
- `make setup` installs Python deps (pandas, geopandas, fastapi, kafka-python, plotly, jupyter). Spark itself is a separate install — `pyspark` is intentionally not in `requirements.txt` because every Spark job in this repo is Scala, invoked via `spark-shell -i`.

### Local (single machine)

```bash
make setup                     # one-time pip install
make download                  # data_ingest/alexj/download.py     -> data/raw/
make geo-download              # data_ingest/alexj/geo_download.py -> data/geo/
make pipeline                  # Clean.scala -> geocode.py -> Features.scala -> score.py
# Or run individual stages:
make clean                     # etl_code/alexj/Clean.scala        (Scala)
make geocode                   # etl_code/alexj/geocode.py         (Python, geopandas)
make features                  # etl_code/alexj/Features.scala     (Scala)
make score                     # etl_code/alexj/score.py           (Python, pandas)

# Streaming (optional):
make stream-up                 # docker compose up (Kafka)
make stream-produce &          # stream/producer.py replays 311 onto the topic
make stream-consume            # etl_code/alexj/Consumer.scala     (Structured Streaming)

# Profiling (exploratory, not part of the production pipeline):
make profile-first             # profiling_code/alexj/FirstCode.scala
make profile-counts            # profiling_code/alexj/CountRecs.scala

# Serving:
make api                       # FastAPI on :8765
make web                       # Leaflet static server on :5173

# Analytics:
# - http://localhost:5173/              → Leaflet map
# - http://localhost:5173/dashboard.html → 4 shared Plotly charts (live)
# - notebooks/analysis.ipynb             → same 4 charts + deeper analysis
jupyter notebook notebooks/analysis.ipynb
```

### Dataproc (NYU cluster)

The Scala scripts are location-transparent: set `HDFS_USER` and they read from `hdfs:///user/$HDFS_USER/data` and write to `hdfs:///user/$HDFS_USER/cleaned`. No code change needed.

```bash
# 1. SSH into Dataproc master, upload raw CSVs to HDFS
hdfs dfs -mkdir -p /user/$USER/data
hdfs dfs -put data/raw/*.csv /user/$USER/data/

# 2. Run the Scala jobs against YARN
HDFS_USER=$USER spark-shell --master yarn --deploy-mode client -i etl_code/alexj/Clean.scala
HDFS_USER=$USER spark-shell --master yarn --deploy-mode client -i etl_code/alexj/Features.scala

# 3. (Optional) Register cleaned Parquet as external Hive tables for SQL access
```

## Future Work

The core pipeline (4 datasets, Scala+Spark batch, Kafka streaming, FastAPI, Leaflet) is in place. Natural next steps:

- **Alternative Scoring Models:** The Newcomer Score (Stage 5) is a transparent weighted z-score. A learned model (e.g. ranking based on user-stated preferences) or multi-profile scores (student, family, retiree) are a natural extension — weights already live in a single dict in `etl_code/alexj/score.py`.
- **Sedona for spatial joins:** Move `etl_code/alexj/geocode.py` to Apache Sedona so the spatial join runs in Spark alongside the rest of the batch — removes the Python hop in the middle of the batch pipeline at the cost of a heavier dependency.
- **ZCTA-based ZIP→NTA:** Replace the restaurant-derived ZIP→NTA lookup with a ZCTA shapefile + area-weighted intersection. More accurate, especially for ZIPs with few restaurants.
- **Additional signals:** Subway access (MTA GTFS), park coverage (NYC Parks GeoJSON), school quality (DOE reports). Each plugs in as: new downloader → new cleaner block in `Clean.scala` → new aggregate in `Features.scala` → optional new weight in `score.py`.
- **Cluster deploy at scale:** The Scala scripts already target Dataproc via `HDFS_USER`. A genuine stress-test with the full NYPD historical dataset (tens of millions of rows) would exercise the shuffle, AQE, and broadcast-join choices documented in `SCALABILITY.md`.
