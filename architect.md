# Architecture

## Purpose

**NYC Neighborhood Insights** turns fragmented public data into a single comparable view of what it's like to live in each New York City neighborhood. The target user is a newcomer trying to decide between Tribeca, the Lower East Side, Astoria, etc. — someone who wants to see safety, food quality, quality-of-life, and affordability side by side rather than hunting through four separate civic portals.

The project is also a demonstration of a modern end-to-end big-data architecture: ingestion → Scala/Spark batch processing → geographic normalization → feature engineering → pre-aggregated analytics → Kafka streaming → a static, backend-free frontend that reads parquet directly. Every computational stage runs on GCP Dataproc; Python is reserved for HTTP data downloads. It is deliberately still runnable on a single machine (no cloud required) for development and demo.

### Design goals

1. **One language for big-data work.** Every stage that processes data is Scala + Apache Spark. Python survives only in `data_ingest/` for HTTP glue; the frontend is static JS. Nothing processes data in pandas / geopandas / pyarrow anywhere in the repo.
2. **Dataproc is the canonical runtime.** All Scala jobs run unmodified on a Dataproc cluster via `gcloud dataproc jobs submit spark`. `ops/submit_dataproc.sh` is the one-liner that does this per stage.
3. **Comparable neighborhoods.** Every signal is aggregated to the same geographic unit (NYC 2020 NTAs, ~260 polygons) via `Geocode.scala`'s broadcast JTS point-in-polygon join.
4. **Batch + streaming in one story.** A classic batch pipeline produces the stable per-neighborhood picture; a Kafka → Spark Structured Streaming path layers near-real-time signals on top.
5. **No backend.** The map and dashboard are a static bundle. `hyparquet` reads the Spark-written parquet directly over HTTP; Plotly.js + Leaflet render it.
6. **Composable, not monolithic.** Each pipeline stage is a standalone Scala script that reads Parquet and writes Parquet. Any stage can be re-run in isolation; downstream stages don't care how upstream produced their input.

## High-level architecture

```
┌───────────────────────────┐      ┌─────────────────────────────┐
│  NYC Open Data + Zillow   │      │  NYC Open Data (GeoJSON)    │
│  CSV downloads (requests) │      │  2020 NTA polygons          │
└──────────────┬────────────┘      └─────────────┬───────────────┘
               │ data_ingest/alexj/download.py   │ data_ingest/alexj/geo_download.py
               ▼                                 │
       ┌──────────────┐                          │
       │  data/raw/   │                          │
       └──────┬───────┘                          │
              │ etl_code/alexj/Clean.scala       │
              ▼                                  │
       ┌────────────────┐                        │
       │  data/cleaned/ │                        │
       └──────┬─────────┘                        │
              │ etl_code/alexj/Geocode.scala (JTS broadcast PIP) ◄──┤
              ▼                                                     │
       ┌───────────────────┐                                        │
       │  data/enriched/   │  (every row has nta_code)              │
       └──────┬────────────┘                                        │
              │ etl_code/alexj/Features.scala
              ▼
       ┌──────────────────────────────────────┐
       │  data/scores/neighborhood_features   │
       └──────┬───────────────────────────────┘
              │ etl_code/alexj/Score.scala
              ▼
       ┌──────────────────────────────────────┐
       │  data/scores/newcomer_score.parquet  │ ◄── single file
       └──────┬───────────────────────────────┘
              │ etl_code/alexj/Analytics.scala
              ▼
       ┌──────────────────────────────────────┐
       │  data/analytics/*.parquet  (9 tables)│
       └──────┬───────────────────────────────┘
              │
              │                                ┌────────────────────┐
              │                                │ stream/Producer.scala│
              │                                │ (Scala, replays 311)│
              │                                └─────────┬──────────┘
              │                                          │
              │                                          ▼
              │                                ┌────────────────────┐
              │                                │ Kafka (Docker/GMK) │
              │                                │ topic: complaints311│
              │                                └─────────┬──────────┘
              │                                          │
              │                                          ▼
              │                     ┌─────────────────────────────────┐
              │                     │ etl_code/alexj/Consumer.scala   │
              │                     │ Spark Structured Streaming      │
              │                     │ 5-min tumbling windows per NTA  │
              │                     └────────┬────────────────────────┘
              │                              │
              │                              ▼
              │                     ┌──────────────────────────────┐
              │                     │ data/stream/windowed/  (hist)│
              │                     │ data/stream/latest.parquet   │
              │                     └────────┬─────────────────────┘
              │                              │
              ▼                              ▼
   ┌──────────────────────────────────────────────────────────┐
   │          Static frontend  (web/ — no backend)            │
   │    hyparquet → parquet       Plotly.js → figures         │
   │    Leaflet → choropleth                                  │
   └──────────────────────────────────────────────────────────┘
```

### Repository layout

Top-level tree follows the assignment rubric: three category directories (`/data_ingest`, `/etl_code`, `/profiling_code`), each with a per-member subdirectory. Serving is a static bundle under `web/`; Dataproc submission helpers live under `ops/`.

| Category | Directory | Contents |
|---|---|---|
| Data ingestion | `data_ingest/alexj/` | `download.py`, `geo_download.py`, `paths.py` |
| ETL / cleaning | `etl_code/alexj/` | `Clean.scala`, `Geocode.scala`, `Features.scala`, `Score.scala`, `Analytics.scala`, `Consumer.scala` |
| Profiling | `profiling_code/alexj/` | `FirstCode.scala`, `CountRecs.scala` |
| Streaming producer | `stream/` | `Producer.scala` |
| Frontend (static) | `web/` | `index.html`, `map.js`, `dashboard.html`, `dashboard.js`, `lib/parquet.js`, `lib/figures.js` |
| Ops | `ops/` | `submit_dataproc.sh` |
| Orchestration | root | `Makefile`, `kafka/docker-compose.yml` |

### Language split at a glance

Everything that processes data is **Scala on Spark**. Python is limited to HTTP downloads. JavaScript runs the display layer in the browser.

| Layer | Scala (Spark) | Python | JavaScript |
|---|---|---|---|
| Ingestion (HTTP) | — | `data_ingest/alexj/download.py`, `geo_download.py` | — |
| Batch cleaning | **`Clean.scala`** | — | — |
| Geographic join | **`Geocode.scala`** (JTS broadcast PIP) | — | — |
| Feature aggregation | **`Features.scala`** | — | — |
| Newcomer score | **`Score.scala`** | — | — |
| Analytics aggregates | **`Analytics.scala`** | — | — |
| Streaming consumer | **`Consumer.scala`** | — | — |
| Streaming producer | **`Producer.scala`** | — | — |
| Profiling / EDA | **`FirstCode.scala`**, **`CountRecs.scala`** | — | — |
| Frontend | — | — | `web/` (Leaflet, Plotly.js, hyparquet) |

The boundary is sharp: **if the file has a `.scala` extension, it's the canonical implementation of that stage.** Python only appears where data is being *pulled into* the repo from the internet.

## Components

### 1. Ingestion

`data_ingest/alexj/download.py` fetches four public CSVs:

| Dataset | Source | ID | Access pattern |
|---|---|---|---|
| NYPD Complaints (YTD) | NYC Open Data | `5uac-w243` | Bulk export CSV |
| Restaurant Inspections | NYC Open Data | `43nn-pn8j` | Bulk export CSV |
| 311 Service Requests | NYC Open Data | `erm2-nwe9` | Socrata API with SoQL filter (food subset only) |
| Zillow ZORI (ZIP level) | Zillow Research | `Zip_zori_uc_sfrcondomfr_sm_month.csv` | Static CSV |

Only a food-safety subset of 311 is pulled (`Food Poisoning`, `Food Establishment`, `Rodent`, `Unsanitary Animal Pvt Property`) to keep the raw file manageable; the Socrata `$where` clause does this server-side. `geo_download.py` separately fetches the NTA GeoJSON and templates a population CSV.

Both scripts import from the self-contained `data_ingest/alexj/paths.py` so the ingestion package has zero dependencies outside the standard library and `requests`.

### 2. Batch cleaning (`Clean.scala`, Scala + Spark)

Canonical cleaner. Run with `make clean` locally or `ops/submit_dataproc.sh clean` on Dataproc. Reads from `$DATA_ROOT/raw` locally or `hdfs:///user/$HDFS_USER/data` on a cluster — same code, same output.

Transformations:

- `UPPER(TRIM(...))` on text keys (`BORO_NM`, `OFNS_DESC`, `CUISINE`, `BORO`, `GRADE`, `COMPLAINT_TYPE`).
- Drop rows with null join keys or null coordinates.
- `IS_FELONY = (LAW_CAT_CD == "FELONY")` and `IS_CRITICAL = (SCORE >= 28)` as binary indicators.
- Zillow's wide "one column per month" layout is melted with `stack(n, ...)`, latest non-null month kept per ZIP via a `row_number()` window.

Profiling scripts (`FirstCode.scala`, `CountRecs.scala`) live under `/profiling_code` deliberately — they answer the "what's in this data?" questions that precede writing ETL and aren't on the critical path.

### 3. Geographic normalization (`Geocode.scala`, Scala + Spark + JTS)

`Geocode.scala` attaches `nta_code` + `nta_name` to every row of the three point datasets (crime, restaurants, 311) by point-in-polygon against the 2020 NTA GeoJSON.

Implementation:

1. Read the NTA GeoJSON from Hadoop FS (`local:`, `hdfs://`, or `gs://`) once on the driver with Jackson.
2. Build `org.locationtech.jts.geom.Geometry` values for each of ~260 polygons (Polygon and MultiPolygon both handled).
3. Broadcast the `Array[NtaPolygon]` to executors — JTS geometries are `Serializable`, so `sparkContext.broadcast(...)` works directly.
4. Apply a Spark UDF that does an envelope pre-filter followed by `Geometry.contains` per point. The UDF returns a `case class NtaHit(nta_code, nta_name)` so the two columns land as a typed struct on the DataFrame.
5. Derive a ZIP→NTA lookup from the (now enriched) restaurants dataset — for each ZIP, the NTA containing the plurality of its restaurants wins — and broadcast-join it onto rent to produce `data/enriched/rent/`.

Why this is big-data work now: the previous pandas/geopandas implementation was single-machine. Moving the spatial join into Spark with JTS keeps the entire ETL surface as distributed code, which is what the professor's rubric requires.

Extras taken care of inline: `spark.sql.legacy.parquet.nanosAsLong=true` is set so downstream readers handle any INT64 nanosecond-timestamp columns gracefully.

### 4. Feature engineering (`Features.scala`, Scala + Spark)

`Features.scala` groups each enriched table by `nta_code` and produces one row per NTA:

- `total_crimes`, `felonies`, `felony_share`
- `n_inspections`, `avg_score`, `critical_rate`
- `n_complaints`
- `median_rent_zori`
- `crimes_per_1k`, `complaints_per_1k` (if `nta_population.csv` is populated; otherwise raw totals are used as the intensity signal)

All four feature tables are outer-joined on `(nta_code, nta_name)`. Structural nulls (e.g. an NTA with no restaurants) are filled with zeros so the scoring stage never has to branch on missing-vs-zero; rent nulls are intentionally left null because that signal is real ("no observations here"). Result: `data/scores/neighborhood_features.parquet`.

### 5. Newcomer score (`Score.scala`, Scala + Spark)

Even though the table is ~260 rows, the scoring step is Scala on Spark so the entire ETL surface remains distributed code. The arithmetic matches the earlier Python prototype bit-for-bit.

1. For each feature, compute a z-score across NTAs using `avg` + `stddev_pop` on a single `groupBy().agg(...)` pass. Nulls are imputed with the column mean before z-scoring so a missing signal stays neutral (z ≈ 0) instead of penalising the NTA.
2. Flip the sign on "bad" features (all six features are "higher is worse" here).
3. Collapse individual features into four sub-scores: `safety`, `food_safety`, `cleanliness`, `affordability`.
4. Weighted sum (defaults: 0.30 / 0.25 / 0.15 / 0.30 — weights sum to 1.0, defined at the top of the file for easy retuning).
5. Re-scale linearly to 0–100 for display.

Output is written as a **single-file parquet** (`.coalesce(1)` → temp dir → rename the lone part file up to the final name). Predictable URL means the frontend can fetch `data/scores/newcomer_score.parquet` without needing a directory listing or manifest.

### 6. Analytics aggregates (`Analytics.scala`, Scala + Spark)

The dashboard's five figures need nine tables of pre-aggregated data. `Analytics.scala` computes them all in a single Spark job and writes each as a tiny single-file parquet under `data/analytics/`:

| File | Shape | Consumer figure |
|---|---|---|
| `summary.parquet` | 1 row per metric: mean, median, min, max, std, n_ntas | summary strip + histogram vlines |
| `distribution.parquet` | 24 equal-width bins per metric | histogram |
| `top_bottom.parquet` | top/bottom 10 per metric | horizontal bar chart |
| `borough_box.parquet` | q1, median, q3, lowerfence, upperfence per (metric, borough) | box plot (Plotly consumes pre-computed 5-number summaries directly) |
| `borough_points.parquet` | raw (metric, borough, nta_name, value) | jittered scatter overlay |
| `correlation.parquet` | long-form (feature_row, feature_col, r) | 6×6 heatmap |
| `rent_vs_feature_bins.parquet` | decile bins × 5 predictors: median + IQR of rent | rent-vs trend line + band |
| `rent_vs_feature_ols.parquet` | per predictor: slope, intercept, Pearson r, R², example prediction | OLS dashed line + annotation |
| `rent_vs_feature_points.parquet` | raw (x, rent) per NTA per predictor | borough-coloured scatter |

Implementation uses `percentile_approx` for all quantiles, `ntile(10)` windowing for rent bins, and Spark's `corr` aggregate for each of the 36 heatmap cells. OLS slope / intercept are derived analytically from `corr`, `avg`, `stddev_pop` — no Spark ML dependency required at this scale.

### 7. Streaming

The streaming layer is a **simulation**, not a live feed — NYC Open Data doesn't expose 311 as a push stream. The value here is demonstrating the architecture.

- **`stream/Producer.scala`** — reads enriched 311 parquet via Spark (so each record carries its `nta_code`), collects to the driver, and publishes JSON payloads via `org.apache.kafka.clients.producer.KafkaProducer` to topic `complaints311`. A fresh `created_date` is stitched in so the consumer's watermark doesn't drop records. Rate is controlled via `STREAM_RPS`.
- **`etl_code/alexj/Consumer.scala`** — Spark Structured Streaming. Kafka source, 10-minute watermark, 5-minute tumbling window keyed by `(nta_code, complaint_type)`. Two sinks: append-mode history at `data/stream/windowed/`, and a `foreachBatch` atomic-rename snapshot at `data/stream/latest.parquet`.

`AdminClient` (from the Kafka client library shipped with `spark-sql-kafka-0-10`) is used to pre-create the topic idempotently at consumer startup. Without it, a consumer started before the producer hits `UnknownTopicOrPartitionException` three times and dies — the broker has auto-create enabled, but auto-creation only fires on first *produce*.

### 8. Frontend

No backend. The dashboard and map are a pure static bundle.

- **`web/lib/parquet.js`** wraps [hyparquet](https://github.com/hyparam/hyparquet) with a `loadParquet(url)` helper that returns `{rows, columns}` with BigInts coerced to numbers (Plotly can't handle BigInt).
- **`web/lib/figures.js`** builds Plotly figure specs from the aggregates. It owns the palette, borough colours, layout defaults, and the five figure builders (`distributionFigure`, `topBottomFigure`, `byBoroughFigure`, `correlationFigure`, `rentVsFeatureFigure`). Each takes plain row arrays from parquet tables and returns `{data, layout}`.
- **`web/map.js`** fetches the NTA GeoJSON and `data/scores/newcomer_score.parquet`, joins them client-side on `nta_code`, and hands the merged FeatureCollection to Leaflet. Metric switching is pure in-place restyling — no network calls.
- **`web/dashboard.js`** loads all nine analytics parquet tables in parallel at startup, then calls the figure builders. Metric / predictor pill clicks re-run the corresponding builder with the cached data.

For local dev, `make web` serves `$(PWD)` on `:5173` via Python's stdlib `http.server` (zero-dep). For production, publish `web/` and `data/` to a GCS website-hosting bucket.

### 9. Orchestration

`Makefile` wraps every step. Each Scala target invokes `spark-shell $(SPARK_OPTS) -i <script>.scala`, with `--packages` as needed (JTS for `geocode`, spark-sql-kafka for `stream-*`).

| Target | Runs | Language |
|---|---|---|
| `make setup` | `pip install requests` | — |
| `make download` | `data_ingest/alexj/download.py` | Python |
| `make geo-download` | `data_ingest/alexj/geo_download.py` | Python |
| `make clean` | `spark-shell -i etl_code/alexj/Clean.scala` | **Scala** |
| `make geocode` | `spark-shell --packages JTS -i etl_code/alexj/Geocode.scala` | **Scala** |
| `make features` | `spark-shell -i etl_code/alexj/Features.scala` | **Scala** |
| `make score` | `spark-shell -i etl_code/alexj/Score.scala` | **Scala** |
| `make analytics` | `spark-shell -i etl_code/alexj/Analytics.scala` | **Scala** |
| `make pipeline` | clean → geocode → features → score → analytics | **Scala** |
| `make stream-up` / `stream-down` | `docker compose` on Kafka | — |
| `make stream-produce` | `spark-shell --packages spark-sql-kafka -i stream/Producer.scala` | **Scala** |
| `make stream-consume` | `spark-shell --packages spark-sql-kafka -i etl_code/alexj/Consumer.scala` | **Scala** |
| `make profile-first` | `spark-shell -i profiling_code/alexj/FirstCode.scala` | **Scala** |
| `make profile-counts` | `spark-shell -i profiling_code/alexj/CountRecs.scala` | **Scala** |
| `make web` | static server on `:5173` | — |
| `make dataproc-submit ARGS="pipeline"` | `ops/submit_dataproc.sh pipeline` | — |

All commands use `DATA_ROOT ?= $(PWD)/data`. Spark targets honour `SPARK_MASTER` (defaults to `local[*]`) and `HDFS_USER` for cluster paths.

## Data contracts

Each stage reads and writes Parquet with documented schemas. The key boundary is `data/enriched/` — once `nta_code` + `nta_name` are attached, every downstream stage (features, score, analytics, streaming aggregations) treats NTAs as the primary key and is source-agnostic. That boundary is what makes adding a 5th dataset (e.g. subway access) a localized change: new cleaner block in `Clean.scala`, new UDF application in `Geocode.scala`, new aggregate in `Features.scala`, new column in `Score.scala` and `Analytics.scala`. Nothing else moves.

Where Spark normally writes parquet *directories*, the score + analytics stages write *single files* (`.coalesce(1)` then rename the lone part file up to the final path). This gives the static frontend a predictable URL to fetch without needing a directory listing or a manifest.

## Design decisions and tradeoffs

| Decision | Rationale | Tradeoff |
|---|---|---|
| Scala + Spark for every computational stage | Matches the professor's rubric: big-data processing runs on a Dataproc cluster. Having a single language for the data surface also simplifies code review and packaging. | The scoring step (~260 NTAs) doesn't *need* Spark; using it anyway adds driver startup time for a payload that would run in milliseconds in pandas. We accept that for the single-language guarantee. |
| Python only in `data_ingest/` | HTTP download with retries / streaming / Socrata paging is what `requests` is for. Rewriting in Scala just to avoid a second language adds boilerplate with no gain. | One more runtime (Python + `requests`) in the setup steps. Acceptable — the downloader is a one-shot. |
| No backend; frontend reads parquet directly | Removes a whole tier. Scala writes parquet, JS reads parquet, Plotly renders it. Dataproc is the compute; GCS (or a local static server) is the serving surface. | The frontend depends on `hyparquet` over a CDN. Static hosting on GCS or any HTTP server works. |
| `Analytics.scala` emits one parquet per figure | The JS side is pure display logic — pick a column, feed it to Plotly. No quartile computation in the browser. | A schema change in `Analytics.scala` needs a matching change in `web/lib/figures.js`; we call this out in both files' headers. |
| JTS broadcast UDF for spatial join (not Sedona) | JTS is a 5 MB pure-Java jar with no runtime deps. 260 polygons × N points is trivial with an envelope pre-filter; Sedona would be overkill. | If polygon count or point volume grows 10×, revisit. Hook is `pointToNta` UDF — one-liner swap to Sedona. |
| Single-file parquet for score + analytics | Predictable URLs mean the frontend can fetch deterministically without a manifest. | `.coalesce(1)` forces a single reducer on the write path — fine at this scale (< 1 MB output). Would need partitioned output for genuinely big analytics tables. |
| Spark Structured Streaming instead of Flink | Already in the stack; same language / packaging as the batch side. | Less expressive windowing than Flink. Acceptable for tumbling-window use cases. |
| NTAs (2020) as the neighborhood unit | Official NYC definition, ~260 polygons; public GeoJSON | Some colloquial neighborhood names don't map 1:1. |
| ZIP→NTA derived from restaurants | Avoids a ZCTA shapefile dependency; the derivation itself is now Scala. | Sparse ZIPs (few restaurants) map noisily. |
| Parquet everywhere instead of Hive / a DB | Zero infra, columnar reads, fast iteration, readable in the browser via hyparquet. | No SQL surface outside the Spark REPL; not a problem at this scale. |
| Newcomer score as a plain weighted z-score | Transparent, retunable by editing one map; reproduces on any cluster. | Not a learned model; sensitive to outliers. |

## Extending the system

- **Add a dataset.** Add a downloader step in `data_ingest/alexj/download.py`, a cleaner block in `Clean.scala` that writes `data/cleaned/<name>/`, a UDF application in `Geocode.scala`, an aggregate in `Features.scala`, and (optionally) a weighted term in `Score.scala`. For dashboard coverage, add a row to `Analytics.scala`'s `METRICS` and a figure in `web/lib/figures.js`.
- **Add a team member.** Mirror `data_ingest/alexj/`, `etl_code/alexj/`, `profiling_code/alexj/` under their username. Everything is referenced via relative path in the Makefile; targets for their work follow the same pattern.
- **Replace ZIP→NTA with a real ZCTA join.** Drop a ZCTA GeoJSON at `data/geo/zcta.geojson` and rewrite the ZIP-to-NTA section of `Geocode.scala` to do an area-weighted intersection. Everything downstream is unchanged.
- **Swap the streaming source.** Point `Producer.scala` at a real feed (webhook, MQTT bridge) or add a second consumer — the latest-window snapshot is a single parquet file, so multiple producers/consumers can coexist.
- **Promote to a cluster.** The Scala scripts read `$DATA_ROOT` locally and fall back to `hdfs:///user/$HDFS_USER/...` when that env var is set. `ops/submit_dataproc.sh pipeline` does the cluster-side submission with GCS paths. The frontend is unaffected — publish `web/` and `data/` to GCS and use bucket-level static hosting.
