# Architecture

## Purpose

**NYC Neighborhood Insights** turns fragmented public data into a single comparable view of what it's like to live in each New York City neighborhood. The target user is a newcomer trying to decide between Tribeca, the Lower East Side, Astoria, etc. — someone who wants to see safety, food quality, quality-of-life, and affordability side by side rather than hunting through four separate civic portals.

The project is also a demonstration of a modern end-to-end big-data architecture: ingestion → Spark batch processing → geographic normalization → feature engineering → streaming analytics → serving → interactive visualization. It is deliberately runnable on a single machine (no cloud dependency) so it can be developed, debugged, and demoed locally.

### Design goals

1. **Comparable neighborhoods.** Every signal is aggregated to the same geographic unit (NYC 2020 Neighborhood Tabulation Areas, ~260 polygons) so datasets that arrive with different keys (borough, lat/long, ZIP) can be joined and ranked consistently.
2. **Batch + streaming in one story.** A classic batch pipeline produces the stable per-neighborhood picture; a Kafka → Spark Structured Streaming path layers near-real-time signals on top without rewriting the batch logic.
3. **Local-first.** No cloud required. Spark runs in `local[*]`, Kafka runs in Docker, the API is a single uvicorn process, the frontend is static. The full stack starts from `make` targets.
4. **Composable, not monolithic.** Each pipeline stage is a standalone module that reads Parquet and writes Parquet. Any stage can be re-run in isolation; downstream stages don't care how upstream produced their input.

## High-level architecture

```
┌───────────────────────────┐      ┌─────────────────────────────┐
│  NYC Open Data + Zillow   │      │  NYC Open Data (GeoJSON)    │
│  CSV downloads            │      │  2020 NTA polygons          │
└──────────────┬────────────┘      └─────────────┬───────────────┘
               │ pipeline/download.py (Python)   │ pipeline/geo_download.py
               ▼                                 │
       ┌──────────────┐                          │
       │  data/raw/   │                          │
       └──────┬───────┘                          │
              │ Clean.scala  (Scala + Spark)     │
              ▼                                  │
       ┌────────────────┐                        │
       │  data/cleaned/ │                        │
       └──────┬─────────┘                        │
              │ pipeline/geocode.py (geopandas sjoin)  ◄──┤
              ▼                                          │
       ┌───────────────────┐                             │
       │  data/enriched/   │  (every row now has nta_code)
       └──────┬────────────┘
              │ Features.scala  (Scala + Spark)
              ▼
       ┌──────────────────────────────────────┐
       │  data/scores/neighborhood_features   │  one row per NTA
       └──────┬───────────────────────────────┘
              │ pipeline/score.py (pandas z-score + weighted sum)
              ▼
       ┌──────────────────────────────────────┐
       │  data/scores/newcomer_score.parquet  │
       └──────┬───────────────────────────────┘
              │
              │                                ┌────────────────────┐
              │                                │ stream/producer.py │
              │                                │ (replays 311)      │
              │                                └─────────┬──────────┘
              │                                          │
              │                                          ▼
              │                                ┌────────────────────┐
              │                                │ Kafka (Docker)     │
              │                                │ topic: complaints311│
              │                                └─────────┬──────────┘
              │                                          │
              │                                          ▼
              │                     ┌─────────────────────────────────┐
              │                     │ Consumer.scala                  │
              │                     │ Spark Structured Streaming      │
              │                     │ 5-min tumbling windows per NTA  │
              │                     └────────┬────────────────────────┘
              │                              │
              │                              ▼
              │                     ┌──────────────────────────────┐
              │                     │ data/stream/windowed/ (hist) │
              │                     │ data/stream/latest.parquet   │
              │                     └────────┬─────────────────────┘
              │                              │
              ▼                              ▼
       ┌─────────────────────────────────────────┐
       │  api/main.py  (FastAPI, Python)         │
       │  /neighborhoods  /neighborhood/{code}   │
       │  /trending                              │
       └────────────────────┬────────────────────┘
                            │ HTTP + JSON/GeoJSON
                            ▼
                   ┌─────────────────────────┐
                   │  web/  (Leaflet static) │
                   │  choropleth + detail    │
                   └─────────────────────────┘
```

### Language split at a glance

Every component that runs on Spark is **Scala**. Everything that doesn't is **Python**.

| Layer | Scala (Spark) | Python |
|---|---|---|
| Ingestion (HTTP) | — | `pipeline/download.py`, `pipeline/geo_download.py` |
| Batch cleaning | **`Clean.scala`** | — |
| Geographic join | — | `pipeline/geocode.py` (geopandas sjoin) |
| Feature aggregation | **`Features.scala`** | — |
| Newcomer score | — | `pipeline/score.py` (pandas, ~260 rows) |
| Streaming consumer | **`Consumer.scala`** | — |
| Streaming producer | — | `stream/producer.py` (Kafka client, not Spark) |
| API | — | `api/main.py` (FastAPI) |
| Frontend | — | `web/` (Leaflet, JS) |

The boundary is simple: anything that builds a `SparkSession` is Scala. Everything else is Python because that's the right tool for the job — geopandas for spatial joins, pandas for 260-row stats, kafka-python for the producer, FastAPI for the web layer.

## Components

### 1. Ingestion

`pipeline/download.py` fetches four public CSVs:

| Dataset | Source | ID | Access pattern |
|---|---|---|---|
| NYPD Complaints (YTD) | NYC Open Data | `5uac-w243` | Bulk export CSV |
| Restaurant Inspections | NYC Open Data | `43nn-pn8j` | Bulk export CSV |
| 311 Service Requests | NYC Open Data | `erm2-nwe9` | Socrata API with SoQL filter (food subset only) |
| Zillow ZORI (ZIP level) | Zillow Research | `Zip_zori_uc_sfrcondomfr_sm_month.csv` | Static CSV |

Only a food-safety subset of 311 is pulled (`Food Poisoning`, `Food Establishment`, `Rodent`, `Unsanitary Animal Pvt Property`) to keep the raw file manageable; the Socrata `$where` clause does this server-side. `pipeline/geo_download.py` separately fetches the NTA GeoJSON and templates a population CSV.

### 2. Batch cleaning (Scala + Spark)

`Clean.scala` is the canonical cleaner. Run with `spark-shell -i Clean.scala` or `make clean`; on Dataproc, set `HDFS_USER=<you>` and paths swap from local filesystem to `hdfs:///user/<you>/...` with no code change. Writes Parquet to `data/cleaned/{crime,restaurants,complaints311,rent}/`.

Transformations:

- `UPPER(TRIM(...))` on text keys (`BORO_NM`, `OFNS_DESC`, `CUISINE`, `BORO`, `GRADE`, `COMPLAINT_TYPE`).
- Drop rows with null join keys or null coordinates.
- `IS_FELONY = (LAW_CAT_CD == "FELONY")` and `IS_CRITICAL = (SCORE >= 28)` as binary indicators.
- Zillow's wide "one column per month" layout is melted with `stack(n, ...)` and the latest non-null month is kept per ZIP via a `row_number()` window.

Row-count and schema parity against the earlier PySpark cleaner was verified across all four datasets before the Python version was removed; see `CHANGELOG.md`. `CountRecs.scala` and `FirstCode.scala` are earlier exploration/analysis scripts and are kept for reference only — they're not part of the production pipeline.

### 3. Geographic normalization

`pipeline/geocode.py` attaches `nta_code` + `nta_name` to every row:

- **Crime, restaurants, 311** — point-in-polygon via `geopandas.sjoin` against the NTA GeoJSON (EPSG:4326). This is CPU-bound but each dataset fits comfortably in memory after cleaning (~500k crime rows, ~250k inspections, ~25k 311).
- **Rent** — ZIP codes don't align to NTAs 1:1, so a ZIP→NTA lookup is derived from the restaurant dataset itself: for each ZIP, the NTA containing the most restaurants wins. That table is written to `data/geo/zip_to_nta.csv` for reproducibility.

The tradeoff: using restaurant points as a ZIP proxy is a pragmatic shortcut. A ZCTA shapefile + area-weighted overlap would be cleaner; the function is a single hook (`build_zip_to_nta`) to swap in later.

### 4. Feature engineering (Scala + Spark)

`Features.scala` groups each enriched table by `nta_code` and produces one row per NTA:

- `total_crimes`, `felonies`, `felony_share`
- `n_inspections`, `avg_score`, `critical_rate`
- `n_complaints`
- `median_rent_zori`
- `crimes_per_1k`, `complaints_per_1k` (if `nta_population.csv` is populated; otherwise the raw totals are used as the intensity signal)

All four feature tables are outer-joined on `(nta_code, nta_name)`. Structural nulls (e.g. an NTA with no restaurants in the sample) are filled with zeros so the scoring stage never has to branch on missing-vs-zero; rent nulls are intentionally left null because that signal is real ("no observations here"). Result: `data/scores/neighborhood_features.parquet`.

Implementation note: `Features.scala` sets `spark.sql.legacy.parquet.nanosAsLong=true` at session level because `geocode.py` writes enriched Parquet through pandas/pyarrow, which uses nanosecond-precision timestamps that Spark 3.5 otherwise rejects. We don't read any of those timestamp columns — but the Parquet reader validates the full file schema up front. This is documented inline in the script.

### 5. Newcomer score

`pipeline/score.py` is pandas rather than Spark — the input is ~260 rows, Spark is overkill. The score is intentionally simple and transparent:

1. For each feature, compute a z-score across NTAs.
2. Flip the sign on "bad" features (crime, critical rate, complaints, rent).
3. Collapse individual features into four sub-scores: `safety`, `food_safety`, `cleanliness`, `affordability`.
4. Weighted sum (defaults: 0.30 / 0.25 / 0.15 / 0.30).
5. Re-scale linearly to 0–100 for display.

Weights live at the top of the file as constants so they can be retuned without touching the code.

### 6. Streaming (Kafka + Spark Structured Streaming)

The streaming layer is a **simulation**, not a live feed — NYC Open Data doesn't expose 311 as a push stream. The value here is demonstrating the architecture; the producer is just a replay.

- `stream/producer.py` (Python, `kafka-python` — not a Spark job) reads `data/enriched/complaints311/` (so each message already carries its `nta_code`), rewrites `created_date` to `now()`, and publishes JSON to Kafka topic `complaints311` on `localhost:9092` at a configurable rate (`--rps`, default 50).
- `Consumer.scala` runs Spark Structured Streaming with the Kafka source, applies a 10-minute watermark and a **5-minute tumbling window** keyed by `(nta_code, complaint_type)`, and writes two sinks:
  - `data/stream/windowed/` — append-mode Parquet, the audit history.
  - `data/stream/latest.parquet` — overwritten each micro-batch via `foreachBatch`, exposing only the most recent window as a compact per-NTA table for the API.

The consumer uses `AdminClient` (from the Kafka client library that ships with `spark-sql-kafka-0-10`) to create the topic idempotently at startup. Without this, a consumer started before the producer would hit `UnknownTopicOrPartitionException` three times and the query would die — the broker has auto-create enabled, but auto-creation only fires on first *produce*. See `CHANGELOG.md` for the history.

This split lets the API get fresh data without reading a growing append log on every request, while still keeping a tamper-proof history on disk.

### 7. API

`api/main.py` is a FastAPI app (uvicorn, `:8000`). It loads the NTA GeoJSON and newcomer-score Parquet into memory at startup (both are small); `/trending` re-reads `data/stream/latest.parquet` on each request so streaming updates are visible without restarting the API.

Endpoints:

| Endpoint | Purpose |
|---|---|
| `GET /health` | Liveness + NTA count |
| `GET /neighborhoods?metric=score\|crime\|food\|rent\|311` | GeoJSON FeatureCollection with selected metric attached to each feature's `properties.metric_value` and the full feature record under `properties.features` |
| `GET /neighborhood/{nta_code}` | Full feature record for one NTA |
| `GET /trending?limit=N` | Top N NTAs by complaint count in the most recent streaming window |

CORS is open and NaN/Inf are sanitized to `null` so the browser-side JSON parser doesn't choke.

### 8. Frontend

`web/index.html` + `web/map.js` + `web/style.css` — plain HTML/JS, no build step. Leaflet on OpenStreetMap tiles. The script:

1. Fetches `/neighborhoods?metric=<selected>`.
2. Computes quantile breaks on the metric values.
3. Colors each NTA polygon along a yellow→red ramp (inverted for higher-is-better metrics like the newcomer score).
4. Click → side panel with all features for that NTA.

A metric dropdown swaps between Newcomer Score, Crime, Food Safety (critical inspection rate), Recent 311 (streaming), and Median Rent.

### 9. Orchestration

`Makefile` wraps every step. The top-level targets point at the Scala implementations; Python equivalents are reachable with a `-py` suffix.

| Target | Runs | Language |
|---|---|---|
| `make setup` | `pip install -r requirements.txt` | — |
| `make download` | fetch four CSVs from NYC Open Data + Zillow | Python |
| `make geo-download` | fetch NTA GeoJSON + population template | Python |
| `make clean` | `spark-shell -i Clean.scala` | **Scala** |
| `make geocode` | `pipeline/geocode.py` | Python (geopandas) |
| `make features` | `spark-shell -i Features.scala` | **Scala** |
| `make score` | `pipeline/score.py` | Python (pandas) |
| `make pipeline` | `clean` → `geocode` → `features` → `score` | mixed |
| `make stream-up` / `stream-down` | `docker compose` on Kafka | — |
| `make stream-produce` | `stream/producer.py` | Python |
| `make stream-consume` | `spark-shell --packages ... -i Consumer.scala` | **Scala** |
| `make api` | `uvicorn api.main:app --port 8765` | Python |
| `make web` | static server on `:5173` | — |

All commands use `DATA_ROOT ?= $(PWD)/data` so a different data directory can be swapped in without editing any file. Spark targets also honour `SPARK_MASTER` (defaults to `local[*]`) and pick up `HDFS_USER` to target a Dataproc cluster instead.

## Data contracts

Each stage reads and writes Parquet with documented schemas. The key boundary is `data/enriched/` — once `nta_code` + `nta_name` are attached, every downstream stage (features, streaming aggregations, API) treats NTAs as the primary key and is source-agnostic. That boundary is what makes adding a 5th dataset (e.g. subway access) a localized change: new cleaner block in `Clean.scala`, new geocoder call, new aggregate in `Features.scala`, new column in `pipeline/score.py`. Nothing else moves.

## Design decisions and tradeoffs

| Decision | Rationale | Tradeoff |
|---|---|---|
| Scala + Spark for every Spark job, Python for the glue | The "big data processing" boundary is clean: anything that builds a `SparkSession` is Scala (`Clean.scala`, `Features.scala`, `Consumer.scala`); anything that doesn't is Python (HTTP, geopandas, pandas, FastAPI, Kafka producer, Plotly analytics). This matches the course's Scala-Spark expectation without forcing Scala implementations for things Python does better (spatial joins, 260-row stats, web) | The earlier PySpark prototypes (`clean.py`, `features.py`, `consumer.py`) were removed once schema parity with the Scala ports was verified (see `CHANGELOG.md`); contributors who want a Python Spark reference have to read the git history |
| Geocoding stays Python (geopandas), not Scala + Sedona | Sedona would add a heavy dependency for one operation that runs on ~1M points in seconds via `geopandas.sjoin`. The enriched Parquet is the handoff back to Spark, so the Python detour is invisible to downstream stages | A strict "Spark everywhere" reviewer might want a Sedona or broadcast-UDF version. The hook is isolated (`pipeline/geocode.py::_sjoin_points`) and would be easy to swap |
| Newcomer score stays Python (pandas), not Scala | 260 NTAs. Spark overhead (driver startup, codegen) dwarfs the actual work. Pandas is the right tool at this scale | If weights ever become per-user and scoring runs per-request, this would move into the API tier — still Python, still pandas |
| Local[*] Spark, not a cluster | Project runs on a laptop; data fits. `HDFS_USER` is the one environment variable that flips every Scala script to cluster paths | Not a scaling demo beyond a single machine; benchmarks live in `SCALABILITY.md` |
| Spark Structured Streaming instead of Flink | Already in the Spark stack; simpler to operate | Less expressive windowing than Flink |
| NTAs (2020) as the neighborhood unit | Official NYC definition, ~260 polygons; public GeoJSON | Some colloquial neighborhood names don't map 1:1 |
| ZIP→NTA derived from restaurants | Avoids a ZCTA shapefile dependency | Sparse ZIPs (few restaurants) map noisily |
| Parquet everywhere instead of Hive / a DB | Zero infra, columnar reads, fast iteration | No SQL surface; if scale grows, add DuckDB or Hive later |
| `/trending` re-reads Parquet per request | Always fresh; trivial to reason about | A few hundred ms of latency per request; fine at this scale |
| Newcomer score as a plain weighted z-score | Transparent, retunable by editing one dict | Not a learned model; sensitive to outliers |

## Extending the system

- **Add a dataset.** Add a downloader step in `pipeline/download.py`, a cleaner block in `Clean.scala` that writes `data/cleaned/<name>/`, a geocode call in `pipeline/geocode.py`, an aggregate in `Features.scala`, and (optionally) a weighted term in `pipeline/score.py`. The API and frontend need only a new entry in `METRIC_COLUMNS`.
- **Replace ZIP→NTA with a real ZCTA join.** Drop a ZCTA GeoJSON at `data/geo/zcta.geojson` and rewrite `build_zip_to_nta` to do an area-weighted intersection. Everything downstream is unchanged.
- **Swap the streaming source.** Point `producer.py` at a real feed (e.g. a webhook ingester) or add a second consumer — the API only reads `latest.parquet`, so multiple producers/consumers can coexist.
- **Promote to a cluster.** The Scala scripts read `$DATA_ROOT` locally and fall back to `hdfs:///user/$HDFS_USER/...` when that env var is set, so `HDFS_USER=<you> make clean` / `make features` runs unchanged on Dataproc. The streaming consumer needs a `kafka.bootstrap.servers` override (`KAFKA_BOOTSTRAP=...`); everything else is location-transparent. The FastAPI + web tier is unaffected.
