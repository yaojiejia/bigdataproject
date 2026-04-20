# NYC Neighborhood Insights

An end-to-end big-data pipeline and interactive map that helps newcomers evaluate New York City neighborhoods across **safety**, **food quality**, **quality of life (311)**, and **affordability (rent)**. Everything runs locally — no Dataproc, no HDFS.

## What it does

1. **Ingests** four public datasets: NYPD crime complaints, DOHMH restaurant inspections, 311 food-safety complaints, and Zillow ZORI rent.
2. **Cleans and enriches** them with Scala + Apache Spark (upper/trim, drop-nulls, `IS_FELONY`, `IS_CRITICAL`, rent wide-to-long melt).
3. **Normalizes geography** by spatially joining each record to a 2020 NYC Neighborhood Tabulation Area (NTA).
4. **Aggregates features** per neighborhood: crime totals/rates, restaurant inspection stats, complaint counts, median rent.
5. **Combines them** into a unified **newcomer score** (z-scored, weighted, 0–100).
6. **Streams** a simulated 311 feed through Kafka → Spark Structured Streaming with 5-minute tumbling windows.
7. **Serves** results via a **FastAPI** backend and renders them on an **interactive Leaflet map** with metric toggles and click-through detail.
8. **Explains** the results through an analytics dashboard (`/dashboard.html`) and a Jupyter notebook (`notebooks/analysis.ipynb`) that both render the same four charts — distribution, top/bottom 10, by-borough box plots, and a correlation heatmap — via a shared `pipeline.analytics` module, so the web and notebook views are guaranteed to match.

## Architecture

```
raw CSVs (NYC Open Data, Zillow)
        │   data_ingest/alexj/download.py            (Python, HTTP)
        ▼
data/raw/
        │   etl_code/alexj/Clean.scala               (Scala + Spark)
        ▼
data/cleaned/
        │   etl_code/alexj/geocode.py                (Python, geopandas sjoin)
        ▼
data/enriched/
        │   etl_code/alexj/Features.scala            (Scala + Spark)
        ▼
data/scores/neighborhood_features.parquet
        │   etl_code/alexj/score.py                  (Python, pandas z-score + weights)
        ▼
data/scores/newcomer_score.parquet ──► FastAPI  ──► Leaflet map
                                        ▲
                                        │ /trending
                    etl_code/alexj/Consumer.scala    (Scala + Structured Streaming)
                                        ▲
                                        │ Kafka topic: complaints311
                    stream/producer.py               (Python, kafka client)
```

**Language split:** every Spark job is Scala (`Clean.scala`, `Features.scala`, `Consumer.scala`). Everything else — HTTP ingestion, spatial join (geopandas), 260-row scoring (pandas), Kafka producer, FastAPI, frontend — is Python or JS, because that's what each tool is best at.

## Project layout

The top-level layout follows the assignment rubric: `/data_ingest`, `/etl_code`, and `/profiling_code`, each with a per-member subdirectory.

```
bigdataproject/
  data_ingest/alexj/
    download.py          # HTTP fetch of 4 raw CSVs
    geo_download.py      # HTTP fetch of NTA GeoJSON + population template

  etl_code/alexj/
    Clean.scala          # Spark batch: 4 datasets -> cleaned parquet
    Features.scala       # Spark batch: per-NTA aggregation + join
    Consumer.scala       # Spark Structured Streaming: Kafka -> windows
    geocode.py           # geopandas point-in-polygon; ZIP->NTA lookup
    score.py             # pandas z-score + weighted sum -> 0-100 score

  profiling_code/alexj/
    FirstCode.scala      # stats (mean/median/mode/stddev) + RDD map/reduce
    CountRecs.scala      # schemas + record counts + distinct-value surveys

  pipeline/              # shared infrastructure & serving layer
    paths.py             # canonical filesystem paths (imported everywhere)
    analytics.py         # shared dashboard figure builders (Plotly)

  stream/
    producer.py          # Kafka producer (replays 311 onto topic)

  notebooks/
    analysis.ipynb       # analytical notebook; imports pipeline.analytics

  api/main.py            # FastAPI on :8765 (map API + /analytics/*)
  web/                   # Leaflet map + Plotly dashboard (:5173)
  kafka/                 # docker-compose.yml (Zookeeper + Kafka)
  Makefile               # one target per stage; `make help`

  data/
    raw/ cleaned/ enriched/ scores/ stream/ geo/
```

Each of the three assignment directories has its own `README.md` explaining what lives in it and how to run each script.

## Prerequisites

- **Python 3.11+** — for ingestion, geocode, score, producer, API, analytics notebook
- **Java 11** — required by Spark
- **Apache Spark 3.5** — the Scala jobs (`Clean.scala`, `Features.scala`, `Consumer.scala`) run via `spark-shell -i`, which must be on `PATH`. Install the standalone distribution from <https://spark.apache.org/downloads.html> (or `brew install apache-spark` / `apt install spark`).
- **Docker** — only for the Kafka streaming layer. The batch pipeline doesn't need it.

`make setup` installs the Python dependencies (pandas, geopandas, fastapi, kafka-python, plotly, jupyter — no PySpark, since every Spark job in this project is Scala).

## Running the full stack, end-to-end

### One-time setup

```bash
make setup               # pip install -r requirements.txt
make download            # ~570 MB of CSVs into data/raw/
make geo-download        # NTA GeoJSON + population template into data/geo/
```

### Batch pipeline (one command)

```bash
make pipeline            # Clean.scala -> geocode.py -> Features.scala -> score.py
```

Produces:

- `data/cleaned/{crime,restaurants,complaints311,rent}/*.parquet`
- `data/enriched/{crime,restaurants,complaints311,rent}/*.parquet`
- `data/scores/neighborhood_features.parquet` (one row per NTA)
- `data/scores/newcomer_score.parquet` (newcomer score 0–100)

Wall time on a laptop: ~1 minute for `clean`, ~4 s for `geocode`, ~12 s for `features`, ~1 s for `score`.

### Run all services (5 terminals)

Once the batch pipeline has produced the Parquet outputs, bring up the live services. Each command is long-running; **open a fresh terminal per command** (tmux, VS Code split terminals, WSL panes — whatever works).

```bash
# Terminal 1: Kafka (via Docker)
make stream-up
# (leave this terminal; it returns once `docker compose up -d` completes)

# Terminal 2: Structured Streaming consumer (Scala)
make stream-consume

# Terminal 3: 311 producer (replays historical 311 into Kafka)
make stream-produce

# Terminal 4: FastAPI backend
make api                 # http://localhost:8765

# Terminal 5: static web server
make web                 # http://localhost:5173
```

Open <http://localhost:5173/> in your browser. The map loads `/neighborhoods?metric=score` from the API; switch metrics in the sidebar dropdown (Newcomer Score, Crime, Food Safety, Recent 311, Median Rent).

Click **Open analytics dashboard** in the sidebar (or go to <http://localhost:5173/dashboard.html>) to see the four analytical charts: distribution histogram, top/bottom 10 NTAs, by-borough box plots, and input-feature correlation heatmap.

### The notebook — same charts, more depth

The dashboard has a Python twin at `notebooks/analysis.ipynb`. Both call the same `pipeline.analytics` functions, so the four shared charts render pixel-for-pixel identically. The notebook adds sub-score decomposition, a safety-vs-affordability scatter, and written interpretation.

```bash
# From the project root, with the venv active:
jupyter notebook notebooks/analysis.ipynb
# or render headless:
jupyter nbconvert --to html --execute notebooks/analysis.ipynb
```

### Shutting down

```bash
# Ctrl-C in terminals 2, 3, 4, 5
make stream-down         # docker compose down
```

## API endpoints

| Endpoint | Purpose |
|---|---|
| `GET /health` | liveness + NTA count |
| `GET /neighborhoods?metric=score\|crime\|food\|rent\|311` | GeoJSON FeatureCollection with selected metric attached |
| `GET /neighborhood/{nta_code}` | full feature record for one NTA |
| `GET /trending?limit=N` | top N NTAs by complaint count in the most recent streaming window |
| `GET /analytics/summary` | descriptive stats per metric (mean/median/min/max/std) |
| `GET /analytics/distribution?metric=...` | Plotly figure JSON: histogram of the metric across NTAs |
| `GET /analytics/top-bottom?metric=...&n=10` | Plotly figure JSON: top-N vs. bottom-N NTAs |
| `GET /analytics/by-borough?metric=...` | Plotly figure JSON: box plot by borough |
| `GET /analytics/correlation` | Plotly figure JSON: correlation heatmap of the six input features |

## Running on Dataproc (optional)

The Scala scripts are location-transparent. Set `HDFS_USER` and paths swap from the local filesystem to `hdfs:///user/$HDFS_USER/...`:

```bash
# On the Dataproc master, after uploading raw CSVs to HDFS:
HDFS_USER=$USER spark-shell --master yarn --deploy-mode client -i etl_code/alexj/Clean.scala
HDFS_USER=$USER spark-shell --master yarn --deploy-mode client -i etl_code/alexj/Features.scala
```

## Data sources

| Dataset | Source | ID |
|---|---|---|
| NYPD Complaint (YTD) | NYC Open Data | `5uac-w243` |
| Restaurant Inspection | NYC Open Data | `43nn-pn8j` |
| 311 Service Requests (food subset) | NYC Open Data | `erm2-nwe9` |
| NTA 2020 polygons | NYC Open Data | `9nt8-h7nd` |
| ZORI rent index (ZIP) | Zillow Research | `Zip_zori_uc_sfrcondomfr_sm_month.csv` |

## Notes

- Population is optional. Drop a populated CSV at `data/geo/nta_population.csv` (columns `nta_code,nta_name,population`) and `Features.scala` will compute per-capita rates. Otherwise raw totals are used as the intensity signal.
- ZIP → NTA mapping is derived from the restaurant dataset's points (each ZIP assigned to the NTA containing the plurality of its restaurants). Replace `etl_code/alexj/geocode.py::build_zip_to_nta` if you have a proper ZCTA shapefile.
- Scala Spark jobs are invoked via `spark-shell -i <path>.scala` with tuning configs (shuffle partitions, AQE, broadcast threshold, driver memory) applied from the Makefile's `SPARK_TUNE` variable. See `SCALABILITY.md`.
