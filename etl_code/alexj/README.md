# etl_code/alexj

All ETL code — Scala/Spark for the heavy joins and streaming, Python for
the small final transforms.

## Files

### Scala + Spark (batch)

| File | Role | Spark features |
|---|---|---|
| `Clean.scala` | Reads the 4 raw CSVs, normalises schemas, filters out rows without lat/lon, casts types, writes `data/cleaned/*.parquet`. | `DataFrame`, `option("multiLine", true)`, type-safe casts, partitioned parquet write |
| `Features.scala` | Joins cleaned crime/restaurants/311/rent with the NTA geocoding result and aggregates to one row per NTA. Writes `data/scores/neighborhood_features.parquet`. | joins, `groupBy`, aggregations, `broadcast` hints, AQE, Parquet nanos-as-long compat |

### Scala + Spark (streaming)

| File | Role | Spark features |
|---|---|---|
| `Consumer.scala` | Reads the `complaints311` Kafka topic, parses JSON, applies a 5-minute tumbling window per NTA (watermark = 10 min), and writes windowed parquet files to `data/stream/agg/`. | Structured Streaming, Kafka source, JSON parsing, `window` + watermark, `foreachBatch` sink |

### Python (glue)

| File | Role | Why Python |
|---|---|---|
| `geocode.py` | Lat/lon -> NTA code via a geopandas spatial join against the NTA GeoJSON. | `geopandas.sjoin` is the right tool; spatial joins aren't Spark's sweet spot. |
| `score.py` | z-score each of the 5 features over the 260 NTA rows, clip, weighted sum, 0-100 normalisation, rank. | 260 rows fits in pandas memory easily; pandas is simpler than Spark here. |

## Run

From the project root:

```bash
make clean              # Scala Spark batch
make geocode            # Python geopandas
make features           # Scala Spark batch
make score              # Python pandas
make pipeline           # all four in order

make stream-up          # Kafka up
make stream-produce     # replay 311 to topic
make stream-consume     # Consumer.scala (Structured Streaming)
```

Or as Python modules:

```bash
python -m etl_code.alexj.geocode
python -m etl_code.alexj.score
```

Scala jobs are invoked via `spark-shell -i <file>` with the Spark tuning
flags defined at the top of the Makefile (`SPARK_TUNE`).

## Imports

`geocode.py` and `score.py` import `pipeline.paths` for canonical filesystem
locations. The Scala jobs read `DATA_ROOT` from the environment (the Makefile
sets it).
