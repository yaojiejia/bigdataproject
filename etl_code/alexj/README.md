# etl_code/alexj

Every ETL stage is Scala on Spark. Python does not appear in this
directory; the `data_ingest/` module is the only place Python touches
data, and its job stops after the raw CSVs land on disk.

## Files

| File | Role | Spark features |
|---|---|---|
| `Clean.scala` | Reads the 4 raw CSVs, normalises schemas, filters out rows without lat/lon, casts types, writes `data/cleaned/*.parquet`. | `DataFrame`, `option("multiLine", true)`, type-safe casts, partitioned parquet write |
| `Geocode.scala` | Point-in-polygon join against the NTA GeoJSON using JTS in a broadcast UDF. Attaches `nta_code` + `nta_name` to every crime, restaurant, and 311 row; derives the ZIP→NTA lookup from restaurants. Writes `data/enriched/*.parquet`. | `sparkContext.broadcast` of 260 JTS geometries, UDF with envelope pre-filter + `Geometry.contains`, `Window` row_number |
| `Features.scala` | Joins the enriched crime / restaurants / 311 / rent DataFrames with population and aggregates to one row per NTA. Writes `data/scores/neighborhood_features.parquet`. | joins, `groupBy`, aggregations, `broadcast` hints, AQE, Parquet nanos-as-long compat |
| `Score.scala` | z-score each of the 5 features over the ~260 NTA rows (mean-impute missing values, `stddev_pop` to match reference), weighted sum, min-max rescale to 0–100, `rank`. Writes `data/scores/newcomer_score.parquet`. | Spark SQL agg + window rescale, `.coalesce(1)` + rename to a single-file parquet |
| `Analytics.scala` | Writes the 9 pre-aggregated tables the frontend dashboard consumes (summary, distribution, top_bottom, borough_box, borough_points, correlation, rent_vs_feature_{bins,ols,points}). | `percentile_approx`, `ntile(10)` windowing, `corr` aggregate (36 cells in one pass), OLS via `corr`/`avg`/`stddev_pop`, `.cache()` on the joined input |

## Run

From the project root:

```bash
make clean       # Clean.scala      (Spark batch)
make geocode     # Geocode.scala    (Spark batch + JTS broadcast UDF)
make features    # Features.scala   (Spark batch)
make score       # Score.scala      (Spark batch)
make analytics   # Analytics.scala  (Spark batch)
make pipeline    # all five in order
```

Each target shells out to `spark-shell $(SPARK_OPTS) -i <file>.scala`
with the Spark tuning flags defined at the top of the Makefile
(`SPARK_TUNE`). `make geocode` adds `--packages org.locationtech.jts:jts-core`
on the first run and caches the jar in `~/.ivy2`.

## Paths

Every script reads the single environment variable `DATA_ROOT` (set by
the Makefile to `$(PWD)/data` locally; pointed at a GCS bucket on
Dataproc) and resolves all inputs and outputs relative to it. There is
no shared helper module — each script builds its paths inline so they
stay self-contained and submittable individually via
`ops/submit_dataproc.sh`.
