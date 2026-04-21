# /etl_code

ETL code. Takes the raw CSVs produced by `/data_ingest` and produces the
cleaned, enriched, feature-aggregated, scored, and pre-aggregated
analytics Parquet files consumed by the static frontend. Every stage is
Scala on Spark; Python does not appear anywhere under `/etl_code`.

Each team member has their own subdirectory.

## Layer map

```
data/raw/              <-- /data_ingest writes here
    |
    v
Clean.scala            raw CSV   -> data/cleaned/*.parquet         (Scala + Spark)
    |
    v
Geocode.scala          JTS point-in-polygon + ZIP->NTA modal       (Scala + Spark + JTS)
                       -> data/enriched/*.parquet
    |
    v
Features.scala         per-NTA aggregations                        (Scala + Spark)
                       -> data/scores/neighborhood_features.parquet
    |
    v
Score.scala            z-score + weighted sum + 0-100 rescale      (Scala + Spark)
                       -> data/scores/newcomer_score.parquet
    |
    v
Analytics.scala        9 pre-aggregated tables for the dashboard   (Scala + Spark)
                       -> data/analytics/*.parquet
```

## Run

From the project root:

```bash
make clean        # Clean.scala      (Spark batch)
make geocode      # Geocode.scala    (Spark batch + JTS broadcast UDF)
make features     # Features.scala   (Spark batch)
make score        # Score.scala      (Spark batch)
make analytics    # Analytics.scala  (Spark batch)
make pipeline     # all five in order
```

Every Scala target invokes `spark-shell $(SPARK_OPTS) -i <script>.scala`
with the shared `SPARK_TUNE` tuning block from the Makefile. `make
geocode` additionally passes `--packages` for JTS, cached in `~/.ivy2`
on first run.
