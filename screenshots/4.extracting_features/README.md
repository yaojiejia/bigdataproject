# Step 4 — Extracting Features

Runs `etl_code/alexj/Features.scala`: groups each enriched table by `nta_code`
and emits one row per NTA with `total_crimes`, `felonies`, `felony_share`,
`n_inspections`, `avg_score`, `critical_rate`, `n_complaints`,
`median_rent_zori`, and optional per-1k intensities if
`data/geo/nta_population.csv` is populated. All four feature tables are
outer-joined on `(nta_code, nta_name)`.

Output: `data/scores/neighborhood_features.parquet/`.

## Run locally

```bash
make features
```

## Run on Dataproc

```bash
make features \
  SPARK_SHELL="spark-shell --deploy-mode client" \
  DATA_ROOT="file:///home/yj2761_nyu_edu/bigdataproject/data"
```

Swap in `DATA_ROOT="hdfs:///user/yj2761_nyu_edu/bigdataproject/data"` to
read/write HDFS instead.
