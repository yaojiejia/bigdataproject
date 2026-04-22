# Step 6 — Generate Analytics

Runs `etl_code/alexj/Analytics.scala`: one Spark job that emits nine tiny
single-file parquet tables the static frontend reads directly (no browser-side
math). Uses `percentile_approx` for quantiles, `ntile(10)` for rent deciles,
and Spark's `corr` aggregate to compute all 36 heatmap cells in one pass. The
joined input DataFrame is `.cache()`'d so the scan happens once across the
nine outputs.

Output under `data/analytics/`:

- `summary.parquet`, `distribution.parquet`, `top_bottom.parquet`
- `borough_box.parquet`, `borough_points.parquet`
- `correlation.parquet`
- `rent_vs_feature_{bins,ols,points}.parquet`

The `WARN WindowExec: No Partition Defined for Window operation!` messages
during this stage are benign — they come from inherently global ranking ops
(`row_number()` for top/bottom-10, `ntile(10)` for rent deciles) on a ~260-row
table, so the single-partition shuffle is a few KB.

## Run locally

```bash
make analytics
```

## Run on Dataproc

```bash
make analytics \
  SPARK_SHELL="spark-shell --deploy-mode client" \
  DATA_ROOT="file:///home/yj2761_nyu_edu/bigdataproject/data"
```

Swap in `DATA_ROOT="hdfs:///user/yj2761_nyu_edu/bigdataproject/data"` to
read/write HDFS instead.
