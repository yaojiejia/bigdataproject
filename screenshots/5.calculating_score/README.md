# Step 5 — Calculating Score

Runs `etl_code/alexj/Score.scala`: z-scores six "higher is worse" features
(`crimes_per_1k`, `felony_share`, `avg_score`, `critical_rate`,
`complaints_per_1k`, `median_rent_zori`), sign-flips them, collapses into four
sub-scores (`safety`, `food_safety`, `cleanliness`, `affordability`), applies
the weighted sum `0.30·safety + 0.25·food_safety + 0.15·cleanliness +
0.30·affordability`, and rescales linearly to 0–100.

Data-starved NTAs (no ZORI rent AND fewer than 20 inspections) have their
composite score nulled out. Written as a single-file parquet via `.coalesce(1)`
+ rename so the frontend can fetch a predictable URL.

Output: `data/scores/newcomer_score.parquet`.

## Run locally

```bash
make score
```

## Run on Dataproc

```bash
make score \
  SPARK_SHELL="spark-shell --deploy-mode client" \
  DATA_ROOT="file:///home/yj2761_nyu_edu/bigdataproject/data"
```

Swap in `DATA_ROOT="hdfs:///user/yj2761_nyu_edu/bigdataproject/data"` to
read/write HDFS instead.
