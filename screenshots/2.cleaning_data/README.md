# Step 2 — Cleaning Data

Runs `etl_code/alexj/Clean.scala`: reads the four raw CSVs from `data/raw/`
and emits typed, normalized parquet under `data/cleaned/`. Per dataset it
upper/trims text keys, drops rows with null join keys or coordinates, derives
`IS_FELONY` (crime) and `IS_CRITICAL` (inspections), and melts Zillow ZORI
wide-to-long keeping the latest non-null month per ZIP.

Output: `data/cleaned/{crime, restaurants, complaints311, rent}/*.parquet`.

## Run locally

```bash
make clean
```

## Run on Dataproc

Dataproc defaults `spark.submit.deployMode=cluster` and `fs.defaultFS=hdfs`,
both of which clash with the local Makefile. Override them:

```bash
make clean \
  SPARK_SHELL="spark-shell --deploy-mode client" \
  DATA_ROOT="file:///home/yj2761_nyu_edu/bigdataproject/data"
```

Swap in `DATA_ROOT="hdfs:///user/yj2761_nyu_edu/bigdataproject/data"` if you
want to read/write HDFS instead of the master node's local disk.
