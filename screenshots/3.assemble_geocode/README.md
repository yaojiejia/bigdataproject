# Step 3 — Assemble / Geocode

Runs `etl_code/alexj/Geocode.scala`: attaches `nta_code` + `nta_name` to every
row of the three point datasets (crime, restaurants, 311) using a JTS
point-in-polygon broadcast UDF against the ~260 NYC NTA polygons. Rent is
joined via a ZIP → NTA modal lookup derived from restaurant locations.

First run resolves `org.locationtech.jts:jts-core:1.19.0` through ivy (~5 MB,
cached in `~/.ivy2/` thereafter).

Output: `data/enriched/{crime, restaurants, complaints311, rent}/*.parquet`
and `data/geo/zip_to_nta.csv`.

## Run locally

```bash
make geocode
```

## Run on Dataproc

```bash
make geocode \
  SPARK_SHELL="spark-shell --deploy-mode client" \
  DATA_ROOT="file:///home/yj2761_nyu_edu/bigdataproject/data"
```

Swap in `DATA_ROOT="hdfs:///user/yj2761_nyu_edu/bigdataproject/data"` to
read/write HDFS instead.
