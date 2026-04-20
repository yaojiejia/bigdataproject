# profiling_code/alexj

Scala + Spark scripts that I ran *first*, before writing any ETL, to
understand the shape and quality of the raw NYC Open Data feeds.

## Files

| File | What it does |
|---|---|
| `CountRecs.scala` | Loads the NYPD crime CSV and the DOHMH restaurant-inspections CSV; prints schemas; counts records; does a RDD `map` + `reduceByKey` roll-up per borough; enumerates distinct values for `OFNS_DESC`, `BORO_NM`, `LAW_CAT_CD`, `BORO`, `CUISINE DESCRIPTION`, `GRADE`. |
| `FirstCode.scala` | Broader profiling on the same two files. **Part A**: mean / median (approxQuantile) / mode / std-dev / `describe()` for the restaurant `SCORE` column and crime `Latitude`/`Longitude`. **Part B**: two cleaning exercises — text normalisation (upper + trim on borough/offense/grade) and derived binary flags (`IS_FELONY`, `IS_CRITICAL`). **Part C**: two MapReduce-style `(key, 1).reduceByKey(_ + _)` rollups per borough. |

## Why these live outside the production pipeline

`/etl_code/alexj/Clean.scala` + `Features.scala` are the canonical jobs
that produce the parquet files the API and dashboards actually read.
The profiling scripts here were used once to answer questions like:

- How many rows in each raw file?
- Which columns have `NULL` dominating the distribution?
- What borough strings need normalising (`"MANHATTAN"` vs `" Manhattan "`)?
- Where's the median restaurant score? Does the mean lie about the tail?
- What's the shape of the lat/lon cloud — do we need to filter `0.0`s?

Keeping them separate means re-running ETL doesn't re-run this work, and
the grader can see exactly which scripts produced the descriptive stats
quoted in my write-up.

## Run

From the project root:

```bash
make profile-first    # -> ./data/cleaned/{crime,restaurants} side-effect
make profile-counts   # -> stdout only; no file output
```

Both read from `$DATA_ROOT/raw` by default (`./data/raw`). If `HDFS_USER`
is set they switch to `hdfs:///user/$HDFS_USER/data/...` instead — that's
the Dataproc path I used for the first pass before porting everything
to local Spark.
