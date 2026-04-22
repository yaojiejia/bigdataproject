# Step 1 — Dependency and Data Setup

Installs the single Python dependency (`requests`) and downloads all raw inputs:
four source CSVs (NYPD complaints, DOHMH inspections, 311 food, Zillow ZORI)
into `data/raw/`, plus the NTA GeoJSON and population template into `data/geo/`.

One-time step; re-run only when source data needs refreshing.

## Run locally

```bash
make setup           # pip install requests
make download        # ~570 MB of CSVs -> data/raw/
make geo-download    # NTA GeoJSON + population template -> data/geo/
```

## Run on Dataproc

Same commands on the master node — these are pure Python, no Spark involved:

```bash
make setup
make download
make geo-download
```

Optionally mirror the downloaded files onto HDFS so later Spark stages can
read them through the distributed filesystem:

```bash
hdfs dfs -mkdir -p /user/yj2761_nyu_edu/bigdataproject
hdfs dfs -put -f data /user/yj2761_nyu_edu/bigdataproject/
```
