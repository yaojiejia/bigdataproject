# /etl_code

ETL / data-cleaning code. Takes the raw files produced by `/data_ingest` and
produces the cleaned, enriched, feature-aggregated, and scored outputs
consumed by the API and the dashboards. Each team member has their own
subdirectory.

## Layer map

```
data/raw/           <-- /data_ingest writes here
    |
    v
Clean.scala         raw CSV    -> data/cleaned/*.parquet   (Scala + Spark)
    |
    v
geocode.py          NTA spatial join (crime + 311)         (Python + geopandas)
    |               -> data/enriched/*.parquet
    v
Features.scala      per-NTA aggregations                   (Scala + Spark)
                    -> data/scores/neighborhood_features.parquet
    |
    v
score.py            z-score + weighted sum -> /rank         (Python + pandas)
                    -> data/scores/newcomer_score.parquet

Consumer.scala      Kafka 311 topic -> 5-min windows       (Scala + Structured Streaming)
                    -> data/stream/agg/
```

## Run

From the project root:

```bash
make clean            # Clean.scala      (Spark batch)
make geocode          # geocode.py       (geopandas)
make features         # Features.scala   (Spark batch)
make score            # score.py         (pandas)
make pipeline         # all four in order

make stream-up        # Kafka up
make stream-produce   # replay 311 onto topic
make stream-consume   # Consumer.scala   (Spark Structured Streaming)
```
