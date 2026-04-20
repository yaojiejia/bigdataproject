.PHONY: setup download geo-download \
        clean geocode features score pipeline \
        stream-up stream-down stream-produce stream-consume \
        profile-first profile-counts \
        api web all help

PYTHON       ?= python3
PIP          ?= pip3
DATA_ROOT    ?= $(PWD)/data
SPARK_SHELL  ?= spark-shell
SPARK_MASTER ?= local[*]
KAFKA_PKG    ?= org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1

# Scalability tuning — applied to every Scala Spark job. See SCALABILITY.md
# for the rationale behind each number.
#   shuffle.partitions=8           -> one partition per core on an 8-core box
#   adaptive.enabled + friends     -> runtime plan rewriting (coalesce, skew)
#   autoBroadcastJoinThreshold=50m -> auto-broadcast small lookups
#   spark.driver.memory=4g         -> enough headroom for the full CSVs
#   session.timeZone=America/NY    -> match NYC Open Data semantics
#   legacy.timeParserPolicy=LEGACY -> tolerate NYC Open Data's mixed date formats
SPARK_TUNE   ?= --conf spark.sql.shuffle.partitions=8 \
                --conf spark.sql.adaptive.enabled=true \
                --conf spark.sql.adaptive.coalescePartitions.enabled=true \
                --conf spark.sql.adaptive.skewJoin.enabled=true \
                --conf spark.sql.autoBroadcastJoinThreshold=50m \
                --conf spark.driver.memory=4g \
                --conf spark.sql.session.timeZone=America/New_York \
                --conf spark.sql.legacy.timeParserPolicy=LEGACY

SPARK_OPTS   ?= --master $(SPARK_MASTER) \
                --conf spark.ui.showConsoleProgress=false \
                --conf spark.log.level=WARN \
                $(SPARK_TUNE)

help:
	@echo "Data ingestion  (data_ingest/alexj/):"
	@echo "  download         - fetch 4 raw CSVs into data/raw/"
	@echo "  geo-download     - fetch NTA GeoJSON + population template"
	@echo ""
	@echo "ETL / batch       (etl_code/alexj/):"
	@echo "  clean            - Clean.scala    : raw CSV -> data/cleaned/*.parquet"
	@echo "  geocode          - geocode.py     : NTA spatial join (geopandas)"
	@echo "  features         - Features.scala : per-NTA aggregations -> data/scores/"
	@echo "  score            - score.py       : z-score + weighted sum (pandas)"
	@echo "  pipeline         - clean -> geocode -> features -> score"
	@echo ""
	@echo "ETL / streaming   (etl_code/alexj/):"
	@echo "  stream-up        - start Kafka via docker-compose"
	@echo "  stream-down      - stop Kafka"
	@echo "  stream-produce   - producer.py replays 311 records onto Kafka topic"
	@echo "  stream-consume   - Consumer.scala : Kafka -> 5-min windows -> parquet"
	@echo ""
	@echo "Profiling         (profiling_code/alexj/):"
	@echo "  profile-first    - FirstCode.scala  : schemas + mean/median/mode + RDD map/reduce"
	@echo "  profile-counts   - CountRecs.scala  : row counts + distinct-value surveys"
	@echo ""
	@echo "Serving layer:"
	@echo "  api              - uvicorn FastAPI on :8765"
	@echo "  web              - Leaflet static frontend on :5173"
	@echo ""
	@echo "Meta:"
	@echo "  setup            - pip install requirements"
	@echo "  all              - download + geo-download + pipeline"

setup:
	$(PIP) install -r requirements.txt

# ---- Data ingestion (code lives in data_ingest/alexj/) --------------------

download:
	$(PYTHON) -m data_ingest.alexj.download

geo-download:
	$(PYTHON) -m data_ingest.alexj.geo_download

# ---- Batch pipeline (code lives in etl_code/alexj/) -----------------------

clean:
	DATA_ROOT=$(DATA_ROOT) $(SPARK_SHELL) $(SPARK_OPTS) -i etl_code/alexj/Clean.scala

geocode: geo-download
	$(PYTHON) -m etl_code.alexj.geocode

features:
	DATA_ROOT=$(DATA_ROOT) $(SPARK_SHELL) $(SPARK_OPTS) -i etl_code/alexj/Features.scala

score:
	$(PYTHON) -m etl_code.alexj.score

pipeline: clean geocode features score

# ---- Streaming (consumer is ETL; producer is glue) ------------------------

stream-up:
	docker compose -f kafka/docker-compose.yml up -d

stream-down:
	docker compose -f kafka/docker-compose.yml down

stream-produce:
	$(PYTHON) -m stream.producer

stream-consume:
	DATA_ROOT=$(DATA_ROOT) $(SPARK_SHELL) $(SPARK_OPTS) --packages $(KAFKA_PKG) -i etl_code/alexj/Consumer.scala

# ---- Profiling (code lives in profiling_code/alexj/) ----------------------
# FirstCode.scala and CountRecs.scala are exploratory surveying scripts
# (record counts, distinct values, mean/median/mode). They aren't part of
# the production pipeline — run them ad-hoc when you want to re-profile a
# dataset after a schema change.

profile-first:
	DATA_ROOT=$(DATA_ROOT) $(SPARK_SHELL) $(SPARK_OPTS) -i profiling_code/alexj/FirstCode.scala

profile-counts:
	DATA_ROOT=$(DATA_ROOT) $(SPARK_SHELL) $(SPARK_OPTS) -i profiling_code/alexj/CountRecs.scala

# ---- Serving --------------------------------------------------------------

api:
	uvicorn api.main:app --reload --host 0.0.0.0 --port 8765

web:
	$(PYTHON) -m http.server 5173 --directory web

all: download geo-download pipeline
