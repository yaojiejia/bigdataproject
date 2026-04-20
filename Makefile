.PHONY: setup download geo-download \
        clean geocode features score pipeline \
        stream-up stream-down stream-produce stream-consume \
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
	@echo "Big-data processing (Scala + Spark, canonical):"
	@echo "  clean          - Clean.scala    : raw CSV -> data/cleaned/*.parquet"
	@echo "  geocode        - geocode.py     : NTA spatial join (Python, geopandas)"
	@echo "  features       - Features.scala : per-NTA aggregations -> data/scores/"
	@echo "  score          - score.py       : 260-row z-score (Python, pandas)"
	@echo "  pipeline       - clean -> geocode -> features -> score"
	@echo ""
	@echo "Streaming (Scala + Spark Structured Streaming):"
	@echo "  stream-up      - start Kafka via docker-compose"
	@echo "  stream-down    - stop Kafka"
	@echo "  stream-produce - producer.py replays 311 records onto Kafka topic"
	@echo "  stream-consume - Consumer.scala : Kafka -> 5-min windows -> parquet"
	@echo ""
	@echo "Data ingestion (Python, not Spark):"
	@echo "  download       - fetch raw CSVs into data/raw/"
	@echo "  geo-download   - fetch NTA GeoJSON + population template"
	@echo ""
	@echo "Serving layer:"
	@echo "  api            - uvicorn FastAPI on :8765"
	@echo "  web            - Leaflet static frontend on :5173"
	@echo ""
	@echo "  setup          - pip install requirements"
	@echo "  all            - download + geo-download + pipeline"

setup:
	$(PIP) install -r requirements.txt

download:
	$(PYTHON) -m pipeline.download

geo-download:
	$(PYTHON) -m pipeline.geo_download

# ---- Batch pipeline -------------------------------------------------------

clean:
	DATA_ROOT=$(DATA_ROOT) $(SPARK_SHELL) $(SPARK_OPTS) -i Clean.scala

geocode: geo-download
	$(PYTHON) -m pipeline.geocode

features:
	DATA_ROOT=$(DATA_ROOT) $(SPARK_SHELL) $(SPARK_OPTS) -i Features.scala

score:
	$(PYTHON) -m pipeline.score

pipeline: clean geocode features score

# ---- Streaming ------------------------------------------------------------

stream-up:
	docker compose -f kafka/docker-compose.yml up -d

stream-down:
	docker compose -f kafka/docker-compose.yml down

stream-produce:
	$(PYTHON) -m stream.producer

stream-consume:
	DATA_ROOT=$(DATA_ROOT) $(SPARK_SHELL) $(SPARK_OPTS) --packages $(KAFKA_PKG) -i Consumer.scala

# ---- Serving --------------------------------------------------------------

api:
	uvicorn api.main:app --reload --host 0.0.0.0 --port 8765

web:
	$(PYTHON) -m http.server 5173 --directory web

all: download geo-download pipeline
