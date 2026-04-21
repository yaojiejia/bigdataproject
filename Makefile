.PHONY: setup download geo-download \
        clean geocode features score analytics pipeline \
        stream-up stream-down stream-produce stream-consume \
        profile-first profile-counts \
        web all help dataproc-submit

PYTHON       ?= python3
PIP          ?= pip3
DATA_ROOT    ?= $(PWD)/data
SPARK_SHELL  ?= spark-shell
SPARK_MASTER ?= local[*]

# Kafka-on-Spark integration (shared by Consumer + Producer).
KAFKA_PKG    ?= org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1

# JTS for Geocode.scala's point-in-polygon join. ~5 MB jar, pure-Java,
# resolved via ivy on first run and cached in ~/.ivy2.
JTS_PKG      ?= org.locationtech.jts:jts-core:1.19.0

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

WEB_PORT     ?= 5173

help:
	@echo "Data ingestion  (data_ingest/alexj/   — Python, HTTP glue only):"
	@echo "  download         - fetch 4 raw CSVs into data/raw/"
	@echo "  geo-download     - fetch NTA GeoJSON + population template"
	@echo ""
	@echo "ETL / batch       (etl_code/alexj/    — Scala/Spark):"
	@echo "  clean            - Clean.scala     : raw CSV -> data/cleaned/*.parquet"
	@echo "  geocode          - Geocode.scala   : JTS point-in-polygon + ZIP->NTA modal"
	@echo "  features         - Features.scala  : per-NTA aggregations -> data/scores/"
	@echo "  score            - Score.scala     : z-scores + weighted sum -> 0-100"
	@echo "  analytics        - Analytics.scala : pre-aggregated parquet for dashboard"
	@echo "  pipeline         - clean -> geocode -> features -> score -> analytics"
	@echo ""
	@echo "ETL / streaming   (stream/ + etl_code/alexj/ — Scala/Spark):"
	@echo "  stream-up        - start Kafka via docker-compose"
	@echo "  stream-down      - stop Kafka"
	@echo "  stream-produce   - Producer.scala  : replay 311 records onto Kafka"
	@echo "  stream-consume   - Consumer.scala  : Kafka -> 5-min windows -> parquet"
	@echo ""
	@echo "Profiling         (profiling_code/alexj/ — Scala/Spark):"
	@echo "  profile-first    - FirstCode.scala  : schemas + mean/median/mode + RDD map/reduce"
	@echo "  profile-counts   - CountRecs.scala  : row counts + distinct-value surveys"
	@echo ""
	@echo "Serving:"
	@echo "  web              - static file server on :$(WEB_PORT) (Python stdlib as convenience)"
	@echo ""
	@echo "Dataproc:"
	@echo "  dataproc-submit  - ops/submit_dataproc.sh (wraps 'gcloud dataproc jobs submit spark')"
	@echo ""
	@echo "Meta:"
	@echo "  setup            - pip install requirements (just 'requests' now)"
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
	DATA_ROOT=$(DATA_ROOT) $(SPARK_SHELL) $(SPARK_OPTS) --packages $(JTS_PKG) -i etl_code/alexj/Geocode.scala

features:
	DATA_ROOT=$(DATA_ROOT) $(SPARK_SHELL) $(SPARK_OPTS) -i etl_code/alexj/Features.scala

score:
	DATA_ROOT=$(DATA_ROOT) $(SPARK_SHELL) $(SPARK_OPTS) -i etl_code/alexj/Score.scala

analytics:
	DATA_ROOT=$(DATA_ROOT) $(SPARK_SHELL) $(SPARK_OPTS) -i etl_code/alexj/Analytics.scala

pipeline: clean geocode features score analytics

# ---- Streaming ------------------------------------------------------------

stream-up:
	docker compose -f kafka/docker-compose.yml up -d

stream-down:
	docker compose -f kafka/docker-compose.yml down

stream-produce:
	DATA_ROOT=$(DATA_ROOT) $(SPARK_SHELL) $(SPARK_OPTS) --packages $(KAFKA_PKG) -i stream/Producer.scala

stream-consume:
	DATA_ROOT=$(DATA_ROOT) $(SPARK_SHELL) $(SPARK_OPTS) --packages $(KAFKA_PKG) -i etl_code/alexj/Consumer.scala

# ---- Profiling (code lives in profiling_code/alexj/) ----------------------
# Profiling is exploratory. Run ad-hoc when you want to re-profile a
# dataset after a schema change; not part of the production pipeline.

profile-first:
	DATA_ROOT=$(DATA_ROOT) $(SPARK_SHELL) $(SPARK_OPTS) -i profiling_code/alexj/FirstCode.scala

profile-counts:
	DATA_ROOT=$(DATA_ROOT) $(SPARK_SHELL) $(SPARK_OPTS) -i profiling_code/alexj/CountRecs.scala

# ---- Static web frontend --------------------------------------------------
# The dashboard + map are a pure static bundle; they read parquet directly
# with hyparquet in the browser. Any static server works — we use Python's
# http.server here only because it's already on the machine (it is NOT part
# of the big-data surface). Substitute `npx http-server web -p $(WEB_PORT)`
# or `caddy file-server --root web --listen :$(WEB_PORT)` if you prefer.

web:
	$(PYTHON) -m http.server $(WEB_PORT) --directory $(PWD) --bind 127.0.0.1

# ---- Dataproc helper ------------------------------------------------------

dataproc-submit:
	bash ops/submit_dataproc.sh $(ARGS)

all: download geo-download pipeline
