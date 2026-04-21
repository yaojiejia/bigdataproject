.PHONY: setup download geo-download \
        clean geocode features score analytics pipeline \
        profile-first profile-counts \
        web all help dataproc-submit \
        clean-data clean-derived

PYTHON       ?= python3
PIP          ?= pip3
DATA_ROOT    ?= $(PWD)/data
SPARK_SHELL  ?= spark-shell
SPARK_MASTER ?= local[*]

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
	@echo "  clean-derived    - delete pipeline outputs under \$$DATA_ROOT (cleaned/enriched/scores/analytics); keeps raw/ and geo/"
	@echo "  clean-data       - DESTRUCTIVE: delete EVERYTHING under \$$DATA_ROOT (incl. raw/ and geo/)"

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

# ---- Data hygiene ---------------------------------------------------------
# clean-derived  : wipe only pipeline outputs (cleaned/enriched/scores/analytics).
#                  Re-run `make pipeline` to rebuild; no re-download needed.
# clean-data     : wipe EVERYTHING under $(DATA_ROOT), including raw/ + geo/.
#                  You'll need `make download` + `make geo-download` afterwards.
#
# Both targets refuse to run if DATA_ROOT is empty or "/", as a guard against
# `make clean-data DATA_ROOT=""` doing something catastrophic.

clean-derived:
	@if [ -z "$(DATA_ROOT)" ] || [ "$(DATA_ROOT)" = "/" ]; then \
		echo "refusing to delete from empty or root DATA_ROOT='$(DATA_ROOT)'" >&2; exit 1; \
	fi
	@echo "removing pipeline outputs under $(DATA_ROOT)"
	rm -rf -- "$(DATA_ROOT)/cleaned" "$(DATA_ROOT)/enriched" "$(DATA_ROOT)/scores" "$(DATA_ROOT)/analytics"

clean-data:
	@if [ -z "$(DATA_ROOT)" ] || [ "$(DATA_ROOT)" = "/" ]; then \
		echo "refusing to delete from empty or root DATA_ROOT='$(DATA_ROOT)'" >&2; exit 1; \
	fi
	@echo "removing ALL contents under $(DATA_ROOT) (raw, geo, cleaned, enriched, scores, analytics)"
	rm -rf -- "$(DATA_ROOT)"
	mkdir -p -- "$(DATA_ROOT)"
