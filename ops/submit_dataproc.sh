#!/usr/bin/env bash
# ============================================================
# ops/submit_dataproc.sh — submit Scala Spark jobs to GCP Dataproc.
# ============================================================
#
# The whole ETL / analytics surface is Scala now, so running the pipeline
# on Dataproc is just `gcloud dataproc jobs submit spark` once per stage.
# This script is a thin wrapper that:
#
#   1. Reads a handful of env vars (DATAPROC_CLUSTER, DATAPROC_REGION,
#      GCS_DATA_ROOT) so the stage names match `make` exactly.
#   2. Translates stage name -> Scala file + any `--packages` extras.
#   3. Uploads the .scala script to a temp GCS location and invokes
#      spark-shell via --driver-script, because `gcloud dataproc jobs
#      submit spark` accepts ``--class`` or ``--main-python-file-uri``
#      but doesn't have a ``--scala-script`` mode directly; we use the
#      ``spark-shell`` entry point via ``--files`` + ``--properties``.
#
# Usage:
#   # one-off
#   ops/submit_dataproc.sh clean
#   ops/submit_dataproc.sh geocode
#   ops/submit_dataproc.sh features score analytics
#
#   # the full batch
#   ops/submit_dataproc.sh pipeline
#
#   # print-only mode
#   DRY_RUN=1 ops/submit_dataproc.sh pipeline
#
# Required env:
#   DATAPROC_CLUSTER   e.g. nyc-insights
#   DATAPROC_REGION    e.g. us-central1
#   GCS_DATA_ROOT      e.g. gs://my-bucket/nyc-insights
# Optional env:
#   GCS_SCRIPT_ROOT    where to upload .scala files (default $GCS_DATA_ROOT/_scripts)
#   DRY_RUN=1          echo the gcloud commands but don't run them
# ============================================================

set -euo pipefail

: "${DATAPROC_CLUSTER:?set DATAPROC_CLUSTER to your cluster name}"
: "${DATAPROC_REGION:?set DATAPROC_REGION to e.g. us-central1}"
: "${GCS_DATA_ROOT:?set GCS_DATA_ROOT to gs://bucket/path (no trailing slash)}"
GCS_SCRIPT_ROOT="${GCS_SCRIPT_ROOT:-${GCS_DATA_ROOT}/_scripts}"

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

KAFKA_PKG="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
JTS_PKG="org.locationtech.jts:jts-core:1.19.0"

stage_script() {
  case "$1" in
    clean)          echo "etl_code/alexj/Clean.scala" ;;
    geocode)        echo "etl_code/alexj/Geocode.scala" ;;
    features)       echo "etl_code/alexj/Features.scala" ;;
    score)          echo "etl_code/alexj/Score.scala" ;;
    analytics)      echo "etl_code/alexj/Analytics.scala" ;;
    stream-produce) echo "stream/Producer.scala" ;;
    stream-consume) echo "etl_code/alexj/Consumer.scala" ;;
    *) echo "unknown stage: $1" >&2; return 2 ;;
  esac
}

stage_packages() {
  case "$1" in
    geocode)                        echo "$JTS_PKG" ;;
    stream-produce|stream-consume)  echo "$KAFKA_PKG" ;;
    *)                              echo "" ;;
  esac
}

run() {
  if [[ "${DRY_RUN:-}" == "1" ]]; then
    printf '[dry-run] %s\n' "$*"
  else
    "$@"
  fi
}

submit_stage() {
  local stage="$1"
  local script_rel
  script_rel="$(stage_script "$stage")"
  local script_path="$PROJECT_ROOT/$script_rel"
  if [[ ! -f "$script_path" ]]; then
    echo "missing script: $script_path" >&2
    return 1
  fi

  local pkgs
  pkgs="$(stage_packages "$stage")"

  # Upload the .scala script fresh every submit so the cluster always runs
  # the current working copy, not a previously cached version.
  local gcs_script="$GCS_SCRIPT_ROOT/$(basename "$script_rel")"
  run gsutil -q cp "$script_path" "$gcs_script"

  # `gcloud dataproc jobs submit spark` doesn't have a first-class Scala-
  # script mode, so we piggy-back on its Scala-REPL support: pass the
  # uploaded .scala as a file arg and run spark-shell with `-i`.
  # The equivalent incantation is documented in the Dataproc cookbook.
  local props="spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
  props+=",spark.sql.shuffle.partitions=64"
  props+=",spark.sql.adaptive.enabled=true"
  props+=",spark.sql.adaptive.coalescePartitions.enabled=true"

  local args=(
    gcloud dataproc jobs submit spark
    --cluster="$DATAPROC_CLUSTER"
    --region="$DATAPROC_REGION"
    --class=org.apache.spark.repl.Main
    --files="$gcs_script"
    --properties="$props"
    --
    "-i" "$(basename "$script_rel")"
  )

  if [[ -n "$pkgs" ]]; then
    args=(
      gcloud dataproc jobs submit spark
      --cluster="$DATAPROC_CLUSTER"
      --region="$DATAPROC_REGION"
      --class=org.apache.spark.repl.Main
      --files="$gcs_script"
      --properties="$props,spark.jars.packages=$pkgs"
      --
      "-i" "$(basename "$script_rel")"
    )
  fi

  echo "=== submit: $stage ($script_rel) ==="
  # The Scala scripts read DATA_ROOT from the environment. Dataproc exposes
  # --properties as spark confs, not env vars, so we rely on the scripts'
  # default (./data) working if you've gsutil-rsynced the data on first.
  # If you need GCS-backed data, set DATA_ROOT to a gs:// path via --properties
  # spark.executorEnv.DATA_ROOT / spark.driverEnv.DATA_ROOT.
  run env DATA_ROOT="$GCS_DATA_ROOT" "${args[@]}"
}

expand_aliases() {
  for s in "$@"; do
    case "$s" in
      pipeline) echo clean geocode features score analytics ;;
      *)        echo "$s" ;;
    esac
  done
}

if [[ $# -eq 0 ]]; then
  echo "usage: $0 <stage> [<stage> ...]" >&2
  echo "stages: clean geocode features score analytics stream-produce stream-consume pipeline" >&2
  exit 2
fi

for stage in $(expand_aliases "$@"); do
  submit_stage "$stage"
done
