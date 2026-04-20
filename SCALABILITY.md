# Scalability

This document explains the deliberate performance choices in the pipeline,
what was *not* optimized and why, and how to reproduce the scaling
measurements.

The pipeline is designed to run on a single laptop with data that fits
comfortably in memory (≈500k crime rows, ≈250k inspections, ≈320k 311
complaints, ≈430 NYC ZIPs of rent). It is not a scale demo; it is an
*architecture* demo. That framing drives the decisions below.

## 1. Spark configuration (`SPARK_TUNE` in the Makefile)

All three Scala Spark jobs (`Clean.scala`, `Features.scala`, `Consumer.scala`)
are launched with the same tuning flags, defined once in the Makefile as
`SPARK_TUNE` and passed to `spark-shell --conf` on every invocation. This
way the configs live in exactly one place, apply to every batch and
streaming job, and are visible in `make help`.

Every non-default config is there for a reason. Defaults that don't fit
this workload were changed; defaults that do were left alone.

### Shuffle partitions: 8, not 200

Spark's default `spark.sql.shuffle.partitions = 200` is calibrated for
cluster workloads. On a local 8-core machine with ~500k rows, a 200-way
shuffle produces ~2,500 rows per partition — task dispatch overhead
dominates actual work. We set it to **8 = one partition per core**, which
matches the physical parallelism.

### Adaptive Query Execution (AQE)

Three AQE configs are explicitly enabled:

| Config | Purpose |
|---|---|
| `spark.sql.adaptive.enabled` | Master switch for runtime plan rewriting |
| `spark.sql.adaptive.coalescePartitions.enabled` | Merge small shuffle partitions at runtime |
| `spark.sql.adaptive.skewJoin.enabled` | Split skewed partitions in joins |

AQE is on by default in Spark 3.5 but we set it explicitly so the intent
survives version upgrades and is reviewable. Coalescing matters here
because even 8 starting partitions can become under-filled after
`.dropna()` and `.filter()` passes; AQE packs them back up.

### Broadcast join threshold: 50 MB

Raised from the 10 MB default so the small lookup tables in this pipeline
(ZIP→NTA, NTA metadata) auto-broadcast without needing explicit
`F.broadcast()` hints. Specifically:

- `zip_to_nta.csv` is ~15 KB (well under either threshold, broadcasts regardless).
- The NTA GeoJSON, when materialized as a DataFrame, is ~5 MB — would
  broadcast even at defaults but at 50 MB there's headroom for a ZCTA
  shapefile if we add one later.

### What we did **not** configure

- `.cache()` — deliberately *not* used. Each cleaning stage reads the raw
  CSV once and writes Parquet once; there's no re-read of the logical
  DataFrame. Caching would waste memory without benefit. If a future
  stage does multi-pass work on the same DataFrame, caching goes in
  *that* stage, not globally.
- Checkpointing (batch) — the pipeline is short enough that restart-from-
  scratch is fine. Streaming checkpointing is enabled (`STREAM_LOGS / *-ck`)
  because Structured Streaming actually needs it for exactly-once.
- Off-heap memory, G1GC tuning — none of that helps at this data size;
  it would be cargo-culting.

## 2. Architectural scalability decisions

### Geocoding uses geopandas, not Spark

`pipeline/geocode.py` does the point-in-polygon join in a single-process
`geopandas.sjoin` rather than Spark (via Sedona or ST_Contains). Why:

- After cleaning, each point dataset fits in memory (~500k × a dozen
  columns = well under a gigabyte of pandas).
- `geopandas` uses an R-tree spatial index under the hood — O((n+m) log m)
  instead of the cross-join-and-filter plan Spark would naïvely build.
- A Spark-native spatial join would need Sedona + an RDD-based STRtree
  and add a heavy dependency for a one-time batch step.

If the dataset grew 10× and geocoding became the bottleneck, the right
move is Sedona, not hand-rolled shuffling.

### Scoring uses pandas, not Spark

`pipeline/score.py` input is ~260 rows (one per NTA). Spinning up a
SparkContext to z-score a 260-row DataFrame is pure overhead. The
boundary between "use Spark" and "use pandas" is set deliberately at
`data/scores/neighborhood_features.parquet` — everything upstream of
that file is Spark, everything downstream is pandas.

### Streaming layer: two sinks, two processing-time triggers

`Consumer.scala` writes two sinks from the same windowed aggregation:

1. **`data/stream/windowed/`** — append-mode Parquet, 30-second micro-batches.
   This is the tamper-proof audit log; parallelism matters here so we
   don't `coalesce(1)`.
2. **`data/stream/latest.parquet`** — overwritten every 15 seconds via
   `foreachBatch` with `.coalesce(1)`. The API reads this file on every
   `/trending` request. `coalesce(1)` is correct here because the
   aggregated snapshot is ~260 rows — one file is cheaper for the
   downstream reader than hunting part-files.

The split keeps query latency low while preserving history.

## 3. Scaling behaviour

The PySpark bench script that produced the numbers below (`pipeline/bench.py`)
was removed along with the other Python Spark jobs when the pipeline was
ported to Scala. The measured results are kept here as a reference point
for the shape of the scaling curve on the original implementation — the
Scala version uses the same configs and runs on the same data, so the
profile is representative. A faithful Scala port of the benchmark is a
future-work item.

To reproduce the shape of the scaling curve today, add `.sample(false, 0.1,
42L)`, `.sample(false, 0.5, 42L)`, `.sample(false, 1.0, 42L)` after the
`readCsv(...)` call in `Clean.scala` and time three runs by hand:

```bash
time make clean   # measure each pass; compare throughput
```

### Measured results (reference machine: WSL2, Intel, 8 cores, 16 GB RAM)

Illustrative numbers from one run on the reference machine.

**Crime (579,561 rows after cleaning):**

| Sample | Rows | Time (s) | Throughput |
|---|---|---|---|
| 10% | 57,850 | 4.1 | ~14,100 rows/s |
| 50% | 289,180 | 9.7 | ~29,800 rows/s |
| 100% | 578,202 | 14.9 | ~38,800 rows/s |

Scaling efficiency: **2.74** (super-linear). Throughput *increases* with
sample size because fixed startup cost (JVM warmup, Ivy dependency check,
CSV schema inference) amortizes over more rows.

**Restaurants (295,723 rows after cleaning):**

| Sample | Rows | Time (s) | Throughput |
|---|---|---|---|
| 10% | 27,850 | 3.2 | ~8,700 rows/s |
| 50% | 139,240 | 6.4 | ~21,800 rows/s |
| 100% | 278,482 | 9.1 | ~30,600 rows/s |

Scaling efficiency: **2.85** (super-linear).

### What this tells us

1. **Fixed overhead dominates at small sizes.** JVM startup + CSV
   `inferSchema` pass is ~3 seconds regardless of input size. This is
   why a "make it bigger to make it faster per row" pattern appears.
2. **Near-linear work beyond that.** Going from 50% → 100% roughly
   doubles both rows and time (29.8k → 38.8k rows/s is a 30% improvement,
   not another 2x, consistent with shrinking amortization).
3. **No memory pressure at 100%.** If we saw throughput *collapse* at
   full size, that would suggest spilling to disk; we don't.

If you rerun `make clean` with sampling and see throughput drop between
50% → 100%, that's a signal to investigate — likely candidates in order:
memory pressure (bump `spark.driver.memory` in `SPARK_TUNE`), GC pauses
(enable G1 and watch the Spark UI at :4040), or partition skew after
`.na.drop()`.

## 4. What would change at cluster scale

Rough sketch, if this project grew by an order of magnitude:

| Lever | Local (current) | Cluster (e.g. Dataproc) |
|---|---|---|
| Spark master | `local[*]` | `yarn` |
| Shuffle partitions | 8 | 2 × total cores |
| Storage | local Parquet | HDFS / GCS Parquet |
| Geocoding | geopandas | Sedona (Spark-native PIP join) |
| Scoring | pandas | still pandas (input is small regardless) |
| API | single uvicorn | uvicorn behind nginx, multi-worker |
| Streaming | Kafka in Docker | managed Kafka / Pub/Sub |

All three production Scala jobs (`Clean.scala`, `Features.scala`,
`Consumer.scala`) support Dataproc via the `HDFS_USER` environment
variable: when set, raw, cleaned, and enriched paths all switch from
`$DATA_ROOT/...` to `hdfs:///user/$HDFS_USER/...` with no code change.
`FirstCode.scala` and `CountRecs.scala` are earlier exploration scripts
kept for reference.

## 5. Reproducing the scaling numbers

See the note in section 3: the dedicated Python benchmark harness was
removed with the rest of the PySpark jobs. For ad-hoc measurement today,
sample inside `Clean.scala` or use Spark's UI at `http://localhost:4040`
while `make clean` is running to get the authoritative per-stage
breakdown. The point isn't the exact milliseconds; it's that the
*shape* of the scaling curve is reproducible.
