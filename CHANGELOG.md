# Changelog

A record of substantive corrections and improvements made to the pipeline
during development. Entries are terse on purpose; the point is to show an
audit trail, not to repeat what `git log` already says.

The code also carries inline `# Iteration note:` comments at the exact call
sites where these changes happened, with the previous (broken or weaker)
version preserved as commented-out code. Searching the repo for that
string surfaces every fix.

## Pipeline

### Dashboard + notebook: rent-vs-feature prediction chart

**Before:** The analytics surface answered descriptive questions only —
distribution, top/bottom, by-borough, correlation. Nothing in the charts
said "given feature X, what rent should we expect?".

**Change:** Added `rent_vs_feature_fig(df, feature)` to
`pipeline/analytics.py`. For each predictor (crimes_per_1k, felony_share,
avg_score, critical_rate, complaints_per_1k) it builds one figure that
layers (1) a decile-binned median rent line as the headline trend, (2) a
shaded IQR band over the bins, (3) a dashed OLS best-fit line with slope,
Pearson r, R², and a plain-English prediction in a corner annotation, and
(4) per-borough NTA scatter underneath for transparency.

Wired through as `GET /analytics/rent-vs?feature=...` (validated against
the five-predictor allowlist — `median_rent_zori` is rejected so nobody
predicts rent from rent by accident), a new full-width chart card on
`dashboard.html` with a predictor pill selector, and a new section 7
("Figure 5") in `notebooks/analysis.ipynb` that loops the builder over all
five predictors and then prints a summary table of slope / R² / predicted
rent at each feature's median.

**Lesson:** Plotly's JSON figure spec is the right sharing boundary between
a server-rendered dashboard and a notebook — the builder is a pure function
of the parquet, so the two surfaces literally can't drift.

### Repo reorganised into `/data_ingest`, `/etl_code`, `/profiling_code`

**Before:** Everything lived together — Scala jobs at the repo root, Python
modules under `pipeline/`, profiling scripts indistinguishable from
production code at a glance.

**Change:** Adopted the assignment's required layout.
- `data_ingest/alexj/` — `download.py`, `geo_download.py`
- `etl_code/alexj/` — `Clean.scala`, `Features.scala`, `Consumer.scala`, `geocode.py`, `score.py`
- `profiling_code/alexj/` — `FirstCode.scala`, `CountRecs.scala`
- `pipeline/` — now only shared infrastructure (`paths.py`, `analytics.py`)

Python imports moved from relative (`from .paths import ...`) to absolute
(`from pipeline.paths import ...`) so the moved modules can be run as
`python -m data_ingest.alexj.download`, etc. The Makefile targets point at
the new paths. Each of the three top-level directories has a `README.md`
explaining what lives there; each per-member subdirectory has its own
`README.md` with a script-by-script breakdown.

**Lesson:** Rubric-shaped layouts aren't just for graders. The three-way
split (ingest / ETL / profiling) makes it obvious which scripts are on the
critical path of `make pipeline` and which are exploratory one-offs, and
forces a clean boundary between "discover the data" and "transform the
data."

### Clean.scala — `cleanRestaurants`: preserve Lat/Long

**Before:** The restaurant projection dropped `Latitude` / `Longitude`,
mirroring an earlier version of the Scala cleaner. Cleaning succeeded.

**Symptom:** `etl_code/alexj/geocode.py` (then at `pipeline/geocode.py`) blew up with
`KeyError: ['Longitude', 'Latitude']` on the restaurants dataset.
Crime and 311 worked because their cleaners kept the columns.

**Fix:** Added `Latitude`, `Longitude` to both the `.select()` and
the `.na.drop(Seq(...))` lists, matching the other point datasets.
The broken projection is preserved inline as a commented warning.

**Lesson:** Cleaner contracts aren't self-enforcing across stage boundaries.
If feature contracts mattered more here we'd add a schema check in
`etl_code/alexj/geocode.py` that fails fast with a readable error instead
of a pandas `KeyError`.

### Features.scala — tolerate nanosecond timestamps from pandas

**Before:** `spark.read.parquet(...)` on `data/enriched/crime/` failed with
`Illegal Parquet type: INT64 (TIMESTAMP(NANOS, false))`.

**Symptom:** `make features` crashed immediately. The enriched Parquet was
written by `pandas.to_parquet` via pyarrow, which emits nanosecond-precision
timestamps; Spark 3.5 rejects that type by default in its vectorized reader
even for columns the query never touches (the reader validates the whole
file schema up front).

**Fix:** Set `spark.sql.legacy.parquet.nanosAsLong=true` at session level
inside `Features.scala`. The feature aggregates never read any timestamp
column, so the legacy decoding is harmless — it just lets Spark finish
reading the file.

**Lesson:** Parquet dialect differences between pandas-pyarrow and Spark
bite silently. A cleaner long-term fix is to have `geocode.py` cast its
string-typed date columns to `datetime64[ms]` before writing.

## Streaming

### Consumer.scala — pre-create the Kafka topic

**Before:** `readStream.format("kafka").option("subscribe", TOPIC)` was
called directly. Broker had `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`.

**Symptom:** Consumer started before producer → topic didn't exist →
`UnknownTopicOrPartitionException` three times → query terminated.

**Fix:** Added `ensureTopic()` using `AdminClient.createTopics`
before the Spark readStream starts. Idempotent via
`TopicExistsException`. This was first developed against the Python
consumer and carried over when we ported to `Consumer.scala`.

**Lesson:** `auto.create.topics.enable` only fires on *produce*, not
subscribe. Treating the topic as a managed resource the consumer
creates on startup decouples process order.

## Frontend

### web/map.js — API_BASE hostname resolution

**Before:** `const API_BASE = window.location.port === "5173" ? "http://localhost:8000" : "";`

**Symptom:** When the page was loaded from a non-`localhost` hostname
(WSL IP, `127.0.0.1`, etc.) the fetch still went to `http://localhost:8000`
which either wasn't reachable from that browser context or was intercepted
by another process bound to port 8000 on Windows.

**Fix (two steps):**
1. Use `window.location.hostname` so the API call goes to the same host the
   page was loaded from.
2. Moved the API from port 8000 → 8765 to avoid a collision with another
   FastAPI instance on the Windows side that was shadowing our server
   through WSL2's localhost forwarding (returned FastAPI's
   `{"detail":"Not Found"}` for our API routes).

### pipeline/analytics.py + /analytics/* + dashboard.html + analysis.ipynb

**Added.** A single `pipeline/analytics.py` module with four Plotly figure
builders (`distribution_fig`, `top_bottom_fig`, `by_borough_fig`,
`correlation_fig`). Both the FastAPI backend (new `/analytics/*` endpoints)
and `notebooks/analysis.ipynb` import from this module, guaranteeing the
dashboard and the notebook render byte-identical charts for the four
shared views. The notebook layers notebook-only analysis on top
(sub-score decomposition waterfall, safety-vs-affordability scatter,
cheap-but-unsafe / safe-but-expensive outlier tables).

### api/main.py — decode Plotly 6 binary array payloads server-side

**Before:** `fig.to_json()` from Plotly 6 encodes numeric arrays as
`{dtype, bdata(base64)}`. Plotly.js 2.29+ understands this, but the
payload is opaque to anything else that inspects the JSON (tests,
stricter browsers, cached older loaders).

**Fix:** `api/main.py::_decode_bdata` walks the figure dict and rewrites
every `bdata` block into a plain Python list before serializing. Payload
is a little larger on the wire, but the JSON is a vanilla Plotly figure
spec and the dashboard renders the same on every browser.

### web/style.css + index.html — versioned asset query strings

**Before:** `<link rel="stylesheet" href="style.css" />`

**Symptom:** Python's `http.server` doesn't send cache-busting headers.
After redesigning the CSS, the browser kept serving the old unstyled
version even after hard reload.

**Fix:** Append `?v=N` to static asset URLs. Bump N on every visual change.

## Language transition

### Python → Scala for all Spark jobs

**Before:** Big-data processing was split between three PySpark scripts
(`pipeline/clean.py`, `pipeline/features.py`, `stream/consumer.py`) and a
shared SparkSession factory (`pipeline/spark_utils.py`), with the original
assignment's Scala scripts kept as reference.

**Symptom:** The assignment expects Scala + Spark for the batch pipeline.
Two implementations of the same job drift over time.

**Fix:** Replaced the three Python Spark jobs with canonical Scala versions:
`Clean.scala` (all 4 datasets), `Features.scala` (per-NTA aggregation),
`Consumer.scala` (Structured Streaming). Spark tuning (shuffle partitions
= 8, AQE on, broadcast threshold 50 MB, driver 4 GB, NYC timezone, legacy
time parser) moved from `spark_utils.py` into `SPARK_TUNE` in the Makefile
so it applies uniformly to every `spark-shell -i` invocation. The Python
Spark scripts and `pipeline/bench.py` were deleted after schema-parity
verification (identical row counts and column lists across all four
cleaned datasets; identical NTA count in features).

**Lesson:** Keep the language boundary where the tool boundary is. Geocoding
stays Python because geopandas is the right tool; scoring stays Python
because 260 rows don't need Spark; the Kafka producer stays Python because
it's not a Spark job. Everything that actually *uses* Spark is Scala.

## Scoring

### etl_code/alexj/score.py — NaN imputation before z-score

**Before:** z-score on raw columns. A neighborhood with NULL `median_rent_zori`
(no Zillow coverage) got `z = NaN` and cascaded into the final score.

**Fix:** Fill NaN with the column mean **before** z-scoring, so missing
data is neutral (z ≈ 0) rather than catastrophic.

**Caveat:** This is documented in the ethics write-up as a bias: mean
imputation is quietly generous to NTAs with incomplete data, which tend
to be lower-income neighborhoods with thinner civic-data coverage.
