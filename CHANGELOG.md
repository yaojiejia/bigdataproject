# Changelog

A record of substantive corrections and improvements made to the pipeline
during development. Entries are terse on purpose; the point is to show an
audit trail, not to repeat what `git log` already says.

The code also carries inline `# Iteration note:` comments at the exact call
sites where these changes happened, with the previous (broken or weaker)
version preserved as commented-out code. Searching the repo for that
string surfaces every fix.

## Architecture: Kafka / streaming layer removed

**Before:** The pipeline shipped a streaming arm alongside the batch
pipeline: `stream/Producer.scala` replayed enriched 311 rows onto a
Kafka topic (`complaints311`), and `etl_code/alexj/Consumer.scala` ran a
Spark Structured Streaming query that aggregated 5-minute tumbling
windows per NTA and wrote them to `data/stream/latest.parquet` for the
frontend to poll. Kafka ran in a single-broker Docker Compose stack
under `kafka/docker-compose.yml`, and the Makefile had
`stream-up / stream-down / stream-produce / stream-consume` targets.

**Change:** Removed the streaming layer end-to-end. The batch pipeline
— `Clean → Geocode → Features → Score → Analytics` — is now the whole
story. Deleted files:

- `etl_code/alexj/Consumer.scala`
- `stream/Producer.scala` (and the empty `stream/` directory)
- `kafka/docker-compose.yml` (and the empty `kafka/` directory)
- the `data/stream/` output tree
- the `stream-*` Makefile targets, the `KAFKA_BOOTSTRAP` env var in
  `ops/submit_dataproc.sh`, and the `spark-sql-kafka-0-10` package in
  the Dataproc wrapper

**Rationale:** The streaming path was always a *simulation* over a
static CSV — NYC Open Data doesn't expose 311 as a push feed — so it
existed to demonstrate the architecture rather than to deliver a
real-time signal. Maintaining it had real costs: a Docker dependency
in the local dev loop, `spark-sql-kafka` on the `--packages` line on
every cluster submission, a second codepath for aggregations that
already live in `Features.scala`, and an extra parquet path the
frontend had to know about. Scoping the project to batch-only keeps
the big-data surface uniform (one language, one trigger model, one
output layout) and makes the repo smaller and easier to follow.

**Docs:** `PROJECT.md`, `architect.md`, `SCALABILITY.md`, and the
`etl_code/` / `etl_code/alexj/` READMEs were all rewritten to drop the
streaming layer from the diagrams, component tables, Makefile
reference, and prerequisites. Historical streaming-specific entries
below (e.g. *Consumer.scala — pre-create the Kafka topic*) are kept
for the audit trail: they describe real fixes to code that existed at
the time and have since been deleted.

## Architecture: Scala-only big-data pipeline on Dataproc

**Before:** Big-data processing was split between Scala Spark (clean,
features, streaming consumer) and Python (geocoding via geopandas,
scoring via pandas, analytics via pandas + Plotly Python, Kafka producer
via `kafka-python`). A FastAPI backend served pre-computed figures as
Plotly JSON and the GeoJSON map layer. Jupyter notebooks mirrored the
dashboard via the shared `pipeline.analytics` module.

**Change:** Collapsed to a single-language big-data surface. Every stage
that touches data is now Scala + Spark, and every stage runs unmodified
on GCP Dataproc:

- `etl_code/alexj/Geocode.scala` replaces `geocode.py` — JTS point-in-
  polygon via a broadcast of 260 parsed NTA polygons + a Spark UDF.
  ZIP→NTA lookup derived via `groupBy` + window function on the enriched
  restaurants dataset. Parses GeoJSON with Jackson (already on the Spark
  classpath) so no extra deps beyond the JTS jar.
- `etl_code/alexj/Score.scala` replaces `score.py` — z-score + weighted
  sum + min-max rescale in Spark SQL. Population std deviation
  (`stddev_pop`) used to match the pandas reference byte-for-byte.
- `etl_code/alexj/Analytics.scala` replaces `pipeline/analytics.py` —
  writes 9 pre-aggregated parquet tables (summary, distribution,
  top_bottom, borough_box, borough_points, correlation,
  rent_vs_feature_{bins,ols,points}) under `data/analytics/`. The
  frontend reads them directly.
- `stream/Producer.scala` replaces `stream/producer.py` — uses
  `org.apache.kafka.clients.producer.KafkaProducer` directly. *(This
  file and the rest of the streaming layer were later removed — see
  "Kafka / streaming layer removed" at the top of this changelog.)*

Python survives only in `data_ingest/alexj/` (HTTP downloads via
`requests` + a tiny self-contained `paths.py`). The FastAPI backend,
`pipeline/` package, `notebooks/` tree, and `__init__.py` shims are all
deleted. `requirements.txt` is trimmed to a single line: `requests`.

**Serving:** No backend. `web/` is a pure static bundle.
`web/lib/parquet.js` wraps [hyparquet](https://github.com/hyparam/hyparquet)
to fetch parquet over HTTP; `web/lib/figures.js` builds Plotly figure
specs from the parquet aggregates (mirroring the old Python builders);
`web/map.js` and `web/dashboard.js` compose these into the two pages.
`make web` serves `$(PWD)` via `python -m http.server :5173` as a
zero-dep dev aid; production publishes `web/` + `data/` to a GCS
website-hosting bucket.

**Dataproc:** `ops/submit_dataproc.sh <stage>` uploads the Scala script
to GCS, then runs `gcloud dataproc jobs submit spark` with the right
`--packages` (JTS for geocode) and `$DATA_ROOT` pointed at a GCS bucket.
Every stage (clean, geocode, features, score, analytics, and the two
profiling scripts) runs unmodified.

**Docs:** `README.md`, `PROJECT.md`, `architect.md`, and `SCALABILITY.md`
rewritten to reflect the Scala-only, no-backend, Dataproc-native
architecture.

**Lesson:** The earlier "use Python where Python is the right tool"
posture was correct for local development but violated the rubric's
"all big-data processing runs on the cluster" requirement. Moving the
scoring + analytics stages into Spark adds measurable driver startup
overhead per run but gives us a single language for code review, one
deployment target (Dataproc), and a uniform answer to "where does this
run?".

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

## Streaming (historical — feature since removed)

The entries below document fixes made to the Kafka streaming layer
while it existed. The layer was removed in its entirety — see
"Kafka / streaming layer removed" at the top of this changelog — and
these entries are kept only as an audit trail of decisions made at
the time.

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
to be lower-income neighborhoods with thinner civic-data coverage. The
next two entries address the two worst failure modes of this policy.

### Score.scala — N/A gate for data-starved NTAs

**Before:** Mean imputation (see previous entry) meant every NTA got a
finite `newcomer_score`, regardless of how much data was actually behind
it. For `MN0191` — *The Battery-Governors Island-Ellis Island-Liberty
Island*, four effectively uninhabited islands grouped into one NTA —
this was catastrophic: the NTA had no ZORI rent and only 4 restaurant
inspections with an unrealistically low `avg_score` of 1.25 (food
carts). Mean-imputed rent + a cartoonishly clean food-safety signal
pushed its raw `newcomer_score` to the *maximum* in the city, so after
min-max rescale it got **100.0 / 100** — ranked #1 in NYC.

**Fix:** Added an explicit data-starved gate in `Score.scala`. Any NTA
with `median_rent_zori IS NULL` **and** `n_inspections < 20` has its
`newcomer_score` / `newcomer_score_100` nulled out. The `score_available`
boolean is written alongside so downstream consumers can distinguish
"unscored" from a genuine low score. Critically, the gate runs
**before** min-max rescale, so removing the Battery outlier also stops
its inflated max from squashing everybody else's 0–100 number.

Frontend consequence: `web/map.js` now renders gated NTAs in the neutral
"rule" colour on the score choropleth, and the detail panel shows an
italic "N/A" with the note *"Not enough rent or restaurant data to
score."* instead of a silent empty badge.

**Lesson:** Imputation is a rendering policy, not a truth policy. The
right move is to keep imputation as the default (so downstream math
doesn't NaN out) but short-circuit the output for rows where the
imputation is *the majority of the signal*.

### Features.scala — curated rent overrides for coverage-gap NTAs

**Before:** Zillow's ZORI only covers ZIPs with an active residential
rental market and a sufficient listing count. Four populous Manhattan
NTAs — `MN0301 Chinatown-Two Bridges`, `MN0402 Hell's Kitchen`,
`MN0601 Stuyvesant Town-Peter Cooper Village`, `MN0602 Gramercy` — came
out as `median_rent_zori = NULL` and got mean-imputed by `Score.scala`.
They have plenty of restaurant inspections, so the new N/A gate doesn't
catch them either; they just silently got an "average" rent signal on a
dimension that carries 30% of the weight. For neighborhoods that are in
reality some of the most expensive in Manhattan, this understated
affordability cost by hundreds of dollars a month.

**Fix:** Added a `RENT_OVERRIDES: Map[String, Double]` in
`Features.scala`, keyed by NTA 2020 code, with curated medians collected
from contemporary listings aggregators (Zumper / StreetEasy /
RentCafe):

| NTA code | NTA name                              | Override ($/mo) |
|----------|---------------------------------------|-----------------|
| MN0301   | Chinatown-Two Bridges                 | 5,500           |
| MN0402   | Hell's Kitchen                        | 4,475           |
| MN0601   | Stuyvesant Town-Peter Cooper Village  | 5,775           |
| MN0602   | Gramercy                              | 6,300           |

Applied via `coalesce(median_rent_zori, element_at(typedLit(map),
nta_code))` **after** the ZIP→NTA aggregation, so any observed ZORI
value always wins. A new `median_rent_imputed` boolean is written
alongside every row so the provenance is preserved in the parquet.

**Why in `features` and not elsewhere:** `clean` works in ZIP space so
it doesn't know the NTA yet; `geocode` is still per-ZIP; `score` and
`analytics` are consumers. `features` is the single stage that emits
one row per NTA with the final rent value, which makes it the right
place for an NTA-keyed override. Keeping the override out of `score`
in particular means any change to the table only re-runs the (cheap)
feature aggregation, not the full scoring path.

**Lesson:** "Missing data is a signal" (the N/A gate above) and "missing
data is fillable from elsewhere" are both valid; the distinction is
whether a human-auditable substitute exists. The override map makes
that judgment explicit and reviewable in one place.

## Frontend redesign

### web/ — editorial light theme, stripped AI-dashboard flourishes

**Before:** The frontend was a dark slate/indigo dashboard (`#0f172a`
background, `#818cf8` indigo-400 accent, pulsing status dot, glowing
brand dot with a `box-shadow` halo, animated-arrow CTA, compass-in-heart
empty-state icon, glassmorphism sticky header with `backdrop-filter:
blur(10px)`, intro paragraphs that name-dropped
`etl_code/alexj/Analytics.scala` and explained the Scala/Spark/Plotly
stack to the reader). The individual pieces were competent but the sum
read unmistakably as an LLM-generated "minimal dark dashboard".

**Change:** Full editorial light theme across `web/style.css`,
`web/dashboard.css`, `web/index.html`, `web/dashboard.html`,
`web/map.js`, and `web/lib/figures.js`:

- **Palette.** Warm paper `#f6f3ec` background, white `#ffffff` cards,
  warm near-black `#1a1815` ink (never pure `#000`). One accent, which
  is simply the ink colour. Data colours are editorial —
  `#3a5a40` forest for *good*, `#8b3a3a` brick for *bad*, `#b08837`
  ochre for *warn*. Borough palette swapped from rainbow primaries to
  navy / forest / ochre / brick / umber.
- **Typography.** Every headline (sidebar H1, dashboard H1, chart card
  titles, score badge, summary-card numbers) now uses a *system* serif
  stack (`Charter, Iowan Old Style, Cambria, Source Serif Pro,
  Georgia`). Inter stays for body and UI. Deliberately no Google
  webfont import for display — system serifs feel handcrafted and
  don't flash-of-unstyled into something generic.
- **Chrome removed.** Glowing brand dot, pulsing status-dot animation,
  animated-arrow CTA, compass empty-state SVG, glassmorphism header
  blur, linear-gradient page background — all deleted. Replaced with
  plain rules, typographic cues (`No. 01 · New York` masthead eyebrow,
  em-dash-prefixed empty states), and single-pixel borders.
- **Copy.** Sidebar H1 `Find your neighborhood` → `The Neighborhood
  Index.` (italicised "Index.", editorial period). Dashboard intro
  rewritten to describe *what the reader is looking at* instead of
  describing the tech stack. Chart hints collapsed to one factual
  sentence each. Status line is now informational ("262 neighborhoods
  · updated from the latest pipeline run") instead of a boast
  ("Dashboard rendered from Scala/Spark parquet.").
- **Map retheme.** Choropleth ramps swapped: indigo → forest-green
  sequential for higher-is-better (`newcomer score`); warm earth
  (cream → umber → burnt sienna) for higher-is-worse metrics. Polygon
  hover/select stroke matches the new ink. Basemap tiles switched
  from `light_all` → `light_nolabels` so street labels don't fight
  with the NTA name tooltip.
- **Plotly retheme.** White paper backgrounds, warm-gray axes, muted
  tick labels, ink-on-paper trend lines. Correlation heatmap uses a
  custom brick → paper → ink-navy scale instead of generic RdBu. IQR
  band in the rent-vs-feature chart is a 7%-opacity ink tint instead
  of translucent indigo. Annotation boxes are white-on-white with a
  1px rule, not semi-transparent slate.

**Lesson:** The single biggest visual signal that something is
AI-generated is the "minimal dark dashboard with a neon accent"
template. Flipping to a warm-paper light theme with a serif display
stack is a one-afternoon change that moves the whole piece out of that
aesthetic with no information-architecture cost.
