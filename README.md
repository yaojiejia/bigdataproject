# NYC Neighborhood Insights

An end-to-end big-data pipeline and interactive site that helps newcomers evaluate NYC neighborhoods across **safety**, **food quality**, **quality of life (311)**, and **affordability (rent)**.

Every computational stage is **Scala + Apache Spark**. Python survives only as HTTP glue in `data_ingest/`. The UI is a **static bundle**: `hyparquet` reads Spark-written parquet in the browser and Plotly.js / Leaflet render it. No backend.

## What it does

1. **Ingests** four public datasets: NYPD crime complaints, DOHMH restaurant inspections, 311 food-safety complaints, and Zillow ZORI rent. (Python + `requests` only.)
2. **Cleans** with Scala/Spark: upper/trim text keys, drop null joins/coords, derive `IS_FELONY` / `IS_CRITICAL`, melt Zillow wide-to-long via `stack(n, ...)` and a `row_number()` window to keep latest non-null month per ZIP.
3. **Normalizes geography** by spatially joining each point record to a 2020 NYC Neighborhood Tabulation Area (NTA, ~260 polygons) with a **JTS point-in-polygon** broadcast UDF in Spark.
4. **Aggregates per-NTA features**: crime totals / felony share, inspection counts + avg score + critical rate, 311 complaint counts, median ZORI rent, plus per-1k intensity if population is provided.
5. **Scores** each NTA via a weighted z-score (safety 0.30, food_safety 0.25, cleanliness 0.15, affordability 0.30, rescaled to 0 to 100).
6. **Pre-computes dashboard aggregates** (nine tiny single-file parquet tables) so the browser does zero math.
7. **Renders** a Leaflet choropleth (`index.html`) and Plotly dashboard (`dashboard.html`) that fetch parquet directly.

## Design goals

1. **One language for big-data work.** Every data-processing stage is Scala + Spark. No pandas / geopandas / pyarrow anywhere.
2. **Composable, not monolithic.** Each stage reads Parquet and writes Parquet; any stage can be re-run in isolation.
3. **Comparable neighborhoods.** Every signal is aggregated to the same geographic unit (2020 NTAs).
4. **No backend.** The frontend reads parquet over HTTP via hyparquet.
5. **Runnable on one laptop.** No cloud required for dev or demo.

## Architecture

```
Python ingestion
data_ingest/alexj/*.py   .>   data/raw/
                               |  etl_code/alexj/Clean.scala
                               v
                           data/cleaned/
                               |  etl_code/alexj/Geocode.scala   (JTS broadcast UDF)
                               v
                           data/enriched/                        (every row has nta_code)
                               |  etl_code/alexj/Features.scala
                               v
                           data/scores/neighborhood_features.parquet
                               |  etl_code/alexj/Score.scala
                               v
                           data/scores/newcomer_score.parquet    (single file)
                               |  etl_code/alexj/Analytics.scala
                               v
                           data/analytics/*.parquet              (9 tables)
                               |
                               v
                  Static frontend (web/, no backend)
                  hyparquet .> parquet ; Plotly.js + Leaflet .> figures
```

**Language split:** Scala/Spark for every computational stage; Python only in `data_ingest/` for HTTP; plain JS + Plotly + Leaflet for display. If a file is `.scala`, it is the canonical implementation of that stage.

## Project layout

Three top-level category directories, `/data_ingest`, `/etl_code`, `/profiling_code`, each with a per-member subdirectory.

```
bigdataproject/
  data_ingest/alexj/             # Python, HTTP ingestion only
    download.py                  # 4 raw CSVs from NYC Open Data + Zillow
    geo_download.py              # NTA GeoJSON + population template
    paths.py                     # self-contained path module

  etl_code/alexj/                # Scala + Spark
    Clean.scala                  # raw CSV   .> data/cleaned/*.parquet
    Geocode.scala                # cleaned   .> data/enriched/*.parquet  (JTS PIP)
    Features.scala               # enriched  .> data/scores/neighborhood_features.parquet
    Score.scala                  # features  .> data/scores/newcomer_score.parquet
    Analytics.scala              # score     .> data/analytics/*.parquet

  profiling_code/alexj/          # Scala + Spark, exploratory profiling
    FirstCode.scala              # schemas + mean/median/mode + RDD map/reduce
    CountRecs.scala              # row counts + distinct-value surveys

  web/                           # Static frontend, no server
    index.html  map.js           # Leaflet choropleth
    dashboard.html  dashboard.js # Plotly dashboard
    lib/parquet.js               # hyparquet wrapper
    lib/figures.js               # Plotly figure builders
    style.css  dashboard.css

  Makefile                       # one target per stage; `make help`
  data/                          # raw/ cleaned/ enriched/ scores/ analytics/ geo/
```

## Data sources

| Dataset | Source | ID | Access | Rows | Size |
|---|---|---|---|---|---|
| NYPD Complaints (YTD) | NYC Open Data | `5uac-w243` | Bulk CSV | ~580k | ~204 MB |
| Restaurant Inspections | NYC Open Data | `43nn-pn8j` | Bulk CSV | ~296k | ~138 MB |
| 311 Service Requests (food subset) | NYC Open Data | `erm2-nwe9` | Socrata `$where` (food-only server-side) | ~325k | ~227 MB |
| ZORI rent index (ZIP) | Zillow Research | `Zip_zori_uc_sfrcondomfr_sm_month.csv` | Static CSV | ~8k (wide, ~430 NYC ZIPs after filter) | ~9 MB |
| NTA 2020 polygons | NYC Open Data | `9nt8-h7nd` | GeoJSON | ~260 polygons | ~4.6 MB |

## Components

### 1. Ingestion (`data_ingest/alexj/`)

`download.py` pulls the four CSVs; `geo_download.py` pulls the NTA GeoJSON and templates a population CSV. Only a food-safety subset of 311 (`Food Poisoning`, `Food Establishment`, `Rodent`, `Unsanitary Animal Pvt Property`) is requested via Socrata `$where` to keep the file manageable. Standard library + `requests` only.

### 2. Clean (`Clean.scala`)

Per-dataset transformations:

| Dataset | Columns kept | Null-drop keys | Normalization | Derived |
|---|---|---|---|---|
| Crime | `CMPLNT_NUM`, `CMPLNT_FR_DT`, `OFNS_DESC`, `LAW_CAT_CD`, `BORO_NM`, `Latitude`, `Longitude` | all keys + coords | `UPPER(TRIM)` on `BORO_NM`, `OFNS_DESC`, `LAW_CAT_CD` | `IS_FELONY = LAW_CAT_CD == "FELONY"` |
| Restaurants | `CAMIS`, `DBA`, `BORO`, `ZIPCODE`, `CUISINE`, `INSPECTION_DATE`, `SCORE`, `GRADE`, `Latitude`, `Longitude` | `BORO`, `SCORE`, coords | `UPPER(TRIM)` on `BORO`, `DBA`, `CUISINE`, `GRADE` | `IS_CRITICAL = SCORE >= 28` |
| 311 food | `COMPLAINT_ID`, `CREATED_DATE`, `COMPLAINT_TYPE`, `DESCRIPTOR`, `ZIPCODE`, `BORO`, `Latitude`, `Longitude` | `COMPLAINT_TYPE`, coords | `UPPER(TRIM)` on `BORO`, `COMPLAINT_TYPE` | . |
| Zillow rent | `ZIPCODE`, `City`, `State`, `Metro`, `month`, `ZORI` | `ZORI` post-melt | wide .> long via `stack(n, ...)`; `row_number()` keeps latest non-null month per ZIP | . |

**Output:** `data/cleaned/{crime, restaurants, complaints311, rent}/*.parquet`.

#### Crime key columns

| Column | Description |
|---|---|
| `CMPLNT_NUM` | Unique complaint identifier |
| `CMPLNT_FR_DT` | Date the crime began |
| `OFNS_DESC` | Offense description (e.g. ROBBERY, FELONY ASSAULT) |
| `LAW_CAT_CD` | FELONY / MISDEMEANOR / VIOLATION |
| `BORO_NM` | Borough name |
| `Latitude` / `Longitude` | Geographic coordinates |

#### Restaurant key columns

| Column | Description |
|---|---|
| `CAMIS` | Unique restaurant identifier |
| `DBA` | Business name |
| `BORO` | Borough code |
| `ZIPCODE` | ZIP code |
| `CUISINE` | Cuisine type |
| `INSPECTION_DATE` | Date of inspection |
| `SCORE` | Numeric inspection score (lower is better) |
| `GRADE` | Letter grade (A / B / C) |

### 3. Geocode (`Geocode.scala`)

Attaches `nta_code` + `nta_name` to every row of the three point datasets.

1. Driver reads NTA GeoJSON via `Hadoop FileSystem` (works local or HDFS).
2. Jackson parses it into ~260 `org.locationtech.jts.geom.Geometry` values (Polygon + MultiPolygon).
3. `sparkContext.broadcast(Array[NtaPolygon])` ships the array to executors once.
4. A Spark UDF does envelope pre-filter then `Geometry.contains`, returning `NtaHit(nta_code, nta_name)` as a typed struct.
5. A ZIP .> NTA lookup is derived from the enriched restaurants data (each ZIP assigned the NTA containing the plurality of its restaurants) and broadcast-joined onto rent.

Complexity is O(n * m) worst case but the envelope pre-filter brings it near O(n * log m) on NYC geometry. If point or polygon volumes grow 10x, swap the UDF body for Sedona; everything else is unchanged. Also sets `spark.sql.legacy.parquet.nanosAsLong=true` for INT64 nanosecond timestamps.

### 4. Features (`Features.scala`)

Groups each enriched table by `nta_code` and emits one row per NTA:

- `total_crimes`, `felonies`, `felony_share`
- `n_inspections`, `avg_score`, `critical_rate`
- `n_complaints`
- `median_rent_zori`
- `crimes_per_1k`, `complaints_per_1k` (if `nta_population.csv` is populated)

All four feature tables outer-joined on `(nta_code, nta_name)`. Structural nulls filled with zero; rent nulls left null (genuinely "no observations"). Output: `data/scores/neighborhood_features.parquet`.

### 5. Score (`Score.scala`)

**Input features (per NTA), all "higher is worse":**

| Feature | Source |
|---|---|
| `crimes_per_1k` | NYPD complaints / NTA population |
| `felony_share` | share of crimes with `LAW_CAT_CD == FELONY` |
| `avg_score` | mean DOHMH inspection score (lower = cleaner kitchens) |
| `critical_rate` | share of inspections with `IS_CRITICAL` (`SCORE >= 28`) |
| `complaints_per_1k` | 311 food-safety complaints / NTA population |
| `median_rent_zori` | Zillow ZORI most-recent-month median rent |

**Method:**

1. Z-score each feature with `avg` + `stddev_pop` in a single `groupBy().agg(...)`. Nulls imputed with the column mean before z-scoring so a missing signal stays neutral (z approximate to 0).
2. Sign-flip all six features (higher is worse). After this step a positive z means "good for a newcomer."
3. Collapse into four sub-scores:
   - `safety` = mean(`z_crimes_per_1k`, `z_felony_share`)
   - `food_safety` = mean(`z_avg_score`, `z_critical_rate`)
   - `cleanliness` = `z_complaints_per_1k`
   - `affordability` = `z_median_rent_zori`
4. Weighted sum: `0.30 * safety + 0.25 * food_safety + 0.15 * cleanliness + 0.30 * affordability` (weights at the top of `Score.scala`).
5. Rescale linearly to 0 to 100 via min-max.

Written as **single-file parquet** (`.coalesce(1)` then rename the lone part file) so the frontend can fetch a predictable URL.

The score is **relative, not absolute**. A 0/100 does not mean "unlivable"; it means worst-scoring of the ~260 NTAs given these six signals and these weights. Tune by editing `WEIGHTS` and rerunning `make score`.

### 6. Analytics (`Analytics.scala`)

One Spark job emits nine tiny single-file parquet tables under `data/analytics/`:

| File | Shape | Consumer figure |
|---|---|---|
| `summary.parquet` | mean/median/min/max/std/n per metric | summary strip + histogram vlines |
| `distribution.parquet` | 24 equal-width bins per metric | histogram |
| `top_bottom.parquet` | top/bottom 10 per metric | horizontal bar |
| `borough_box.parquet` | q1/median/q3/fences per (metric, borough) | box plot |
| `borough_points.parquet` | raw (metric, borough, nta_name, value) | jitter overlay |
| `correlation.parquet` | long-form (feature_row, feature_col, r) | 6x6 heatmap |
| `rent_vs_feature_bins.parquet` | decile bins x 5 predictors: median + IQR | trend line + band |
| `rent_vs_feature_ols.parquet` | slope, intercept, Pearson r, R^2, example prediction | OLS line + annotation |
| `rent_vs_feature_points.parquet` | raw (x, rent) per NTA per predictor | borough-coloured scatter |

Uses `percentile_approx` for quantiles, `ntile(10)` for rent deciles, Spark's `corr` aggregate for all 36 heatmap cells in one pass. OLS slope/intercept derived analytically from `corr`, `avg`, `stddev_pop`, so no Spark ML dependency. `.cache()` the joined input so the scan happens once across nine outputs.

### 7. Profiling (`profiling_code/alexj/`)

`FirstCode.scala` (schemas, mean/median/mode, RDD map/reduce) and `CountRecs.scala` (row counts, distinct-value surveys) answer the "what's in this data?" questions that precede writing ETL.

### 8. Frontend (`web/`)

- `lib/parquet.js` wraps [hyparquet](https://github.com/hyparam/hyparquet) with `loadParquet(url)` returning `{rows, columns}` (BigInts coerced to numbers because Plotly cannot handle BigInt).
- `lib/figures.js` owns palette, borough colours, layout defaults, and five figure builders (`distributionFigure`, `topBottomFigure`, `byBoroughFigure`, `correlationFigure`, `rentVsFeatureFigure`).
- `map.js` fetches the NTA GeoJSON and `data/scores/newcomer_score.parquet`, joins on `nta_code` client-side, hands the merged FeatureCollection to Leaflet. Metric switching is pure in-place restyling.
- `dashboard.js` loads all nine analytics parquets in parallel at startup, then re-runs figure builders against cached data on pill clicks.

A schema change in `Analytics.scala` needs a matching change in `web/lib/figures.js`; this is called out in both files' headers.

## Data contract

The key boundary is `data/enriched/`. Once `nta_code` + `nta_name` are attached, every downstream stage treats NTAs as the primary key and is source-agnostic. Adding a 5th dataset (e.g. subway access) is a localized change: new cleaner block in `Clean.scala`, UDF application in `Geocode.scala`, aggregate in `Features.scala`, column in `Score.scala` / `Analytics.scala`, figure in `figures.js`.

Score + analytics stages write **single-file parquet** (`.coalesce(1)` then rename). Clean / Geocode / Features write standard part-file directories because Spark reads them natively downstream.

## Build

There is no compile step. The Scala jobs are run interpreted via `spark-shell -i <script>.scala`, so `make pipeline` is the full build-and-run. The first invocation of `make geocode` resolves `org.locationtech.jts:jts-core:1.19.0` through ivy and caches it in `~/.ivy2/` (~5 MB); subsequent runs are offline. `make setup` installs the single Python dependency (`requests`) for the downloaders.

```bash
make setup               # pip install requests (one-time)
# no javac / sbt / mvn step: spark-shell -i compiles on the fly
```

## Running the full pipeline

### One-time setup

```bash
make setup               # pip install requests
make download            # ~570 MB of CSVs into data/raw/
make geo-download        # NTA GeoJSON + population template into data/geo/
```

### Batch pipeline

```bash
make pipeline            # Clean .> Geocode .> Features .> Score .> Analytics
```

### Where to find the results

| Stage | Output path | Format |
|---|---|---|
| `make download` | `data/raw/*.csv` | raw CSVs |
| `make geo-download` | `data/geo/nta.geojson`, `data/geo/nta_population.csv` | GeoJSON + CSV |
| `make clean` | `data/cleaned/{crime,restaurants,complaints311,rent}/` | Spark parquet dirs |
| `make geocode` | `data/enriched/{crime,restaurants,complaints311,rent}/`, `data/geo/zip_to_nta.csv` | Spark parquet dirs + CSV |
| `make features` | `data/scores/neighborhood_features.parquet/` | Spark parquet dir |
| `make score` | `data/scores/newcomer_score.parquet` | single file (read by frontend) |
| `make analytics` | `data/analytics/*.parquet` | nine single-file tables |
| `make web` | <http://localhost:5173/web/> (map), <http://localhost:5173/web/dashboard.html> (dashboard) | browser |

Wall time on an 8-core laptop: ~1 min `clean`, ~45 s `geocode` (JTS PIP on ~500k points), ~12 s `features`, ~5 s `score`, ~10 s `analytics`.

### Serve the static site

```bash
make web                 # http://localhost:5173
# equivalent: npx http-server . -p 5173
```

Open <http://localhost:5173/web/>. Click **Open analytics dashboard** for the five-figure dashboard.

## Prerequisites

- **Java 11**, required by Spark.
- **Apache Spark 3.5**, Scala jobs run via `spark-shell -i`. First run pulls `org.locationtech.jts:jts-core:1.19.0` via ivy into `~/.ivy2`.
- **Python 3.11+**, only for `data_ingest/alexj/*.py`; single dependency `requests`.

## Orchestration (`Makefile`)

Every target wraps `spark-shell $(SPARK_OPTS) -i <script>.scala`, with `--packages` as needed (JTS for geocode). `DATA_ROOT ?= $(PWD)/data`. `SPARK_MASTER` defaults to `local[*]`.

| Target | Runs |
|---|---|
| `make setup` | `pip install requests` |
| `make download` | `data_ingest/alexj/download.py` |
| `make geo-download` | `data_ingest/alexj/geo_download.py` |
| `make clean` | `Clean.scala` |
| `make geocode` | `Geocode.scala` (with JTS) |
| `make features` | `Features.scala` |
| `make score` | `Score.scala` |
| `make analytics` | `Analytics.scala` |
| `make pipeline` | clean .> geocode .> features .> score .> analytics |
| `make profile-first` | `FirstCode.scala` |
| `make profile-counts` | `CountRecs.scala` |
| `make web` | static server on `:5173` |
| `make clean-derived` | wipe cleaned/enriched/scores/analytics |
| `make clean-data` | wipe everything under `$(DATA_ROOT)` (destructive) |

## Scalability

### `SPARK_TUNE` (same flags for every stage)

Defined once in the Makefile, passed via `spark-shell --conf` so configs live in one place.

**Shuffle partitions: 8, not 200.** Spark's default is calibrated for clusters. On an 8-core laptop with ~500k rows, 200 partitions means ~2,500 rows each and task dispatch dominates. 8 = one per core matches physical parallelism.

**AQE on explicitly** (`adaptive.enabled`, `coalescePartitions.enabled`, `skewJoin.enabled`). Default in Spark 3.5 but set explicitly so intent survives upgrades. Coalescing matters because even 8 starting partitions can end up under-filled after `.dropna()` / `.filter()`.

**Broadcast join threshold: 50 MB** (up from 10 MB default) so small lookup tables auto-broadcast without explicit `broadcast()` hints. `zip_to_nta.csv` is ~15 KB; the JTS polygon array (~5 MB serialized) is broadcast explicitly via `sparkContext.broadcast(...)` because it is not a DataFrame.

**What we deliberately do not configure:** `.cache()` is avoided on the batch path because each stage reads its Parquet input once and writes once; `Analytics.scala` is the lone exception (one cached DataFrame feeds nine outputs). No checkpointing, no off-heap, no G1GC tuning; the pipeline is short enough that `make <stage>` is the recovery story, and those knobs would be cargo-culting at this data size.

### Measured scaling (reference, WSL2 Intel, 8 cores, 16 GB)

The original PySpark bench harness was removed during the Scala port. Reference numbers from that run:

**Crime (579,561 rows cleaned):**

| Sample | Rows | Time (s) | Throughput |
|---|---|---|---|
| 10% | 57,850 | 4.1 | ~14,100 rows/s |
| 50% | 289,180 | 9.7 | ~29,800 rows/s |
| 100% | 578,202 | 14.9 | ~38,800 rows/s |

Scaling efficiency 2.74 (super-linear): throughput increases with size because fixed startup cost (JVM warmup, Ivy resolve, CSV schema inference) amortizes.

**Restaurants (295,723 rows cleaned):**

| Sample | Rows | Time (s) | Throughput |
|---|---|---|---|
| 10% | 27,850 | 3.2 | ~8,700 rows/s |
| 50% | 139,240 | 6.4 | ~21,800 rows/s |
| 100% | 278,482 | 9.1 | ~30,600 rows/s |

Scaling efficiency 2.85 (super-linear).

**Takeaways:** fixed overhead (~3 s) dominates at small sizes; near-linear work beyond that; no memory pressure at 100% (no throughput collapse, so no spill). To reproduce today, add `.sample(false, f, 42L)` after `readCsv(...)` in `Clean.scala` and `time make clean`. If throughput drops 50% .> 100% on rerun, investigate in order: driver memory (bump `spark.driver.memory`), GC pauses (enable G1 and watch `:4040`), partition skew after `.na.drop()`.

## Findings and insights

What the dashboard surfaces once the pipeline finishes. The correlation heatmap (`correlation.parquet`) and the rent-vs-predictor OLS tables (`rent_vs_feature_ols.parquet`) are where most of these come from.

1. **Rent is weakly, not strongly, correlated with safety.** `median_rent_zori` vs `crimes_per_1k` has Pearson r approximately -0.31 across the ~260 NTAs, and the OLS R^2 sits around 0.10. The folk model ("expensive neighborhoods are safe neighborhoods") is only a 10% explanation; the other 90% is geography, housing stock age, and zoning. This is the first thing a newcomer should internalize: do not proxy safety by rent.

2. **Restaurant inspection scores barely track anything else.** `avg_score` has the lowest off-diagonal correlations in the 6x6 heatmap (|r| < 0.15 against rent, crime, and 311). Health-code compliance appears to be a per-venue operational trait, not a neighborhood trait. This flattens `food_safety` as a differentiator between NTAs and was the reason it got 0.25 weight instead of 0.33 (see the `Score.scala` iteration notes).

3. **311 food complaints are a density signal, not a cleanliness signal.** `n_complaints` correlates roughly r approximately +0.55 with raw restaurant count per NTA. Central Manhattan has "bad" cleanliness in this data because there are more places where food can be bad, and more people inclined to call 311. Per-capita normalization (`complaints_per_1k`) dampens it but does not fully remove the bias, which is why `cleanliness` carries only 0.15 weight.

4. **Outlier: the Battery / Governors / Ellis / Liberty NTA.** Grouped into one NTA for ~260-polygon reasons but essentially uninhabited. It had 4 inspections with an unrealistically low `avg_score` of 1.25 and no ZORI rent, so before the data-starved gate was added (`Score.scala:MIN_INSPECTIONS`) its composite pinned to the city maximum and compressed every other NTA's 0-100 score. The gate nulls the score for NTAs with no rent AND fewer than 20 inspections; 3-5 NTAs per run are suppressed this way.

5. **Stuyvesant Town and a handful of Manhattan NTAs have no ZORI at all.** Zillow requires an active single-family + multi-family listing threshold per ZIP, which Stuy Town (~11,000 rent-stabilized units, ~zero active MLS listings) does not meet. Without an override these come out NULL on affordability, get mean-imputed, and look "average" on rent despite being $5k+/mo apartments. `Features.scala:RENT_OVERRIDES` patches four of these from contemporary listing aggregators with `median_rent_imputed = true` so downstream consumers can treat them separately.

Connecting to broader context: New York's 2020 NTA boundaries were drawn for statistical stability (~15-30k residents each) by NYC DCP, so small residential enclaves like Stuy Town or Chinatown-Two Bridges get grouped into NTAs whose centroid lies elsewhere. Any rent-vs-feature story inherits that grouping, which is why the OLS explains only a sliver of variance. A ZCTA-level model (USPS ZIP geography + Census-bureau areal weights) would likely lift R^2 for rent-vs-safety substantially, at the cost of more fragile polygon data. This is the single biggest methodological lever left in the pipeline.

## Ethical guardrails and governance

Every dataset here encodes a social process, not a ground truth. Treating them as neutral is itself a bias.

### Known biases in each input

| Dataset | What it actually measures | Bias direction |
|---|---|---|
| NYPD Complaints | Crimes **reported to and recorded by** NYPD | Over-policed neighborhoods (historically lower-income, higher-minority) show inflated totals; under-policed and under-reporting neighborhoods show deflated totals. The data reflects enforcement as much as criminality. |
| DOHMH Restaurant Inspections | Inspections **performed**, not inspection-worthy conditions | Inspection frequency varies by cuisine, prior violations, and borough. NTAs with more chain restaurants (more scheduled inspections) look better controlled; independent-heavy NTAs look noisier. |
| 311 Food complaints | Who **calls 311**, not who gets sick | Correlates with English fluency, tenure, age, and civic-engagement norms. Immigrant-heavy and renter-heavy NTAs under-report; gentrifying NTAs over-report. |
| Zillow ZORI | **Market-rate listings** aggregated across Zillow's inventory | Excludes rent-stabilized and rent-controlled units (~44% of NYC rental stock per NYC HPD 2023). Missing where listings are sparse (Stuy Town, Chinatown-Two Bridges). Biases affordability *upward* in mixed-stock neighborhoods. |

### What the pipeline does about this

- **Per-capita normalization.** `crimes_per_1k` and `complaints_per_1k` are computed from NYC DCP population (`Features.scala`) so raw volume differences between Manhattan and Staten Island do not dominate. This mitigates but does not remove reporting bias.
- **Missing-signal neutrality.** In `Score.scala`, NTAs missing a feature have that feature imputed with the city mean **before** z-scoring, so they stay at z approximately 0 on that axis instead of being penalized for the absence. Absence of data is not absence of a quality.
- **Data-starved gate (`Score.scala:MIN_INSPECTIONS`).** NTAs with no ZORI rent AND fewer than 20 inspections have `newcomer_score` nulled entirely. Publishing a number for uninhabited islands or data-sparse NTAs would overclaim; the dashboard renders them as "Unscored."
- **`median_rent_imputed` flag.** Overrides in `Features.scala:RENT_OVERRIDES` are surfaced as a boolean column rather than silently patched, so the dashboard can display them with different styling and the provenance stays auditable.
- **Transparent, non-learned scoring.** `Score.scala` is a weighted z-score with the weights at the top of the file in a one-map `WEIGHTS`. Anyone can retune and rerun `make score` in seconds. No learned model means no opaque feature interactions, no training-set leakage, and no model card to chase.

### Alternatives considered

- **Down-weight NYPD complaints.** Replace raw `crimes_per_1k` with a blend of reported crime and National Crime Victimization Survey estimates, which are enforcement-independent. Out of scope for this project (NCVS is not NTA-level) but a natural next step.
- **Multi-profile scores.** One `WEIGHTS` map is a single user archetype (a generic newcomer). `Score.scala` could emit three parallel scores (student, family, retiree) with different weightings, and the frontend could let users pick. Plumbing exists; just needs three maps instead of one.
- **Sensitivity sweep.** Rerun `Score.scala` with each weight perturbed by +/- 20% and report rank-stability per NTA. NTAs whose rank flips under small weight changes should be flagged in the UI as "weight-sensitive."
- **Equity-adjusted crime signal.** Residualize `crimes_per_1k` on per-NTA policing intensity (NYPD patrol-hours, publicly available) before z-scoring, so the metric captures crime net of enforcement. Open question: patrol-hour data is lagged and coarse-grained, and conditioning on it can itself introduce bias.

### What this project deliberately does not do

- **No personally identifying information** is loaded or written at any stage. NYPD `CMPLNT_NUM` and DOHMH `CAMIS` are opaque identifiers; no lat/lon is rounded tighter than the source.
- **No protected-attribute inference.** NTAs are not labelled by demographic composition anywhere in the ETL, so the score cannot condition on race, income, or immigration status even accidentally.
- **No absolute claims.** The 0 to 100 score is explicitly relative, and the README and dashboard both say so. A newcomer reading "82/100 on affordability" should take it as "less expensive than most NYC NTAs given ZORI", not as a universal affordability statement.

## Design decisions and tradeoffs

| Decision | Rationale | Tradeoff |
|---|---|---|
| Scala + Spark for every computational stage | Single-language big-data surface; every stage is horizontally scalable on a Spark cluster. | Score/analytics on ~260 rows adds driver startup time; accepted. |
| Python only in `data_ingest/` | HTTP retries / streaming / Socrata paging are what `requests` is for. | One more runtime at setup. One-shot, acceptable. |
| No backend | Scala writes parquet, JS reads parquet, Plotly renders. Fewer tiers. | Frontend depends on hyparquet over a CDN. |
| JTS broadcast UDF (not Sedona) | Pure-Java jar, zero deps, 260 polygons x N points is trivial with envelope pre-filter. | Revisit at 10x volume; one-line UDF swap. |
| Single-file parquet for score + analytics | Predictable URLs, no manifest needed. | `.coalesce(1)` forces one reducer on write; fine at <1 MB. |
| NTAs (2020) as the unit | Official NYC definition, public GeoJSON. | Colloquial neighborhood names do not always map 1:1. |
| ZIP .> NTA derived from restaurants | Avoids a ZCTA shapefile dependency; derivation is Scala. | Sparse ZIPs map noisily. |
| Parquet everywhere | Zero infra, columnar, browser-readable via hyparquet. | No SQL surface outside the Spark REPL. |
| Weighted z-score, not a learned model | Transparent, retunable by editing one map. | Sensitive to outliers. |

## Extending the system

- **Add a dataset.** Downloader step in `download.py`, cleaner block in `Clean.scala` writing `data/cleaned/<name>/`, UDF application in `Geocode.scala`, aggregate in `Features.scala`, optional weighted term in `Score.scala`. For dashboard coverage, add a row to `Analytics.scala`'s `METRICS` and a figure in `web/lib/figures.js`.
- **Add a team member.** Mirror `data_ingest/alexj/`, `etl_code/alexj/`, `profiling_code/alexj/` under their username; Makefile targets follow the same pattern.
- **Replace ZIP .> NTA with a real ZCTA join.** Drop a ZCTA GeoJSON at `data/geo/zcta.geojson` and rewrite the ZIP-to-NTA section of `Geocode.scala` to do area-weighted intersection. Downstream unchanged.

## Future work

- **Alternative scoring models.** Replace the weighted z-score with a learned ranking model, or ship multi-profile scores (student / family / retiree) by swapping the `WEIGHTS` map.
- **Apache Sedona for the spatial join.** JTS in a broadcast UDF is more than enough for ~260 polygons; Sedona is a drop-in upgrade if polygon count or point volume grows by an order of magnitude.
- **ZCTA-based ZIP .> NTA.** Replace the restaurant-derived lookup with a ZCTA shapefile + area-weighted intersection.
- **Additional signals.** Subway access (MTA GTFS), park coverage (NYC Parks GeoJSON), school quality (DOE reports). Each plugs in as a downloader + cleaner block + `Features.scala` aggregate + optional `Score.scala` weight.

## Notes

- **Population is optional.** Drop a populated CSV at `data/geo/nta_population.csv` (columns `nta_code,nta_name,population`) and `Features.scala` will compute per-capita rates. Otherwise raw totals are used.
- **No Python at serve time.** Dashboard and map are pure static files; Python runtime is only `requests` in the two downloaders.
