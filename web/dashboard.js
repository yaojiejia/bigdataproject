// ============================================================
// web/dashboard.js — analytics dashboard, no backend.
// ============================================================
//
// Loads every aggregate parquet file written by
// `etl_code/alexj/Analytics.scala` once at startup, then re-builds the
// Plotly specs in-browser whenever the user switches metric or rent
// predictor. No API calls, no server — just parquet + Plotly.
// ============================================================

import { loadParquet } from "./lib/parquet.js";
import {
  METRICS, RENT_PREDICTOR_META, INPUT_FEATURE_LABELS,
  distributionFigure, topBottomFigure, byBoroughFigure,
  correlationFigure, rentVsFeatureFigure,
} from "./lib/figures.js";

const DATA = "../data/analytics";

const PARQUETS = {
  summary:       `${DATA}/summary.parquet`,
  distribution:  `${DATA}/distribution.parquet`,
  topBottom:     `${DATA}/top_bottom.parquet`,
  boroughBox:    `${DATA}/borough_box.parquet`,
  boroughPoints: `${DATA}/borough_points.parquet`,
  correlation:   `${DATA}/correlation.parquet`,
  rentBins:      `${DATA}/rent_vs_feature_bins.parquet`,
  rentOls:       `${DATA}/rent_vs_feature_ols.parquet`,
  rentPoints:    `${DATA}/rent_vs_feature_points.parquet`,
};

const INPUT_FEATURES_ORDER = [
  "crimes_per_1k", "felony_share", "avg_score",
  "critical_rate", "complaints_per_1k", "median_rent_zori",
];

const PLOTLY_CONFIG = {
  displaylogo: false,
  responsive: true,
  modeBarButtonsToRemove: [
    "lasso2d", "select2d", "autoScale2d", "hoverClosestCartesian",
    "hoverCompareCartesian", "toggleSpikelines",
  ],
  toImageButtonOptions: { format: "png", filename: "nyc-insights-chart", scale: 2 },
};

const STATE = {
  metric:    "score",
  predictor: "crimes_per_1k",
  tables:    {},
};

function setStatus(msg, state = "ok") {
  const dot = document.getElementById("status-dot");
  const txt = document.getElementById("status");
  if (txt) txt.textContent = msg;
  if (dot) dot.className = "status-dot" + (state === "ok" ? "" : " " + state);
}

function renderError(divId, err) {
  const el = document.getElementById(divId);
  if (!el) return;
  el.innerHTML = `<div class="chart-error">⚠ ${err.message || err}</div>`;
}

async function draw(divId, fig) {
  try {
    await Plotly.react(divId, fig.data, fig.layout, PLOTLY_CONFIG);
  } catch (err) {
    console.error(divId, err);
    renderError(divId, err);
  }
}

// ---- Header strip ---------------------------------------------------------

function renderSummaryStrip() {
  const summary = STATE.tables.summary;
  const strip   = document.getElementById("summary-strip");
  const rows    = summary.rows;
  const nNtas   = Number(rows[0]?.n_ntas || 0);
  const cards   = [{
    label: "Neighborhoods",
    value: nNtas.toLocaleString(),
    sub:   "across 5 boroughs",
  }];
  for (const key of Object.keys(METRICS)) {
    const meta = METRICS[key];
    const row  = rows.find((r) => r.metric === key);
    if (!row) continue;
    cards.push({
      label: meta.label,
      value: meta.fmt(row.median) + (meta.unit ? ` ${meta.unit}` : ""),
      sub:   `range ${meta.fmt(row.min)} – ${meta.fmt(row.max)}`,
    });
  }
  strip.innerHTML = cards.map((c) => `
    <div class="summary-card">
      <span class="label">${c.label}</span>
      <span class="value">${c.value}</span>
      <span class="sub">${c.sub}</span>
    </div>
  `).join("");
}

// ---- Per-metric redraws ---------------------------------------------------

function renderMetricFigures(metric) {
  const T = STATE.tables;
  draw("fig-distribution", distributionFigure(T.distribution.rows, T.summary.rows, metric));
  draw("fig-top-bottom",   topBottomFigure(T.topBottom.rows, metric, 10));
  draw("fig-by-borough",   byBoroughFigure(T.boroughBox.rows, T.boroughPoints.rows, metric));
}

function renderCorrelation() {
  const T = STATE.tables;
  draw("fig-correlation", correlationFigure(T.correlation.rows, INPUT_FEATURES_ORDER));
}

function renderRentVs(feature) {
  const T = STATE.tables;
  draw("fig-rent-vs", rentVsFeatureFigure(T.rentBins.rows, T.rentOls.rows, T.rentPoints.rows, feature));
}

// ---- Wire up pills --------------------------------------------------------

document.getElementById("metric-pills").addEventListener("click", (ev) => {
  const btn = ev.target.closest("button[data-metric]");
  if (!btn) return;
  document.querySelectorAll("#metric-pills button")
    .forEach((b) => b.classList.toggle("active", b === btn));
  STATE.metric = btn.dataset.metric;
  renderMetricFigures(STATE.metric);
});

document.getElementById("predictor-pills").addEventListener("click", (ev) => {
  const btn = ev.target.closest("button[data-feature]");
  if (!btn) return;
  document.querySelectorAll("#predictor-pills button")
    .forEach((b) => b.classList.toggle("active", b === btn));
  STATE.predictor = btn.dataset.feature;
  renderRentVs(STATE.predictor);
});

// ---- Boot -----------------------------------------------------------------

(async function init() {
  setStatus("Loading analytics parquet…", "loading");
  try {
    const [summary, distribution, topBottom, boroughBox, boroughPoints,
           correlation, rentBins, rentOls, rentPoints] = await Promise.all([
      loadParquet(PARQUETS.summary),
      loadParquet(PARQUETS.distribution),
      loadParquet(PARQUETS.topBottom),
      loadParquet(PARQUETS.boroughBox),
      loadParquet(PARQUETS.boroughPoints),
      loadParquet(PARQUETS.correlation),
      loadParquet(PARQUETS.rentBins),
      loadParquet(PARQUETS.rentOls),
      loadParquet(PARQUETS.rentPoints),
    ]);
    STATE.tables = {
      summary, distribution, topBottom, boroughBox, boroughPoints,
      correlation, rentBins, rentOls, rentPoints,
    };
    renderSummaryStrip();
    renderMetricFigures(STATE.metric);
    renderCorrelation();
    renderRentVs(STATE.predictor);
    setStatus("Dashboard rendered from Scala/Spark parquet.", "ok");
  } catch (err) {
    console.error(err);
    setStatus(
      "Couldn't load analytics parquet. Did you run `make analytics`?",
      "error"
    );
  }
})();
