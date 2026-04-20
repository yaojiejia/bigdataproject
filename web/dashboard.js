// Analytics dashboard. Fetches figure specs from `/analytics/*` and
// renders them with Plotly.js. The same figure builder runs in
// `notebooks/analysis.ipynb`, so the charts match exactly.

const API_BASE =
  window.location.port === "8765" ? "" : `http://${window.location.hostname}:8765`;

const METRICS = [
  { key: "score", label: "Newcomer Score", fmt: (v) => v.toFixed(1), unit: "/ 100" },
  { key: "crime", label: "Crimes / 1k", fmt: (v) => v.toFixed(1), unit: "per 1k" },
  { key: "food",  label: "Critical rate", fmt: (v) => (v * 100).toFixed(1) + "%", unit: "" },
  { key: "rent",  label: "Median rent",   fmt: (v) => "$" + Math.round(v).toLocaleString(), unit: "/ mo" },
  { key: "311",   label: "311 / 1k",      fmt: (v) => v.toFixed(1), unit: "per 1k" },
];

let currentMetric = "score";

const PLOTLY_CONFIG = {
  displaylogo: false,
  responsive: true,
  modeBarButtonsToRemove: [
    "lasso2d", "select2d", "autoScale2d", "hoverClosestCartesian",
    "hoverCompareCartesian", "toggleSpikelines",
  ],
  toImageButtonOptions: { format: "png", filename: "nyc-insights-chart", scale: 2 },
};

function setStatus(msg, state = "ok") {
  const dot = document.getElementById("status-dot");
  const txt = document.getElementById("status");
  if (txt) txt.textContent = msg;
  if (dot) dot.className = "status-dot" + (state === "ok" ? "" : " " + state);
}

async function fetchFigure(path) {
  const r = await fetch(`${API_BASE}${path}`);
  if (!r.ok) throw new Error(`${path} → HTTP ${r.status}`);
  return r.json();
}

function renderError(divId, err) {
  const el = document.getElementById(divId);
  if (!el) return;
  el.innerHTML = `<div class="chart-error">⚠ ${err.message || err}</div>`;
}

async function renderFigure(divId, path) {
  try {
    const fig = await fetchFigure(path);
    await Plotly.react(divId, fig.data, fig.layout, PLOTLY_CONFIG);
  } catch (err) {
    console.error(path, err);
    renderError(divId, err);
  }
}

async function renderSummary() {
  try {
    const r = await fetch(`${API_BASE}/analytics/summary`);
    if (!r.ok) throw new Error(`summary → HTTP ${r.status}`);
    const summary = await r.json();
    const strip = document.getElementById("summary-strip");
    const cards = [
      {
        label: "Neighborhoods",
        value: (summary.n_ntas ?? 0).toLocaleString(),
        sub: `across ${summary.n_boroughs ?? 0} boroughs`,
      },
    ];
    for (const m of METRICS) {
      const stats = summary[m.key];
      if (!stats) continue;
      cards.push({
        label: m.label,
        value: m.fmt(stats.median) + (m.unit ? ` ${m.unit}` : ""),
        sub: `range ${m.fmt(stats.min)} – ${m.fmt(stats.max)}`,
      });
    }
    strip.innerHTML = cards
      .map(
        (c) => `
      <div class="summary-card">
        <span class="label">${c.label}</span>
        <span class="value">${c.value}</span>
        <span class="sub">${c.sub}</span>
      </div>`,
      )
      .join("");
  } catch (err) {
    console.error("summary", err);
  }
}

async function renderAll(metric) {
  setStatus(`Rendering charts for ${metric}…`, "loading");
  const qs = `?metric=${encodeURIComponent(metric)}`;
  await Promise.all([
    renderFigure("fig-distribution", `/analytics/distribution${qs}`),
    renderFigure("fig-top-bottom",   `/analytics/top-bottom${qs}&n=10`),
    renderFigure("fig-by-borough",   `/analytics/by-borough${qs}`),
  ]);
  setStatus("All charts in sync with the notebook.", "ok");
}

async function renderStatic() {
  await renderFigure("fig-correlation", "/analytics/correlation");
}

document.getElementById("metric-pills").addEventListener("click", (ev) => {
  const btn = ev.target.closest("button[data-metric]");
  if (!btn) return;
  document
    .querySelectorAll("#metric-pills button")
    .forEach((b) => b.classList.toggle("active", b === btn));
  currentMetric = btn.dataset.metric;
  renderAll(currentMetric);
});

(async function init() {
  await renderSummary();
  await Promise.all([renderAll(currentMetric), renderStatic()]);
})();
