// Leaflet front-end for NYC Neighborhood Insights.

// Frontend is served on port 5173 (python http.server), API is on port 8765
// (FastAPI). When accessed via the API directly, use relative URLs.
const API_BASE = window.location.port === "8765" ? "" : `http://${window.location.hostname}:8765`;

const METRIC_LABELS = {
  score: "Newcomer Score",
  crime: "Crime intensity",
  food: "Critical inspection rate",
  rent: "Median rent (ZORI)",
  "311": "Recent 311 complaints",
};

const METRIC_UNITS = {
  score: "/ 100",
  crime: "per 1k residents",
  food: "% critical",
  rent: "$ / month",
  "311": "per 1k residents",
};

// Higher-is-better metrics (others are inverted so darker = worse).
const HIGHER_IS_BETTER = new Set(["score"]);

// Two sequential ramps: purple (higher-is-better) and orange-red (lower-is-better).
// Picked from d3-scale-chromatic interpolatePurples and interpolateOrRd.
const RAMP_POS = ["#f3f0ff", "#dcd6f7", "#b9acee", "#9580e0", "#7459cc", "#5a3ab2", "#422388", "#2e1065"];
const RAMP_NEG = ["#fff5eb", "#fee6ce", "#fdd0a2", "#fdae6b", "#fd8d3c", "#f16913", "#d94801", "#7f2704"];

const map = L.map("map", { preferCanvas: true, zoomControl: false })
  .setView([40.73, -73.97], 11);
L.control.zoom({ position: "bottomright" }).addTo(map);

// CartoDB Positron — a clean, minimal basemap that makes the choropleth pop.
L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
  attribution: "© OpenStreetMap · © CARTO",
  subdomains: "abcd",
  maxZoom: 19,
}).addTo(map);

let geoLayer = null;
let currentMetric = "score";
let currentBreaks = [];
let selectedLayer = null;
let lastGeoJson = null;

function statusText(msg, state = "ok") {
  document.getElementById("status").textContent = msg;
  const dot = document.getElementById("status-dot");
  dot.className = "status-dot" + (state === "ok" ? "" : " " + state);
}

function currentRamp() {
  return HIGHER_IS_BETTER.has(currentMetric) ? RAMP_POS : RAMP_NEG;
}

function quantileBreaks(values, n = 7) {
  const sorted = values.filter((v) => v != null && Number.isFinite(v)).slice().sort((a, b) => a - b);
  if (!sorted.length) return [];
  const breaks = [];
  for (let i = 1; i < n; i++) {
    const idx = Math.floor((i / n) * sorted.length);
    breaks.push(sorted[Math.min(idx, sorted.length - 1)]);
  }
  return breaks;
}

function colorFor(value, breaks) {
  if (value == null || !Number.isFinite(value)) return "#e2e8f0";
  const ramp = currentRamp();
  let i = 0;
  while (i < breaks.length && value > breaks[i]) i++;
  return ramp[i];
}

function styleFeature(f) {
  const v = f.properties && f.properties.metric_value;
  return {
    fillColor: colorFor(v, currentBreaks),
    fillOpacity: 0.82,
    color: "#ffffff",
    weight: 0.6,
  };
}

function fmt(v, digits = 2) {
  return v == null || !Number.isFinite(v)
    ? "—"
    : Number(v).toLocaleString(undefined, { maximumFractionDigits: digits });
}

function fmtPct(v) {
  return v == null || !Number.isFinite(v) ? "—" : (v * 100).toFixed(1) + "%";
}

function fmtMoney(v) {
  return v == null || !Number.isFinite(v)
    ? "—"
    : "$" + Number(v).toLocaleString(undefined, { maximumFractionDigits: 0 });
}

function renderLegend(breaks) {
  const legend = document.getElementById("legend");
  const ramp = currentRamp();
  const title = METRIC_LABELS[currentMetric];
  const lo = breaks.length ? breaks[0] : null;
  const hi = breaks.length ? breaks[breaks.length - 1] : null;
  const formatter =
    currentMetric === "food" ? fmtPct :
    currentMetric === "rent" ? fmtMoney :
    (v) => fmt(v, 1);
  legend.innerHTML = `
    <div class="legend-title">${title}</div>
    <div class="legend-bar">${ramp.map((c) => `<span style="background:${c}"></span>`).join("")}</div>
    <div class="legend-scale">
      <span>${formatter(lo)}</span>
      <span>${formatter(hi)}</span>
    </div>
  `;
}

function renderEmptyDetail() {
  document.getElementById("detail").innerHTML = `
    <div class="empty">
      <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor"
           stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
        <path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z"/>
        <circle cx="12" cy="10" r="3"/>
      </svg>
      <p>Click a neighborhood on the map to see its full profile.</p>
    </div>
  `;
}

// Cache min/max of each metric across the current feature collection so
// we can draw proportional bars in the detail panel.
function computeMetricRanges(geo) {
  const keys = [
    "newcomer_score_100", "total_crimes", "felony_share",
    "n_inspections", "avg_score", "critical_rate",
    "n_complaints", "median_rent_zori",
  ];
  const ranges = {};
  for (const k of keys) {
    let lo = Infinity, hi = -Infinity;
    for (const f of geo.features) {
      const v = f.properties && f.properties.features && f.properties.features[k];
      if (v != null && Number.isFinite(v)) { if (v < lo) lo = v; if (v > hi) hi = v; }
    }
    ranges[k] = { lo: Number.isFinite(lo) ? lo : 0, hi: Number.isFinite(hi) ? hi : 1 };
  }
  return ranges;
}

function barPct(value, range, invert = false) {
  if (value == null || !Number.isFinite(value) || range.hi === range.lo) return 0;
  const frac = (value - range.lo) / (range.hi - range.lo);
  const clamped = Math.max(0, Math.min(1, frac));
  return (invert ? 1 - clamped : clamped) * 100;
}

function renderDetail(props, ranges) {
  const el = document.getElementById("detail");
  const f = props.features || {};
  const score = f.newcomer_score_100;
  const rows = [
    { key: "Total crimes", val: fmt(f.total_crimes, 0), pct: barPct(f.total_crimes, ranges.total_crimes, true) },
    { key: "Felony share", val: fmtPct(f.felony_share), pct: barPct(f.felony_share, ranges.felony_share, true) },
    { key: "Restaurant inspections", val: fmt(f.n_inspections, 0), pct: barPct(f.n_inspections, ranges.n_inspections) },
    { key: "Avg inspection score", val: fmt(f.avg_score, 1), pct: barPct(f.avg_score, ranges.avg_score, true) },
    { key: "Critical-violation rate", val: fmtPct(f.critical_rate), pct: barPct(f.critical_rate, ranges.critical_rate, true) },
    { key: "311 complaints", val: fmt(f.n_complaints, 0), pct: barPct(f.n_complaints, ranges.n_complaints, true) },
    { key: "Median rent (ZORI)", val: fmtMoney(f.median_rent_zori), pct: barPct(f.median_rent_zori, ranges.median_rent_zori, true) },
  ];

  const scoreColor = score == null ? "var(--text-dim)" :
    score >= 66 ? "var(--good)" :
    score >= 33 ? "var(--accent)" : "var(--bad)";

  el.innerHTML = `
    <div class="detail-head">
      <h2>${props.nta_name || props.ntaname || props.nta_code || "Unknown"}</h2>
      <div class="code">${props.nta_code || ""}</div>
    </div>
    ${score != null ? `
      <div class="score-badge">
        <span class="value" style="color:${scoreColor}">${fmt(score, 1)}</span>
        <span class="out-of">/ 100</span>
        <span class="label">Newcomer&nbsp;Score</span>
      </div>` : ""}
    <div class="metric-list">
      ${rows.map((r) => `
        <div class="metric-row">
          <div class="metric-row-top">
            <span class="key">${r.key}</span>
            <span class="val">${r.val}</span>
          </div>
          <div class="metric-row-bar"><div style="width:${r.pct.toFixed(1)}%"></div></div>
        </div>
      `).join("")}
    </div>
  `;
}

function onEachFeature(feature, layer) {
  const name = feature.properties && (feature.properties.nta_name || feature.properties.ntaname);
  if (name) layer.bindTooltip(name, { sticky: true, direction: "top", offset: [0, -4] });

  layer.on({
    mouseover: (e) => {
      e.target.setStyle({ weight: 2, color: "#0f172a" });
      e.target.bringToFront();
    },
    mouseout: (e) => {
      if (e.target !== selectedLayer) {
        e.target.setStyle({ weight: 0.6, color: "#ffffff" });
      }
    },
    click: () => {
      if (selectedLayer && selectedLayer !== layer) {
        selectedLayer.setStyle({ weight: 0.6, color: "#ffffff" });
      }
      layer.setStyle({ weight: 2.5, color: "#0f172a" });
      layer.bringToFront();
      selectedLayer = layer;
      const ranges = computeMetricRanges(lastGeoJson);
      renderDetail(feature.properties || {}, ranges);
    },
  });
}

async function loadMetric(metric) {
  statusText(`Loading ${METRIC_LABELS[metric]}…`, "loading");
  currentMetric = metric;

  // Update pill active state.
  document.querySelectorAll("#metric-pills button").forEach((b) => {
    b.classList.toggle("active", b.dataset.metric === metric);
  });

  let r;
  try {
    r = await fetch(`${API_BASE}/neighborhoods?metric=${encodeURIComponent(metric)}`);
  } catch (err) {
    statusText(`Couldn't reach API — is it running?`, "error");
    return;
  }
  if (!r.ok) { statusText(`API error ${r.status}`, "error"); return; }

  const geo = await r.json();
  lastGeoJson = geo;
  const values = geo.features.map((f) => f.properties.metric_value);
  currentBreaks = quantileBreaks(values);
  if (geoLayer) geoLayer.remove();
  geoLayer = L.geoJSON(geo, { style: styleFeature, onEachFeature }).addTo(map);
  try { map.fitBounds(geoLayer.getBounds(), { padding: [20, 20] }); } catch (_) {}
  renderLegend(currentBreaks);
  statusText(`${geo.features.length} neighborhoods loaded`, "ok");

  // Re-render detail if a neighborhood is still selected.
  if (selectedLayer) {
    const ranges = computeMetricRanges(geo);
    renderDetail(selectedLayer.feature.properties || {}, ranges);
  }
}

document.getElementById("metric-pills").addEventListener("click", (ev) => {
  const btn = ev.target.closest("button[data-metric]");
  if (!btn) return;
  loadMetric(btn.dataset.metric);
});

loadMetric(currentMetric);
