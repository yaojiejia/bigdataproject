// ============================================================
// web/map.js — Leaflet choropleth, no backend.
// ============================================================
//
// Fetches the NTA GeoJSON + the single-file parquet written by
// `etl_code/alexj/Score.scala` directly over the static server, joins
// them in-browser on `nta_code`, and hands the resulting GeoJSON to
// Leaflet. Metric switching is a pure re-style (no new network calls).
// ============================================================

import { loadParquet } from "./lib/parquet.js";

const NTA_GEOJSON   = "../data/geo/nta.geojson";
const SCORE_PARQUET = "../data/scores/newcomer_score.parquet";

// Attempt to also read the streaming sink (may not exist on first boot —
// we handle the 404 gracefully). The Consumer.scala writes a directory,
// so we need a deterministic filename inside it. We publish a manifest
// at init time by polling both shapes.
const STREAM_LATEST_DIR = "../data/stream/latest.parquet";

const METRIC_LABELS = {
  score: "Newcomer Score",
  crime: "Crime intensity",
  food:  "Critical inspection rate",
  rent:  "Median rent (ZORI)",
  "311": "Recent 311 complaints",
};

const METRIC_UNITS = {
  score: "/ 100",
  crime: "per 1k residents",
  food:  "% critical",
  rent:  "$ / month",
  "311": "per 1k residents",
};

// Map metric pill -> column in newcomer_score.parquet.
const METRIC_COLUMN = {
  score: "newcomer_score_100",
  crime: "crimes_per_1k",
  food:  "critical_rate",
  rent:  "median_rent_zori",
  "311": "complaints_per_1k",
};

const HIGHER_IS_BETTER = new Set(["score"]);

const RAMP_POS = ["#f3f0ff", "#dcd6f7", "#b9acee", "#9580e0", "#7459cc", "#5a3ab2", "#422388", "#2e1065"];
const RAMP_NEG = ["#fff5eb", "#fee6ce", "#fdd0a2", "#fdae6b", "#fd8d3c", "#f16913", "#d94801", "#7f2704"];

const map = L.map("map", { preferCanvas: true, zoomControl: false })
  .setView([40.73, -73.97], 11);
L.control.zoom({ position: "bottomright" }).addTo(map);

L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
  attribution: "© OpenStreetMap · © CARTO",
  subdomains: "abcd",
  maxZoom: 19,
}).addTo(map);

// ---- State ---------------------------------------------------------------

const STATE = {
  metric: "score",
  geoLayer: null,
  breaks: [],
  selectedLayer: null,
  scoreByNta: new Map(),  // nta_code -> row
  geojson: null,
  merged: null,           // GeoJSON with properties.features populated
};

function statusText(msg, state = "ok") {
  document.getElementById("status").textContent = msg;
  const dot = document.getElementById("status-dot");
  dot.className = "status-dot" + (state === "ok" ? "" : " " + state);
}

function currentRamp() {
  return HIGHER_IS_BETTER.has(STATE.metric) ? RAMP_POS : RAMP_NEG;
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
    fillColor: colorFor(v, STATE.breaks),
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
  const title = METRIC_LABELS[STATE.metric];
  const lo = breaks.length ? breaks[0] : null;
  const hi = breaks.length ? breaks[breaks.length - 1] : null;
  const formatter =
    STATE.metric === "food" ? fmtPct :
    STATE.metric === "rent" ? fmtMoney :
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

// ---- Detail panel --------------------------------------------------------

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
    { key: "Total crimes",           val: fmt(f.total_crimes, 0),       pct: barPct(f.total_crimes,     ranges.total_crimes,     true) },
    { key: "Felony share",           val: fmtPct(f.felony_share),       pct: barPct(f.felony_share,     ranges.felony_share,     true) },
    { key: "Restaurant inspections", val: fmt(f.n_inspections, 0),      pct: barPct(f.n_inspections,    ranges.n_inspections)         },
    { key: "Avg inspection score",   val: fmt(f.avg_score, 1),          pct: barPct(f.avg_score,        ranges.avg_score,        true) },
    { key: "Critical-violation rate",val: fmtPct(f.critical_rate),      pct: barPct(f.critical_rate,    ranges.critical_rate,    true) },
    { key: "311 complaints",         val: fmt(f.n_complaints, 0),       pct: barPct(f.n_complaints,     ranges.n_complaints,     true) },
    { key: "Median rent (ZORI)",     val: fmtMoney(f.median_rent_zori), pct: barPct(f.median_rent_zori, ranges.median_rent_zori, true) },
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
      if (e.target !== STATE.selectedLayer) {
        e.target.setStyle({ weight: 0.6, color: "#ffffff" });
      }
    },
    click: () => {
      if (STATE.selectedLayer && STATE.selectedLayer !== layer) {
        STATE.selectedLayer.setStyle({ weight: 0.6, color: "#ffffff" });
      }
      layer.setStyle({ weight: 2.5, color: "#0f172a" });
      layer.bringToFront();
      STATE.selectedLayer = layer;
      const ranges = computeMetricRanges(STATE.merged);
      renderDetail(feature.properties || {}, ranges);
    },
  });
}

// ---- Join parquet onto GeoJSON -------------------------------------------

const NTA_CODE_FIELDS = ["nta_code", "NTACode", "ntacode", "ntacode2020", "NTA2020", "nta2020"];
const NTA_NAME_FIELDS = ["nta_name", "NTAName", "ntaname", "NTA_NAME"];

function pickField(props, fields) {
  for (const f of fields) {
    if (props[f] != null && props[f] !== "") return props[f];
  }
  return null;
}

function buildMergedGeoJson(geoJsonRaw, scoreByNta) {
  const features = [];
  for (const feat of geoJsonRaw.features) {
    const p    = feat.properties || {};
    const code = pickField(p, NTA_CODE_FIELDS);
    const name = pickField(p, NTA_NAME_FIELDS);
    const row  = code ? scoreByNta.get(code) : null;
    features.push({
      type: "Feature",
      geometry: feat.geometry,
      properties: {
        nta_code: code,
        nta_name: name,
        features: row || {},
      },
    });
  }
  return { type: "FeatureCollection", features };
}

function applyMetric(metric) {
  const col = METRIC_COLUMN[metric];
  for (const f of STATE.merged.features) {
    const v = f.properties.features[col];
    f.properties.metric_value = v;
  }
  const values = STATE.merged.features.map((f) => f.properties.metric_value);
  STATE.breaks = quantileBreaks(values);
  if (STATE.geoLayer) STATE.geoLayer.remove();
  STATE.geoLayer = L.geoJSON(STATE.merged, { style: styleFeature, onEachFeature }).addTo(map);
  try { map.fitBounds(STATE.geoLayer.getBounds(), { padding: [20, 20] }); } catch (_) {}
  renderLegend(STATE.breaks);
  statusText(`${STATE.merged.features.length} neighborhoods loaded`, "ok");
  if (STATE.selectedLayer) {
    const ranges = computeMetricRanges(STATE.merged);
    renderDetail(STATE.selectedLayer.feature.properties || {}, ranges);
  }
}

function selectMetric(metric) {
  statusText(`Loading ${METRIC_LABELS[metric]}…`, "loading");
  STATE.metric = metric;
  document.querySelectorAll("#metric-pills button").forEach((b) => {
    b.classList.toggle("active", b.dataset.metric === metric);
  });
  applyMetric(metric);
}

document.getElementById("metric-pills").addEventListener("click", (ev) => {
  const btn = ev.target.closest("button[data-metric]");
  if (!btn) return;
  selectMetric(btn.dataset.metric);
});

// ---- Boot ----------------------------------------------------------------

(async function init() {
  statusText("Loading neighborhood data…", "loading");
  try {
    const [geoResp, scoreTable] = await Promise.all([
      fetch(NTA_GEOJSON),
      loadParquet(SCORE_PARQUET),
    ]);
    if (!geoResp.ok) throw new Error(`NTA GeoJSON HTTP ${geoResp.status}`);
    const geoJsonRaw = await geoResp.json();

    STATE.scoreByNta = new Map();
    for (const r of scoreTable.rows) {
      if (r.nta_code) STATE.scoreByNta.set(r.nta_code, r);
    }
    STATE.geojson = geoJsonRaw;
    STATE.merged  = buildMergedGeoJson(geoJsonRaw, STATE.scoreByNta);
    applyMetric(STATE.metric);
  } catch (err) {
    console.error(err);
    statusText(
      "Couldn't load data. Did you run `make pipeline`?",
      "error"
    );
  }
})();
