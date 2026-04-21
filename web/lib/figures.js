// ============================================================
// web/lib/figures.js — Plotly figure builders for the dashboard.
// ============================================================
//
// Every figure in the dashboard is assembled from pre-aggregated parquet
// tables written by `etl_code/alexj/Analytics.scala`. No server-side
// figure generation, no pandas — just column arrays in, Plotly spec out.
//
// The visual language (palette, typography, borough colours, layout
// margins) is a direct port of what the previous Python `analytics.py`
// produced so the dashboard looks identical to the screenshots people
// have seen before, even though the codepath is now completely
// different.
//
// Contract reminder: if you change a column name in Analytics.scala you
// MUST update the corresponding builder here — there's no middle-tier
// schema validator to catch it.

import { filterRows, groupBy, sortBy } from "./parquet.js";

// ---- Palette ---------------------------------------------------------------

export const COLORS = {
  bg:     "#0f172a",
  panel:  "#1e293b",
  border: "#334155",
  text:   "#f1f5f9",
  muted:  "#94a3b8",
  accent: "#818cf8",
  good:   "#34d399",
  bad:    "#f87171",
  grid:   "rgba(148,163,184,0.15)",
};

export const BOROUGH_COLORS = {
  Manhattan:       "#818cf8",
  Brooklyn:        "#34d399",
  Queens:          "#fbbf24",
  Bronx:           "#f87171",
  "Staten Island": "#60a5fa",
  Other:           "#64748b",
};

// ---- Metric + predictor metadata (mirror Analytics.scala constants) --------

export const METRICS = {
  score: {
    column: "newcomer_score_100",
    label:  "Newcomer Score",
    unit:   "/ 100",
    higherIsBetter: true,
    fmt: (v) => v == null ? "—" : Number(v).toFixed(1),
  },
  crime: {
    column: "crimes_per_1k",
    label:  "Crimes per 1k residents",
    unit:   "per 1k",
    higherIsBetter: false,
    fmt: (v) => v == null ? "—" : Number(v).toFixed(1),
  },
  food: {
    column: "critical_rate",
    label:  "Critical inspection rate",
    unit:   "share",
    higherIsBetter: false,
    fmt: (v) => v == null ? "—" : (v * 100).toFixed(1) + "%",
  },
  rent: {
    column: "median_rent_zori",
    label:  "Median rent (ZORI)",
    unit:   "$ / month",
    higherIsBetter: false,
    fmt: (v) => v == null ? "—" : "$" + Math.round(v).toLocaleString(),
  },
  "311": {
    column: "complaints_per_1k",
    label:  "311 complaints per 1k",
    unit:   "per 1k",
    higherIsBetter: false,
    fmt: (v) => v == null ? "—" : Number(v).toFixed(1),
  },
};

export const RENT_PREDICTOR_META = {
  crimes_per_1k: {
    label: "Crimes per 1,000 residents",
    short: "crimes / 1k",
    fmt: (v) => Number(v).toFixed(1),
  },
  felony_share: {
    label: "Share of crimes classified as felonies",
    short: "felony share",
    fmt: (v) => (v * 100).toFixed(1) + "%",
  },
  avg_score: {
    label: "Average DOHMH inspection score (lower = cleaner)",
    short: "inspection score",
    fmt: (v) => Number(v).toFixed(1),
  },
  critical_rate: {
    label: "Critical inspection violation rate",
    short: "critical rate",
    fmt: (v) => (v * 100).toFixed(1) + "%",
  },
  complaints_per_1k: {
    label: "311 food-safety complaints per 1,000 residents",
    short: "311 / 1k",
    fmt: (v) => Number(v).toFixed(1),
  },
};

export const INPUT_FEATURE_LABELS = {
  crimes_per_1k:     "Crimes / 1k",
  felony_share:      "Felony share",
  avg_score:         "Avg inspection score",
  critical_rate:     "Critical viol. rate",
  complaints_per_1k: "311 / 1k",
  median_rent_zori:  "Median rent",
};

// ---- Shared layout ---------------------------------------------------------

export function baseLayout(title, height = 420, extra = {}) {
  const layout = {
    title: {
      text: title, x: 0, xanchor: "left",
      font: { size: 15, color: COLORS.text },
    },
    paper_bgcolor: COLORS.bg,
    plot_bgcolor:  COLORS.bg,
    font: { family: "Inter, -apple-system, sans-serif", color: COLORS.text, size: 12 },
    margin: { l: 60, r: 20, t: 50, b: 50 },
    height,
    xaxis: {
      gridcolor: COLORS.grid, zerolinecolor: COLORS.grid,
      linecolor: COLORS.border, tickcolor: COLORS.border,
    },
    yaxis: {
      gridcolor: COLORS.grid, zerolinecolor: COLORS.grid,
      linecolor: COLORS.border, tickcolor: COLORS.border,
    },
    legend: {
      bgcolor: "rgba(0,0,0,0)",
      bordercolor: COLORS.border,
      borderwidth: 1,
    },
  };
  // Shallow-merge extras so callers can override axis titles, etc.
  for (const k of Object.keys(extra)) {
    if (k === "xaxis" || k === "yaxis" || k === "legend" || k === "title") {
      layout[k] = Object.assign({}, layout[k], extra[k]);
    } else {
      layout[k] = extra[k];
    }
  }
  return layout;
}

// ---- 1. Distribution histogram --------------------------------------------

export function distributionFigure(distRows, summaryRows, metricKey) {
  const meta = METRICS[metricKey];
  const bins = filterRows(distRows, (r) => r.metric === metricKey);
  const summary = summaryRows.find((r) => r.metric === metricKey) || {};

  const width = bins.length >= 2 ? (bins[1].bin_lo - bins[0].bin_lo) : 1;
  const x = bins.map((b) => b.bin_center);
  const y = bins.map((b) => Number(b.count));
  const barColor = meta.higherIsBetter ? COLORS.accent : COLORS.bad;

  const trace = {
    type: "bar",
    x, y,
    marker: { color: barColor, line: { color: COLORS.bg, width: 1 } },
    width: new Array(x.length).fill(width * 0.95),
    opacity: 0.85,
    name: meta.label,
    hovertemplate: "<b>%{x}</b><br>%{y} NTAs<extra></extra>",
  };

  const mean   = summary.mean;
  const median = summary.median;
  const lineAnn = (val, color, dash, posX, posY, label) => ({
    type: "line",
    xref: "x", yref: "paper",
    x0: val, x1: val, y0: 0, y1: 1,
    line: { color, width: 2, dash },
  });
  const layout = baseLayout(
    `Distribution of ${meta.label} across ${Number(summary.n_ntas || 0)} NTAs`,
    420,
    {
      xaxis: { title: meta.label },
      yaxis: { title: "Number of neighborhoods" },
      bargap: 0.05,
      showlegend: false,
      shapes: [
        mean != null ? lineAnn(mean, COLORS.accent, "dash") : null,
        median != null ? lineAnn(median, COLORS.good, "dot") : null,
      ].filter(Boolean),
      annotations: [
        mean != null ? {
          x: mean, y: 1, yref: "paper", xanchor: "left", yanchor: "top",
          text: `mean ${Number(mean).toFixed(1)}`, showarrow: false,
          font: { color: COLORS.accent },
        } : null,
        median != null ? {
          x: median, y: 1, yref: "paper", xanchor: "right", yanchor: "top",
          text: `median ${Number(median).toFixed(1)}`, showarrow: false,
          font: { color: COLORS.good },
        } : null,
      ].filter(Boolean),
    }
  );
  return { data: [trace], layout };
}

// ---- 2. Top / bottom horizontal bars --------------------------------------

export function topBottomFigure(topBottomRows, metricKey, n = 10) {
  const meta = METRICS[metricKey];
  const top = sortBy(
    filterRows(topBottomRows, (r) => r.metric === metricKey && r.kind === "top"),
    "rank", "asc"
  ).slice(0, n);
  const bottom = sortBy(
    filterRows(topBottomRows, (r) => r.metric === metricKey && r.kind === "bottom"),
    "rank", "asc"
  ).slice(0, n);

  // Reverse so the top-ranked sits at the top of the bar chart.
  const reverse = (arr) => arr.slice().reverse();
  const topR = reverse(top);
  const botR = reverse(bottom);

  const bar = (rows, color, axis) => ({
    type: "bar",
    orientation: "h",
    x: rows.map((r) => r.value),
    y: rows.map((r) => r.nta_name),
    xaxis: axis.x, yaxis: axis.y,
    marker: { color, line: { color: COLORS.bg, width: 1 } },
    text: rows.map((r) => Number(r.value).toLocaleString(undefined, { maximumFractionDigits: 1 })),
    textposition: "outside",
    textfont: { color: COLORS.text, size: 11 },
    hovertemplate: "<b>%{y}</b><br>" + meta.label + ": %{x:,.2f}<extra></extra>",
    showlegend: false,
  });

  const traces = [
    bar(topR, COLORS.good, { x: "x",  y: "y"  }),
    bar(botR, COLORS.bad,  { x: "x2", y: "y2" }),
  ];

  const axisStyle = {
    gridcolor: COLORS.grid, zerolinecolor: COLORS.grid,
    linecolor: COLORS.border, tickcolor: COLORS.border,
  };

  const layout = baseLayout(
    `${meta.label} — top vs. bottom ${n} NTAs`,
    Math.max(380, 40 + n * 28),
    {
      grid: { rows: 1, columns: 2, pattern: "independent" },
      xaxis:  Object.assign({ domain: [0, 0.44], title: meta.label, anchor: "y"  }, axisStyle),
      xaxis2: Object.assign({ domain: [0.56, 1], title: meta.label, anchor: "y2" }, axisStyle),
      yaxis:  Object.assign({ automargin: true, anchor: "x"  }, axisStyle),
      yaxis2: Object.assign({ automargin: true, anchor: "x2" }, axisStyle),
      annotations: [
        { x: 0.22, y: 1.06, xref: "paper", yref: "paper", showarrow: false,
          text: `Top ${n} (best)`, font: { color: COLORS.muted, size: 12 } },
        { x: 0.78, y: 1.06, xref: "paper", yref: "paper", showarrow: false,
          text: `Bottom ${n} (worst)`, font: { color: COLORS.muted, size: 12 } },
      ],
    }
  );
  return { data: traces, layout };
}

// ---- 3. By-borough box + points overlay ------------------------------------

export function byBoroughFigure(boxRows, pointRows, metricKey) {
  const meta  = METRICS[metricKey];
  const boxes = filterRows(boxRows, (r) => r.metric === metricKey);
  const pts   = filterRows(pointRows, (r) => r.metric === metricKey);

  // Sort boroughs by median so the eye moves down the ranking.
  boxes.sort((a, b) => {
    const av = a.median == null ? Infinity : a.median;
    const bv = b.median == null ? Infinity : b.median;
    return meta.higherIsBetter ? bv - av : av - bv;
  });
  const order = boxes.map((b) => b.borough);

  // Pre-computed 5-number summary boxes.
  const boxTraces = boxes.map((b) => ({
    type: "box",
    name: b.borough,
    x: [b.borough],
    q1: [b.q1], median: [b.median], q3: [b.q3],
    lowerfence: [b.lowerfence], upperfence: [b.upperfence],
    marker: { color: BOROUGH_COLORS[b.borough] || COLORS.accent },
    line:   { color: BOROUGH_COLORS[b.borough] || COLORS.accent, width: 1.5 },
    fillcolor: BOROUGH_COLORS[b.borough] || COLORS.accent,
    opacity: 0.35,
    boxpoints: false,
    showlegend: false,
    hovertemplate:
      `${b.borough}<br>` +
      `Q1: %{q1[0]:,.2f}<br>` +
      `Median: %{median[0]:,.2f}<br>` +
      `Q3: %{q3[0]:,.2f}` +
      "<extra></extra>",
  }));

  // Jittered scatter overlay so the reader sees actual NTA values.
  const byBorough = groupBy(pts, "borough");
  const scatterTraces = [];
  for (const [borough, rows] of byBorough.entries()) {
    if (!order.includes(borough)) continue;
    const rnd = rows.map(() => (Math.random() - 0.5) * 0.5);
    scatterTraces.push({
      type: "scatter",
      mode: "markers",
      x: rows.map((r, i) => {
        // Plotly categorical x can take the borough string directly; it
        // renders the marker on that category band. Jitter is applied by
        // nudging the x index slightly via a secondary numeric axis.
        return borough;
      }),
      y: rows.map((r) => r.value),
      text: rows.map((r) => r.nta_name),
      marker: {
        color: BOROUGH_COLORS[borough] || COLORS.accent,
        size: 5, opacity: 0.75,
        line: { color: COLORS.bg, width: 0.5 },
      },
      showlegend: false,
      hovertemplate: "<b>%{text}</b><br>" + meta.label + ": %{y:,.2f}<extra>" + borough + "</extra>",
    });
  }

  const layout = baseLayout(
    `${meta.label} by borough`,
    420,
    {
      xaxis: { title: "", type: "category", categoryorder: "array", categoryarray: order },
      yaxis: { title: meta.label },
      boxmode: "overlay",
    }
  );
  return { data: [...boxTraces, ...scatterTraces], layout };
}

// ---- 4. Correlation heatmap -----------------------------------------------

export function correlationFigure(corrRows, featureList) {
  const features = featureList;
  const labels   = features.map((f) => INPUT_FEATURE_LABELS[f] || f);

  // Build a (n x n) matrix from the long-form rows.
  const idx = new Map(features.map((f, i) => [f, i]));
  const n = features.length;
  const z = Array.from({ length: n }, () => new Array(n).fill(null));
  for (const r of corrRows) {
    const i = idx.get(r.feature_row);
    const j = idx.get(r.feature_col);
    if (i != null && j != null) z[i][j] = r.r;
  }
  const text = z.map((row) => row.map((v) => v == null ? "" : (v >= 0 ? "+" : "") + Number(v).toFixed(2)));

  const trace = {
    type: "heatmap",
    z, x: labels, y: labels,
    zmin: -1, zmax: 1,
    colorscale: "RdBu", reversescale: true,
    text, texttemplate: "%{text}",
    textfont: { color: COLORS.text, size: 11 },
    hovertemplate: "%{y} ↔ %{x}<br>r = %{z:.2f}<extra></extra>",
    colorbar: {
      title: { text: "Pearson r", side: "right" },
      tickcolor: COLORS.border,
      outlinecolor: COLORS.border,
    },
  };
  const layout = baseLayout(
    "Correlation of the six Newcomer Score inputs",
    460,
    {
      xaxis: { side: "bottom", tickangle: -25 },
      yaxis: { autorange: "reversed" },
    }
  );
  return { data: [trace], layout };
}

// ---- 5. Rent vs. feature (bins + OLS + scatter) ---------------------------

export function rentVsFeatureFigure(binRows, olsRows, pointRows, feature) {
  const meta = RENT_PREDICTOR_META[feature];
  const bins = sortBy(filterRows(binRows, (r) => r.feature === feature), "x_mid", "asc");
  const ols  = olsRows.find((r) => r.feature === feature);
  const pts  = filterRows(pointRows, (r) => r.feature === feature);

  const xBin = bins.map((b) => b.x_mid);
  const yBin = bins.map((b) => b.rent_median);
  const yQ1  = bins.map((b) => b.rent_q1);
  const yQ3  = bins.map((b) => b.rent_q3);

  const traces = [];

  // Scatter of individual NTAs, one trace per borough.
  const byBorough = groupBy(pts, "borough");
  for (const [borough, rows] of byBorough.entries()) {
    traces.push({
      type: "scatter", mode: "markers",
      name: borough,
      x: rows.map((r) => r.x),
      y: rows.map((r) => r.y),
      text: rows.map((r) => r.nta_name),
      marker: {
        color: BOROUGH_COLORS[borough] || COLORS.accent,
        size: 6, opacity: 0.45,
        line: { color: COLORS.bg, width: 0.5 },
      },
      hovertemplate:
        "<b>%{text}</b><br>" +
        meta.short + ": %{x}<br>" +
        "median rent: $%{y:,.0f}" +
        `<extra>${borough}</extra>`,
    });
  }

  // IQR fill band.
  if (xBin.length) {
    traces.push({
      type: "scatter",
      name: "IQR band (25th-75th)",
      x: [...xBin, ...xBin.slice().reverse()],
      y: [...yQ3, ...yQ1.slice().reverse()],
      fill: "toself",
      fillcolor: "rgba(129,140,248,0.18)",
      line: { color: "rgba(0,0,0,0)" },
      hoverinfo: "skip",
      showlegend: true,
    });
  }

  // Binned median — headline trend.
  traces.push({
    type: "scatter", mode: "lines+markers",
    name: "Binned median rent",
    x: xBin, y: yBin,
    line:   { color: COLORS.accent, width: 3 },
    marker: { size: 8, color: COLORS.accent, line: { color: COLORS.bg, width: 1.5 } },
    customdata: bins.map((b) => [Number(b.n_ntas), b.rent_q1, b.rent_q3]),
    hovertemplate:
      `<b>bin median ${meta.short}</b>: %{x}` +
      "<br>median rent: $%{y:,.0f}" +
      "<br>IQR: $%{customdata[1]:,.0f} – $%{customdata[2]:,.0f}" +
      "<br>NTAs in bin: %{customdata[0]}" +
      "<extra></extra>",
  });

  // OLS fit line.
  let annotationText = "";
  if (ols && ols.x_min != null && ols.x_max != null) {
    const slope = Number(ols.slope || 0);
    const intercept = Number(ols.intercept || 0);
    const xFit = [Number(ols.x_min), Number(ols.x_max)];
    const yFit = xFit.map((x) => slope * x + intercept);
    traces.push({
      type: "scatter", mode: "lines",
      name: `OLS fit (R² = ${Number(ols.r2 || 0).toFixed(2)})`,
      x: xFit, y: yFit,
      line: { color: COLORS.good, width: 2, dash: "dash" },
      hovertemplate:
        meta.short + ": %{x}" +
        "<br>predicted rent: $%{y:,.0f}<extra>OLS fit</extra>",
    });
    const xEx = Number(ols.x_example_median);
    const yEx = Number(ols.y_example_predicted);
    const r   = Number(ols.pearson_r || 0);
    const r2  = Number(ols.r2 || 0);
    const slopeLine = `Δrent ≈ <b>$${(slope >= 0 ? "+" : "") + Math.round(slope).toLocaleString()}</b> per unit ${meta.short}`;
    const predLine  = `At ${meta.short} = ${meta.fmt(xEx)}, model predicts <b>$${Math.max(0, Math.round(yEx)).toLocaleString()}/mo</b>`;
    const r2Line    = `Pearson r = ${(r >= 0 ? "+" : "") + r.toFixed(2)}, R² = ${r2.toFixed(2)}`;
    annotationText  = [slopeLine, predLine, r2Line].join("<br>");
  }

  const layout = baseLayout(
    `Median rent vs ${meta.short} — ${pts.length} NTAs`,
    460,
    {
      xaxis: { title: meta.label },
      yaxis: { title: "Median rent (ZORI, $/month)" },
      legend: { orientation: "h", y: -0.18, x: 0 },
      annotations: annotationText
        ? [{
            xref: "paper", yref: "paper", x: 0.99, y: 0.99,
            xanchor: "right", yanchor: "top",
            text: annotationText, showarrow: false, align: "right",
            bgcolor: "rgba(15,23,42,0.85)",
            bordercolor: COLORS.border, borderwidth: 1,
            borderpad: 8,
            font: { color: COLORS.text, size: 11 },
          }]
        : [],
    }
  );
  return { data: traces, layout };
}
