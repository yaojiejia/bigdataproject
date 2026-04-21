// ============================================================
// web/lib/figures.js — Plotly figure builders for the dashboard.
// ============================================================
//
// Every figure is assembled from pre-aggregated parquet tables written
// by `etl_code/alexj/Analytics.scala`. No server-side figure
// generation, no pandas — just column arrays in, Plotly spec out.
//
// Visual language: editorial light theme. Ink on warm paper, a muted
// earthen palette for boroughs, no neon, no gradients. Trace colours
// for "good" and "bad" directions are forest + brick — deliberately
// editorial, not dashboard-default red/green.
//
// Contract: if you change a column name in Analytics.scala you MUST
// update the corresponding builder here — there's no middle-tier
// schema validator to catch it.

import { filterRows, groupBy, sortBy } from "./parquet.js";

// ---- Palette ---------------------------------------------------------------

export const COLORS = {
  paper:   "#ffffff",          // chart background inside a card
  ink:     "#1a1815",          // primary text / axis
  muted:   "#58504a",          // secondary text
  dim:     "#8b837b",          // tertiary text / tick labels
  rule:    "#e4ded3",          // borders, axis lines
  grid:    "rgba(26,24,21,0.08)",
  // Data directions — used for the single-colour traces (histograms, bars).
  good:    "#3a5a40",          // forest, for "higher is better"
  bad:     "#8b3a3a",          // brick, for "higher is worse"
  // Neutral anchor for median/trend lines.
  anchor:  "#1a1815",
};

// Borough colours — warm editorial set, not primary-dashboard palette.
// Manhattan = ink-blue, Brooklyn = forest, Queens = ochre, Bronx = brick,
// Staten Island = umber. Deliberately muted so no single borough
// dominates the eye.
export const BOROUGH_COLORS = {
  Manhattan:       "#2c5475",
  Brooklyn:        "#3a5a40",
  Queens:          "#b08837",
  Bronx:           "#8b3a3a",
  "Staten Island": "#6b4226",
  Other:           "#8b837b",
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
  // Titles are rendered in the HTML card head now (serif, designed).
  // We pass `title` through only so existing call sites don't have to
  // rewire; if a caller still wants it, it's drawn small and muted as
  // a fallback label.
  const layout = {
    title: {
      text: "",
      x: 0, xanchor: "left",
      font: { size: 13, color: COLORS.muted, family: "Inter, sans-serif" },
    },
    paper_bgcolor: COLORS.paper,
    plot_bgcolor:  COLORS.paper,
    font: { family: "Inter, -apple-system, sans-serif", color: COLORS.ink, size: 12 },
    margin: { l: 60, r: 20, t: 24, b: 50 },
    height,
    xaxis: {
      gridcolor: COLORS.grid, zerolinecolor: COLORS.grid,
      linecolor: COLORS.rule, tickcolor: COLORS.rule,
      tickfont: { color: COLORS.muted, size: 11 },
      titlefont: { color: COLORS.muted, size: 12 },
    },
    yaxis: {
      gridcolor: COLORS.grid, zerolinecolor: COLORS.grid,
      linecolor: COLORS.rule, tickcolor: COLORS.rule,
      tickfont: { color: COLORS.muted, size: 11 },
      titlefont: { color: COLORS.muted, size: 12 },
    },
    legend: {
      bgcolor: "rgba(0,0,0,0)",
      bordercolor: COLORS.rule,
      borderwidth: 0,
      font: { color: COLORS.muted, size: 11 },
    },
  };
  void title; // now carried by the card head, not the figure
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
  const barColor = meta.higherIsBetter ? COLORS.good : COLORS.bad;

  const trace = {
    type: "bar",
    x, y,
    marker: { color: barColor, line: { color: COLORS.paper, width: 1 } },
    width: new Array(x.length).fill(width * 0.95),
    opacity: 0.92,
    name: meta.label,
    hovertemplate: "<b>%{x}</b><br>%{y} neighborhoods<extra></extra>",
  };

  const mean   = summary.mean;
  const median = summary.median;
  const lineAnn = (val, color, dash) => ({
    type: "line",
    xref: "x", yref: "paper",
    x0: val, x1: val, y0: 0, y1: 1,
    line: { color, width: 1.25, dash },
  });
  const layout = baseLayout(
    `Distribution of ${meta.label}`,
    420,
    {
      xaxis: { title: meta.label },
      yaxis: { title: "Neighborhoods" },
      bargap: 0.05,
      showlegend: false,
      shapes: [
        mean != null ? lineAnn(mean, COLORS.anchor, "dash") : null,
        median != null ? lineAnn(median, COLORS.muted, "dot") : null,
      ].filter(Boolean),
      annotations: [
        mean != null ? {
          x: mean, y: 1.02, yref: "paper", xanchor: "left", yanchor: "bottom",
          text: `mean ${Number(mean).toFixed(1)}`, showarrow: false,
          font: { color: COLORS.anchor, size: 11 },
        } : null,
        median != null ? {
          x: median, y: 1.02, yref: "paper", xanchor: "right", yanchor: "bottom",
          text: `median ${Number(median).toFixed(1)}`, showarrow: false,
          font: { color: COLORS.muted, size: 11 },
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
    marker: { color, line: { color: COLORS.paper, width: 1 } },
    text: rows.map((r) => Number(r.value).toLocaleString(undefined, { maximumFractionDigits: 1 })),
    textposition: "outside",
    textfont: { color: COLORS.muted, size: 10.5 },
    hovertemplate: "<b>%{y}</b><br>" + meta.label + ": %{x:,.2f}<extra></extra>",
    showlegend: false,
  });

  const traces = [
    bar(topR, COLORS.good, { x: "x",  y: "y"  }),
    bar(botR, COLORS.bad,  { x: "x2", y: "y2" }),
  ];

  const axisStyle = {
    gridcolor: COLORS.grid, zerolinecolor: COLORS.grid,
    linecolor: COLORS.rule,  tickcolor: COLORS.rule,
    tickfont:  { color: COLORS.muted, size: 10.5 },
    titlefont: { color: COLORS.muted, size: 12 },
  };

  const layout = baseLayout(
    `${meta.label} — best vs. worst`,
    Math.max(380, 40 + n * 28),
    {
      grid: { rows: 1, columns: 2, pattern: "independent" },
      xaxis:  Object.assign({ domain: [0, 0.44], title: meta.label, anchor: "y"  }, axisStyle),
      xaxis2: Object.assign({ domain: [0.56, 1], title: meta.label, anchor: "y2" }, axisStyle),
      yaxis:  Object.assign({ automargin: true, anchor: "x"  }, axisStyle),
      yaxis2: Object.assign({ automargin: true, anchor: "x2" }, axisStyle),
      annotations: [
        { x: 0.22, y: 1.04, xref: "paper", yref: "paper", showarrow: false,
          text: `Best ${n}`, font: { color: COLORS.muted, size: 11 } },
        { x: 0.78, y: 1.04, xref: "paper", yref: "paper", showarrow: false,
          text: `Worst ${n}`, font: { color: COLORS.muted, size: 11 } },
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
    marker: { color: BOROUGH_COLORS[b.borough] || COLORS.anchor },
    line:   { color: BOROUGH_COLORS[b.borough] || COLORS.anchor, width: 1.25 },
    fillcolor: BOROUGH_COLORS[b.borough] || COLORS.anchor,
    opacity: 0.18,
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
      x: rows.map(() => borough),
      y: rows.map((r) => r.value),
      text: rows.map((r) => r.nta_name),
      marker: {
        color: BOROUGH_COLORS[borough] || COLORS.anchor,
        size: 5, opacity: 0.8,
        line: { color: COLORS.paper, width: 0.5 },
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

  // Custom RdBu-ish scale tuned to the warm paper background. Endpoints
  // are our editorial brick (-1) and ink-navy (+1); mid-point is the
  // paper tint itself so that r ≈ 0 cells disappear into the card.
  const colorscale = [
    [0.00, "#8b3a3a"],
    [0.25, "#c2876f"],
    [0.50, "#f4ebdc"],
    [0.75, "#6b8aa6"],
    [1.00, "#2c5475"],
  ];

  const trace = {
    type: "heatmap",
    z, x: labels, y: labels,
    zmin: -1, zmax: 1,
    colorscale,
    text, texttemplate: "%{text}",
    textfont: { color: COLORS.ink, size: 11 },
    hovertemplate: "%{y} ↔ %{x}<br>r = %{z:.2f}<extra></extra>",
    xgap: 2, ygap: 2,
    colorbar: {
      title: { text: "Pearson r", side: "right", font: { color: COLORS.muted, size: 11 } },
      tickcolor:    COLORS.rule,
      outlinecolor: COLORS.rule,
      tickfont:     { color: COLORS.muted, size: 10.5 },
      thickness: 12,
      len: 0.7,
    },
  };
  const layout = baseLayout(
    "Correlations among the score's inputs",
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
        color: BOROUGH_COLORS[borough] || COLORS.anchor,
        size: 6, opacity: 0.55,
        line: { color: COLORS.paper, width: 0.5 },
      },
      hovertemplate:
        "<b>%{text}</b><br>" +
        meta.short + ": %{x}<br>" +
        "median rent: $%{y:,.0f}" +
        `<extra>${borough}</extra>`,
    });
  }

  // IQR fill band — subtle ink tint, not a coloured accent.
  if (xBin.length) {
    traces.push({
      type: "scatter",
      name: "IQR of rent",
      x: [...xBin, ...xBin.slice().reverse()],
      y: [...yQ3, ...yQ1.slice().reverse()],
      fill: "toself",
      fillcolor: "rgba(26,24,21,0.07)",
      line: { color: "rgba(0,0,0,0)" },
      hoverinfo: "skip",
      showlegend: true,
    });
  }

  // Binned median — headline trend. Ink on paper, single confident stroke.
  traces.push({
    type: "scatter", mode: "lines+markers",
    name: "Median rent per bin",
    x: xBin, y: yBin,
    line:   { color: COLORS.anchor, width: 2 },
    marker: { size: 6, color: COLORS.anchor, line: { color: COLORS.paper, width: 1 } },
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
      line: { color: COLORS.bad, width: 1.5, dash: "dash" },
      hovertemplate:
        meta.short + ": %{x}" +
        "<br>predicted rent: $%{y:,.0f}<extra>OLS fit</extra>",
    });
    const xEx = Number(ols.x_example_median);
    const yEx = Number(ols.y_example_predicted);
    const r   = Number(ols.pearson_r || 0);
    const r2  = Number(ols.r2 || 0);
    const slopeLine = `Δrent ≈ <b>$${(slope >= 0 ? "+" : "") + Math.round(slope).toLocaleString()}</b> per unit ${meta.short}`;
    const predLine  = `At ${meta.short} = ${meta.fmt(xEx)}, fit predicts <b>$${Math.max(0, Math.round(yEx)).toLocaleString()}/mo</b>`;
    const r2Line    = `r = ${(r >= 0 ? "+" : "") + r.toFixed(2)},   R² = ${r2.toFixed(2)}`;
    annotationText  = [slopeLine, predLine, r2Line].join("<br>");
  }

  const layout = baseLayout(
    `Median rent vs ${meta.short}`,
    460,
    {
      xaxis: { title: meta.label },
      yaxis: { title: "Median rent ($/month)" },
      legend: { orientation: "h", y: -0.18, x: 0 },
      annotations: annotationText
        ? [{
            xref: "paper", yref: "paper", x: 0.99, y: 0.99,
            xanchor: "right", yanchor: "top",
            text: annotationText, showarrow: false, align: "right",
            bgcolor: "rgba(255,255,255,0.92)",
            bordercolor: COLORS.rule, borderwidth: 1,
            borderpad: 10,
            font: { color: COLORS.ink, size: 11 },
          }]
        : [],
    }
  );
  return { data: traces, layout };
}
