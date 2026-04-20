"""Shared analytical dashboard logic.

Both the FastAPI backend (`api/main.py`) and the Jupyter notebook
(`notebooks/analysis.ipynb`) import from this module, so the charts
they render are guaranteed to be identical — same aggregation, same
figure spec, same Plotly rendering engine.

Each `*_fig` function returns a `plotly.graph_objects.Figure` built
on top of `NEWCOMER_SCORE` (the per-NTA parquet emitted by
`etl_code/alexj/score.py`). The API serialises the figure with
`fig.to_json()`; the notebook calls `fig.show()` directly.
"""
from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go

# ---------------------------------------------------------------------------
# Constants shared with the rest of the app
# ---------------------------------------------------------------------------

# Canonical set of metric keys the dashboard understands. These match the
# keys accepted by `/neighborhoods?metric=...` so the map and dashboard
# stay in lockstep.
METRICS: dict[str, dict[str, Any]] = {
    "score": {
        "column": "newcomer_score_100",
        "label": "Newcomer Score",
        "unit": "/ 100",
        "higher_is_better": True,
        "fmt": ".1f",
    },
    "crime": {
        "column": "crimes_per_1k",
        "label": "Crimes per 1k residents",
        "unit": "per 1k",
        "higher_is_better": False,
        "fmt": ".1f",
    },
    "food": {
        "column": "critical_rate",
        "label": "Critical inspection rate",
        "unit": "share",
        "higher_is_better": False,
        "fmt": ".1%",
    },
    "rent": {
        "column": "median_rent_zori",
        "label": "Median rent (ZORI)",
        "unit": "$ / month",
        "higher_is_better": False,
        "fmt": "$,.0f",
    },
    "311": {
        "column": "complaints_per_1k",
        "label": "311 complaints per 1k",
        "unit": "per 1k",
        "higher_is_better": False,
        "fmt": ".1f",
    },
}

# Six inputs that feed the Newcomer Score, used for the correlation heatmap.
INPUT_FEATURES: list[str] = [
    "crimes_per_1k",
    "felony_share",
    "avg_score",
    "critical_rate",
    "complaints_per_1k",
    "median_rent_zori",
]

INPUT_FEATURE_LABELS = {
    "crimes_per_1k": "Crimes / 1k",
    "felony_share": "Felony share",
    "avg_score": "Avg inspection score",
    "critical_rate": "Critical viol. rate",
    "complaints_per_1k": "311 / 1k",
    "median_rent_zori": "Median rent",
}

# Features that can be used to predict rent. Excludes `median_rent_zori`
# itself, since predicting rent from rent is silly.
RENT_PREDICTORS: list[str] = [
    "crimes_per_1k",
    "felony_share",
    "avg_score",
    "critical_rate",
    "complaints_per_1k",
]

# Human-readable X-axis labels + hover-value formatters for each predictor.
# Kept separate from INPUT_FEATURE_LABELS so the rent-vs chart can use longer,
# more specific titles ("Crimes per 1,000 residents" vs "Crimes / 1k").
RENT_PREDICTOR_META: dict[str, dict[str, Any]] = {
    "crimes_per_1k": {
        "label": "Crimes per 1,000 residents",
        "short": "crimes / 1k",
        "fmt": ".1f",
    },
    "felony_share": {
        "label": "Share of crimes classified as felonies",
        "short": "felony share",
        "fmt": ".1%",
    },
    "avg_score": {
        "label": "Average DOHMH inspection score (lower = cleaner)",
        "short": "inspection score",
        "fmt": ".1f",
    },
    "critical_rate": {
        "label": "Critical inspection violation rate",
        "short": "critical rate",
        "fmt": ".1%",
    },
    "complaints_per_1k": {
        "label": "311 food-safety complaints per 1,000 residents",
        "short": "311 / 1k",
        "fmt": ".1f",
    },
}

BOROUGH_FROM_CODE = {
    "MN": "Manhattan",
    "BK": "Brooklyn",
    "QN": "Queens",
    "BX": "Bronx",
    "SI": "Staten Island",
}

# Match the colour palette used by `web/style.css` so the dashboard blends
# with the dark UI without needing a separate theme.
COLORS = {
    "bg": "#0f172a",
    "panel": "#1e293b",
    "border": "#334155",
    "text": "#f1f5f9",
    "muted": "#94a3b8",
    "accent": "#818cf8",
    "good": "#34d399",
    "bad": "#f87171",
    "grid": "rgba(148,163,184,0.15)",
}

# Per-borough colours — reused across every chart so MN is always the same
# purple, BK the same teal, etc. Makes multi-chart comparison effortless.
BOROUGH_COLORS = {
    "Manhattan":     "#818cf8",
    "Brooklyn":      "#34d399",
    "Queens":        "#fbbf24",
    "Bronx":         "#f87171",
    "Staten Island": "#60a5fa",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _require_metric(metric: str) -> dict[str, Any]:
    if metric not in METRICS:
        raise ValueError(
            f"unknown metric {metric!r}; expected one of {sorted(METRICS)}"
        )
    return METRICS[metric]


def with_borough(df: pd.DataFrame) -> pd.DataFrame:
    """Add a `borough` column derived from the 2-letter NTA prefix."""
    out = df.copy()
    out["borough"] = out["nta_code"].str[:2].map(BOROUGH_FROM_CODE).fillna("Other")
    return out


def _base_layout(title: str, height: int = 420, **extra: Any) -> dict[str, Any]:
    """Shared layout that gives every figure the same dark look."""
    layout: dict[str, Any] = dict(
        title=dict(text=title, x=0, xanchor="left",
                   font=dict(size=15, color=COLORS["text"])),
        paper_bgcolor=COLORS["bg"],
        plot_bgcolor=COLORS["bg"],
        font=dict(family="Inter, -apple-system, sans-serif",
                  color=COLORS["text"], size=12),
        margin=dict(l=60, r=20, t=50, b=50),
        height=height,
        xaxis=dict(gridcolor=COLORS["grid"], zerolinecolor=COLORS["grid"],
                   linecolor=COLORS["border"], tickcolor=COLORS["border"]),
        yaxis=dict(gridcolor=COLORS["grid"], zerolinecolor=COLORS["grid"],
                   linecolor=COLORS["border"], tickcolor=COLORS["border"]),
        legend=dict(bgcolor="rgba(0,0,0,0)",
                    bordercolor=COLORS["border"], borderwidth=1),
    )
    layout.update(extra)
    return layout


# ---------------------------------------------------------------------------
# Figure builders
# ---------------------------------------------------------------------------

def distribution_fig(df: pd.DataFrame, metric: str) -> go.Figure:
    """Histogram of the metric across all NTAs, with mean / median rules."""
    meta = _require_metric(metric)
    col = meta["column"]
    values = df[col].dropna().astype(float)

    bar_color = COLORS["accent"] if meta["higher_is_better"] else COLORS["bad"]

    fig = go.Figure()
    fig.add_trace(
        go.Histogram(
            x=values,
            nbinsx=24,
            marker=dict(color=bar_color,
                        line=dict(color=COLORS["bg"], width=1)),
            opacity=0.85,
            name=meta["label"],
            hovertemplate=f"<b>%{{x}}</b><br>%{{y}} NTAs<extra></extra>",
        )
    )

    mean_v = float(values.mean())
    median_v = float(values.median())
    fig.add_vline(x=mean_v, line=dict(color=COLORS["accent"], dash="dash", width=2),
                  annotation_text=f"mean {mean_v:.1f}",
                  annotation_position="top right",
                  annotation_font_color=COLORS["accent"])
    fig.add_vline(x=median_v, line=dict(color=COLORS["good"], dash="dot", width=2),
                  annotation_text=f"median {median_v:.1f}",
                  annotation_position="top left",
                  annotation_font_color=COLORS["good"])

    fig.update_layout(**_base_layout(
        f"Distribution of {meta['label']} across {len(values)} NTAs",
        xaxis_title=meta["label"],
        yaxis_title="Number of neighborhoods",
        bargap=0.05,
        showlegend=False,
    ))
    return fig


def top_bottom_fig(df: pd.DataFrame, metric: str, n: int = 10) -> go.Figure:
    """Horizontal bars of the top and bottom N NTAs (two-panel subplot)."""
    from plotly.subplots import make_subplots

    meta = _require_metric(metric)
    col = meta["column"]
    clean = df.dropna(subset=[col]).copy()

    ascending_for_worst = not meta["higher_is_better"]
    # "Top" = best-ranked, "Bottom" = worst-ranked, regardless of direction.
    best = clean.sort_values(col, ascending=ascending_for_worst).head(n)
    worst = clean.sort_values(col, ascending=not ascending_for_worst).head(n)

    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=(f"Top {n} (best)", f"Bottom {n} (worst)"),
        horizontal_spacing=0.18,
    )

    def _bar(df_: pd.DataFrame, color: str):
        return go.Bar(
            x=df_[col][::-1],
            y=df_["nta_name"][::-1],
            orientation="h",
            marker=dict(color=color,
                        line=dict(color=COLORS["bg"], width=1)),
            text=[f"{v:,.1f}" for v in df_[col][::-1]],
            textposition="outside",
            textfont=dict(color=COLORS["text"], size=11),
            hovertemplate="<b>%{y}</b><br>" + meta["label"] + ": %{x:,.2f}<extra></extra>",
            showlegend=False,
        )

    fig.add_trace(_bar(best, COLORS["good"]), row=1, col=1)
    fig.add_trace(_bar(worst, COLORS["bad"]), row=1, col=2)

    layout = _base_layout(
        f"{meta['label']} — top vs. bottom {n} NTAs",
        height=max(380, 40 + n * 28),
    )
    # Per-subplot axis styling.
    for i in (1, 2):
        layout[f"xaxis{'' if i == 1 else i}"] = dict(
            gridcolor=COLORS["grid"], zerolinecolor=COLORS["grid"],
            linecolor=COLORS["border"], tickcolor=COLORS["border"],
            title=meta["label"],
        )
        layout[f"yaxis{'' if i == 1 else i}"] = dict(
            gridcolor=COLORS["grid"], linecolor=COLORS["border"],
            tickcolor=COLORS["border"], tickfont=dict(size=11),
            automargin=True,
        )
    fig.update_layout(**layout)
    # Subplot titles need to pick up our font colour explicitly.
    for ann in fig.layout.annotations:
        ann.font = dict(color=COLORS["muted"], size=12)
    return fig


def by_borough_fig(df: pd.DataFrame, metric: str) -> go.Figure:
    """Box plot of the metric grouped by borough (ordered by median)."""
    meta = _require_metric(metric)
    col = meta["column"]
    d = with_borough(df).dropna(subset=[col])
    # Order boroughs by median so the reader's eye moves down the ranking.
    order = (
        d.groupby("borough")[col].median()
         .sort_values(ascending=not meta["higher_is_better"])
         .index.tolist()
    )

    fig = go.Figure()
    for b in order:
        sub = d[d["borough"] == b]
        fig.add_trace(go.Box(
            y=sub[col],
            name=b,
            boxpoints="all",
            jitter=0.5,
            pointpos=0,
            marker=dict(color=BOROUGH_COLORS.get(b, COLORS["accent"]),
                        size=5, opacity=0.75,
                        line=dict(color=COLORS["bg"], width=0.5)),
            line=dict(color=BOROUGH_COLORS.get(b, COLORS["accent"]), width=1.5),
            fillcolor=BOROUGH_COLORS.get(b, COLORS["accent"]),
            opacity=0.35,
            hovertemplate="<b>%{text}</b><br>" + meta["label"] + ": %{y:,.2f}<extra>" + b + "</extra>",
            text=sub["nta_name"],
            showlegend=False,
        ))

    fig.update_layout(**_base_layout(
        f"{meta['label']} by borough",
        yaxis_title=meta["label"],
        xaxis_title="",
    ))
    return fig


def correlation_fig(df: pd.DataFrame) -> go.Figure:
    """Pearson correlation heatmap of the six input features."""
    sub = df[INPUT_FEATURES].astype(float)
    corr = sub.corr(method="pearson").round(2)
    labels = [INPUT_FEATURE_LABELS[c] for c in INPUT_FEATURES]

    fig = go.Figure(data=go.Heatmap(
        z=corr.values,
        x=labels,
        y=labels,
        zmin=-1, zmax=1,
        colorscale="RdBu_r",
        reversescale=False,
        text=[[f"{v:+.2f}" for v in row] for row in corr.values],
        texttemplate="%{text}",
        textfont=dict(color=COLORS["text"], size=11),
        hovertemplate="%{y} ↔ %{x}<br>r = %{z:.2f}<extra></extra>",
        colorbar=dict(
            title=dict(text="Pearson r", side="right"),
            tickcolor=COLORS["border"],
            outlinecolor=COLORS["border"],
        ),
    ))
    fig.update_layout(**_base_layout(
        "Correlation of the six Newcomer Score inputs",
        height=460,
        xaxis=dict(side="bottom", tickangle=-25,
                   linecolor=COLORS["border"], tickcolor=COLORS["border"]),
        yaxis=dict(autorange="reversed",
                   linecolor=COLORS["border"], tickcolor=COLORS["border"]),
    ))
    return fig


def rent_vs_feature_fig(df: pd.DataFrame, feature: str, n_bins: int = 10) -> go.Figure:
    """Line chart: median rent as a function of a single input feature.

    Three layers on the same axes so the reader can triangulate:

    1. **Binned median rent** — NTAs are placed into ``n_bins`` quantile bins
       of ``feature``; within each bin we take median rent and the 25th/75th
       percentiles. The line connecting those medians is the answer to
       "if this neighbourhood has X crime, what's the typical rent?".
    2. **IQR band** — the shaded area around the line shows the middle 50%
       of rents in each bin. Tight band = strong relationship; fat band =
       this feature alone doesn't pin rent down.
    3. **OLS regression** — a simple best-fit line over the raw (feature,
       rent) points. The annotation in the corner reports slope, intercept,
       Pearson r, R², and a plain-English prediction at the feature's median.

    The scatter of individual NTAs is rendered faintly underneath, coloured
    by borough, so power users can see the cloud behind the aggregate.
    """
    if feature not in RENT_PREDICTOR_META:
        raise ValueError(
            f"unknown rent predictor {feature!r}; expected one of "
            f"{sorted(RENT_PREDICTOR_META)}"
        )
    meta = RENT_PREDICTOR_META[feature]

    d = with_borough(df).dropna(subset=[feature, "median_rent_zori"]).copy()
    if d.empty:
        fig = go.Figure()
        fig.update_layout(**_base_layout(
            f"Rent vs {meta['short']} — no data",
            xaxis_title=meta["label"],
            yaxis_title="Median rent ($/mo)",
        ))
        return fig

    x_all = d[feature].astype(float).to_numpy()
    y_all = d["median_rent_zori"].astype(float).to_numpy()

    # --- Layer 1+2: quantile-binned median + IQR band -----------------------
    # qcut with duplicates='drop' handles features (like felony_share) whose
    # lower tail has many ties — we'd rather get fewer bins than crash.
    try:
        bins = pd.qcut(d[feature], q=n_bins, duplicates="drop")
    except ValueError:
        bins = pd.qcut(d[feature].rank(method="first"), q=n_bins, duplicates="drop")
    grouped = d.groupby(bins, observed=True).agg(
        x_mid=(feature, "median"),
        rent_median=("median_rent_zori", "median"),
        rent_q1=("median_rent_zori", lambda s: float(np.percentile(s, 25))),
        rent_q3=("median_rent_zori", lambda s: float(np.percentile(s, 75))),
        n_ntas=("nta_code", "count"),
    ).reset_index(drop=True).sort_values("x_mid")

    x_bin = grouped["x_mid"].to_numpy()
    y_bin = grouped["rent_median"].to_numpy()
    y_q1 = grouped["rent_q1"].to_numpy()
    y_q3 = grouped["rent_q3"].to_numpy()

    # --- Layer 3: OLS regression on the raw points --------------------------
    slope, intercept = np.polyfit(x_all, y_all, 1)
    pearson_r = float(np.corrcoef(x_all, y_all)[0, 1])
    r2 = pearson_r ** 2
    x_fit = np.linspace(float(x_all.min()), float(x_all.max()), 100)
    y_fit = slope * x_fit + intercept
    x_example = float(np.median(x_all))
    y_example = float(slope * x_example + intercept)

    # --- Figure assembly ----------------------------------------------------
    fig = go.Figure()

    # Faint scatter underneath, coloured by borough.
    for borough, sub in d.groupby("borough"):
        fig.add_trace(go.Scatter(
            x=sub[feature],
            y=sub["median_rent_zori"],
            mode="markers",
            name=borough,
            marker=dict(
                color=BOROUGH_COLORS.get(borough, COLORS["accent"]),
                size=6,
                opacity=0.45,
                line=dict(color=COLORS["bg"], width=0.5),
            ),
            text=sub["nta_name"],
            hovertemplate=(
                "<b>%{text}</b><br>"
                + meta["short"] + f": %{{x:{meta['fmt']}}}"
                + "<br>median rent: $%{y:,.0f}"
                + f"<extra>{borough}</extra>"
            ),
        ))

    # IQR band (Q3 high, then Q1 reversed, filled between).
    fig.add_trace(go.Scatter(
        x=np.concatenate([x_bin, x_bin[::-1]]),
        y=np.concatenate([y_q3, y_q1[::-1]]),
        fill="toself",
        fillcolor="rgba(129,140,248,0.18)",
        line=dict(color="rgba(0,0,0,0)"),
        hoverinfo="skip",
        name="IQR band (25th–75th)",
        showlegend=True,
    ))

    # Binned median line — the headline trend.
    fig.add_trace(go.Scatter(
        x=x_bin,
        y=y_bin,
        mode="lines+markers",
        name="Binned median rent",
        line=dict(color=COLORS["accent"], width=3),
        marker=dict(size=8, color=COLORS["accent"],
                    line=dict(color=COLORS["bg"], width=1.5)),
        customdata=np.stack([grouped["n_ntas"].to_numpy(),
                             y_q1, y_q3], axis=-1),
        hovertemplate=(
            f"<b>bin median {meta['short']}</b>: %{{x:{meta['fmt']}}}"
            "<br>median rent: $%{y:,.0f}"
            "<br>IQR: $%{customdata[1]:,.0f} – $%{customdata[2]:,.0f}"
            "<br>NTAs in bin: %{customdata[0]}"
            "<extra></extra>"
        ),
    ))

    # OLS fit on top (dashed so it doesn't compete visually with the bins).
    fig.add_trace(go.Scatter(
        x=x_fit,
        y=y_fit,
        mode="lines",
        name=f"OLS fit (R² = {r2:.2f})",
        line=dict(color=COLORS["good"], width=2, dash="dash"),
        hovertemplate=(
            f"{meta['short']}: %{{x:{meta['fmt']}}}"
            "<br>predicted rent: $%{y:,.0f}<extra>OLS fit</extra>"
        ),
    ))

    # Plain-English annotation. Guard against pathological cases where the
    # intercept is negative (chart shows $0 floor either way) by clamping
    # the example prediction to a sane integer string.
    slope_line = (
        f"Δrent ≈ <b>${slope:+,.0f}</b> per unit {meta['short']}"
    )
    prediction_line = (
        f"At {meta['short']} = {x_example:{meta['fmt']}}, "
        f"model predicts <b>${max(0, y_example):,.0f}/mo</b>"
    )
    r2_line = f"Pearson r = {pearson_r:+.2f}, R² = {r2:.2f}"
    annotation_text = "<br>".join([slope_line, prediction_line, r2_line])

    fig.add_annotation(
        xref="paper", yref="paper",
        x=0.99, y=0.99, xanchor="right", yanchor="top",
        text=annotation_text,
        showarrow=False,
        align="right",
        bgcolor="rgba(15,23,42,0.85)",
        bordercolor=COLORS["border"],
        borderwidth=1,
        borderpad=8,
        font=dict(color=COLORS["text"], size=11),
    )

    fig.update_layout(**_base_layout(
        f"Median rent vs {meta['short']} — {len(d)} NTAs",
        xaxis_title=meta["label"],
        yaxis_title="Median rent (ZORI, $/month)",
        height=460,
        legend=dict(orientation="h", y=-0.18, x=0,
                    bgcolor="rgba(0,0,0,0)",
                    bordercolor=COLORS["border"], borderwidth=1),
    ))
    return fig


# ---------------------------------------------------------------------------
# Summary stats — handy textual insights for notebook narrative and API
# ---------------------------------------------------------------------------

def summary_stats(df: pd.DataFrame) -> dict[str, Any]:
    """Coarse descriptive stats consumed by the dashboard header strip."""
    d = with_borough(df)
    out: dict[str, Any] = {
        "n_ntas": int(len(d)),
        "n_boroughs": int(d["borough"].nunique()),
    }
    for m, meta in METRICS.items():
        s = d[meta["column"]].dropna().astype(float)
        if len(s):
            out[m] = {
                "mean": float(s.mean()),
                "median": float(s.median()),
                "min": float(s.min()),
                "max": float(s.max()),
                "std": float(s.std(ddof=0)),
                "label": meta["label"],
                "unit": meta["unit"],
            }
    return out
