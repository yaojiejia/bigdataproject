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
