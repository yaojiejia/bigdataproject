"""FastAPI backend for the NYC Neighborhood Insights map and dashboard.

Endpoints:
  GET /health
  GET /neighborhoods?metric=score|crime|food|rent|311
      -> GeoJSON FeatureCollection of NTAs, with the chosen metric value
         attached as `properties.metric_value`
  GET /neighborhood/{nta_code}
      -> full feature record for a single NTA
  GET /trending?limit=20
      -> recent windowed complaint counts from the streaming sink

  GET /analytics/summary
      -> descriptive stats (mean / median / min / max / std) per metric
  GET /analytics/distribution?metric=...
      -> Plotly figure JSON: histogram of the metric across all NTAs
  GET /analytics/top-bottom?metric=...&n=10
      -> Plotly figure JSON: top-N vs. bottom-N NTAs
  GET /analytics/by-borough?metric=...
      -> Plotly figure JSON: box plot by borough
  GET /analytics/correlation
      -> Plotly figure JSON: correlation heatmap of the six input features

Every `/analytics/*` endpoint that returns a figure uses the exact same
builder functions as `notebooks/analysis.ipynb` (from `pipeline.analytics`),
so the web dashboard and the notebook render byte-identical charts.

Static frontend assets are served under `/web` for convenience.
"""
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles

from pipeline import analytics
from pipeline.paths import (
    NEWCOMER_SCORE,
    NTA_GEOJSON,
    PROJECT_ROOT,
    STREAM_LATEST,
)

METRIC_COLUMNS = {
    "score": "newcomer_score_100",
    "crime": "crimes_per_1k",
    "food": "critical_rate",
    "rent": "median_rent_zori",
    "311": "complaints_per_1k",
}

app = FastAPI(title="NYC Neighborhood Insights")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def _sanitize(value: Any) -> Any:
    """Convert NaN/inf to None so JSON serialisation doesn't blow up."""
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _load_geojson() -> dict:
    if not NTA_GEOJSON.exists():
        raise RuntimeError(f"missing {NTA_GEOJSON}; run `make geo-download` first")
    return json.loads(NTA_GEOJSON.read_text())


def _load_scores() -> pd.DataFrame:
    if not NEWCOMER_SCORE.exists():
        raise RuntimeError(f"missing {NEWCOMER_SCORE}; run the batch pipeline first")
    return pd.read_parquet(NEWCOMER_SCORE)


STATE: dict[str, Any] = {}


@app.on_event("startup")
def _startup() -> None:
    STATE["geojson"] = _load_geojson()
    STATE["scores"] = _load_scores()
    # Index by nta_code for fast lookup.
    STATE["scores_by_code"] = STATE["scores"].set_index("nta_code").to_dict(orient="index")
    print(f"[api] loaded {len(STATE['geojson']['features'])} NTA polygons, "
          f"{len(STATE['scores'])} score rows")


@app.get("/health")
def health() -> dict:
    return {"ok": True, "ntas": len(STATE.get("geojson", {}).get("features", []))}


@app.get("/neighborhoods")
def neighborhoods(metric: str = Query("score", pattern="^(score|crime|food|rent|311)$")) -> JSONResponse:
    col = METRIC_COLUMNS[metric]
    geo = STATE["geojson"]
    by_code = STATE["scores_by_code"]

    features = []
    for f in geo["features"]:
        props = dict(f.get("properties") or {})
        code = (
            props.get("ntacode") or props.get("nta_code") or
            props.get("NTACode") or props.get("NTA2020") or props.get("nta2020")
        )
        row = by_code.get(code)
        if row is None:
            value = None
            row_clean: dict[str, Any] = {}
        else:
            value = _sanitize(row.get(col))
            row_clean = {k: _sanitize(v) for k, v in row.items()}
        props["nta_code"] = code
        props["metric"] = metric
        props["metric_value"] = value
        props["features"] = row_clean
        features.append({
            "type": "Feature",
            "geometry": f.get("geometry"),
            "properties": props,
        })

    return JSONResponse({"type": "FeatureCollection", "features": features})


@app.get("/neighborhood/{nta_code}")
def neighborhood(nta_code: str) -> dict:
    row = STATE["scores_by_code"].get(nta_code)
    if row is None:
        raise HTTPException(404, f"nta_code {nta_code} not found")
    return {"nta_code": nta_code, **{k: _sanitize(v) for k, v in row.items()}}


_METRIC_PATTERN = "^(score|crime|food|rent|311)$"

_BDATA_DTYPE_MAP = {
    "f8": "<f8", "f4": "<f4",
    "i8": "<i8", "i4": "<i4", "i2": "<i2", "i1": "<i1",
    "u8": "<u8", "u4": "<u4", "u2": "<u2", "u1": "<u1",
}


def _decode_bdata(obj: Any) -> Any:
    """Plotly 6 serialises numeric arrays as {dtype, bdata(base64), shape?}.

    Plotly.js 2.29+ understands this, but the encoding trips up any caller
    that inspects the JSON directly (tests, alt renderers, older caches).
    Walk the dict and rewrite those blobs into plain Python lists so the
    payload is a vanilla figure spec — the wire is a little bigger but
    every downstream consumer just works.
    """
    if isinstance(obj, dict):
        if "bdata" in obj and "dtype" in obj and isinstance(obj["bdata"], str):
            import base64
            import numpy as np
            dtype = _BDATA_DTYPE_MAP.get(obj["dtype"], obj["dtype"])
            arr = np.frombuffer(base64.b64decode(obj["bdata"]), dtype=dtype)
            shape = obj.get("shape")
            if shape:
                if isinstance(shape, str):
                    shape = tuple(int(s) for s in shape.split(","))
                arr = arr.reshape(shape)
            return arr.tolist()
        return {k: _decode_bdata(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_decode_bdata(v) for v in obj]
    return obj


def _figure_response(fig) -> Response:
    """Return a Plotly figure as plain-array JSON."""
    payload = _decode_bdata(json.loads(fig.to_json()))
    return Response(content=json.dumps(payload), media_type="application/json")


@app.get("/analytics/summary")
def analytics_summary() -> dict:
    return analytics.summary_stats(STATE["scores"])


@app.get("/analytics/distribution")
def analytics_distribution(
    metric: str = Query("score", pattern=_METRIC_PATTERN),
) -> Response:
    fig = analytics.distribution_fig(STATE["scores"], metric)
    return _figure_response(fig)


@app.get("/analytics/top-bottom")
def analytics_top_bottom(
    metric: str = Query("score", pattern=_METRIC_PATTERN),
    n: int = Query(10, ge=3, le=30),
) -> Response:
    fig = analytics.top_bottom_fig(STATE["scores"], metric, n=n)
    return _figure_response(fig)


@app.get("/analytics/by-borough")
def analytics_by_borough(
    metric: str = Query("score", pattern=_METRIC_PATTERN),
) -> Response:
    fig = analytics.by_borough_fig(STATE["scores"], metric)
    return _figure_response(fig)


@app.get("/analytics/correlation")
def analytics_correlation() -> Response:
    fig = analytics.correlation_fig(STATE["scores"])
    return _figure_response(fig)


@app.get("/trending")
def trending(limit: int = 20) -> dict:
    if not STREAM_LATEST.exists():
        return {"window_end": None, "items": []}
    df = pd.read_parquet(STREAM_LATEST)
    if df.empty:
        return {"window_end": None, "items": []}
    window_end = str(df["window_end"].max())
    items = (
        df.sort_values("n_complaints_recent", ascending=False)
        .head(limit)
        .to_dict(orient="records")
    )
    items = [{k: _sanitize(v) for k, v in item.items()} for item in items]
    return {"window_end": window_end, "items": items}


# Serve the static frontend under /web for convenience, but the primary
# frontend entrypoint is the separate static server on port 5173.
WEB_DIR = PROJECT_ROOT / "web"
if WEB_DIR.exists():
    app.mount("/web", StaticFiles(directory=str(WEB_DIR), html=True), name="web")
