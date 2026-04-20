"""Combine per-NTA features into a unified newcomer score.

Z-score each feature, invert the sign for "bad" features, then take a
weighted sum. Weights are at the top of the file for easy tuning.
"""
from __future__ import annotations

import pandas as pd

from .paths import NEIGHBORHOOD_FEATURES, NEWCOMER_SCORE, ensure_dirs

# Weights sum to 1.0
WEIGHTS = {
    "safety": 0.30,         # driven by crimes_per_1k, felony_share
    "food_safety": 0.25,    # avg_score, critical_rate
    "cleanliness": 0.15,    # complaints_per_1k
    "affordability": 0.30,  # median_rent_zori (inverted)
}

# Direction: +1 means higher is better, -1 means higher is worse.
FEATURE_DIRECTION = {
    "crimes_per_1k": -1,
    "felony_share": -1,
    "avg_score": -1,          # lower health score is cleaner (better)
    "critical_rate": -1,
    "complaints_per_1k": -1,
    "median_rent_zori": -1,   # higher rent = worse affordability
}


def _zscore(s: pd.Series) -> pd.Series:
    mu = s.mean()
    sd = s.std(ddof=0)
    if not sd or pd.isna(sd):
        return pd.Series([0.0] * len(s), index=s.index)
    return (s - mu) / sd


def compute_subscores(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col, direction in FEATURE_DIRECTION.items():
        if col not in out.columns:
            out[col] = 0.0
        out[f"z_{col}"] = _zscore(out[col].astype(float).fillna(out[col].astype(float).mean())) * direction

    out["safety_score"] = (out["z_crimes_per_1k"] + out["z_felony_share"]) / 2
    out["food_safety_score"] = (out["z_avg_score"] + out["z_critical_rate"]) / 2
    out["cleanliness_score"] = out["z_complaints_per_1k"]
    out["affordability_score"] = out["z_median_rent_zori"]

    out["newcomer_score"] = (
        WEIGHTS["safety"] * out["safety_score"]
        + WEIGHTS["food_safety"] * out["food_safety_score"]
        + WEIGHTS["cleanliness"] * out["cleanliness_score"]
        + WEIGHTS["affordability"] * out["affordability_score"]
    )
    # Re-scale to 0-100 for readability.
    s = out["newcomer_score"]
    if s.max() != s.min():
        out["newcomer_score_100"] = 100 * (s - s.min()) / (s.max() - s.min())
    else:
        out["newcomer_score_100"] = 50.0
    return out


def main() -> None:
    ensure_dirs()
    df = pd.read_parquet(NEIGHBORHOOD_FEATURES)
    scored = compute_subscores(df)
    NEWCOMER_SCORE.parent.mkdir(parents=True, exist_ok=True)
    scored.to_parquet(NEWCOMER_SCORE, index=False)
    top = scored.sort_values("newcomer_score", ascending=False).head(10)
    bot = scored.sort_values("newcomer_score", ascending=True).head(10)
    print(f"[score] wrote {NEWCOMER_SCORE} ({len(scored)} NTAs)")
    print("\nTop 10 NTAs:")
    print(top[["nta_code", "nta_name", "newcomer_score", "newcomer_score_100"]].to_string(index=False))
    print("\nBottom 10 NTAs:")
    print(bot[["nta_code", "nta_name", "newcomer_score", "newcomer_score_100"]].to_string(index=False))


if __name__ == "__main__":
    main()
