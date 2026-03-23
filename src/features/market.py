"""Market odds feature engineering."""
from __future__ import annotations

import numpy as np
import pandas as pd


def odds_features(runners_df: pd.DataFrame) -> pd.DataFrame:
    """Add odds-derived feature columns to the runners DataFrame.

    Input columns required: runner_id, race_id, morning_odds, final_odds
    (NaN allowed in odds columns).

    Adds:
        morning_odds_rank           -- 1 = favourite (ascending rank within race)
        final_odds_rank             -- same for final odds
        odds_drift_pct              -- (final - morning) / morning; negative = shortening
        morning_implied_prob        -- 1 / morning_odds
        morning_implied_prob_norm   -- normalised within race to sum to 1
    """
    df = runners_df.copy()

    df["morning_odds_rank"] = (
        df.groupby("race_id")["morning_odds"]
        .rank(method="min", ascending=True)
    )
    df["final_odds_rank"] = (
        df.groupby("race_id")["final_odds"]
        .rank(method="min", ascending=True)
    )

    df["odds_drift_pct"] = (df["final_odds"] - df["morning_odds"]) / df["morning_odds"]

    df["morning_implied_prob"] = 1.0 / df["morning_odds"]

    race_sum = df.groupby("race_id")["morning_implied_prob"].transform("sum")
    df["morning_implied_prob_norm"] = df["morning_implied_prob"] / race_sum

    df["final_implied_prob"] = 1.0 / df["final_odds"]
    race_sum_final = df.groupby("race_id")["final_implied_prob"].transform("sum")
    df["final_implied_prob_norm"] = df["final_implied_prob"] / race_sum_final

    # Rank movement: positive = horse drifted out (market less confident)
    df["odds_rank_change"] = df["morning_odds_rank"] - df["final_odds_rank"]

    # Favourite flag
    df["is_favorite"] = (df["morning_odds_rank"] == 1).astype(float)

    # Shannon entropy of implied probs within the race (race predictability)
    def _entropy(s: pd.Series) -> float:
        p = s.dropna()
        p = p[p > 0]
        return float(-(p * np.log(p + 1e-10)).sum()) if len(p) else 0.0

    df["field_entropy"] = df.groupby("race_id")["morning_implied_prob_norm"].transform(_entropy)

    return df
