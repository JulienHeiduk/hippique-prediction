"""Market odds feature engineering."""
from __future__ import annotations

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

    return df
