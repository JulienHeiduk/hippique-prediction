"""Probability calibration helpers."""
from __future__ import annotations

import pandas as pd


def blend_with_market(
    model_prob: pd.Series,
    implied_prob: pd.Series,
    w: float = 0.4,
) -> pd.Series:
    """Linear blend of model probability with market-implied probability.

    p_final = w * model_prob + (1 - w) * implied_prob

    Inputs must share the same index (typically runner_id within one race).
    The caller is responsible for re-normalising to sum to 1 per race.
    """
    if not 0.0 <= w <= 1.0:
        raise ValueError(f"w must be in [0, 1], got {w}")
    return w * model_prob + (1.0 - w) * implied_prob
