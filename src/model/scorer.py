"""Rule-based scorers and weight optimisation."""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.model.backtest import backtest


def score_baseline(df: pd.DataFrame) -> pd.Series:
    """M0: 1 / morning_odds. NaN → 0. Market-implied score."""
    scores = 1.0 / df["morning_odds"].replace(0, np.nan)
    return scores.fillna(0.0).set_axis(df["runner_id"])


def score_form(df: pd.DataFrame) -> pd.Series:
    """M1: form_score column. NaN → 0."""
    return df["form_score"].fillna(0.0).set_axis(df["runner_id"])


def score_market(df: pd.DataFrame) -> pd.Series:
    """M2: morning_implied_prob_norm * (1 - odds_drift_pct * 0.5).
    Rewards high implied prob and shortening (negative drift = bonus).
    """
    prob = df["morning_implied_prob_norm"].fillna(0.0)
    drift = df["odds_drift_pct"].fillna(0.0)
    raw = prob * (1.0 - drift * 0.5)
    return raw.clip(lower=0).set_axis(df["runner_id"])


def score_combined(
    df: pd.DataFrame,
    w_form: float = 0.35,
    w_odds: float = 0.40,
    w_drift: float = 0.15,
    w_jockey: float = 0.10,
) -> pd.Series:
    """M4: weighted sum of rank-normalised components within each race.

    Components (all 0-1 scale, higher = better):
      odds   : 1 - (morning_odds_rank - 1) / (n_runners - 1)
      form   : form_score / max(form_score) within race
      drift  : clip(-odds_drift_pct, 0, 1) normalised within race
      jockey : jockey_win_rate / max(jockey_win_rate) within race
    Uses groupby.transform for index-safe per-race normalisation.
    """
    total_w = w_form + w_odds + w_drift + w_jockey
    if total_w == 0:
        return pd.Series(0.0, index=df["runner_id"])

    df = df.copy()

    # odds component: rank-based, favourite gets score 1
    n_runners = df.groupby("race_id")["runner_id"].transform("count")
    odds_comp = (
        (n_runners - df["morning_odds_rank"]) / (n_runners - 1).clip(lower=1)
    ).fillna(0.5)

    # form component: form_score / max(form_score) within race
    df["_fs"] = df["form_score"].fillna(0.0)
    race_max_form = df.groupby("race_id")["_fs"].transform("max")
    form_comp = (df["_fs"] / race_max_form.replace(0, 1.0)).where(race_max_form > 0, 0.0)

    # drift component: shortening (negative drift) -> positive score
    df["_neg_drift"] = (-df["odds_drift_pct"].fillna(0.0)).clip(lower=0, upper=1)
    race_max_drift = df.groupby("race_id")["_neg_drift"].transform("max")
    drift_comp = (
        df["_neg_drift"] / race_max_drift.replace(0, 1.0)
    ).where(race_max_drift > 0, 0.0)

    # jockey component
    df["_jr"] = df["jockey_win_rate"].fillna(0.0)
    race_max_jr = df.groupby("race_id")["_jr"].transform("max")
    jockey_comp = (df["_jr"] / race_max_jr.replace(0, 1.0)).where(race_max_jr > 0, 0.0)

    combined = (
        w_odds * odds_comp
        + w_form * form_comp
        + w_drift * drift_comp
        + w_jockey * jockey_comp
    ) / total_w

    return combined.set_axis(df["runner_id"])


def _vectorized_roi(
    df: pd.DataFrame,
    scores: pd.Series,
    bet_type: str = "win",
    min_field_size: int = 4,
) -> tuple[float, float, int, float]:
    """Fully vectorized P&L computation (win bets only for grid search).

    Returns (roi, hit_rate, n_bets, total_pnl).
    ~100x faster than the per-race Python loop in backtest().
    """
    df = df.copy()
    score_map = dict(zip(scores.index, scores.values))
    df["_score"] = df["runner_id"].map(score_map).fillna(0.0)
    # Actual runner count per race (consistent with backtest's len(race_df))
    df["_n"] = df.groupby("race_id")["runner_id"].transform("count")
    df["_odds"] = df["morning_odds"].fillna(df["_n"])  # fallback = actual group size

    # Top-1 horse per race (highest score)
    top_idx = df.groupby("race_id")["_score"].idxmax()
    top = df.loc[top_idx].copy()
    top = top[top["_n"] >= min_field_size]

    if len(top) == 0:
        return 0.0, 0.0, 0, 0.0

    if bet_type == "win":
        stake = 1.0
        hit = (top["finish_position"] == 1).astype(float)
        pnl = hit * (top["_odds"] - 1) - (1 - hit)

    elif bet_type == "place":
        stake = 1.0
        cutoff = top["field_size"].apply(lambda n: 2 if n < 5 else 3)
        hit = (top["finish_position"] <= cutoff).astype(float)
        pnl = hit * (top["_odds"] / 4.0 - 1) - (1 - hit)

    else:
        raise ValueError(f"bet_type {bet_type!r} not supported in vectorized path")

    n_bets = len(top)
    total_pnl = float(pnl.sum())
    roi = total_pnl / (n_bets * stake)
    hit_rate = float(hit.mean())
    return roi, hit_rate, n_bets, total_pnl


def optimize_weights(
    features_df: pd.DataFrame,
    bet_type: str = "win",
) -> tuple[dict, pd.DataFrame]:
    """Grid search to maximise ROI for score_combined.

    Searches w_form, w_odds, w_drift, w_jockey over [0.0, 0.25, 0.50, 0.75, 1.0].
    Uses a vectorized ROI computation (not the full backtest loop) for speed.
    Returns (best_weights_dict, full_results_DataFrame sorted by ROI desc).
    """
    grid = [0.0, 0.25, 0.50, 0.75, 1.0]
    results = []

    for wf in grid:
        for wo in grid:
            for wd in grid:
                for wj in grid:
                    if wf + wo + wd + wj == 0:
                        continue
                    weights = {
                        "w_form": wf, "w_odds": wo,
                        "w_drift": wd, "w_jockey": wj,
                    }
                    scores = score_combined(features_df, **weights)
                    roi, hit_rate, n_bets, total_pnl = _vectorized_roi(
                        features_df, scores, bet_type=bet_type,
                    )
                    results.append({
                        **weights,
                        "roi": roi,
                        "hit_rate": hit_rate,
                        "n_bets": n_bets,
                        "total_pnl": total_pnl,
                    })

    df_results = pd.DataFrame(results).sort_values("roi", ascending=False).reset_index(drop=True)
    best_row = df_results.iloc[0]
    best_weights = {
        "w_form":   best_row["w_form"],
        "w_odds":   best_row["w_odds"],
        "w_drift":  best_row["w_drift"],
        "w_jockey": best_row["w_jockey"],
    }
    return best_weights, df_results
