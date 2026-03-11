"""Adaptive Bayesian Kelly: Dirichlet-Multinomial model for race win probabilities.

Based on: 'An Adaptive Version of Kelly's Horse Model'.

The gambler maintains a Dirichlet posterior over runner win probabilities:

    alpha_i = kappa * implied_prob_i   (bookmaker prior, weight = kappa observations)
            + historical_wins_i        (Bayesian update from past races)

    Posterior mean: p_i = alpha_i / sum(alpha_j)

The bookmaker implied probs serve as an informative prior (vs. a flat Dirichlet),
directly exploiting the information in the odds distribution as described in the paper.
"""
from __future__ import annotations

from typing import Callable

import pandas as pd

_DEFAULT_KAPPA = 5.0  # prior strength: bookmaker prior counts as 5 pseudo-observations


def bayes_scorer(df: pd.DataFrame, kappa: float = _DEFAULT_KAPPA) -> pd.Series:
    """Compute Dirichlet-Multinomial scores for all runners across all races.

    For each runner i in race r:
        alpha_i = kappa * implied_prob_i + historical_wins_i

    where:
        implied_prob_i   = morning_implied_prob_norm  (bookmaker market prior)
        historical_wins_i = horse_win_rate * horse_n_runs  (observed wins from DB)

    Returns a pd.Series indexed by runner_id with values proportional to the
    posterior mean win probability. The engine normalises per-race so only the
    relative magnitudes within a race matter.

    Args:
        df:    Features DataFrame with columns: morning_implied_prob_norm,
               horse_win_rate, horse_n_runs, runner_id, race_id.
        kappa: Prior strength — how many pseudo-observations the bookmaker
               prior is worth. Higher kappa = more weight on bookmaker odds,
               less on individual horse history. Typical range: 2–20.
    """
    n_runners = df.groupby("race_id")["runner_id"].transform("count")
    flat_prior = 1.0 / n_runners.clip(lower=1)

    # Bookmaker prior: use normalised implied prob, fall back to flat 1/n
    implied_prob = df["morning_implied_prob_norm"].fillna(flat_prior)

    # Bayesian update: historical wins act as observed successes in Dirichlet
    win_rate = df["horse_win_rate"].fillna(0.0)
    n_runs = df["horse_n_runs"].fillna(0.0)
    historical_wins = (win_rate * n_runs).clip(lower=0.0)

    alpha = kappa * implied_prob + historical_wins

    return alpha.clip(lower=1e-6).set_axis(df["runner_id"])


def build_bayes_scorer(kappa: float = _DEFAULT_KAPPA) -> Callable[[pd.DataFrame], pd.Series]:
    """Return a scorer function compatible with generate_bets(scorer_fn=...).

    Args:
        kappa: Prior strength passed to bayes_scorer.
    """
    def _scorer(df: pd.DataFrame) -> pd.Series:
        return bayes_scorer(df, kappa=kappa)

    _scorer.__name__ = f"bayes_scorer(kappa={kappa})"
    return _scorer
