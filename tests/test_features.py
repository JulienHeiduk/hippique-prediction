"""Unit tests for src/features/form.py and src/features/market.py."""
import math

import pandas as pd
import pytest

from src.features.form import parse_musique, form_score
from src.features.market import odds_features


# ---------------------------------------------------------------------------
# parse_musique
# ---------------------------------------------------------------------------

def test_parse_musique_basic():
    assert parse_musique("1a3a2a") == [1, 3, 2]


def test_parse_musique_year_markers():
    assert parse_musique("1a(25)3a") == [1, 3]


def test_parse_musique_disqualified():
    assert parse_musique("Da1a") == [None, 1]


def test_parse_musique_dnf():
    assert parse_musique("0a1a") == [0, 1]


def test_parse_musique_empty():
    assert parse_musique("") == []
    assert parse_musique(None) == []


# ---------------------------------------------------------------------------
# form_score
# ---------------------------------------------------------------------------

def test_form_score_weighted():
    # Recent 1st scores higher than old 1st (same number of results)
    recent_winner = form_score("1a5a5a5a5a")   # 1st place is most recent
    old_winner    = form_score("5a5a5a5a1a")   # 1st place is oldest
    assert recent_winner is not None
    assert old_winner is not None
    assert recent_winner > old_winner


def test_form_score_insufficient():
    # Only 1 valid result → None
    assert form_score("1a") is None
    assert form_score("Da") is None       # 1 DQ, 0 valid
    assert form_score("0a") is None       # 1 DNF, 0 valid


# ---------------------------------------------------------------------------
# odds_features
# ---------------------------------------------------------------------------

def _make_runners():
    return pd.DataFrame({
        "runner_id":    ["r1", "r2", "r3"],
        "race_id":      ["race1", "race1", "race1"],
        "morning_odds": [2.0, 5.0, 10.0],
        "final_odds":   [1.8, 5.5, 12.0],
    })


def test_odds_features_rank():
    df = odds_features(_make_runners())
    # Lowest odds (2.0) → rank 1 (favourite)
    r1 = df[df["runner_id"] == "r1"].iloc[0]
    assert r1["morning_odds_rank"] == 1.0


def test_odds_features_implied_prob_norm_sums_to_one():
    df = odds_features(_make_runners())
    total = df.groupby("race_id")["morning_implied_prob_norm"].sum().iloc[0]
    assert math.isclose(total, 1.0, rel_tol=1e-9)


def test_odds_features_drift_sign():
    df = odds_features(_make_runners())
    # r1: 2.0 → 1.8, drift = (1.8-2.0)/2.0 = -0.1 (shortening)
    r1_drift = df[df["runner_id"] == "r1"]["odds_drift_pct"].iloc[0]
    assert r1_drift < 0

    # r2: 5.0 → 5.5, drift > 0 (lengthening)
    r2_drift = df[df["runner_id"] == "r2"]["odds_drift_pct"].iloc[0]
    assert r2_drift > 0


def test_odds_features_nan_odds():
    """NaN odds should propagate gracefully (no crash)."""
    df_input = pd.DataFrame({
        "runner_id":    ["r1", "r2"],
        "race_id":      ["race1", "race1"],
        "morning_odds": [float("nan"), 5.0],
        "final_odds":   [float("nan"), 5.0],
    })
    df = odds_features(df_input)
    assert "morning_implied_prob_norm" in df.columns
