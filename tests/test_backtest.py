"""Unit tests for src/model/scorer.py and src/model/backtest.py."""
import math

import pandas as pd
import pytest

from src.model.backtest import backtest, BacktestReport
from src.model.scorer import score_baseline, score_combined


# ---------------------------------------------------------------------------
# Helpers to build synthetic feature DataFrames
# ---------------------------------------------------------------------------

def _race(
    race_id: str,
    date: str,
    runners: list[dict],
) -> pd.DataFrame:
    """Build a minimal feature DataFrame for one race."""
    rows = []
    for i, r in enumerate(runners):
        rows.append({
            "runner_id":              f"{race_id}_r{i+1}",
            "race_id":                race_id,
            "date":                   date,
            "hippodrome":             "TEST",
            "distance_metres":        2100,
            "field_size":             len(runners),
            "horse_name":             r.get("horse_name", f"Horse{i+1}"),
            "jockey_name":            r.get("jockey_name", "J"),
            "morning_odds":           r.get("morning_odds", 5.0),
            "final_odds":             r.get("final_odds", 5.0),
            "morning_odds_rank":      r.get("morning_odds_rank", i + 1),
            "final_odds_rank":        r.get("final_odds_rank", i + 1),
            "odds_drift_pct":         r.get("odds_drift_pct", 0.0),
            "morning_implied_prob":   1.0 / r.get("morning_odds", 5.0),
            "morning_implied_prob_norm": r.get("morning_implied_prob_norm", 1.0 / len(runners)),
            "form_score":             r.get("form_score", 5.0),
            "jockey_win_rate":        r.get("jockey_win_rate", 0.1),
            "finish_position":        r.get("finish_position", i + 1),
        })
    return pd.DataFrame(rows)


def _concat(*dfs: pd.DataFrame) -> pd.DataFrame:
    return pd.concat(dfs, ignore_index=True)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_backtest_win_profit():
    """Rank-1 horse wins → profit = morning_odds - 1."""
    df = _race("r1", "2025-01-01", [
        {"morning_odds": 3.0, "finish_position": 1, "morning_odds_rank": 1},
        {"morning_odds": 5.0, "finish_position": 2, "morning_odds_rank": 2},
        {"morning_odds": 8.0, "finish_position": 3, "morning_odds_rank": 3},
        {"morning_odds": 10.0, "finish_position": 4, "morning_odds_rank": 4},
    ])
    report = backtest(df, score_baseline, "test", bet_type="win")
    assert len(report.bets) == 1
    bet = report.bets[0]
    assert bet.hit is True
    assert math.isclose(bet.pnl, 3.0 - 1.0, rel_tol=1e-9)


def test_backtest_win_loss():
    """Rank-1 horse doesn't win → loss = -1."""
    df = _race("r1", "2025-01-01", [
        {"morning_odds": 3.0, "finish_position": 2, "morning_odds_rank": 1},
        {"morning_odds": 5.0, "finish_position": 1, "morning_odds_rank": 2},
        {"morning_odds": 8.0, "finish_position": 3, "morning_odds_rank": 3},
        {"morning_odds": 10.0, "finish_position": 4, "morning_odds_rank": 4},
    ])
    report = backtest(df, score_baseline, "test", bet_type="win")
    assert len(report.bets) == 1
    bet = report.bets[0]
    assert bet.hit is False
    assert math.isclose(bet.pnl, -1.0)


def test_backtest_roi():
    """ROI = sum(pnl) / sum(stake) over 3 synthetic races."""
    r1 = _race("r1", "2025-01-01", [
        {"morning_odds": 4.0, "finish_position": 1, "morning_odds_rank": 1},
        {"morning_odds": 6.0, "finish_position": 2, "morning_odds_rank": 2},
        {"morning_odds": 8.0, "finish_position": 3, "morning_odds_rank": 3},
        {"morning_odds": 10.0, "finish_position": 4, "morning_odds_rank": 4},
    ])
    r2 = _race("r2", "2025-01-02", [
        {"morning_odds": 2.0, "finish_position": 2, "morning_odds_rank": 1},
        {"morning_odds": 5.0, "finish_position": 1, "morning_odds_rank": 2},
        {"morning_odds": 8.0, "finish_position": 3, "morning_odds_rank": 3},
        {"morning_odds": 10.0, "finish_position": 4, "morning_odds_rank": 4},
    ])
    r3 = _race("r3", "2025-01-03", [
        {"morning_odds": 6.0, "finish_position": 1, "morning_odds_rank": 1},
        {"morning_odds": 7.0, "finish_position": 2, "morning_odds_rank": 2},
        {"morning_odds": 8.0, "finish_position": 3, "morning_odds_rank": 3},
        {"morning_odds": 10.0, "finish_position": 4, "morning_odds_rank": 4},
    ])
    df = _concat(r1, r2, r3)
    report = backtest(df, score_baseline, "test", bet_type="win")
    assert len(report.bets) == 3
    expected_pnl = (4.0 - 1.0) + (-1.0) + (6.0 - 1.0)  # 3 + (-1) + 5 = 7
    expected_roi = expected_pnl / 3.0
    assert math.isclose(report.roi, expected_roi, rel_tol=1e-9)


def test_backtest_place_top3():
    """Place bet pays when rank-1 horse finishes 3rd (field >= 5)."""
    runners = [
        {"morning_odds": 3.0, "finish_position": 3, "morning_odds_rank": 1},
        {"morning_odds": 5.0, "finish_position": 1, "morning_odds_rank": 2},
        {"morning_odds": 6.0, "finish_position": 2, "morning_odds_rank": 3},
        {"morning_odds": 8.0, "finish_position": 4, "morning_odds_rank": 4},
        {"morning_odds": 10.0, "finish_position": 5, "morning_odds_rank": 5},
    ]
    df = _race("r1", "2025-01-01", runners)
    report = backtest(df, score_baseline, "test", bet_type="place")
    assert len(report.bets) == 1
    bet = report.bets[0]
    assert bet.hit is True
    assert math.isclose(bet.pnl, 3.0 / 4.0 - 1.0, rel_tol=1e-9)


def test_ev_filter_skips_bets():
    """EV filter reduces bet count when model_prob < implied_prob * threshold."""
    # Build a race where baseline score ~= implied_prob, so EV filter at threshold 2.0
    # will reject all bets (model_prob can never be > 2x implied_prob for the winner)
    df = _race("r1", "2025-01-01", [
        {"morning_odds": 3.0, "finish_position": 1, "morning_odds_rank": 1,
         "morning_implied_prob_norm": 0.5},
        {"morning_odds": 5.0, "finish_position": 2, "morning_odds_rank": 2,
         "morning_implied_prob_norm": 0.3},
        {"morning_odds": 10.0, "finish_position": 3, "morning_odds_rank": 3,
         "morning_implied_prob_norm": 0.2},
        {"morning_odds": 12.0, "finish_position": 4, "morning_odds_rank": 4,
         "morning_implied_prob_norm": 0.0},
    ])
    # Without filter
    r_no_filter = backtest(df, score_baseline, "no_ev", bet_type="win", ev_filter=False)
    # With aggressive threshold (model_prob must be > 2x implied_prob — impossible here)
    r_with_filter = backtest(df, score_baseline, "ev", bet_type="win",
                             ev_filter=True, ev_threshold=2.0)
    assert len(r_no_filter.bets) == 1
    assert len(r_with_filter.bets) == 0
