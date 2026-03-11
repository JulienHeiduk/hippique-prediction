"""Unit tests for src/trading/kelly.py, src/trading/engine.py, src/trading/bayes.py."""
from __future__ import annotations

import math

import duckdb
import pandas as pd
import pytest

from config.settings import KELLY_FRACTION, UNIT_STAKE
from src.scraper.storage import init_schema, upsert_bet
from src.trading.kelly import kelly_stake
from src.trading.engine import generate_bets, resolve_bets
from src.trading.bayes import bayes_scorer, build_bayes_scorer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_conn() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB with full schema."""
    conn = duckdb.connect(":memory:")
    init_schema(conn)
    return conn


def _insert_race(
    conn: duckdb.DuckDBPyConnection,
    race_id: str,
    date: str,
    field_size: int = 4,
    is_trot: bool = True,
) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO races VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [race_id, date, "TEST", None, 2100, None, "TROT", is_trot, field_size, 1, 1, None],
    )


def _insert_runner(
    conn: duckdb.DuckDBPyConnection,
    runner_id: str,
    race_id: str,
    horse_number: int,
    scratch: bool = False,
    finish_position: int | None = None,
) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO runners VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            runner_id, race_id, horse_number,
            f"Horse{horse_number}", "Jockey", "Trainer",
            horse_number, None, None,
            False, scratch,
            None,  # musique
            finish_position, None,
        ],
    )


def _insert_morning_odds(
    conn: duckdb.DuckDBPyConnection,
    runner_id: str,
    race_id: str,
    morning_odds: float,
) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO odds VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            f"o_{runner_id}", runner_id, race_id,
            None, "morning", morning_odds, 1.0 / morning_odds,
        ],
    )


def _flat_scorer(df: pd.DataFrame) -> pd.Series:
    """Returns equal scores for all runners (model_prob = 1/N)."""
    return pd.Series(1.0, index=df["runner_id"])


# ---------------------------------------------------------------------------
# Kelly stake tests
# ---------------------------------------------------------------------------

def test_kelly_positive_ev():
    """p=0.5, odds=3.0 → f=0.25 → stake = 0.25 * 0.25 * 2.0 = 0.125."""
    # b = 2.0, q = 0.5, f = (0.5*2 - 0.5)/2 = 0.25
    stake = kelly_stake(model_prob=0.5, decimal_odds=3.0)
    assert math.isclose(stake, 0.25 * KELLY_FRACTION * UNIT_STAKE, rel_tol=1e-9)


def test_kelly_negative_ev():
    """p=0.2, odds=2.0 → f < 0 → returns 0.0."""
    # b = 1.0, f = (0.2*1 - 0.8)/1 = -0.6 < 0
    stake = kelly_stake(model_prob=0.2, decimal_odds=2.0)
    assert stake == 0.0


def test_kelly_edge_unit_odds():
    """decimal_odds=1.0 → b=0 → returns 0.0 (division guard)."""
    stake = kelly_stake(model_prob=0.9, decimal_odds=1.0)
    assert stake == 0.0


def test_kelly_cap():
    """Large fraction parameter → capped at 3 * UNIT_STAKE."""
    # With fraction=20, any positive EV case will exceed the cap
    # p=0.5, odds=3.0: f=0.25 → uncapped = 0.25 * 20 * 2 = 10 > 3*2=6 → capped at 6
    stake = kelly_stake(model_prob=0.5, decimal_odds=3.0, fraction=20.0)
    assert math.isclose(stake, 3.0 * UNIT_STAKE, rel_tol=1e-9)


# ---------------------------------------------------------------------------
# generate_bets tests
# ---------------------------------------------------------------------------

def test_generate_bets_ev_filter():
    """EV+ race produces a win bet; no-EV race produces nothing."""
    conn = _make_conn()
    date = "20250101"

    # --- Race 1: EV+ (runner 1 is longshot → low implied_prob < model_prob=0.25) ---
    _insert_race(conn, "r_ev", date, field_size=4)
    # Runner with horse_number=1 will be first in SQL result (ORDER BY horse_number)
    _insert_runner(conn, "r_ev_1", "r_ev", 1)
    _insert_morning_odds(conn, "r_ev_1", "r_ev", 20.0)   # low implied prob ≈ 0.05/total
    for i in [2, 3, 4]:
        _insert_runner(conn, f"r_ev_{i}", "r_ev", i)
        _insert_morning_odds(conn, f"r_ev_{i}", "r_ev", 4.0)

    # --- Race 2: no EV (runner 1 is a heavy favourite → high implied_prob > model_prob=0.25) ---
    _insert_race(conn, "r_no_ev", date, field_size=4)
    _insert_runner(conn, "r_no_ev_1", "r_no_ev", 1)
    _insert_morning_odds(conn, "r_no_ev_1", "r_no_ev", 1.5)  # implied_prob_norm >> 0.25
    for i in [2, 3, 4]:
        _insert_runner(conn, f"r_no_ev_{i}", "r_no_ev", i)
        _insert_morning_odds(conn, f"r_no_ev_{i}", "r_no_ev", 15.0)

    bets = generate_bets(conn, date, scorer_fn=_flat_scorer, bet_types=["win"])

    race_ids_bet = {b["race_id"] for b in bets}
    assert "r_ev" in race_ids_bet, "Expected EV+ race to produce a bet"
    assert "r_no_ev" not in race_ids_bet, "Expected no-EV race to be skipped"

    # Verify it was persisted in the DB
    stored = conn.execute("SELECT * FROM bets WHERE date = ?", [date]).df()
    assert len(stored) == len(bets)


def test_generate_bets_empty_no_ev():
    """All runners have equal implied_prob = model_prob → EV ratio = 1.0 → 0 bets."""
    conn = _make_conn()
    date = "20250102"

    _insert_race(conn, "r_equal", date, field_size=4)
    for i in range(1, 5):
        _insert_runner(conn, f"r_eq_{i}", "r_equal", i)
        _insert_morning_odds(conn, f"r_eq_{i}", "r_equal", 4.0)  # equal odds → equal implied prob

    # flat scorer: model_prob = 1/4 = 0.25 = implied_prob_norm → not strictly greater → no bet
    bets = generate_bets(conn, date, scorer_fn=_flat_scorer, bet_types=["win"])
    assert bets == [], f"Expected 0 bets, got {len(bets)}"


# ---------------------------------------------------------------------------
# resolve_bets test
# ---------------------------------------------------------------------------

def test_resolve_bets_win():
    """Pending win bet on the winner → status='won', pnl=(odds-1)*UNIT_STAKE."""
    conn = _make_conn()
    date = "20250103"
    race_id = "r_resolve"
    runner_id = "runner_w1"
    morning_odds_val = 3.0
    expected_pnl = (morning_odds_val - 1.0) * UNIT_STAKE  # = 4.0

    # Setup race + runners (with finish positions already set — "results are in")
    _insert_race(conn, race_id, date, field_size=4)
    _insert_runner(conn, runner_id, race_id, 1, finish_position=1)     # winner
    for i in [2, 3, 4]:
        _insert_runner(conn, f"runner_w{i}", race_id, i, finish_position=i)

    # Insert a pending bet manually
    upsert_bet(conn, {
        "bet_id": f"{race_id}_win",
        "race_id": race_id,
        "date": date,
        "hippodrome": "TEST",
        "bet_type": "win",
        "runner_id_1": runner_id,
        "runner_id_2": None,
        "horse_name_1": "Horse1",
        "horse_name_2": None,
        "morning_odds": morning_odds_val,
        "model_prob": 0.6,
        "implied_prob": 0.3,
        "ev_ratio": 2.0,
        "kelly_stake": 0.1,
        "stake": UNIT_STAKE,
        "status": "pending",
        "pnl": None,
        "created_at": None,
        "resolved_at": None,
    })

    summary = resolve_bets(conn, date)

    assert not summary.empty, "Expected a non-empty summary"
    assert summary.iloc[0]["n_bets"] == 1
    assert summary.iloc[0]["n_won"] == 1
    assert math.isclose(summary.iloc[0]["total_pnl"], expected_pnl, rel_tol=1e-9)

    # Verify DB was updated
    stored = conn.execute(
        "SELECT status, pnl FROM bets WHERE bet_id = ?", [f"{race_id}_win"]
    ).df()
    assert stored.iloc[0]["status"] == "won"
    assert math.isclose(stored.iloc[0]["pnl"], expected_pnl, rel_tol=1e-9)


# ---------------------------------------------------------------------------
# bayes_scorer tests
# ---------------------------------------------------------------------------

def _make_race_df(
    race_id: str,
    runner_ids: list[str],
    implied_probs: list[float],
    win_rates: list[float],
    n_runs: list[int],
) -> pd.DataFrame:
    """Build a minimal features DataFrame for bayes_scorer tests."""
    return pd.DataFrame({
        "race_id": race_id,
        "runner_id": runner_ids,
        "morning_implied_prob_norm": implied_probs,
        "horse_win_rate": win_rates,
        "horse_n_runs": n_runs,
    })


def test_bayes_scorer_prior_only():
    """With no history (n_runs=0), scores are proportional to bookmaker implied probs."""
    df = _make_race_df(
        "r1",
        ["h1", "h2", "h3"],
        [0.5, 0.3, 0.2],
        [0.0, 0.0, 0.0],
        [0, 0, 0],
    )
    scores = bayes_scorer(df, kappa=10.0)
    assert list(scores.index) == ["h1", "h2", "h3"]
    # With no history, alpha_i = kappa * implied_prob_i → ratios match implied probs
    assert scores["h1"] > scores["h2"] > scores["h3"]
    assert math.isclose(scores["h1"] / scores["h2"], 0.5 / 0.3, rel_tol=1e-6)


def test_bayes_scorer_history_shifts_probabilities():
    """A horse with many wins accumulates alpha and rises above its market rank."""
    df = _make_race_df(
        "r2",
        ["h1", "h2"],
        [0.4, 0.6],   # h2 is market favourite
        [0.5, 0.1],   # h1 has strong form history
        [20, 5],      # h1: 10 wins, h2: 0.5 wins
    )
    scores = bayes_scorer(df, kappa=1.0)  # low kappa → history dominates
    # h1: alpha = 1*0.4 + 0.5*20 = 10.4, h2: alpha = 1*0.6 + 0.1*5 = 1.1
    assert scores["h1"] > scores["h2"]


def test_bayes_scorer_missing_implied_prob_falls_back_to_flat():
    """NaN implied_prob falls back to 1/n flat prior."""
    df = _make_race_df(
        "r3",
        ["h1", "h2", "h3", "h4"],
        [float("nan")] * 4,
        [0.0] * 4,
        [0] * 4,
    )
    scores = bayes_scorer(df, kappa=5.0)
    # All equal with flat prior and no history
    vals = scores.values
    assert all(math.isclose(v, vals[0], rel_tol=1e-6) for v in vals)


def test_build_bayes_scorer_compatible_with_generate_bets():
    """build_bayes_scorer() produces a scorer that generate_bets() accepts."""
    conn = _make_conn()
    date = "20250110"

    _insert_race(conn, "r_bayes", date, field_size=4)
    _insert_runner(conn, "rb_1", "r_bayes", 1)
    _insert_morning_odds(conn, "rb_1", "r_bayes", 20.0)  # longshot → EV+ with flat model
    for i in [2, 3, 4]:
        _insert_runner(conn, f"rb_{i}", "r_bayes", i)
        _insert_morning_odds(conn, f"rb_{i}", "r_bayes", 4.0)

    scorer = build_bayes_scorer(kappa=5.0)
    bets = generate_bets(conn, date, scorer_fn=scorer, bet_types=["win"], model_source="bayes")
    # Should not raise; bets may be empty or non-empty depending on EV filter
    assert isinstance(bets, list)
    for b in bets:
        assert b["model_source"] == "bayes"
