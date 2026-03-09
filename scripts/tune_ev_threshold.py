"""EV threshold grid search — find optimal ev_threshold for WIN and DUO bets.

Usage:
    python scripts/tune_ev_threshold.py

Runs walk-forward backtests for thresholds [1.0, 1.1, 1.2, 1.5, 2.0] on both:
  - WIN  bets with the rule-based scorer (score_combined with saved weights)
  - DUO  bets with LightGBM scorer (walk-forward, no data leakage)

Prints a summary table and recommends the best threshold per bet type.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd
from loguru import logger

from src.scraper import get_connection
from src.features.pipeline import compute_features
from src.model.backtest import backtest, BacktestReport
from src.model.scorer import score_combined, load_rule_weights
from src.model.lgbm import train_lgbm, score_lgbm, backtest_lgbm_walkforward


EV_THRESHOLDS = [1.0, 1.1, 1.2, 1.5, 2.0]
MIN_TRAIN_DAYS = 30


def _rules_walkforward(
    df: pd.DataFrame,
    weights: dict,
    bet_type: str,
    ev_threshold: float,
) -> BacktestReport:
    """Walk-forward backtest for rule-based scorer — identical split logic to lgbm."""
    dates = sorted(df["date"].unique())
    if len(dates) <= MIN_TRAIN_DAYS:
        raise ValueError(f"Need > {MIN_TRAIN_DAYS} dates, got {len(dates)}")

    test_dates = dates[MIN_TRAIN_DAYS:]
    full_report = BacktestReport(model_name="rules_walkforward", bet_type=bet_type)

    for test_date in test_dates:
        test_df = df[df["date"] == test_date]
        if test_df.empty:
            continue

        scorer = lambda d, w=weights: score_combined(d, **w)
        day_report = backtest(
            test_df, scorer,
            model_name="rules",
            bet_type=bet_type,
            ev_filter=True,
            ev_threshold=ev_threshold,
        )
        full_report.bets.extend(day_report.bets)

    return full_report


def run_grid_search(df: pd.DataFrame) -> pd.DataFrame:
    weights = load_rule_weights()
    logger.info("Loaded rule weights: {}", weights)

    rows = []

    # --- WIN bets: rules scorer ---
    logger.info("=== WIN bets (rules) — EV threshold grid search ===")
    for thresh in EV_THRESHOLDS:
        report = _rules_walkforward(df, weights, bet_type="win", ev_threshold=thresh)
        rows.append({
            "scorer": "rules",
            "bet_type": "win",
            "ev_threshold": thresh,
            "n_bets": len(report.bets),
            "roi": report.roi,
            "hit_rate": report.hit_rate,
            "pnl": sum(b.pnl for b in report.bets),
        })
        logger.info(
            "  thresh={:.1f} | bets={} | ROI={:.1%} | hit={:.1%} | P&L={:.2f}",
            thresh, len(report.bets), report.roi, report.hit_rate,
            sum(b.pnl for b in report.bets),
        )

    # --- DUO bets: LightGBM walk-forward ---
    logger.info("=== DUO bets (LightGBM) — EV threshold grid search ===")
    for thresh in EV_THRESHOLDS:
        report = backtest_lgbm_walkforward(
            df, min_train_days=MIN_TRAIN_DAYS,
            bet_type="duo",
            ev_filter=True,
            ev_threshold=thresh,
        )
        rows.append({
            "scorer": "lgbm",
            "bet_type": "duo",
            "ev_threshold": thresh,
            "n_bets": len(report.bets),
            "roi": report.roi,
            "hit_rate": report.hit_rate,
            "pnl": sum(b.pnl for b in report.bets),
        })
        logger.info(
            "  thresh={:.1f} | bets={} | ROI={:.1%} | hit={:.1%} | P&L={:.2f}",
            thresh, len(report.bets), report.roi, report.hit_rate,
            sum(b.pnl for b in report.bets),
        )

    return pd.DataFrame(rows)


def main() -> None:
    logger.info("Loading historical features...")
    conn = get_connection()
    try:
        df = compute_features(conn)
    finally:
        conn.close()

    logger.info(
        "Dataset: {} dates / {} races / {} runners",
        df["date"].nunique(),
        df["race_id"].nunique(),
        len(df),
    )

    results = run_grid_search(df)

    print("\n" + "=" * 70)
    print("EV THRESHOLD GRID SEARCH RESULTS")
    print("=" * 70)

    for scorer_bt, group in results.groupby(["scorer", "bet_type"]):
        scorer, bt = scorer_bt
        print(f"\n{scorer.upper()} — {bt.upper()} bets:")
        print(f"  {'thresh':>6}  {'bets':>5}  {'ROI':>7}  {'hit':>6}  {'P&L':>8}")
        for _, row in group.iterrows():
            print(
                f"  {row['ev_threshold']:>6.1f}  {int(row['n_bets']):>5}  "
                f"{row['roi']:>7.1%}  {row['hit_rate']:>6.1%}  {row['pnl']:>8.2f}"
            )
        best = group.loc[group["roi"].idxmax()]
        print(
            f"  -> Best: thresh={best['ev_threshold']:.1f}  "
            f"ROI={best['roi']:.1%}  bets={int(best['n_bets'])}"
        )

    print("\n" + "=" * 70)
    best_win = results[results["bet_type"] == "win"].loc[
        results[results["bet_type"] == "win"]["roi"].idxmax()
    ]
    best_duo = results[results["bet_type"] == "duo"].loc[
        results[results["bet_type"] == "duo"]["roi"].idxmax()
    ]
    print(
        f"RECOMMENDATION:\n"
        f"  WIN (rules): EV_THRESHOLD = {best_win['ev_threshold']:.1f}  "
        f"-> ROI={best_win['roi']:.1%}, bets={int(best_win['n_bets'])}\n"
        f"  DUO (lgbm):  EV_THRESHOLD = {best_duo['ev_threshold']:.1f}  "
        f"-> ROI={best_duo['roi']:.1%}, bets={int(best_duo['n_bets'])}"
    )
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
