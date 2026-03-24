"""EV threshold grid search — find optimal ev_threshold for WIN bets (LightGBM).

Usage:
    python scripts/tune_ev_threshold.py

Runs a walk-forward backtest for each threshold in EV_THRESHOLDS using the
LightGBM WIN scorer. Prints a summary table and recommends the best threshold.
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
from src.model.lgbm import train_lgbm, score_lgbm, backtest_lgbm_walkforward


EV_THRESHOLDS = [0.7, 0.8, 0.9, 1.0, 1.1, 1.2]
MIN_TRAIN_DAYS = 30


def run_grid_search(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    logger.info("=== WIN bets (LightGBM) — EV threshold grid search ===")
    for thresh in EV_THRESHOLDS:
        report = backtest_lgbm_walkforward(
            df, min_train_days=MIN_TRAIN_DAYS,
            bet_type="win",
            ev_filter=True,
            ev_threshold=thresh,
        )
        rows.append({
            "ev_threshold": thresh,
            "n_bets": len(report.bets),
            "roi": report.roi,
            "hit_rate": report.hit_rate,
            "pnl": sum(b.pnl for b in report.bets),
        })
        logger.info(
            "  thresh={:.2f} | bets={} | ROI={:.1%} | hit={:.1%} | P&L={:.2f}",
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

    print("\n" + "=" * 60)
    print("EV THRESHOLD GRID SEARCH — WIN bets (LightGBM)")
    print("=" * 60)
    print(f"  {'thresh':>6}  {'bets':>5}  {'ROI':>7}  {'hit':>6}  {'P&L':>8}")
    for _, row in results.iterrows():
        print(
            f"  {row['ev_threshold']:>6.2f}  {int(row['n_bets']):>5}  "
            f"{row['roi']:>7.1%}  {row['hit_rate']:>6.1%}  {row['pnl']:>8.2f}"
        )

    best = results.loc[results["roi"].idxmax()]
    print("\n" + "=" * 60)
    print(
        f"RECOMMENDATION: WIN_EV_THRESHOLD = {best['ev_threshold']:.2f}  "
        f"-> ROI={best['roi']:.1%}, bets={int(best['n_bets'])}"
    )
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
