"""Walk-forward backtest of the plat binary classifier with market-blend sweep.

Trains an `objective='binary'` LightGBM on plat history (label = won),
then sweeps the market-blend weight w in {0.0, 0.2, 0.4, 0.6, 0.8, 1.0}
and reports ROI / hit rate / avg odds bet for each w on out-of-sample
test days.

Usage:
    .venv/Scripts/python.exe -m scripts.backtest_plat_binary
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
from loguru import logger

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.scraper import get_connection, close_connection
from src.features.pipeline import compute_features
from src.model.backtest import backtest, BacktestReport
from src.model.lgbm import train_lgbm_binary, score_lgbm_binary


def _walkforward_one_w(
    df: pd.DataFrame,
    w: float,
    min_train_days: int,
    bet_type: str,
    ev_filter: bool,
    ev_threshold: float,
) -> BacktestReport:
    """Walk-forward backtest at a single blend weight."""
    dates = sorted(df["date"].unique())
    if len(dates) <= min_train_days:
        raise ValueError(
            f"Need >{min_train_days} dates of data, got {len(dates)}"
        )
    test_dates = dates[min_train_days:]
    report = BacktestReport(model_name=f"plat_binary_w{w:.1f}", bet_type=bet_type)

    for i, test_date in enumerate(test_dates):
        train_df = df[df["date"] < test_date]
        test_df = df[df["date"] == test_date]
        if train_df.empty or test_df.empty:
            continue
        try:
            model = train_lgbm_binary(train_df, discipline="plat")
        except Exception as exc:
            logger.warning("Train failed for {}: {}", test_date, exc)
            continue

        scorer = lambda d, m=model, ww=w: score_lgbm_binary(
            d, m, discipline="plat", blend_w=ww,
        )
        day_report = backtest(
            test_df, scorer,
            model_name=report.model_name,
            bet_type=bet_type,
            ev_filter=ev_filter,
            ev_threshold=ev_threshold,
        )
        report.bets.extend(day_report.bets)

        if (i + 1) % 10 == 0:
            logger.info(
                "  w={:.1f}: {}/{} dates done | running ROI={:.1%}",
                w, i + 1, len(test_dates), report.roi,
            )

    return report


def _avg_odds(report: BacktestReport) -> float:
    if not report.bets:
        return 0.0
    # P&L for win bets: hit → odds-1, miss → -1; reverse-engineer odds:
    odds = [b.pnl + 1.0 if b.hit else None for b in report.bets if b.bet_type == "win"]
    odds = [o for o in odds if o is not None]
    return float(sum(odds) / len(odds)) if odds else 0.0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-train-days", type=int, default=30)
    parser.add_argument("--bet-type", choices=["win", "place"], default="win")
    parser.add_argument("--ev-filter", action="store_true")
    parser.add_argument("--ev-threshold", type=float, default=1.0)
    parser.add_argument(
        "--weights", type=str, default="0.0,0.2,0.4,0.6,0.8,1.0",
        help="Comma-separated blend weights to sweep.",
    )
    args = parser.parse_args()

    weights = [float(x) for x in args.weights.split(",")]

    logger.info("Loading plat features…")
    conn = get_connection()
    try:
        df = compute_features(conn, discipline="plat")
    finally:
        close_connection()

    if df.empty:
        logger.error("No plat features available — aborting.")
        return 1

    df = df[df["finish_position"].notna()].copy()
    logger.info(
        "Plat resolved set: {} runners across {} races / {} dates",
        len(df), df["race_id"].nunique(), df["date"].nunique(),
    )

    rows = []
    for w in weights:
        logger.info("=== Sweeping w={:.1f} ===", w)
        rep = _walkforward_one_w(
            df, w=w,
            min_train_days=args.min_train_days,
            bet_type=args.bet_type,
            ev_filter=args.ev_filter,
            ev_threshold=args.ev_threshold,
        )
        rows.append({
            "w": w,
            "n_bets": len(rep.bets),
            "roi": rep.roi,
            "hit_rate": rep.hit_rate,
            "avg_odds": _avg_odds(rep),
            "total_pnl": sum(b.pnl for b in rep.bets),
        })

    summary = pd.DataFrame(rows)
    print()
    print("=== Plat binary classifier — market-blend sweep ===")
    print(f"bet_type={args.bet_type}  ev_filter={args.ev_filter}  ev_threshold={args.ev_threshold}")
    print(summary.to_string(index=False, float_format=lambda x: f"{x:.3f}"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
