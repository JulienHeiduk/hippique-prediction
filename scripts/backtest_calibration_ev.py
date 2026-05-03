"""Walk-forward backtest: isotonic calibration + EV ≥ 1.5 gate (plat only).

Mirrors the production engine pipeline:
  - Train binary classifier on all history before *test_date*.
  - Fit isotonic on a temporal holdout of the train slice.
  - Score test_date with the fresh model.
  - Apply calibrator to the top-1 model_prob.
  - Compare calibrated_prob vs implied_prob × ev_threshold.

Compares 4 cells:
  baseline      : no calibration, ev_threshold=1.0
  calib_only    : calibration on, ev_threshold=1.0
  ev_only       : no calibration, ev_threshold=1.5
  prod          : calibration on, ev_threshold=1.5  ← shipping config

Gate: prod ROI ≥ +3% on the full walk-forward period.

Usage:
    .venv/Scripts/python.exe -m scripts.backtest_calibration_ev
"""
from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.scraper import get_connection, close_connection
from src.features.pipeline import compute_features
from src.model.lgbm import train_for_discipline, score_for_discipline
from src.model.calibration import IsotonicProbCalibrator, fit_calibrator_temporal_holdout


@dataclass
class Cell:
    name: str
    use_calibrator: bool
    ev_threshold: float
    bets: list = None

    def __post_init__(self):
        if self.bets is None:
            self.bets = []

    @property
    def roi(self) -> float:
        if not self.bets:
            return 0.0
        stake = sum(b["stake"] for b in self.bets)
        pnl = sum(b["pnl"] for b in self.bets)
        return pnl / stake if stake else 0.0

    @property
    def hit_rate(self) -> float:
        if not self.bets:
            return 0.0
        return sum(1 for b in self.bets if b["hit"]) / len(self.bets)

    @property
    def avg_odds_won(self) -> float:
        winners = [b["pnl"] + 1.0 for b in self.bets if b["hit"]]
        return float(np.mean(winners)) if winners else 0.0


def _score_top1_for_race(race_df: pd.DataFrame, scores: pd.Series) -> tuple[pd.Series, float]:
    """Return (top1_row, model_prob_top1) using normalised scores within the race."""
    score_map = dict(zip(scores.index, scores.values))
    rd = race_df.copy()
    rd["_score"] = rd["runner_id"].map(score_map).fillna(0.0)
    total = rd["_score"].sum()
    if total <= 0:
        return None, 0.0
    rd["model_prob"] = rd["_score"] / total
    rd = rd.sort_values("_score", ascending=False).reset_index(drop=True)
    top1 = rd.iloc[0]
    return top1, float(top1["model_prob"])


def _walkforward(df: pd.DataFrame, cells: list[Cell], min_train_days: int = 30) -> None:
    dates = sorted(df["date"].unique())
    if len(dates) <= min_train_days:
        raise ValueError(f"Need >{min_train_days} dates, got {len(dates)}")
    test_dates = dates[min_train_days:]
    logger.info("Walk-forward over {} test days ({} → {})", len(test_dates), test_dates[0], test_dates[-1])

    for i, test_date in enumerate(test_dates):
        train_df = df[df["date"] < test_date]
        test_df = df[df["date"] == test_date]
        if train_df.empty or test_df.empty:
            continue

        try:
            model = train_for_discipline(train_df, discipline="plat")
        except Exception as exc:
            logger.warning("Train failed for {}: {}", test_date, exc)
            continue

        # Fit isotonic on a temporal holdout of the *train* slice (out-of-sample).
        calibrator = None
        try:
            calibrator = fit_calibrator_temporal_holdout(
                train_df,
                train_fn=lambda d: train_for_discipline(d, discipline="plat"),
                score_fn=lambda d, m: score_for_discipline(d, m, discipline="plat"),
                calib_frac=0.2,
            )
        except Exception as exc:
            logger.debug("Calibrator fit failed for {}: {}", test_date, exc)

        # Score the test day once with the fresh model.
        scores = score_for_discipline(test_df, model, discipline="plat")

        for race_id, race_df in test_df.groupby("race_id"):
            race_df = race_df.copy().reset_index(drop=True)
            field_size = len(race_df)
            if field_size < 4:
                continue

            top1, raw_prob = _score_top1_for_race(race_df, scores)
            if top1 is None:
                continue

            morning_odds = top1.get("morning_odds")
            if morning_odds is None or pd.isna(morning_odds) or float(morning_odds) > 100.0 or float(morning_odds) <= 1.0:
                continue
            morning_odds = float(morning_odds)

            implied = top1.get("morning_implied_prob_norm")
            if implied is None or pd.isna(implied) or implied <= 0:
                implied = 1.0 / field_size
            implied = float(implied)

            calibrated_prob = (
                float(calibrator.transform([raw_prob])[0])
                if calibrator is not None else raw_prob
            )

            pos1 = top1.get("finish_position")
            if pos1 is None or pd.isna(pos1):
                continue
            hit = int(pos1) == 1
            pnl_unit = (morning_odds - 1.0) if hit else -1.0

            for c in cells:
                p = calibrated_prob if c.use_calibrator else raw_prob
                if p > implied * c.ev_threshold:
                    c.bets.append({
                        "race_id": race_id,
                        "date": str(test_df["date"].iloc[0]),
                        "stake": 1.0,
                        "pnl": pnl_unit,
                        "hit": hit,
                        "odds": morning_odds,
                        "model_prob": p,
                        "implied": implied,
                    })

        if (i + 1) % 10 == 0:
            line = " | ".join(
                f"{c.name}: n={len(c.bets)} ROI={c.roi:+.1%}" for c in cells
            )
            logger.info("  {}/{} {} :: {}", i + 1, len(test_dates), test_date, line)


def main() -> int:
    logger.info("Loading plat features…")
    conn = get_connection()
    try:
        df = compute_features(conn, discipline="plat")
    finally:
        close_connection()

    if df.empty:
        logger.error("No plat features — aborting.")
        return 1

    df = df[df["finish_position"].notna()].copy()
    logger.info("Plat resolved set: {} runners / {} races / {} dates",
                len(df), df["race_id"].nunique(), df["date"].nunique())

    cells = [
        Cell("baseline       (no calib, EV>=1.0)", use_calibrator=False, ev_threshold=1.0),
        Cell("calib_only     (calib,     EV>=1.0)", use_calibrator=True,  ev_threshold=1.0),
        Cell("ev_only        (no calib, EV>=1.5)", use_calibrator=False, ev_threshold=1.5),
        Cell("prod           (calib,     EV>=1.5)", use_calibrator=True,  ev_threshold=1.5),
    ]
    _walkforward(df, cells, min_train_days=30)

    print()
    print("=== Plat — calibration × EV-gate sweep ===")
    rows = []
    for c in cells:
        rows.append({
            "cell": c.name,
            "n_bets": len(c.bets),
            "ROI": f"{c.roi:+.2%}",
            "hit_rate": f"{c.hit_rate:.1%}",
            "avg_odds_won": f"{c.avg_odds_won:.2f}",
            "P&L_units": f"{sum(b['pnl'] for b in c.bets):+.2f}",
        })
    print(pd.DataFrame(rows).to_string(index=False))

    prod = next(c for c in cells if c.name.startswith("prod"))
    print()
    if prod.roi >= 0.03:
        print(f"GATE PASS — prod ROI {prod.roi:+.2%} >= +3%")
        return 0
    print(f"GATE FAIL — prod ROI {prod.roi:+.2%} < +3%")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
