"""Walk-forward backtest harness for lab :class:`Variant` objects.

For each test date, the harness:
  1. Trains the variant's model on every prior date.
  2. Optionally fits an isotonic calibrator on a temporal holdout of the
     train slice (out-of-sample (model_prob, outcome) pairs).
  3. Scores the test date.
  4. For each race, takes the top-1 runner, applies the calibrator, and
     bets if calibrated_prob > implied_prob × variant.ev_threshold.
  5. Records P&L using morning_odds (1-unit stake; +odds-1 on win, -1 on loss).

This mirrors the production engine logic in :func:`src.trading.engine.generate_bets`
so a variant's backtest score is comparable to its expected live behaviour.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import duckdb
import numpy as np
import pandas as pd
from loguru import logger

from src.lab.registry import Variant
from src.model.calibration import fit_calibrator_temporal_holdout


@dataclass
class _Bet:
    date: str
    race_id: str
    odds: float
    pnl: float
    hit: bool


@dataclass
class WalkforwardResult:
    variant: str
    bets: list[_Bet] = field(default_factory=list)

    @property
    def n_bets(self) -> int:
        return len(self.bets)

    @property
    def roi(self) -> float:
        # Each bet stakes 1 unit; total_stake == n_bets.
        if not self.bets:
            return 0.0
        return sum(b.pnl for b in self.bets) / len(self.bets)

    @property
    def hit_rate(self) -> float:
        if not self.bets:
            return 0.0
        return sum(1 for b in self.bets if b.hit) / len(self.bets)

    @property
    def avg_odds_won(self) -> float:
        odds = [b.odds for b in self.bets if b.hit]
        return float(np.mean(odds)) if odds else 0.0

    @property
    def pnl(self) -> float:
        return float(sum(b.pnl for b in self.bets))


def _walkforward_one(
    df: pd.DataFrame,
    variant: Variant,
    min_train_days: int = 30,
) -> WalkforwardResult:
    res = WalkforwardResult(variant=variant.name)

    df = df[df["finish_position"].notna()].copy()
    if df.empty:
        logger.warning("[{}] no resolved rows — nothing to backtest", variant.name)
        return res

    dates = sorted(df["date"].unique())
    if len(dates) <= min_train_days:
        raise ValueError(
            f"[{variant.name}] need >{min_train_days} dates, got {len(dates)}"
        )
    test_dates = dates[min_train_days:]
    logger.info(
        "[{}] walk-forward over {} test days ({} → {})",
        variant.name, len(test_dates), test_dates[0], test_dates[-1],
    )

    for i, test_date in enumerate(test_dates):
        train_df = df[df["date"] < test_date]
        test_df  = df[df["date"] == test_date]
        if train_df.empty or test_df.empty:
            continue

        try:
            model = variant.train_fn(train_df)
        except Exception as exc:
            logger.warning("[{}] train failed for {}: {}", variant.name, test_date, exc)
            continue

        calibrator = None
        if variant.use_calibrator:
            try:
                calibrator = fit_calibrator_temporal_holdout(
                    train_df,
                    train_fn=variant.train_fn,
                    score_fn=variant.score_fn,
                    calib_frac=0.2,
                )
            except Exception as exc:
                logger.debug(
                    "[{}] calibrator fit failed for {}: {}",
                    variant.name, test_date, exc,
                )

        try:
            scores = variant.score_fn(test_df, model)
        except Exception as exc:
            logger.warning("[{}] score failed for {}: {}", variant.name, test_date, exc)
            continue

        score_map = dict(zip(scores.index, scores.values))

        for race_id, race_df in test_df.groupby("race_id"):
            race_df = race_df.copy().reset_index(drop=True)
            field_size = len(race_df)
            if field_size < 4:
                continue

            race_df["_score"] = race_df["runner_id"].map(score_map).fillna(0.0)
            total = race_df["_score"].sum()
            if total <= 0:
                continue
            race_df["model_prob"] = race_df["_score"] / total
            race_df = race_df.sort_values("_score", ascending=False).reset_index(drop=True)
            top1 = race_df.iloc[0]

            morning_odds = top1.get("morning_odds")
            if (
                morning_odds is None
                or pd.isna(morning_odds)
                or float(morning_odds) > 100.0
                or float(morning_odds) <= 1.0
            ):
                continue
            morning_odds = float(morning_odds)

            implied = top1.get("morning_implied_prob_norm")
            if implied is None or pd.isna(implied) or implied <= 0:
                implied = 1.0 / field_size
            implied = float(implied)

            raw_prob = float(top1["model_prob"])
            prob = (
                float(calibrator.transform([raw_prob])[0])
                if calibrator is not None else raw_prob
            )

            if prob <= implied * variant.ev_threshold:
                continue

            pos1 = top1.get("finish_position")
            if pos1 is None or pd.isna(pos1):
                continue
            hit = int(pos1) == 1
            pnl = (morning_odds - 1.0) if hit else -1.0

            res.bets.append(_Bet(
                date=str(test_df["date"].iloc[0]),
                race_id=str(race_id),
                odds=morning_odds,
                pnl=pnl,
                hit=hit,
            ))

        if (i + 1) % 20 == 0:
            logger.info(
                "[{}] {}/{} {} :: n={} ROI={:+.2%}",
                variant.name, i + 1, len(test_dates), test_date,
                res.n_bets, res.roi,
            )

    return res


def walkforward(
    conn: duckdb.DuckDBPyConnection,
    variants: list[Variant],
    min_train_days: int = 30,
) -> pd.DataFrame:
    """Run walk-forward for every variant; return a one-row-per-variant summary.

    Variants that share the same ``feature_fn`` object reuse the cached
    feature DataFrame to avoid recomputation.
    """
    rows = []
    feature_cache: dict[int, pd.DataFrame] = {}

    for v in variants:
        logger.info("=== Variant: {} (discipline={}) ===", v.name, v.discipline)
        key = id(v.feature_fn)
        if key not in feature_cache:
            feature_cache[key] = v.feature_fn(conn)
        df = feature_cache[key]
        if df.empty:
            logger.warning("[{}] empty feature df — skipping", v.name)
            continue

        result = _walkforward_one(df, v, min_train_days=min_train_days)
        rows.append({
            "variant":      result.variant,
            "n_bets":       result.n_bets,
            "roi":          result.roi,
            "hit_rate":     result.hit_rate,
            "avg_odds_won": result.avg_odds_won,
            "pnl":          result.pnl,
        })

    return pd.DataFrame(rows)
