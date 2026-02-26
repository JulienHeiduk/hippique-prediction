"""Backtesting engine: BetResult, BacktestReport, and backtest()."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

import pandas as pd

from config.settings import EV_THRESHOLD


@dataclass
class BetResult:
    race_id: str
    date: str
    bet_type: str   # "win" | "place" | "duo"
    stake: float
    pnl: float      # profit/loss in units
    hit: bool


@dataclass
class BacktestReport:
    model_name: str
    bet_type: str
    bets: list[BetResult] = field(default_factory=list)

    @property
    def roi(self) -> float:
        total_stake = sum(b.stake for b in self.bets)
        if total_stake == 0:
            return 0.0
        return sum(b.pnl for b in self.bets) / total_stake

    @property
    def hit_rate(self) -> float:
        if not self.bets:
            return 0.0
        return sum(1 for b in self.bets if b.hit) / len(self.bets)

    def summary(self) -> str:
        return (
            f"{self.model_name} | {self.bet_type} | "
            f"bets={len(self.bets)} | "
            f"ROI={self.roi:.1%} | "
            f"hit={self.hit_rate:.1%} | "
            f"P&L={sum(b.pnl for b in self.bets):.2f}u"
        )

    def pnl_series(self) -> pd.Series:
        """Cumulative P&L indexed by date string."""
        if not self.bets:
            return pd.Series(dtype=float)
        df = pd.DataFrame(
            {"date": b.date, "pnl": b.pnl} for b in self.bets
        )
        daily = df.groupby("date")["pnl"].sum().sort_index()
        return daily.cumsum()


def backtest(
    features_df: pd.DataFrame,
    scorer_fn: Callable[[pd.DataFrame], pd.Series],
    model_name: str,
    bet_type: str = "win",
    ev_filter: bool = False,
    ev_threshold: float = EV_THRESHOLD,
    min_field_size: int = 4,
) -> BacktestReport:
    """Run a backtest of scorer_fn on features_df.

    For each race:
      - Score all non-scratch runners.
      - Select top-ranked runner(s) based on bet_type.
      - Optionally skip if EV filter not satisfied.
      - Compute P&L using morning_odds.

    P&L rules (1-unit stake):
      win   : rank-1 wins 1st  → profit = morning_odds - 1 ; loss = -1
      place : rank-1 in top 3 (top 2 if field < 5) → profit = morning_odds/4 - 1 ; loss = -1
      duo   : rank-1 AND rank-2 both in top 2 (any order)
              → stake=2, profit = field_size² / 4 capped at 50 ; loss = -2
    """
    report = BacktestReport(model_name=model_name, bet_type=bet_type)

    if features_df.empty:
        return report

    for race_id, race_df in features_df.groupby("race_id"):
        race_df = race_df.copy().reset_index(drop=True)
        date_str = str(race_df["date"].iloc[0])
        field_size = len(race_df)

        if field_size < min_field_size:
            continue

        # --- Score runners ---
        try:
            scores = scorer_fn(race_df)
        except Exception:
            continue

        # Align scores back to race_df by runner_id
        score_map = dict(zip(scores.index, scores.values))
        race_df["_score"] = race_df["runner_id"].map(score_map).fillna(0.0)

        # Fallback: if morning_odds missing, use 1/field_size
        race_df["morning_odds"] = race_df["morning_odds"].fillna(field_size)

        race_df = race_df.sort_values("_score", ascending=False).reset_index(drop=True)

        top1 = race_df.iloc[0]
        morning_odds_top1 = top1["morning_odds"]

        # --- EV filter ---
        if ev_filter:
            total_score = race_df["_score"].sum()
            if total_score > 0:
                model_prob = top1["_score"] / total_score
                implied_prob = top1.get("morning_implied_prob_norm", 1.0 / field_size)
                if pd.isna(implied_prob):
                    implied_prob = 1.0 / field_size
                if model_prob < implied_prob * ev_threshold:
                    continue
            else:
                continue

        # --- P&L calculation ---
        if bet_type == "win":
            stake = 1.0
            hit = int(top1["finish_position"]) == 1
            pnl = (morning_odds_top1 - 1.0) if hit else -1.0

        elif bet_type == "place":
            stake = 1.0
            cutoff = 2 if field_size < 5 else 3
            hit = int(top1["finish_position"]) <= cutoff
            pnl = (morning_odds_top1 / 4.0 - 1.0) if hit else -1.0

        elif bet_type == "duo":
            stake = 2.0
            if len(race_df) < 2:
                continue
            top2 = race_df.iloc[1]
            pos1 = int(top1["finish_position"])
            pos2 = int(top2["finish_position"])
            hit = set([pos1, pos2]) == {1, 2}
            if hit:
                raw_pnl = field_size ** 2 / 4.0
                pnl = min(raw_pnl, 50.0) - 2.0
            else:
                pnl = -2.0

        else:
            raise ValueError(f"Unknown bet_type: {bet_type!r}")

        report.bets.append(BetResult(
            race_id=str(race_id),
            date=date_str,
            bet_type=bet_type,
            stake=stake,
            pnl=pnl,
            hit=hit,
        ))

    return report
