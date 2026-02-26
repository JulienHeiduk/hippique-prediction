"""Public API for the model package."""
from src.model.backtest import backtest, BacktestReport, BetResult
from src.model.scorer import (
    score_baseline,
    score_form,
    score_market,
    score_combined,
    optimize_weights,
)

__all__ = [
    "backtest",
    "BacktestReport",
    "BetResult",
    "score_baseline",
    "score_form",
    "score_market",
    "score_combined",
    "optimize_weights",
]
