"""Fractional Kelly stake calculator."""
from __future__ import annotations

from config.settings import KELLY_FRACTION, UNIT_STAKE


def kelly_stake(
    model_prob: float,
    decimal_odds: float,
    fraction: float = KELLY_FRACTION,
    unit: float = UNIT_STAKE,
) -> float:
    """Fractional Kelly: f = max(0, (p*b - q)/b) * fraction.

    Returns stake in euros, capped at 3 * unit.
    Returns 0.0 for negative EV (p*b <= q) or invalid inputs.
    """
    b = decimal_odds - 1.0
    if b <= 0 or model_prob <= 0:
        return 0.0
    q = 1.0 - model_prob
    f = (model_prob * b - q) / b
    return min(max(0.0, f) * fraction * unit, 3.0 * unit)
