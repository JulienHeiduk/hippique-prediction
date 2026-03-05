"""Form string parsing and score computation."""
from __future__ import annotations

import re

_YEAR_MARKER = re.compile(r'\(\d+\)')
_RESULT_TOKEN = re.compile(r'([0-9]+|[Dd])([a-zA-Z])')


def parse_musique(musique: str | None) -> list[int | None]:
    """Most-recent-first list of positions.

    Returns:
        int 1-9+: finish position (1 = win)
        0: DNF
        None: disqualified (D/d token)
        [] if musique is empty or None
    """
    if not musique:
        return []
    cleaned = _YEAR_MARKER.sub("", musique)
    tokens = _RESULT_TOKEN.findall(cleaned)
    result: list[int | None] = []
    for digit_or_d, _letter in tokens:
        if digit_or_d in ("D", "d"):
            result.append(None)
        else:
            result.append(int(digit_or_d))
    return result


def extended_form_features(musique: str | None, n: int = 5) -> dict:
    """Extended form features derived from the musique string.

    Returns a dict with:
        win_rate_last5     : wins / valid runs in last n races
        top3_rate_last5    : top-3 / valid runs in last n races
        form_trend         : avg position last 3 minus avg position prev 3
                             (negative = improving)
        best_position_last5: minimum finish position in last n races
        n_valid_runs       : total valid finishes in full musique (experience)
    Values are None when there are fewer than 2 valid results.
    """
    positions = parse_musique(musique)
    valid = [p for p in positions if p is not None and p != 0]
    n_valid = len(valid)

    out: dict = {
        "win_rate_last5": None,
        "top3_rate_last5": None,
        "form_trend": None,
        "best_position_last5": None,
        "n_valid_runs": float(n_valid),
    }

    if n_valid < 2:
        return out

    last_n = valid[:n]
    out["win_rate_last5"]      = last_n.count(1) / len(last_n)
    out["top3_rate_last5"]     = sum(1 for p in last_n if p <= 3) / len(last_n)
    out["best_position_last5"] = float(min(last_n))

    if n_valid >= 4:
        last3 = valid[:3]
        prev3 = valid[3:6]
        if prev3:
            out["form_trend"] = sum(last3) / len(last3) - sum(prev3) / len(prev3)

    return out


def form_score(musique: str | None, n: int = 5) -> float | None:
    """Weighted recency form score from musique string.

    Score = sum(max(0, 10 - pos) * 0.75**i) for the last n valid positions
    where valid means position 1-9 (not DNF=0, not DQ=None).

    Returns None if fewer than 2 valid results in the full musique.
    """
    positions = parse_musique(musique)
    valid = [p for p in positions if p is not None and p != 0]
    if len(valid) < 2:
        return None
    last_n = valid[:n]
    return sum(max(0, 10 - pos) * (0.75 ** i) for i, pos in enumerate(last_n))
