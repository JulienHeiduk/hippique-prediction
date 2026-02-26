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
