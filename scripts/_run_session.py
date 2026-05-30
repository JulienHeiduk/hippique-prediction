"""Manually invoke a scheduler session: retrain | morning | evening."""
import sys

from src.trading.scheduler import (
    run_retrain_model,
    run_morning_session,
    run_evening_session,
)

SESSIONS = {
    "retrain": run_retrain_model,
    "morning": run_morning_session,
    "evening": run_evening_session,
}

if __name__ == "__main__":
    name = sys.argv[1] if len(sys.argv) > 1 else ""
    fn = SESSIONS.get(name)
    if fn is None:
        print(f"usage: _run_session.py [{'|'.join(SESSIONS)}]")
        sys.exit(2)
    fn()
