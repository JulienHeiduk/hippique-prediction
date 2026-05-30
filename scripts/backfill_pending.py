"""One-off: backfill (resolve) all pending bets by re-scraping race results.

For each date that still has pending bets, re-run the scraper pipeline (past-date
responses include finish_position) then resolve_bets to settle them.
"""
from __future__ import annotations

import duckdb

from config.settings import DB_PATH
from src.scraper import pipeline
from src.trading.engine import resolve_bets


def main() -> None:
    conn = duckdb.connect(str(DB_PATH))
    dates = [
        r[0]
        for r in conn.execute(
            "SELECT DISTINCT date FROM bets WHERE status='pending' ORDER BY date"
        ).fetchall()
    ]
    conn.close()
    print(f"[backfill] {len(dates)} date(s) with pending bets: {dates}", flush=True)

    for d in dates:
        print(f"[backfill] === {d} : scraping results ===", flush=True)
        res = pipeline.run(date=d)
        print(
            f"[backfill] {d}: pipeline {res.races_fetched} races, "
            f"{res.runners_fetched} runners, {len(res.errors)} errors",
            flush=True,
        )

        conn = duckdb.connect(str(DB_PATH))
        try:
            summary = resolve_bets(conn, d)
            remaining = conn.execute(
                "SELECT COUNT(*) FROM bets WHERE date=? AND status='pending'", [d]
            ).fetchone()[0]
        finally:
            conn.close()

        if not summary.empty:
            row = summary.iloc[0]
            print(
                f"[backfill] {d}: resolved n={int(row['n_bets'])} "
                f"won={int(row['n_won'])} pnl={float(row['total_pnl']):.2f} "
                f"roi={float(row['roi']):.1%} | still_pending={remaining}",
                flush=True,
            )
        else:
            print(
                f"[backfill] {d}: nothing resolved | still_pending={remaining}",
                flush=True,
            )

    # Final tally
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    total_pending = conn.execute(
        "SELECT COUNT(*) FROM bets WHERE status='pending'"
    ).fetchone()[0]
    conn.close()
    print(f"[backfill] DONE — total pending remaining across all dates: {total_pending}", flush=True)


if __name__ == "__main__":
    main()
