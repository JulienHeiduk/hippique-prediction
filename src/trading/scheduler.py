"""APScheduler-based daily pipeline: morning scrape + bet generation, evening resolution."""
from __future__ import annotations

from datetime import datetime

from loguru import logger

from src.scraper import get_connection, run_pipeline
from src.trading.engine import generate_bets, resolve_bets
from src.trading.reporter import export_bets_html


def run_morning_session(date: str | None = None) -> None:
    """Run the morning pipeline: scrape today's races and log EV+ bets.

    Args:
        date: YYYYMMDD string. Defaults to today.
    """
    if date is None:
        date = datetime.today().strftime("%Y%m%d")

    logger.info("=== Morning session starting for {} ===", date)

    # Scrape today's races (pipeline.run() accepts YYYYMMDD)
    pipeline_result = run_pipeline(date)
    logger.info(
        "Scraper: {} races, {} runners, {} errors",
        pipeline_result.races_fetched,
        pipeline_result.runners_fetched,
        len(pipeline_result.errors),
    )

    conn = get_connection()
    try:
        bets = generate_bets(conn, date)
        logger.info("=== {} bets logged for {} ===", len(bets), date)
        report_path = export_bets_html(bets, conn, date)
        logger.info("Bet sheet saved → {}", report_path)
    finally:
        conn.close()


def run_evening_session(date: str | None = None) -> None:
    """Run the evening pipeline: scrape results and resolve pending bets.

    Args:
        date: YYYYMMDD string. Defaults to today.
    """
    if date is None:
        date = datetime.today().strftime("%Y%m%d")

    logger.info("=== Evening session starting for {} ===", date)

    # Re-scrape to pull in finish_position (results available after races)
    pipeline_result = run_pipeline(date)
    logger.info(
        "Scraper: {} races, {} runners, {} errors",
        pipeline_result.races_fetched,
        pipeline_result.runners_fetched,
        len(pipeline_result.errors),
    )

    conn = get_connection()
    try:
        summary = resolve_bets(conn, date)
        if not summary.empty:
            row = summary.iloc[0]
            logger.info(
                "=== Resolved {} bets | {} won | P&L={:.2f} | ROI={:.1%} ===",
                int(row["n_bets"]),
                int(row["n_won"]),
                float(row["total_pnl"]),
                float(row["roi"]),
            )
    finally:
        conn.close()


def start_scheduler() -> None:
    """Start the APScheduler with 09:00 morning + 22:00 evening daily jobs."""
    from apscheduler.schedulers.blocking import BlockingScheduler

    scheduler = BlockingScheduler()
    scheduler.add_job(run_morning_session, "cron", hour=9, minute=0)
    scheduler.add_job(run_evening_session, "cron", hour=22, minute=0)

    logger.info("Scheduler starting — morning @ 09:00, evening @ 22:00")
    scheduler.start()
