"""APScheduler-based daily pipeline: morning scrape + bet generation, evening resolution."""
from __future__ import annotations

import subprocess
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger

from src.scraper import get_connection, run_pipeline
from src.features.pipeline import compute_features
from src.model.lgbm import train_lgbm, save_lgbm_model, load_lgbm_model, score_lgbm
from src.trading.engine import generate_bets, resolve_bets
from src.trading.reporter import export_bets_html


def _git_push(path: Path) -> None:
    """Stage *path*, commit, and push to the remote origin.

    If nothing changed (git diff --cached is empty) the commit step is
    skipped.  Errors are logged as warnings so the scheduler keeps running
    even if git is unavailable or the remote is unreachable.
    """
    try:
        subprocess.run(["git", "add", str(path)], check=True, capture_output=True)

        # Skip commit when there is nothing new to record
        cached = subprocess.run(
            ["git", "diff", "--cached", "--quiet"],
            capture_output=True,
        )
        if cached.returncode == 0:
            logger.debug("git: no changes in {} — skipping commit", path.name)
            return

        date_label = path.stem.replace("bets_", "")
        msg = f"chore(data): update {date_label} bet sheet"
        subprocess.run(["git", "commit", "-m", msg], check=True, capture_output=True)
        subprocess.run(["git", "push"], check=True, capture_output=True)
        logger.info("git: pushed {} to GitHub", path.name)
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or b"").decode(errors="replace").strip()
        logger.warning("git push failed: {}", stderr or exc)
    except Exception as exc:
        logger.warning("git push error: {}", exc)


def run_morning_session(date: str | None = None) -> None:
    """Scrape today's races, log EV+ bets, export the HTML bet sheet, and push to GitHub.

    Args:
        date: YYYYMMDD string. Defaults to today.
    """
    if date is None:
        date = datetime.today().strftime("%Y%m%d")

    logger.info("=== Morning session starting for {} ===", date)

    pipeline_result = run_pipeline(date)
    logger.info(
        "Scraper: {} races, {} runners, {} errors",
        pipeline_result.races_fetched,
        pipeline_result.runners_fetched,
        len(pipeline_result.errors),
    )

    conn = get_connection()
    try:
        # Retrain LightGBM on all historical data (races with known results)
        hist_df = compute_features(conn)
        lgbm_model = None
        if not hist_df.empty:
            try:
                lgbm_model = train_lgbm(hist_df)
                save_lgbm_model(lgbm_model)
            except Exception as exc:
                logger.warning("LightGBM training failed: {} — skipping", exc)

        # WIN bets: rule-based scorer (best ROI on win bets in backtest)
        bets_win = generate_bets(conn, date, bet_types=["win"])

        # DUO bets: LightGBM scorer (best ROI on duo bets in backtest)
        bets_duo = []
        if lgbm_model is not None:
            lgbm_scorer = lambda df, m=lgbm_model: score_lgbm(df, m)
            bets_duo = generate_bets(
                conn, date,
                scorer_fn=lgbm_scorer, model_source="lgbm",
                bet_types=["duo"],
            )

        logger.info(
            "=== {} win (règles) + {} duo (lgbm) bets logged for {} ===",
            len(bets_win), len(bets_duo), date,
        )
        report_path = export_bets_html(conn, date)
        logger.info("Bet sheet saved → {}", report_path)
        _git_push(report_path)
    finally:
        conn.close()


def run_hourly_update(date: str | None = None) -> None:
    """Re-scrape odds, refresh bet recommendations, update the HTML sheet, and push to GitHub.

    Called every hour between 10:00 and 22:00 so that odds drifts and any
    late-programme races are picked up progressively during the day.
    The operation is fully idempotent — resolved bets are never overwritten.

    Args:
        date: YYYYMMDD string. Defaults to today.
    """
    if date is None:
        date = datetime.today().strftime("%Y%m%d")

    logger.info("=== Hourly update starting for {} ===", date)

    pipeline_result = run_pipeline(date)
    logger.info(
        "Scraper: {} races, {} runners, {} errors",
        pipeline_result.races_fetched,
        pipeline_result.runners_fetched,
        len(pipeline_result.errors),
    )

    conn = get_connection()
    try:
        now = datetime.now(tz=timezone.utc)
        lgbm_model = load_lgbm_model()
        lgbm_scorer = (lambda df, m=lgbm_model: score_lgbm(df, m)) if lgbm_model else None

        bets_win = generate_bets(conn, date, bet_types=["win"], min_race_time=now)
        bets_duo = []
        if lgbm_scorer:
            bets_duo = generate_bets(
                conn, date,
                scorer_fn=lgbm_scorer, model_source="lgbm",
                bet_types=["duo"], min_race_time=now,
            )

        logger.info(
            "{} win (règles) + {} duo (lgbm) bets refreshed for {}",
            len(bets_win), len(bets_duo), date,
        )
        report_path = export_bets_html(conn, date)
        logger.info("Bet sheet updated → {}", report_path)
        _git_push(report_path)
    finally:
        conn.close()


def run_evening_session(date: str | None = None) -> None:
    """Scrape results, resolve pending bets, update the HTML sheet, and push to GitHub.

    Args:
        date: YYYYMMDD string. Defaults to today.
    """
    if date is None:
        date = datetime.today().strftime("%Y%m%d")

    logger.info("=== Evening session starting for {} ===", date)

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
        report_path = export_bets_html(conn, date)
        logger.info("Bet sheet updated → {}", report_path)
        _git_push(report_path)
    finally:
        conn.close()


def start_scheduler() -> None:
    """Start the APScheduler with daily jobs.

    Schedule (all times local):
      08:30        — morning scrape (programme + early odds) → GitHub push
      10:00–22:00  — hourly odds refresh + bet regen         → GitHub push
      22:30        — evening scrape (results + P&L)          → GitHub push
    """
    from apscheduler.schedulers.blocking import BlockingScheduler

    scheduler = BlockingScheduler()

    # Morning: single run at 08:30 (odds not yet stable before that)
    scheduler.add_job(run_morning_session, "cron", hour=8, minute=30)

    # Hourly refresh from 10:00 to 22:00 inclusive (13 runs)
    scheduler.add_job(run_hourly_update, "cron", hour="10-22", minute=0)

    # Evening resolution at 22:30 (results published ~22:00)
    scheduler.add_job(run_evening_session, "cron", hour=22, minute=30)

    logger.info(
        "Scheduler starting — 08:30 morning / 10:00–22:00 hourly / 22:30 evening"
    )
    scheduler.start()
