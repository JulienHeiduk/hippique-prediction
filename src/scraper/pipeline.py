"""Orchestrator: client → parser → saver → storage."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date as date_type
from datetime import datetime, timedelta, timezone
from pathlib import Path

from loguru import logger

from config.settings import DB_PATH
from src.scraper.client import PipelineError, PMUClient
from src.scraper.parser import (
    parse_odds,
    parse_reunions,
    parse_runners,
)
from src.scraper.saver import save_raw
from src.scraper.storage import (
    close_connection,
    get_connection,
    upsert_odds,
    upsert_race,
    upsert_runners,
)


@dataclass
class PipelineResult:
    date: str
    races_fetched: int = 0
    runners_fetched: int = 0
    errors: list[str] = field(default_factory=list)


def run(
    date: str | None = None,
    db_path: Path = DB_PATH,
    min_race_time: datetime | None = None,
) -> PipelineResult:
    """Run the full scraping pipeline for *date* (YYYYMMDD). Defaults to today."""
    if date is None:
        date = date_type.today().strftime("%Y%m%d")

    result = PipelineResult(date=date)
    conn = get_connection(db_path)
    snapshot_time = datetime.now(tz=timezone.utc)

    try:
        with PMUClient() as client:
            # ---- Step 1: fetch programme (contains race metadata + course list) ---
            try:
                raw_programme = client.fetch_reunions(date)
            except PipelineError as exc:
                msg = f"Failed to fetch programme for {date}: {exc}"
                logger.error(msg)
                result.errors.append(msg)
                return result

            save_raw(raw_programme, date, "reunions.json")
            trot_races = parse_reunions(raw_programme, date)

            if not trot_races:
                logger.info("No trot races found for {}.", date)
                return result

            # ---- Step 2: per-race participants fetch ----------------------------
            for race in trot_races:
                # Skip races that have already started when a cutoff is provided
                if min_race_time is not None and race.race_datetime is not None:
                    cutoff = min_race_time.replace(tzinfo=None)
                    race_dt = race.race_datetime.replace(tzinfo=None) if race.race_datetime.tzinfo else race.race_datetime
                    if race_dt <= cutoff:
                        logger.debug("Hourly update: skipping past race {} ({})", race.race_id, race_dt)
                        continue

                r_num = race.reunion_number
                c_num = race.course_number
                race_filename = f"R{r_num}_C{c_num}.json"

                try:
                    raw_participants = client.fetch_race(date, r_num, c_num)
                except PipelineError as exc:
                    msg = f"Failed to fetch participants R{r_num}/C{c_num} for {date}: {exc}"
                    logger.warning(msg)
                    result.errors.append(msg)
                    continue

                raw_path = save_raw(raw_participants, date, race_filename)
                race.raw_file_path = str(raw_path)

                runners = parse_runners(raw_participants, race.race_id)
                odds_list = parse_odds(raw_participants, race.race_id, snapshot_time)

                upsert_race(conn, race.__dict__)
                upsert_runners(conn, [r.__dict__ for r in runners])
                upsert_odds(conn, [o.__dict__ for o in odds_list])

                result.races_fetched += 1
                result.runners_fetched += len(runners)
    finally:
        close_connection()
    logger.info(
        "Pipeline complete for {}: {} races, {} runners, {} errors",
        date,
        result.races_fetched,
        result.runners_fetched,
        len(result.errors),
    )
    return result


@dataclass
class BackfillResult:
    dates_attempted: int = 0
    dates_ok: int = 0
    total_races: int = 0
    total_runners: int = 0
    failed_dates: list[str] = field(default_factory=list)


def backfill(
    days: int = 60,
    end_date: str | None = None,
    db_path: Path = DB_PATH,
) -> BackfillResult:
    """Fetch *days* calendar days of past race data ending on *end_date* (YYYYMMDD, default: yesterday).

    All writes are idempotent — safe to re-run. Past-date responses include
    finish_position and km_time so results are stored alongside pre-race features.
    """
    if end_date is None:
        end_dt = date_type.today() - timedelta(days=1)
    else:
        end_dt = date_type(int(end_date[:4]), int(end_date[4:6]), int(end_date[6:8]))

    result = BackfillResult()

    for offset in range(days - 1, -1, -1):   # oldest → newest
        target = end_dt - timedelta(days=offset)
        date_str = target.strftime("%Y%m%d")
        result.dates_attempted += 1

        day_result = run(date=date_str, db_path=db_path)

        if day_result.errors and day_result.races_fetched == 0:
            logger.warning("Backfill: no data for {} — {}", date_str, day_result.errors[0])
            result.failed_dates.append(date_str)
        else:
            result.dates_ok += 1
            result.total_races += day_result.races_fetched
            result.total_runners += day_result.runners_fetched

        logger.info(
            "Backfill [{}/{}] {} — {} races, {} runners",
            result.dates_attempted, days, date_str,
            day_result.races_fetched, day_result.runners_fetched,
        )

    logger.info(
        "Backfill complete: {}/{} dates OK, {} races, {} runners, {} failed",
        result.dates_ok, result.dates_attempted,
        result.total_races, result.total_runners,
        len(result.failed_dates),
    )
    return result
