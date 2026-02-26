"""Orchestrator: client → parser → saver → storage."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date as date_type
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger

from config.settings import DB_PATH
from src.scraper.client import PipelineError, PMUClient
from src.scraper.parser import (
    parse_horse_history,
    parse_odds,
    parse_race,
    parse_reunions,
    parse_runners,
    sanitize_horse_name,
)
from src.scraper.saver import save_raw
from src.scraper.storage import (
    get_connection,
    upsert_horse_history,
    upsert_odds,
    upsert_race,
    upsert_runners,
)


@dataclass
class PipelineResult:
    date: str
    races_fetched: int = 0
    runners_fetched: int = 0
    horses_fetched: int = 0
    errors: list[str] = field(default_factory=list)


def run(
    date: str | None = None,
    fetch_horse_history: bool = True,
    db_path: Path = DB_PATH,
) -> PipelineResult:
    """Run the full scraping pipeline for *date* (YYYYMMDD). Defaults to today."""
    if date is None:
        date = date_type.today().strftime("%Y%m%d")

    result = PipelineResult(date=date)
    conn = get_connection(db_path)
    snapshot_time = datetime.now(tz=timezone.utc)
    fetched_horses: set[str] = set()

    with PMUClient() as client:
        # ---- Step 1: fetch program ----------------------------------------
        try:
            raw_reunions = client.fetch_reunions(date)
        except PipelineError as exc:
            msg = f"Failed to fetch reunions for {date}: {exc}"
            logger.error(msg)
            result.errors.append(msg)
            return result

        save_raw(raw_reunions, date, "reunions.json")
        trot_races = parse_reunions(raw_reunions, date)

        if not trot_races:
            logger.info("No trot races found for {}.", date)
            return result

        # ---- Step 2: per-race fetching -------------------------------------
        for race_meta in trot_races:
            r_num = race_meta.reunion_number
            c_num = race_meta.course_number
            race_filename = f"R{r_num}_C{c_num}.json"

            try:
                raw_race = client.fetch_race(date, r_num, c_num)
            except PipelineError as exc:
                msg = f"Failed to fetch race R{r_num}/C{c_num} for {date}: {exc}"
                logger.warning(msg)
                result.errors.append(msg)
                continue

            raw_path = save_raw(raw_race, date, race_filename)
            race = parse_race(raw_race, date, r_num, c_num, raw_file_path=str(raw_path))
            runners = parse_runners(raw_race, race.race_id)
            odds_list = parse_odds(raw_race, race.race_id, snapshot_time)

            # Persist
            upsert_race(conn, race.__dict__)
            upsert_runners(conn, [r.__dict__ for r in runners])
            upsert_odds(conn, [o.__dict__ for o in odds_list])

            result.races_fetched += 1
            result.runners_fetched += len(runners)

            # ---- Step 3: horse history -------------------------------------
            if fetch_horse_history:
                for runner in runners:
                    if not runner.horse_name:
                        continue
                    horse_key = sanitize_horse_name(runner.horse_name)
                    if horse_key in fetched_horses:
                        continue
                    fetched_horses.add(horse_key)

                    horse_filename = f"horse_{horse_key}.json"
                    try:
                        raw_horse = client.fetch_horse(runner.horse_name)
                    except Exception as exc:
                        msg = f"Failed to fetch horse {runner.horse_name}: {exc}"
                        logger.warning(msg)
                        result.errors.append(msg)
                        continue

                    if raw_horse is None:
                        logger.debug("No history for horse {}", runner.horse_name)
                        continue

                    save_raw(raw_horse, date, horse_filename)
                    history_rows = parse_horse_history(
                        raw_horse, runner.horse_name, race.race_id
                    )
                    upsert_horse_history(conn, [h.__dict__ for h in history_rows])
                    result.horses_fetched += 1

    conn.close()
    logger.info(
        "Pipeline complete for {}: {} races, {} runners, {} horses, {} errors",
        date,
        result.races_fetched,
        result.runners_fetched,
        result.horses_fetched,
        len(result.errors),
    )
    return result
