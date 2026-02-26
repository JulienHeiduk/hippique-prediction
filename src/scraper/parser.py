"""Pure parsing functions: raw API dicts → typed dataclasses."""
from __future__ import annotations

import hashlib
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from loguru import logger


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class RaceData:
    race_id: str
    date: str
    hippodrome: str | None
    race_datetime: datetime | None
    distance_metres: int | None
    track_condition: str | None
    discipline: str | None
    is_trot: bool
    field_size: int | None
    reunion_number: int
    course_number: int
    raw_file_path: str | None = None


@dataclass
class RunnerData:
    runner_id: str
    race_id: str
    horse_number: int | None
    horse_name: str | None
    jockey_name: str | None
    trainer_name: str | None
    draw_position: int | None
    weight_kg: float | None
    handicap_distance: int | None
    deferre: bool
    scratch: bool


@dataclass
class OddsData:
    odds_id: str
    runner_id: str
    race_id: str
    snapshot_time: datetime | None
    odds_type: str
    decimal_odds: float | None
    implied_prob: float | None


@dataclass
class HistoryRow:
    history_id: str
    horse_name: str
    fetched_for_race: str
    past_race_date: str | None
    track: str | None
    finish_position: int | None
    field_size: int | None
    gap_to_winner: float | None
    disqualified: bool


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_trot(discipline: str | None) -> bool:
    if not discipline:
        return False
    return discipline.upper().replace(" ", "").startswith("TROT")


def safe_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        result = float(val)
        return result
    except (ValueError, TypeError):
        return None


def safe_int(val: Any) -> int | None:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def sanitize_horse_name(name: str) -> str:
    """Strip accents and replace spaces with underscores for filenames/URLs."""
    normalized = unicodedata.normalize("NFD", name)
    ascii_str = "".join(c for c in normalized if unicodedata.category(c) != "Mn")
    return ascii_str.replace(" ", "_").upper()


def _parse_datetime(ts_ms: Any) -> datetime | None:
    if ts_ms is None:
        return None
    try:
        return datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc)
    except (ValueError, TypeError, OSError):
        return None


def _make_history_id(horse_name: str, past_race_date: str | None, track: str | None, position: Any) -> str:
    raw = f"{horse_name}-{past_race_date}-{track}-{position}"
    suffix = hashlib.md5(raw.encode()).hexdigest()[:8]
    return f"{sanitize_horse_name(horse_name)}-{past_race_date or 'unknown'}-{suffix}"


# ---------------------------------------------------------------------------
# Parse functions
# ---------------------------------------------------------------------------

def parse_reunions(raw: dict, date: str) -> list[RaceData]:
    """Extract trot races from the reunions response."""
    races: list[RaceData] = []
    reunions = raw.get("reunions") or raw.get("programme", {}).get("reunions") or []
    if not reunions:
        logger.info("Empty program for {}", date)
        return races

    for reunion in reunions:
        reunion_num = safe_int(reunion.get("numOfficiel") or reunion.get("numero"))
        courses = reunion.get("courses") or []
        for course in courses:
            discipline = course.get("discipline") or course.get("specialite") or ""
            if not _is_trot(discipline):
                continue
            course_num = safe_int(course.get("numOrdre") or course.get("numero"))
            race_id = f"{date}-R{reunion_num}-C{course_num}"
            race = RaceData(
                race_id=race_id,
                date=date,
                hippodrome=reunion.get("hippodrome", {}).get("libelleCourt") or reunion.get("hippodrome", {}).get("libelle"),
                race_datetime=_parse_datetime(course.get("heureDepart")),
                distance_metres=safe_int(course.get("distance") or course.get("distanceMetres")),
                track_condition=course.get("etatPiste") or course.get("conditionPiste"),
                discipline=discipline,
                is_trot=True,
                field_size=safe_int(course.get("nombreDeclaresPartants") or course.get("nombrePartants")),
                reunion_number=reunion_num,
                course_number=course_num,
            )
            races.append(race)

    logger.info("Found {} trot race(s) for {}", len(races), date)
    return races


def parse_race(raw: dict, date: str, reunion: int, course: int, raw_file_path: str | None = None) -> RaceData:
    """Parse a single race detail response."""
    discipline = raw.get("discipline") or raw.get("specialite") or ""
    race_id = f"{date}-R{reunion}-C{course}"
    hippodrome_data = raw.get("hippodrome") or {}
    return RaceData(
        race_id=race_id,
        date=date,
        hippodrome=hippodrome_data.get("libelleCourt") or hippodrome_data.get("libelle") or raw.get("hippodrome"),
        race_datetime=_parse_datetime(raw.get("heureDepart")),
        distance_metres=safe_int(raw.get("distance") or raw.get("distanceMetres")),
        track_condition=raw.get("etatPiste") or raw.get("conditionPiste"),
        discipline=discipline,
        is_trot=_is_trot(discipline),
        field_size=safe_int(raw.get("nombreDeclaresPartants") or raw.get("nombrePartants")),
        reunion_number=reunion,
        course_number=course,
        raw_file_path=raw_file_path,
    )


def parse_runners(raw: dict, race_id: str) -> list[RunnerData]:
    """Parse participants from a race detail response."""
    participants = raw.get("participants") or raw.get("partants") or []
    runners: list[RunnerData] = []

    for p in participants:
        cheval = p.get("cheval")
        if isinstance(cheval, dict):
            horse_name = p.get("nom") or cheval.get("nom")
        else:
            horse_name = p.get("nom") or p.get("nomCheval")
        if not horse_name:
            logger.warning("Skipping runner with no horse name in race {}", race_id)
            continue

        horse_num = safe_int(p.get("numPmu") or p.get("numero"))
        runner_id = f"{race_id}-{horse_num}"

        scratch_val = p.get("statut") or p.get("scratch") or ""
        scratch = str(scratch_val).upper() in ("NON_PARTANT", "SCRATCHED", "TRUE", "1")

        deferre_val = p.get("deferre") or ""
        deferre = str(deferre_val).upper() in ("TRUE", "1", "OUI", "DEFERRE_ANTERIEURS_POSTERIEURS", "DEFERRE_ANTERIEURS", "DEFERRE_POSTERIEURS")

        runners.append(RunnerData(
            runner_id=runner_id,
            race_id=race_id,
            horse_number=horse_num,
            horse_name=horse_name,
            jockey_name=p.get("driver") or p.get("jockey") or p.get("nomJockey"),
            trainer_name=p.get("entraineur") or p.get("nomEntraineur"),
            draw_position=safe_int(p.get("placeCorde") or p.get("draw")),
            weight_kg=safe_float(p.get("poidsKg") or p.get("poids")),
            handicap_distance=safe_int(p.get("distanceHandicap") or p.get("handicap")),
            deferre=deferre,
            scratch=scratch,
        ))

    return runners


def parse_odds(raw: dict, race_id: str, snapshot_time: datetime | None = None) -> list[OddsData]:
    """Parse odds from a race detail response."""
    if snapshot_time is None:
        snapshot_time = datetime.now(tz=timezone.utc)

    participants = raw.get("participants") or raw.get("partants") or []
    odds_list: list[OddsData] = []

    for p in participants:
        horse_num = safe_int(p.get("numPmu") or p.get("numero"))
        runner_id = f"{race_id}-{horse_num}"

        # Try morning odds
        morning_raw = p.get("coteMatin") or p.get("coteOuverture")
        morning_val = safe_float(morning_raw)
        if morning_val is not None:
            snap_str = snapshot_time.isoformat() if snapshot_time else "morning"
            odds_id = f"{runner_id}-morning"
            odds_list.append(OddsData(
                odds_id=odds_id,
                runner_id=runner_id,
                race_id=race_id,
                snapshot_time=snapshot_time,
                odds_type="morning",
                decimal_odds=morning_val,
                implied_prob=round(1.0 / morning_val, 6) if morning_val > 0 else None,
            ))

        # Try final/live odds
        final_raw = p.get("coteDirect") or p.get("coteActuelle") or p.get("rapportDirect")
        final_val = safe_float(final_raw)
        if final_val is not None:
            odds_id = f"{runner_id}-final"
            odds_list.append(OddsData(
                odds_id=odds_id,
                runner_id=runner_id,
                race_id=race_id,
                snapshot_time=snapshot_time,
                odds_type="final",
                decimal_odds=final_val,
                implied_prob=round(1.0 / final_val, 6) if final_val > 0 else None,
            ))

    return odds_list


def parse_horse_history(raw: dict, horse_name: str, fetched_for_race: str) -> list[HistoryRow]:
    """Parse historical race performances for a horse."""
    performances = (
        raw.get("performances")
        or raw.get("historiquePerformances")
        or raw.get("coursesCourues")
        or []
    )
    rows: list[HistoryRow] = []

    for perf in performances:
        past_date = perf.get("date") or perf.get("dateReunion") or perf.get("dateCourse")
        if isinstance(past_date, int):
            dt = _parse_datetime(past_date)
            past_date = dt.strftime("%Y%m%d") if dt else None

        track = perf.get("hippodrome") or perf.get("libelleHippodrome") or perf.get("lieu")
        position = safe_int(perf.get("ordreArrivee") or perf.get("position") or perf.get("place"))
        history_id = _make_history_id(horse_name, past_date, track, position)

        disq_val = perf.get("disqualifie") or perf.get("disqualified") or False
        disqualified = str(disq_val).upper() in ("TRUE", "1", "OUI")

        rows.append(HistoryRow(
            history_id=history_id,
            horse_name=horse_name,
            fetched_for_race=fetched_for_race,
            past_race_date=past_date,
            track=track,
            finish_position=position,
            field_size=safe_int(perf.get("nombrePartants") or perf.get("fieldSize")),
            gap_to_winner=safe_float(perf.get("ecartVainqueur") or perf.get("gap")),
            disqualified=disqualified,
        ))

    return rows
