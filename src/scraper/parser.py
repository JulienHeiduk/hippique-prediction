"""Pure parsing functions: raw API dicts → typed dataclasses."""
from __future__ import annotations

import unicodedata
from dataclasses import dataclass
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
    musique: str | None
    finish_position: int | None   # None pre-race; ordreArrivee post-race
    km_time: str | None           # reductionKilometrique e.g. "1'12''3" (trot only)


@dataclass
class OddsData:
    odds_id: str
    runner_id: str
    race_id: str
    snapshot_time: datetime | None
    odds_type: str
    decimal_odds: float | None
    implied_prob: float | None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_trot(specialite: str | None) -> bool:
    """TROT_ATTELE and TROT_MONTE are trot; PLAT, OBSTACLE, etc. are not."""
    if not specialite:
        return False
    return specialite.upper().startswith("TROT")


def safe_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
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


# ---------------------------------------------------------------------------
# Parse functions
# ---------------------------------------------------------------------------

def parse_reunions(raw: dict, date: str) -> list[RaceData]:
    """Extract trot races from the /programme/{date} response.

    Response structure:
        {"programme": {"reunions": [{"numOfficiel": 1, "hippodrome": {...},
            "courses": [{"numOrdre": 1, "specialite": "TROT_ATTELE", ...}]}]}}
    """
    programme = raw.get("programme") or {}
    reunions = programme.get("reunions") or []
    if not reunions:
        logger.info("Empty programme for {}", date)
        return []

    races: list[RaceData] = []
    for reunion in reunions:
        reunion_num = safe_int(reunion.get("numOfficiel"))
        hippo = reunion.get("hippodrome") or {}
        hippodrome = hippo.get("libelleCourt") or hippo.get("libelle")

        for course in reunion.get("courses") or []:
            specialite = course.get("specialite") or ""
            if not _is_trot(specialite):
                continue

            # Skip courses not available for online betting (terminal-only races
            # have hasEParis=False and no E_* paris types — they don't appear on
            # pmu.fr/turf where the user places bets).
            if course.get("hasEParis") is False:
                course_num = safe_int(course.get("numOrdre"))
                logger.info(
                    "Skipping R{}C{} (hasEParis=False — terminal only, not on pmu.fr/turf)",
                    reunion_num, course_num,
                )
                continue

            course_num = safe_int(course.get("numOrdre"))
            race_id = f"{date}-R{reunion_num}-C{course_num}"
            races.append(RaceData(
                race_id=race_id,
                date=date,
                hippodrome=hippodrome,
                race_datetime=_parse_datetime(course.get("heureDepart")),
                distance_metres=safe_int(course.get("distance")),
                track_condition=course.get("etatPiste") or course.get("conditionPiste"),
                discipline=specialite,
                is_trot=True,
                field_size=safe_int(course.get("nombreDeclaresPartants")),
                reunion_number=reunion_num,
                course_number=course_num,
            ))

    logger.info("Found {} trot race(s) for {}", len(races), date)
    return races


def parse_race(raw: dict, date: str, reunion: int, course: int, raw_file_path: str | None = None) -> RaceData:
    """Parse a single race from programme course data (used for direct detail calls)."""
    specialite = raw.get("specialite") or raw.get("discipline") or ""
    hippo = raw.get("hippodrome") or {}
    race_id = f"{date}-R{reunion}-C{course}"
    return RaceData(
        race_id=race_id,
        date=date,
        hippodrome=hippo.get("libelleCourt") or hippo.get("libelle") if isinstance(hippo, dict) else raw.get("hippodrome"),
        race_datetime=_parse_datetime(raw.get("heureDepart")),
        distance_metres=safe_int(raw.get("distance")),
        track_condition=raw.get("etatPiste") or raw.get("conditionPiste"),
        discipline=specialite,
        is_trot=_is_trot(specialite),
        field_size=safe_int(raw.get("nombreDeclaresPartants")),
        reunion_number=reunion,
        course_number=course,
        raw_file_path=raw_file_path,
    )


def parse_runners(raw: dict, race_id: str) -> list[RunnerData]:
    """Parse participants from the /participants endpoint response.

    Response structure:
        {"participants": [{"nom": "...", "numPmu": 1, "driver": "...",
            "handicapDistance": 0, "deferre": "...", "statut": "PARTANT",
            "musique": "1a3a2a...", ...}]}
    """
    participants = raw.get("participants") or []
    runners: list[RunnerData] = []

    for p in participants:
        horse_name = p.get("nom")
        if not horse_name:
            logger.warning("Skipping runner with no horse name in race {}", race_id)
            continue

        horse_num = safe_int(p.get("numPmu"))
        runner_id = f"{race_id}-{horse_num}"

        scratch_val = str(p.get("statut") or "").upper()
        scratch = scratch_val in ("NON_PARTANT", "SCRATCHED", "RETIRE", "ABANDONNE")

        deferre_val = str(p.get("deferre") or "").upper()
        deferre = deferre_val not in ("", "SANS_FERS", "INCONNU")

        runners.append(RunnerData(
            runner_id=runner_id,
            race_id=race_id,
            horse_number=horse_num,
            horse_name=horse_name,
            jockey_name=p.get("driver"),
            trainer_name=p.get("entraineur"),
            draw_position=safe_int(p.get("placeCorde")),
            weight_kg=safe_float(p.get("poidsKg") or p.get("poids")),
            handicap_distance=safe_int(p.get("handicapDistance")),
            deferre=deferre,
            scratch=scratch,
            musique=p.get("musique"),
            finish_position=safe_int(p.get("ordreArrivee")),
            km_time=p.get("reductionKilometrique"),
        ))

    return runners


def parse_odds(raw: dict, race_id: str, snapshot_time: datetime | None = None) -> list[OddsData]:
    """Parse odds from the /participants endpoint response.

    Odds are in:
      - dernierRapportReference.rapport  → "morning" / reference odds
      - dernierRapportDirect.rapport     → "final" / live odds
    """
    if snapshot_time is None:
        snapshot_time = datetime.now(tz=timezone.utc)

    participants = raw.get("participants") or []
    odds_list: list[OddsData] = []

    for p in participants:
        horse_num = safe_int(p.get("numPmu"))
        runner_id = f"{race_id}-{horse_num}"

        ref = p.get("dernierRapportReference") or {}
        ref_val = safe_float(ref.get("rapport"))
        if ref_val is not None and ref_val > 0:
            odds_list.append(OddsData(
                odds_id=f"{runner_id}-morning",
                runner_id=runner_id,
                race_id=race_id,
                snapshot_time=snapshot_time,
                odds_type="morning",
                decimal_odds=ref_val,
                implied_prob=round(1.0 / ref_val, 6),
            ))

        direct = p.get("dernierRapportDirect") or {}
        direct_val = safe_float(direct.get("rapport"))
        if direct_val is not None and direct_val > 0:
            odds_list.append(OddsData(
                odds_id=f"{runner_id}-final",
                runner_id=runner_id,
                race_id=race_id,
                snapshot_time=snapshot_time,
                odds_type="final",
                decimal_odds=direct_val,
                implied_prob=round(1.0 / direct_val, 6),
            ))

    return odds_list
