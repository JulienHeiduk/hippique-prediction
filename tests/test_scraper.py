"""Tests for the scraper package."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pytest

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def load_fixture(name: str) -> dict:
    with open(FIXTURES_DIR / name, encoding="utf-8") as fh:
        return json.load(fh)


@pytest.fixture()
def reunions_raw():
    return load_fixture("reunions_response.json")


@pytest.fixture()
def race_raw():
    return load_fixture("race_detail_response.json")


@pytest.fixture()
def race_no_odds_raw():
    return load_fixture("race_detail_no_odds.json")


@pytest.fixture()
def mem_conn():
    """In-memory DuckDB connection with schema initialised."""
    from src.scraper.storage import init_schema
    conn = duckdb.connect(":memory:")
    init_schema(conn)
    return conn


# ---------------------------------------------------------------------------
# Parser tests
# ---------------------------------------------------------------------------

class TestParseReunions:
    def test_filters_trot_only(self, reunions_raw):
        from src.scraper.parser import parse_reunions
        races = parse_reunions(reunions_raw, "20240226")
        assert len(races) == 2
        assert all(r.is_trot for r in races)
        assert all(r.hippodrome == "VINCENNES" for r in races)

    def test_empty_program(self):
        from src.scraper.parser import parse_reunions
        races = parse_reunions({}, "20240226")
        assert races == []

    def test_race_ids_formatted(self, reunions_raw):
        from src.scraper.parser import parse_reunions
        races = parse_reunions(reunions_raw, "20240226")
        assert races[0].race_id == "20240226-R1-C1"
        assert races[1].race_id == "20240226-R1-C2"

    def test_discipline_stored_as_specialite(self, reunions_raw):
        from src.scraper.parser import parse_reunions
        races = parse_reunions(reunions_raw, "20240226")
        assert races[0].discipline == "TROT_ATTELE"
        assert races[1].discipline == "TROT_MONTE"

    def test_filters_terminal_only_courses(self, reunions_raw):
        """Courses with hasEParis=False (terminal-only) must be excluded."""
        from src.scraper.parser import parse_reunions
        races = parse_reunions(reunions_raw, "20240226")
        # Fixture has 3 trot courses but C3 has hasEParis=False → only 2 returned
        assert len(races) == 2
        race_ids = [r.race_id for r in races]
        assert "20240226-R1-C3" not in race_ids


class TestParseRunners:
    def test_scratch_flag(self, race_raw):
        from src.scraper.parser import parse_runners
        runners = parse_runners(race_raw, "20240226-R1-C1")
        scratched = [r for r in runners if r.scratch]
        assert len(scratched) == 1
        assert scratched[0].horse_name == "DERBY DES BOIS"

    def test_deferre_flag(self, race_raw):
        from src.scraper.parser import parse_runners
        runners = parse_runners(race_raw, "20240226-R1-C1")
        deferred = [r for r in runners if r.deferre]
        assert len(deferred) == 1
        assert deferred[0].horse_name == "BELLO STAR"

    def test_skips_runner_without_name(self):
        from src.scraper.parser import parse_runners
        raw = {"participants": [{"numPmu": 1}]}
        runners = parse_runners(raw, "20240226-R1-C1")
        assert runners == []

    def test_runner_count(self, race_raw):
        from src.scraper.parser import parse_runners
        runners = parse_runners(race_raw, "20240226-R1-C1")
        assert len(runners) == 3

    def test_musique_stored(self, race_raw):
        from src.scraper.parser import parse_runners
        runners = parse_runners(race_raw, "20240226-R1-C1")
        assert runners[0].musique == "1a2a3a1a(5)a"


class TestParseOdds:
    def test_computes_implied_prob(self, race_raw):
        from src.scraper.parser import parse_odds
        odds = parse_odds(race_raw, "20240226-R1-C1")
        morning = next(o for o in odds if o.odds_type == "morning" and o.runner_id == "20240226-R1-C1-1")
        assert morning.decimal_odds == 4.5
        assert abs(morning.implied_prob - 1 / 4.5) < 1e-6

    def test_missing_odds_field_returns_empty(self, race_no_odds_raw):
        from src.scraper.parser import parse_odds
        odds = parse_odds(race_no_odds_raw, "20240226-R1-C1")
        assert odds == []

    def test_null_direct_rapport_not_included(self, race_raw):
        from src.scraper.parser import parse_odds
        odds = parse_odds(race_raw, "20240226-R1-C1")
        # Runner 3 has null dernierRapportDirect — no "final" entry
        final_runner3 = [o for o in odds if o.odds_type == "final" and o.runner_id == "20240226-R1-C1-3"]
        assert final_runner3 == []


class TestHelpers:
    def test_safe_float_returns_none_on_invalid(self):
        from src.scraper.parser import safe_float
        assert safe_float("N/A") is None
        assert safe_float(None) is None
        assert safe_float("") is None

    def test_safe_float_parses_string(self):
        from src.scraper.parser import safe_float
        assert safe_float("4.50") == 4.5

    def test_is_trot_handles_variants(self):
        from src.scraper.parser import _is_trot
        assert _is_trot("TROT_ATTELE") is True
        assert _is_trot("TROT_MONTE") is True
        assert _is_trot("PLAT") is False
        assert _is_trot("OBSTACLE") is False
        assert _is_trot(None) is False

    def test_sanitize_horse_name(self):
        from src.scraper.parser import sanitize_horse_name
        assert sanitize_horse_name("Étoile d'Or") == "ETOILE_D'OR"

    def test_date_conversion(self):
        from src.scraper.client import _to_api_date
        assert _to_api_date("20240226") == "26022024"
        assert _to_api_date("20260101") == "01012026"


# ---------------------------------------------------------------------------
# Client tests
# ---------------------------------------------------------------------------

class TestPMUClient:
    def test_fetch_reunions_retries_on_500(self):
        from src.scraper.client import PMUClient

        call_count = 0

        def mock_get(url, **_):
            nonlocal call_count
            call_count += 1
            resp = MagicMock()
            if call_count < 3:
                resp.status_code = 500
                resp.raise_for_status.side_effect = Exception("500")
            else:
                resp.status_code = 200
                resp.raise_for_status.return_value = None
                resp.json.return_value = {"programme": {"reunions": []}}
            return resp

        with patch("httpx.Client.get", side_effect=mock_get):
            with patch("time.sleep"):
                client = PMUClient(max_retries=3, backoff_base=0.01)
                result = client.fetch_reunions("20240226")
                assert result == {"programme": {"reunions": []}}
                assert call_count == 3

    def test_fetch_reunions_raises_after_max_retries(self):
        from src.scraper.client import PipelineError, PMUClient

        def mock_get(url, **_):
            resp = MagicMock()
            resp.status_code = 503
            resp.raise_for_status.side_effect = Exception("503")
            return resp

        with patch("httpx.Client.get", side_effect=mock_get):
            with patch("time.sleep"):
                client = PMUClient(max_retries=2, backoff_base=0.01)
                with pytest.raises(PipelineError):
                    client.fetch_reunions("20240226")

    def test_fetch_reunions_uses_ddmmyyyy_format(self):
        """URL sent to the API must use DDMMYYYY, not YYYYMMDD."""
        from src.scraper.client import PMUClient

        called_urls = []

        def mock_get(url, **_):
            called_urls.append(url)
            resp = MagicMock()
            resp.status_code = 200
            resp.raise_for_status.return_value = None
            resp.json.return_value = {"programme": {"reunions": []}}
            return resp

        with patch("httpx.Client.get", side_effect=mock_get):
            with patch("time.sleep"):
                client = PMUClient()
                client.fetch_reunions("20240226")

        assert called_urls[0].endswith("26022024")


# ---------------------------------------------------------------------------
# Storage tests
# ---------------------------------------------------------------------------

class TestStorage:
    def test_init_schema_idempotent(self, mem_conn):
        from src.scraper.storage import init_schema
        init_schema(mem_conn)
        tables = mem_conn.execute("SHOW TABLES").fetchall()
        table_names = {t[0] for t in tables}
        assert {"races", "runners", "odds", "horse_history"}.issubset(table_names)

    def test_upsert_race_overwrites_on_same_id(self, mem_conn):
        from src.scraper.storage import upsert_race
        race = {
            "race_id": "20240226-R1-C1",
            "date": "20240226",
            "hippodrome": "VINCENNES",
            "race_datetime": None,
            "distance_metres": 2700,
            "track_condition": "BON",
            "discipline": "TROT_ATTELE",
            "is_trot": True,
            "field_size": 12,
            "reunion_number": 1,
            "course_number": 1,
            "raw_file_path": None,
        }
        upsert_race(mem_conn, race)
        race["field_size"] = 14
        upsert_race(mem_conn, race)
        row = mem_conn.execute(
            "SELECT field_size FROM races WHERE race_id = ?",
            ["20240226-R1-C1"]
        ).fetchone()
        assert row[0] == 14

    def test_upsert_runners_batch_count(self, mem_conn):
        from src.scraper.storage import upsert_runners
        runners = [
            {
                "runner_id": f"20240226-R1-C1-{i}",
                "race_id": "20240226-R1-C1",
                "horse_number": i,
                "horse_name": f"HORSE_{i}",
                "jockey_name": "JOCKEY",
                "trainer_name": "TRAINER",
                "draw_position": i,
                "weight_kg": None,
                "handicap_distance": 0,
                "deferre": False,
                "scratch": False,
                "musique": "1a2a3a",
            }
            for i in range(1, 6)
        ]
        upsert_runners(mem_conn, runners)
        count = mem_conn.execute("SELECT COUNT(*) FROM runners").fetchone()[0]
        assert count == 5

    def test_musique_persisted(self, mem_conn):
        from src.scraper.storage import upsert_runners
        runners = [{
            "runner_id": "20240226-R1-C1-1",
            "race_id": "20240226-R1-C1",
            "horse_number": 1,
            "horse_name": "BELLO STAR",
            "jockey_name": None,
            "trainer_name": None,
            "draw_position": None,
            "weight_kg": None,
            "handicap_distance": 0,
            "deferre": False,
            "scratch": False,
            "musique": "1a2a3a1a",
        }]
        upsert_runners(mem_conn, runners)
        row = mem_conn.execute("SELECT musique FROM runners WHERE runner_id = ?", ["20240226-R1-C1-1"]).fetchone()
        assert row[0] == "1a2a3a1a"


# ---------------------------------------------------------------------------
# Pipeline integration tests
# ---------------------------------------------------------------------------

class TestPipeline:
    def _make_reunions_response(self):
        return {
            "programme": {
                "reunions": [
                    {
                        "numOfficiel": 1,
                        "hippodrome": {"libelleCourt": "VINCENNES"},
                        "courses": [
                            {"numOrdre": 1, "specialite": "TROT_ATTELE", "nombreDeclaresPartants": 2},
                            {"numOrdre": 2, "specialite": "PLAT", "nombreDeclaresPartants": 10},
                        ],
                    }
                ]
            }
        }

    def _make_participants_response(self):
        return {
            "participants": [
                {"numPmu": 1, "nom": "HORSE_A", "driver": "JOCKEY_A", "statut": "PARTANT", "musique": "1a2a"},
                {"numPmu": 2, "nom": "HORSE_B", "driver": "JOCKEY_B", "statut": "PARTANT", "musique": "3a1a"},
            ]
        }

    def test_skips_non_trot_races(self, tmp_path):
        from src.scraper.pipeline import run

        with (
            patch("src.scraper.pipeline.PMUClient") as MockClient,
            patch("src.scraper.pipeline.save_raw"),
            patch("src.scraper.pipeline.get_connection") as mock_conn,
        ):
            instance = MockClient.return_value.__enter__.return_value
            instance.fetch_reunions.return_value = self._make_reunions_response()
            instance.fetch_race.return_value = self._make_participants_response()

            mock_conn.return_value = duckdb.connect(":memory:")
            from src.scraper.storage import init_schema
            init_schema(mock_conn.return_value)

            result = run(date="20240226", db_path=tmp_path / "test.duckdb")

        # Only 1 trot race (PLAT is skipped)
        assert result.races_fetched == 1

    def test_continues_on_single_race_error(self, tmp_path):
        from src.scraper.client import PipelineError
        from src.scraper.pipeline import run

        reunion_data = {
            "programme": {
                "reunions": [
                    {
                        "numOfficiel": 1,
                        "hippodrome": {"libelleCourt": "VINCENNES"},
                        "courses": [
                            {"numOrdre": 1, "specialite": "TROT_ATTELE"},
                            {"numOrdre": 2, "specialite": "TROT_ATTELE"},
                        ],
                    }
                ]
            }
        }

        call_count = {"n": 0}

        def fetch_race_side_effect(date, reunion, course):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise PipelineError("Simulated failure")
            return self._make_participants_response()

        with (
            patch("src.scraper.pipeline.PMUClient") as MockClient,
            patch("src.scraper.pipeline.save_raw"),
            patch("src.scraper.pipeline.get_connection") as mock_conn,
        ):
            instance = MockClient.return_value.__enter__.return_value
            instance.fetch_reunions.return_value = reunion_data
            instance.fetch_race.side_effect = fetch_race_side_effect

            mock_conn.return_value = duckdb.connect(":memory:")
            from src.scraper.storage import init_schema
            init_schema(mock_conn.return_value)

            result = run(date="20240226", db_path=tmp_path / "test.duckdb")

        assert result.races_fetched == 1
        assert len(result.errors) == 1
