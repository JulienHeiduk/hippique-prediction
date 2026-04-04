"""DuckDB schema initialisation and upsert helpers."""
from __future__ import annotations

from pathlib import Path

import duckdb
from loguru import logger

from config.settings import DB_PATH

# Process-level singleton — one open connection per process at a time.
_singleton: duckdb.DuckDBPyConnection | None = None
_singleton_path: Path | None = None


def get_connection(db_path: Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    """Return the process-level singleton DuckDB connection, creating it if needed."""
    global _singleton, _singleton_path
    db_path.parent.mkdir(parents=True, exist_ok=True)

    if _singleton is not None and _singleton_path == db_path:
        try:
            _singleton.execute("SELECT 1")
            return _singleton
        except Exception:
            # Connection is dead — fall through and reopen
            _singleton = None

    conn = duckdb.connect(str(db_path))
    init_schema(conn)
    _singleton = conn
    _singleton_path = db_path
    return _singleton


def close_connection() -> None:
    """Close the singleton connection and reset it."""
    global _singleton, _singleton_path
    if _singleton is not None:
        try:
            _singleton.close()
        except Exception:
            pass
    _singleton = None
    _singleton_path = None


def init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS races (
            race_id          VARCHAR PRIMARY KEY,
            date             VARCHAR NOT NULL,
            hippodrome       VARCHAR,
            race_datetime    TIMESTAMP,
            distance_metres  INTEGER,
            track_condition  VARCHAR,
            discipline       VARCHAR,
            is_trot          BOOLEAN DEFAULT FALSE,
            field_size       INTEGER,
            reunion_number   INTEGER,
            course_number    INTEGER,
            raw_file_path    VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS runners (
            runner_id          VARCHAR PRIMARY KEY,
            race_id            VARCHAR NOT NULL,
            horse_number       INTEGER,
            horse_name         VARCHAR,
            jockey_name        VARCHAR,
            trainer_name       VARCHAR,
            draw_position      INTEGER,
            weight_kg          DOUBLE,
            handicap_distance  INTEGER,
            deferre            BOOLEAN DEFAULT FALSE,
            scratch            BOOLEAN DEFAULT FALSE,
            musique            VARCHAR,
            finish_position    INTEGER,
            km_time            VARCHAR
        )
    """)
    # Idempotent migrations — no-op if columns already exist
    conn.execute("ALTER TABLE runners ADD COLUMN IF NOT EXISTS finish_position INTEGER")
    conn.execute("ALTER TABLE runners ADD COLUMN IF NOT EXISTS km_time VARCHAR")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS odds (
            odds_id        VARCHAR PRIMARY KEY,
            runner_id      VARCHAR NOT NULL,
            race_id        VARCHAR NOT NULL,
            snapshot_time  TIMESTAMP,
            odds_type      VARCHAR,
            decimal_odds   DOUBLE,
            implied_prob   DOUBLE
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS horse_history (
            history_id       VARCHAR PRIMARY KEY,
            horse_name       VARCHAR NOT NULL,
            fetched_for_race VARCHAR,
            past_race_date   VARCHAR,
            track            VARCHAR,
            finish_position  INTEGER,
            field_size       INTEGER,
            gap_to_winner    DOUBLE,
            disqualified     BOOLEAN DEFAULT FALSE
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bets (
            bet_id        VARCHAR PRIMARY KEY,
            race_id       VARCHAR NOT NULL,
            date          VARCHAR NOT NULL,
            hippodrome    VARCHAR,
            bet_type      VARCHAR NOT NULL,
            runner_id_1   VARCHAR NOT NULL,
            runner_id_2   VARCHAR,
            horse_name_1  VARCHAR,
            horse_name_2  VARCHAR,
            morning_odds  DOUBLE,
            model_prob    DOUBLE,
            implied_prob  DOUBLE,
            ev_ratio      DOUBLE,
            kelly_stake   DOUBLE,
            stake         DOUBLE,
            status        VARCHAR DEFAULT 'pending',
            pnl           DOUBLE,
            created_at    TIMESTAMP,
            resolved_at   TIMESTAMP,
            model_source  VARCHAR DEFAULT 'lgbm',
            discipline    VARCHAR
        )
    """)
    # Idempotent migrations for existing DBs
    conn.execute(
        "ALTER TABLE bets ADD COLUMN IF NOT EXISTS model_source VARCHAR DEFAULT 'rule_based'"
    )
    conn.execute(
        "ALTER TABLE bets ADD COLUMN IF NOT EXISTS discipline VARCHAR"
    )
    logger.debug("DuckDB schema initialised (idempotent)")


def upsert_race(conn: duckdb.DuckDBPyConnection, race: dict) -> None:
    conn.execute("""
        INSERT OR REPLACE INTO races VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    """, [
        race.get("race_id"),
        race.get("date"),
        race.get("hippodrome"),
        race.get("race_datetime"),
        race.get("distance_metres"),
        race.get("track_condition"),
        race.get("discipline"),
        race.get("is_trot", False),
        race.get("field_size"),
        race.get("reunion_number"),
        race.get("course_number"),
        race.get("raw_file_path"),
    ])


def upsert_runners(conn: duckdb.DuckDBPyConnection, runners: list[dict]) -> None:
    if not runners:
        return
    conn.executemany("""
        INSERT OR REPLACE INTO runners VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    """, [
        [
            r.get("runner_id"),
            r.get("race_id"),
            r.get("horse_number"),
            r.get("horse_name"),
            r.get("jockey_name"),
            r.get("trainer_name"),
            r.get("draw_position"),
            r.get("weight_kg"),
            r.get("handicap_distance"),
            r.get("deferre", False),
            r.get("scratch", False),
            r.get("musique"),
            r.get("finish_position"),
            r.get("km_time"),
        ]
        for r in runners
    ])


def upsert_odds(conn: duckdb.DuckDBPyConnection, odds_list: list[dict]) -> None:
    if not odds_list:
        return
    conn.executemany("""
        INSERT OR REPLACE INTO odds VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [
        [
            o.get("odds_id"),
            o.get("runner_id"),
            o.get("race_id"),
            o.get("snapshot_time"),
            o.get("odds_type"),
            o.get("decimal_odds"),
            o.get("implied_prob"),
        ]
        for o in odds_list
    ])


def upsert_horse_history(conn: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    if not rows:
        return
    conn.executemany("""
        INSERT OR REPLACE INTO horse_history VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        [
            h.get("history_id"),
            h.get("horse_name"),
            h.get("fetched_for_race"),
            h.get("past_race_date"),
            h.get("track"),
            h.get("finish_position"),
            h.get("field_size"),
            h.get("gap_to_winner"),
            h.get("disqualified", False),
        ]
        for h in rows
    ])


def upsert_bet(conn: duckdb.DuckDBPyConnection, bet: dict) -> None:
    conn.execute("""
        INSERT OR REPLACE INTO bets VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    """, [
        bet.get("bet_id"),
        bet.get("race_id"),
        bet.get("date"),
        bet.get("hippodrome"),
        bet.get("bet_type"),
        bet.get("runner_id_1"),
        bet.get("runner_id_2"),
        bet.get("horse_name_1"),
        bet.get("horse_name_2"),
        bet.get("morning_odds"),
        bet.get("model_prob"),
        bet.get("implied_prob"),
        bet.get("ev_ratio"),
        bet.get("kelly_stake"),
        bet.get("stake"),
        bet.get("status", "pending"),
        bet.get("pnl"),
        bet.get("created_at"),
        bet.get("resolved_at"),
        bet.get("model_source", "rule_based"),
        bet.get("discipline"),
    ])
