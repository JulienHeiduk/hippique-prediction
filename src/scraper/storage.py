"""DuckDB schema initialisation and upsert helpers."""
from __future__ import annotations

from pathlib import Path

import duckdb
from loguru import logger

from config.settings import DB_PATH


def get_connection(db_path: Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    init_schema(conn)
    return conn


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
            scratch            BOOLEAN DEFAULT FALSE
        )
    """)
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
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
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
