"""Full feature pipeline: query DuckDB → one row per (race, runner)."""
from __future__ import annotations

from datetime import datetime

import pandas as pd
import duckdb

from src.features.form import form_score, extended_form_features
from src.features.market import odds_features


def _days_diff(race_date, last_race_date) -> float:
    """Compute days between two YYYYMMDD strings. Returns NaN on failure."""
    try:
        d1 = datetime.strptime(str(int(float(race_date))), "%Y%m%d")
        d2 = datetime.strptime(str(int(float(last_race_date))), "%Y%m%d")
        return float((d1 - d2).days)
    except (ValueError, TypeError, OSError):
        return float("nan")


def compute_features(
    conn: duckdb.DuckDBPyConnection,
    race_ids: list[str] | None = None,
) -> pd.DataFrame:
    """Build feature DataFrame from DuckDB.

    Filters to trot races where finish_position IS NOT NULL.
    Returns one row per runner.
    """
    race_filter = ""
    params: list = []
    if race_ids:
        placeholders = ", ".join(["?"] * len(race_ids))
        race_filter = f"AND ra.race_id IN ({placeholders})"
        params = list(race_ids)

    # --- 1. Base runners + race info ---
    base_sql = f"""
        SELECT
            ru.runner_id,
            ru.race_id,
            ra.date,
            ra.hippodrome,
            ra.race_datetime,
            ra.distance_metres,
            ra.field_size,
            ru.horse_name,
            ru.jockey_name,
            ru.trainer_name,
            ru.musique,
            ru.scratch,
            ru.finish_position,
            ru.draw_position,
            ru.handicap_distance,
            CAST(ru.deferre AS INTEGER) AS deferre
        FROM runners ru
        JOIN races ra ON ra.race_id = ru.race_id
        WHERE ra.is_trot = TRUE
          AND ru.finish_position IS NOT NULL
          AND ru.scratch = FALSE
          {race_filter}
        ORDER BY ra.date, ru.race_id, ru.horse_number
    """
    base_df = conn.execute(base_sql, params).df()

    if base_df.empty:
        return pd.DataFrame()

    race_ids_in = base_df["race_id"].unique().tolist()
    placeholders = ", ".join(["?"] * len(race_ids_in))

    # --- 2. Morning and final odds ---
    odds_sql = f"""
        SELECT
            runner_id,
            race_id,
            MAX(CASE WHEN odds_type = 'morning' THEN decimal_odds END) AS morning_odds,
            MAX(CASE WHEN odds_type = 'final'   THEN decimal_odds END) AS final_odds
        FROM odds
        WHERE race_id IN ({placeholders})
        GROUP BY runner_id, race_id
    """
    odds_df = conn.execute(odds_sql, race_ids_in).df()

    # --- 3. Rolling jockey win rate (no leakage) ---
    jockey_sql = f"""
        SELECT
            ru.runner_id,
            COUNT(hr.race_id)                                                     AS jockey_starts_before,
            COALESCE(SUM(CASE WHEN hr.race_id IS NOT NULL
                               AND h.finish_position = 1 THEN 1 ELSE 0 END), 0)  AS jockey_wins_before
        FROM runners ru
        JOIN races ra ON ra.race_id = ru.race_id
        LEFT JOIN runners h ON h.jockey_name = ru.jockey_name
                           AND h.finish_position IS NOT NULL
                           AND h.scratch = FALSE
        LEFT JOIN races hr  ON hr.race_id = h.race_id
                           AND hr.date < ra.date
        WHERE ru.race_id IN ({placeholders})
          AND ru.scratch = FALSE
        GROUP BY ru.runner_id
    """
    jockey_df = conn.execute(jockey_sql, race_ids_in).df()
    jockey_df["jockey_win_rate"] = (
        jockey_df["jockey_wins_before"] / jockey_df["jockey_starts_before"]
    )

    # --- 4. Rolling trainer win rate (same pattern, no leakage) ---
    trainer_sql = f"""
        SELECT
            ru.runner_id,
            COUNT(hr.race_id)                                                     AS trainer_starts_before,
            COALESCE(SUM(CASE WHEN hr.race_id IS NOT NULL
                               AND h.finish_position = 1 THEN 1 ELSE 0 END), 0)  AS trainer_wins_before
        FROM runners ru
        JOIN races ra ON ra.race_id = ru.race_id
        LEFT JOIN runners h ON h.trainer_name = ru.trainer_name
                           AND h.finish_position IS NOT NULL
                           AND h.scratch = FALSE
        LEFT JOIN races hr  ON hr.race_id = h.race_id
                           AND hr.date < ra.date
        WHERE ru.race_id IN ({placeholders})
          AND ru.scratch = FALSE
        GROUP BY ru.runner_id
    """
    trainer_df = conn.execute(trainer_sql, race_ids_in).df()
    trainer_df["trainer_win_rate"] = (
        trainer_df["trainer_wins_before"] / trainer_df["trainer_starts_before"]
    )

    # --- 5. Horse-level stats from our own race history (no leakage) ---
    # COUNT(hr.race_id) instead of COUNT(prev.runner_id): hr is NULL when the
    # LEFT JOIN date filter fails (future races), so only past races are counted.
    horse_sql = f"""
        SELECT
            ru.runner_id,
            ra.date                                                                AS race_date,
            ra.hippodrome                                                          AS race_hippodrome,
            COUNT(hr.race_id)                                                      AS horse_n_runs,
            COALESCE(SUM(CASE WHEN hr.race_id IS NOT NULL
                               AND prev.finish_position = 1 THEN 1.0 ELSE 0.0 END), 0)
                / NULLIF(COUNT(hr.race_id), 0)                                    AS horse_win_rate,
            MAX(hr.date)                                                           AS last_race_date,
            SUM(CASE WHEN hr.hippodrome = ra.hippodrome
                      AND prev.finish_position = 1 THEN 1.0 ELSE 0.0 END)
                / NULLIF(SUM(CASE WHEN hr.hippodrome = ra.hippodrome
                                  THEN 1.0 ELSE 0.0 END), 0)                     AS horse_win_rate_at_track
        FROM runners ru
        JOIN races ra ON ra.race_id = ru.race_id
        LEFT JOIN runners prev ON prev.horse_name = ru.horse_name
                              AND prev.scratch = FALSE
                              AND prev.finish_position IS NOT NULL
        LEFT JOIN races hr ON hr.race_id = prev.race_id
                          AND hr.date < ra.date
        WHERE ru.race_id IN ({placeholders})
          AND ru.scratch = FALSE
        GROUP BY ru.runner_id, ra.date, ra.hippodrome
    """
    horse_df = conn.execute(horse_sql, race_ids_in).df()
    horse_df["days_since_last_race"] = horse_df.apply(
        lambda r: _days_diff(r["race_date"], r["last_race_date"]), axis=1
    )

    # --- 6. Extended form features from musique ---
    base_df["form_score"] = base_df["musique"].apply(form_score)
    form_extra = pd.DataFrame(
        base_df["musique"].apply(extended_form_features).tolist()
    )
    base_df = pd.concat([base_df.reset_index(drop=True), form_extra], axis=1)

    # --- 7. Race hour ---
    base_df["race_hour"] = pd.to_datetime(
        base_df["race_datetime"], errors="coerce"
    ).dt.hour

    # --- 8. Merge odds and apply market features ---
    df = base_df.merge(odds_df, on=["runner_id", "race_id"], how="left")
    df = odds_features(df)

    # --- 9. Merge jockey + trainer stats ---
    df = df.merge(jockey_df[["runner_id", "jockey_win_rate"]], on="runner_id", how="left")
    df = df.merge(trainer_df[["runner_id", "trainer_win_rate"]], on="runner_id", how="left")

    # --- 10. Merge horse stats ---
    df = df.merge(
        horse_df[["runner_id", "horse_n_runs", "horse_win_rate",
                  "horse_win_rate_at_track", "days_since_last_race"]],
        on="runner_id", how="left",
    )

    # --- 11. Select final columns ---
    keep = [
        "runner_id", "race_id", "date", "hippodrome", "race_datetime",
        "distance_metres", "field_size", "horse_name", "jockey_name",
        "morning_odds", "final_odds",
        "morning_odds_rank", "final_odds_rank", "odds_drift_pct",
        "morning_implied_prob", "morning_implied_prob_norm",
        "odds_rank_change", "is_favorite", "field_entropy",
        "form_score",
        "win_rate_last5", "top3_rate_last5", "form_trend",
        "best_position_last5", "n_valid_runs",
        "draw_position", "handicap_distance", "deferre", "race_hour",
        "jockey_win_rate", "trainer_win_rate",
        "horse_n_runs", "horse_win_rate", "horse_win_rate_at_track",
        "days_since_last_race",
        "finish_position",
    ]
    available = [c for c in keep if c in df.columns]
    return df[available].reset_index(drop=True)
