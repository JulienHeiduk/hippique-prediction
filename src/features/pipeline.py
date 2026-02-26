"""Full feature pipeline: query DuckDB → one row per (race, runner)."""
from __future__ import annotations

import pandas as pd
import duckdb

from src.features.form import form_score
from src.features.market import odds_features


def compute_features(
    conn: duckdb.DuckDBPyConnection,
    race_ids: list[str] | None = None,
) -> pd.DataFrame:
    """Build feature DataFrame from DuckDB.

    Filters to trot races where finish_position IS NOT NULL.
    Returns one row per runner with columns:
        runner_id, race_id, date, hippodrome, distance_metres,
        field_size, horse_name, jockey_name,
        morning_odds, final_odds,
        morning_odds_rank, final_odds_rank, odds_drift_pct,
        morning_implied_prob, morning_implied_prob_norm,
        form_score, jockey_win_rate, finish_position
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
            ra.distance_metres,
            ra.field_size,
            ru.horse_name,
            ru.jockey_name,
            ru.musique,
            ru.scratch,
            ru.finish_position
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

    # --- 2. Morning and final odds ---
    race_ids_in = base_df["race_id"].unique().tolist()
    placeholders = ", ".join(["?"] * len(race_ids_in))
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

    # --- 3. Rolling jockey win rate (no leakage: strictly before race date) ---
    jockey_sql = f"""
        SELECT
            ru.runner_id,
            COUNT(h.runner_id)                                                    AS jockey_starts_before,
            COALESCE(SUM(CASE WHEN h.finish_position = 1 THEN 1 ELSE 0 END), 0)  AS jockey_wins_before
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

    # --- 4. Compute form_score in Python ---
    base_df["form_score"] = base_df["musique"].apply(form_score)

    # --- 5. Merge odds and apply market features ---
    df = base_df.merge(odds_df, on=["runner_id", "race_id"], how="left")
    df = odds_features(df)

    # --- 6. Merge jockey stats ---
    df = df.merge(
        jockey_df[["runner_id", "jockey_win_rate"]],
        on="runner_id",
        how="left",
    )

    # --- 7. Select final columns ---
    keep = [
        "runner_id", "race_id", "date", "hippodrome", "distance_metres",
        "field_size", "horse_name", "jockey_name",
        "morning_odds", "final_odds",
        "morning_odds_rank", "final_odds_rank", "odds_drift_pct",
        "morning_implied_prob", "morning_implied_prob_norm",
        "form_score", "jockey_win_rate", "finish_position",
    ]
    available = [c for c in keep if c in df.columns]
    return df[available].reset_index(drop=True)
