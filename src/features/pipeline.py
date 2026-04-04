"""Full feature pipeline: query DuckDB -> one row per (race, runner)."""
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


# ---------------------------------------------------------------------------
# Shared column list (superset of training + production)
# ---------------------------------------------------------------------------
_KEEP_COLUMNS = [
    "runner_id", "race_id", "date", "hippodrome", "race_datetime",
    "distance_metres", "field_size", "horse_name", "jockey_name",
    "morning_odds", "final_odds",
    "morning_odds_rank", "final_odds_rank", "odds_drift_pct",
    "morning_implied_prob", "morning_implied_prob_norm",
    "final_implied_prob", "final_implied_prob_norm",
    "odds_rank_change", "is_favorite", "field_entropy",
    "form_score",
    "win_rate_last5", "top3_rate_last5", "form_trend",
    "best_position_last5", "n_valid_runs",
    "avg_position_last3", "avg_position_last5",
    "draw_position", "handicap_distance", "deferre", "race_hour",
    "weight_kg",
    "jockey_win_rate", "jockey_win_rate_at_track",
    "trainer_win_rate", "trainer_win_rate_at_track",
    "horse_n_runs", "horse_win_rate", "horse_win_rate_at_track",
    "days_since_last_race", "days_since_last_win",
    "avg_km_time_hist", "best_km_time_hist",
    "horse_win_rate_at_distance", "horse_avg_position_at_distance",
    "horse_jockey_win_rate", "horse_jockey_n_races",
    "finish_position",
    "race_discipline",
]


# ---------------------------------------------------------------------------
# Shared enrichment (used by both training and production paths)
# ---------------------------------------------------------------------------

def enrich_base_df(
    conn: duckdb.DuckDBPyConnection,
    base_df: pd.DataFrame,
    *,
    morning_odds_fallback: bool = False,
) -> pd.DataFrame:
    """Enrich a base runners DataFrame with all features.

    Adds: odds + market features, jockey/trainer/horse/horse-jockey stats,
    form features, race hour.

    Args:
        conn: DuckDB connection.
        base_df: Must contain runner_id, race_id, horse_name, jockey_name,
                 trainer_name, musique, race_datetime, etc.
        morning_odds_fallback: If True, fall back to live (final) odds when
            morning reference odds are not yet published (production mode).
    """
    if base_df.empty:
        return pd.DataFrame()

    race_ids_in = base_df["race_id"].unique().tolist()
    placeholders = ", ".join(["?"] * len(race_ids_in))

    # --- 1. Morning and final odds ---
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

    if morning_odds_fallback:
        # When reference price not yet published (e.g. late races at 08:30),
        # use live odds so that implied-prob features are not all NaN.
        odds_df["morning_odds"] = odds_df["morning_odds"].combine_first(
            odds_df["final_odds"]
        )

    # --- 2. Rolling jockey win rate (no leakage) ---
    jockey_sql = f"""
        SELECT
            ru.runner_id,
            COUNT(hr.race_id)                                                     AS jockey_starts_before,
            COALESCE(SUM(CASE WHEN hr.race_id IS NOT NULL
                               AND h.finish_position = 1 THEN 1 ELSE 0 END), 0)  AS jockey_wins_before,
            COUNT(CASE WHEN hr.race_id IS NOT NULL
                        AND hr.hippodrome = ra.hippodrome THEN 1 END)             AS jockey_starts_at_track,
            COALESCE(SUM(CASE WHEN hr.race_id IS NOT NULL
                               AND hr.hippodrome = ra.hippodrome
                               AND h.finish_position = 1 THEN 1 ELSE 0 END), 0)  AS jockey_wins_at_track
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
    jockey_df["jockey_win_rate_at_track"] = (
        jockey_df["jockey_wins_at_track"] / jockey_df["jockey_starts_at_track"]
    )

    # --- 3. Rolling trainer win rate (no leakage) ---
    trainer_sql = f"""
        SELECT
            ru.runner_id,
            COUNT(hr.race_id)                                                     AS trainer_starts_before,
            COALESCE(SUM(CASE WHEN hr.race_id IS NOT NULL
                               AND h.finish_position = 1 THEN 1 ELSE 0 END), 0)  AS trainer_wins_before,
            COUNT(CASE WHEN hr.race_id IS NOT NULL
                        AND hr.hippodrome = ra.hippodrome THEN 1 END)             AS trainer_starts_at_track,
            COALESCE(SUM(CASE WHEN hr.race_id IS NOT NULL
                               AND hr.hippodrome = ra.hippodrome
                               AND h.finish_position = 1 THEN 1 ELSE 0 END), 0)  AS trainer_wins_at_track
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
    trainer_df["trainer_win_rate_at_track"] = (
        trainer_df["trainer_wins_at_track"] / trainer_df["trainer_starts_at_track"]
    )

    # --- 4. Horse-level stats from race history (no leakage) ---
    horse_sql = f"""
        SELECT
            ru.runner_id,
            ra.date                                                                AS race_date,
            ra.hippodrome                                                          AS race_hippodrome,
            ra.distance_metres                                                     AS race_distance,
            COUNT(hr.race_id)                                                      AS horse_n_runs,
            COALESCE(SUM(CASE WHEN hr.race_id IS NOT NULL
                               AND prev.finish_position = 1 THEN 1.0 ELSE 0.0 END), 0)
                / NULLIF(COUNT(hr.race_id), 0)                                    AS horse_win_rate,
            MAX(hr.date)                                                           AS last_race_date,
            SUM(CASE WHEN hr.hippodrome = ra.hippodrome
                      AND prev.finish_position = 1 THEN 1.0 ELSE 0.0 END)
                / NULLIF(SUM(CASE WHEN hr.hippodrome = ra.hippodrome
                                  THEN 1.0 ELSE 0.0 END), 0)                     AS horse_win_rate_at_track,
            AVG(CASE WHEN hr.race_id IS NOT NULL
                      AND prev.km_time IS NOT NULL AND TRIM(prev.km_time) != ''
                     THEN TRY_CAST(prev.km_time AS DOUBLE) END)                  AS avg_km_time_hist,
            MIN(CASE WHEN hr.race_id IS NOT NULL
                      AND prev.km_time IS NOT NULL AND TRIM(prev.km_time) != ''
                     THEN TRY_CAST(prev.km_time AS DOUBLE) END)                  AS best_km_time_hist,
            COALESCE(SUM(CASE WHEN hr.race_id IS NOT NULL
                               AND ABS(hr.distance_metres - ra.distance_metres) <= 500
                               AND prev.finish_position = 1 THEN 1.0 ELSE 0.0 END), 0)
                / NULLIF(SUM(CASE WHEN hr.race_id IS NOT NULL
                                   AND ABS(hr.distance_metres - ra.distance_metres) <= 500
                                  THEN 1.0 ELSE 0.0 END), 0)                    AS horse_win_rate_at_distance,
            AVG(CASE WHEN hr.race_id IS NOT NULL
                      AND ABS(hr.distance_metres - ra.distance_metres) <= 500
                     THEN CAST(prev.finish_position AS DOUBLE) END)              AS horse_avg_position_at_distance,
            MAX(CASE WHEN hr.race_id IS NOT NULL
                      AND prev.finish_position = 1 THEN hr.date END)             AS last_win_date
        FROM runners ru
        JOIN races ra ON ra.race_id = ru.race_id
        LEFT JOIN runners prev ON prev.horse_name = ru.horse_name
                              AND prev.scratch = FALSE
                              AND prev.finish_position IS NOT NULL
        LEFT JOIN races hr ON hr.race_id = prev.race_id
                          AND hr.date < ra.date
        WHERE ru.race_id IN ({placeholders})
          AND ru.scratch = FALSE
        GROUP BY ru.runner_id, ra.date, ra.hippodrome, ra.distance_metres
    """
    horse_df = conn.execute(horse_sql, race_ids_in).df()
    horse_df["days_since_last_race"] = horse_df.apply(
        lambda r: _days_diff(r["race_date"], r["last_race_date"]), axis=1
    )
    horse_df["days_since_last_win"] = horse_df.apply(
        lambda r: _days_diff(r["race_date"], r["last_win_date"]), axis=1
    )

    # --- 5. Horse-jockey pair stats ---
    horse_jockey_sql = f"""
        SELECT
            ru.runner_id,
            COUNT(hr.race_id)                                                      AS hj_starts,
            COALESCE(SUM(CASE WHEN hr.race_id IS NOT NULL
                               AND prev.finish_position = 1 THEN 1.0 ELSE 0.0 END), 0)
                / NULLIF(COUNT(hr.race_id), 0)                                    AS horse_jockey_win_rate
        FROM runners ru
        JOIN races ra ON ra.race_id = ru.race_id
        LEFT JOIN runners prev ON prev.horse_name  = ru.horse_name
                              AND prev.jockey_name = ru.jockey_name
                              AND prev.scratch = FALSE
                              AND prev.finish_position IS NOT NULL
        LEFT JOIN races hr ON hr.race_id = prev.race_id
                          AND hr.date < ra.date
        WHERE ru.race_id IN ({placeholders})
          AND ru.scratch = FALSE
        GROUP BY ru.runner_id
    """
    hj_df = conn.execute(horse_jockey_sql, race_ids_in).df()
    hj_df = hj_df.rename(columns={"hj_starts": "horse_jockey_n_races"})

    # --- 6. Extended form features from musique ---
    base_df = base_df.copy()
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
    df = df.merge(
        jockey_df[["runner_id", "jockey_win_rate", "jockey_win_rate_at_track"]],
        on="runner_id", how="left",
    )
    df = df.merge(
        trainer_df[["runner_id", "trainer_win_rate", "trainer_win_rate_at_track"]],
        on="runner_id", how="left",
    )

    # --- 10. Merge horse stats ---
    df = df.merge(
        horse_df[["runner_id", "horse_n_runs", "horse_win_rate",
                  "horse_win_rate_at_track", "days_since_last_race",
                  "avg_km_time_hist", "best_km_time_hist",
                  "horse_win_rate_at_distance", "horse_avg_position_at_distance",
                  "days_since_last_win"]],
        on="runner_id", how="left",
    )

    # --- 10b. Merge horse-jockey pair stats ---
    df = df.merge(
        hj_df[["runner_id", "horse_jockey_win_rate", "horse_jockey_n_races"]],
        on="runner_id", how="left",
    )

    # --- 11. Select final columns ---
    available = [c for c in _KEEP_COLUMNS if c in df.columns]
    return df[available].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------

def _discipline_filter(discipline: str) -> str:
    """Return SQL WHERE clause fragment for the given discipline."""
    if discipline == "trot":
        return "ra.is_trot = TRUE"
    elif discipline == "plat":
        return "ra.discipline = 'PLAT'"
    else:
        raise ValueError(f"Unknown discipline: {discipline!r}")


def compute_features(
    conn: duckdb.DuckDBPyConnection,
    race_ids: list[str] | None = None,
    discipline: str = "trot",
) -> pd.DataFrame:
    """Build feature DataFrame from DuckDB (training / backtest path).

    Filters to *discipline* races where finish_position IS NOT NULL.
    Returns one row per runner.

    Args:
        discipline: "trot" (default) or "plat".
    """
    race_filter = ""
    params: list = []
    if race_ids:
        ph = ", ".join(["?"] * len(race_ids))
        race_filter = f"AND ra.race_id IN ({ph})"
        params = list(race_ids)

    disc_sql = _discipline_filter(discipline)

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
            CAST(ru.deferre AS INTEGER) AS deferre,
            ru.weight_kg
        FROM runners ru
        JOIN races ra ON ra.race_id = ru.race_id
        WHERE {disc_sql}
          AND ru.finish_position IS NOT NULL
          AND ru.scratch = FALSE
          {race_filter}
        ORDER BY ra.date, ru.race_id, ru.horse_number
    """
    base_df = conn.execute(base_sql, params).df()

    return enrich_base_df(conn, base_df, morning_odds_fallback=False)
