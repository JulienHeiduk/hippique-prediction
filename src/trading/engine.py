"""Live paper trading engine: feature computation, bet generation, resolution, ledger."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Callable

import pandas as pd
import duckdb
from loguru import logger

from config.settings import EV_THRESHOLD, UNIT_STAKE
from src.features.form import form_score
from src.features.market import odds_features
from src.scraper.storage import upsert_bet
from src.trading.kelly import kelly_stake


def compute_today_features(
    conn: duckdb.DuckDBPyConnection,
    date: str,
) -> pd.DataFrame:
    """Build feature DataFrame for today's races (no finish_position required).

    Same structure as compute_features() but filters by date instead of
    finish_position IS NOT NULL. final_odds will be NULL (race hasn't run).

    Returns one row per runner with the same columns as compute_features(),
    except finish_position will be NULL.
    """
    # --- 1. Base runners + race info for today ---
    base_sql = """
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
          AND ra.date = ?
          AND ru.scratch = FALSE
        ORDER BY ru.race_id, ru.horse_number
    """
    base_df = conn.execute(base_sql, [date]).df()

    if base_df.empty:
        return pd.DataFrame()

    # --- 2. Morning odds, with final_odds as fallback ---
    # dernierRapportReference ('morning') is the reference price published before
    # races. For late-programme reunions it may not yet be available when the
    # morning session runs; in that case we fall back to dernierRapportDirect
    # ('final') so implied probabilities are computed from real market odds
    # rather than 1/field_size.
    race_ids_in = base_df["race_id"].unique().tolist()
    placeholders = ", ".join(["?"] * len(race_ids_in))
    odds_sql = f"""
        SELECT
            runner_id,
            race_id,
            MAX(CASE WHEN odds_type = 'morning' THEN decimal_odds END) AS morning_odds_raw,
            MAX(CASE WHEN odds_type = 'final'   THEN decimal_odds END) AS final_odds
        FROM odds
        WHERE race_id IN ({placeholders})
        GROUP BY runner_id, race_id
    """
    odds_df = conn.execute(odds_sql, race_ids_in).df()
    # Effective morning_odds: prefer reference price; fall back to live odds
    odds_df["morning_odds"] = odds_df["morning_odds_raw"].combine_first(odds_df["final_odds"])
    odds_df = odds_df.drop(columns=["morning_odds_raw"])

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


def generate_bets(
    conn: duckdb.DuckDBPyConnection,
    date: str,
    ev_threshold: float = EV_THRESHOLD,
    scorer_fn: Callable[[pd.DataFrame], pd.Series] | None = None,
    bet_types: list[str] | None = None,
) -> list[dict]:
    """Score today's runners and log EV+ bets to the bets table.

    Steps:
    1. compute_today_features() → df
    2. Score all runners with scorer_fn (defaults to score_combined)
    3. For each race: compute model_prob, compare to implied_prob
    4. Log 'win' bets for EV+ top-1 runners
    5. Log 'duo' bets (if field >= 4) when top-2 combo is EV+
    6. upsert_bet() each qualifying bet (idempotent via deterministic bet_id)
    7. Return list of bet dicts
    """
    if scorer_fn is None:
        from src.model.scorer import score_combined
        scorer_fn = score_combined

    if bet_types is None:
        bet_types = ["win", "duo"]

    df = compute_today_features(conn, date)

    if df.empty:
        logger.warning("No today features for {} — no bets generated", date)
        return []

    try:
        scores = scorer_fn(df)
    except Exception as exc:
        logger.error("Scorer failed: {}", exc)
        return []

    score_map = dict(zip(scores.index, scores.values))
    df["_score"] = df["runner_id"].map(score_map).fillna(0.0)

    bets: list[dict] = []
    now = datetime.now(tz=timezone.utc)

    for race_id, race_df in df.groupby("race_id"):
        race_df = race_df.copy().reset_index(drop=True)
        field_size = len(race_df)

        if field_size < 4:
            continue

        total_score = race_df["_score"].sum()
        if total_score <= 0:
            continue

        race_df = race_df.sort_values("_score", ascending=False).reset_index(drop=True)
        race_df["model_prob"] = race_df["_score"] / total_score

        hippodrome = race_df["hippodrome"].iloc[0] if "hippodrome" in race_df.columns else None

        # --- 'win' bet: top-1 runner ---
        if "win" in bet_types:
            top1 = race_df.iloc[0]
            morning_odds_top1 = top1.get("morning_odds")
            implied_prob_top1 = top1.get("morning_implied_prob_norm")

            if pd.isna(implied_prob_top1) or implied_prob_top1 is None:
                implied_prob_top1 = 1.0 / field_size

            model_prob_top1 = float(top1["model_prob"])
            ev_ratio = model_prob_top1 / implied_prob_top1 if implied_prob_top1 > 0 else 0.0

            if model_prob_top1 > implied_prob_top1 * ev_threshold:
                morning_odds_val = float(morning_odds_top1) if not pd.isna(morning_odds_top1) else None
                ks = kelly_stake(model_prob_top1, morning_odds_val or field_size)
                bet = {
                    "bet_id": f"{race_id}_win",
                    "race_id": str(race_id),
                    "date": date,
                    "hippodrome": hippodrome,
                    "bet_type": "win",
                    "runner_id_1": str(top1["runner_id"]),
                    "runner_id_2": None,
                    "horse_name_1": top1.get("horse_name"),
                    "horse_name_2": None,
                    "morning_odds": morning_odds_val,
                    "model_prob": model_prob_top1,
                    "implied_prob": float(implied_prob_top1),
                    "ev_ratio": ev_ratio,
                    "kelly_stake": ks,
                    "stake": UNIT_STAKE,
                    "status": "pending",
                    "pnl": None,
                    "created_at": now,
                    "resolved_at": None,
                }
                upsert_bet(conn, bet)
                bets.append(bet)
                logger.info(
                    "BET win | {} | {} | model_prob={:.2%} implied={:.2%} EV={:.2f}",
                    race_id, top1.get("horse_name"), model_prob_top1,
                    implied_prob_top1, ev_ratio,
                )

        # --- 'duo' bet: top-1 + top-2 ---
        if "duo" in bet_types and field_size >= 4 and len(race_df) >= 2:
            top1 = race_df.iloc[0]
            top2 = race_df.iloc[1]
            combined_model_prob = float(top1["model_prob"]) + float(top2["model_prob"])

            implied_prob_top1 = top1.get("morning_implied_prob_norm")
            implied_prob_top2 = top2.get("morning_implied_prob_norm")
            if pd.isna(implied_prob_top1) or implied_prob_top1 is None:
                implied_prob_top1 = 1.0 / field_size
            if pd.isna(implied_prob_top2) or implied_prob_top2 is None:
                implied_prob_top2 = 1.0 / field_size

            combined_implied_prob = float(implied_prob_top1) + float(implied_prob_top2)
            ev_ratio_duo = combined_model_prob / combined_implied_prob if combined_implied_prob > 0 else 0.0

            if combined_model_prob > combined_implied_prob * ev_threshold:
                morning_odds_top1 = top1.get("morning_odds")
                morning_odds_val = float(morning_odds_top1) if not pd.isna(morning_odds_top1) else None
                ks = kelly_stake(combined_model_prob, morning_odds_val or field_size)
                bet = {
                    "bet_id": f"{race_id}_duo",
                    "race_id": str(race_id),
                    "date": date,
                    "hippodrome": hippodrome,
                    "bet_type": "duo",
                    "runner_id_1": str(top1["runner_id"]),
                    "runner_id_2": str(top2["runner_id"]),
                    "horse_name_1": top1.get("horse_name"),
                    "horse_name_2": top2.get("horse_name"),
                    "morning_odds": morning_odds_val,
                    "model_prob": combined_model_prob,
                    "implied_prob": combined_implied_prob,
                    "ev_ratio": ev_ratio_duo,
                    "kelly_stake": ks,
                    "stake": UNIT_STAKE * 2,
                    "status": "pending",
                    "pnl": None,
                    "created_at": now,
                    "resolved_at": None,
                }
                upsert_bet(conn, bet)
                bets.append(bet)
                logger.info(
                    "BET duo | {} | {}+{} | model_prob={:.2%} implied={:.2%} EV={:.2f}",
                    race_id, top1.get("horse_name"), top2.get("horse_name"),
                    combined_model_prob, combined_implied_prob, ev_ratio_duo,
                )

    logger.info("{} bets generated for {}", len(bets), date)
    return bets


def resolve_bets(
    conn: duckdb.DuckDBPyConnection,
    date: str,
) -> pd.DataFrame:
    """Resolve pending bets for *date* using actual finish positions.

    Applies the same P&L rules as backtest.py:
      win:   hit = pos1 == 1;  pnl = morning_odds - 1  or -1
      place: hit = pos1 <= 3 (<=2 if field<5); pnl = morning_odds/4 - 1  or -1
      duo:   hit = {pos1,pos2} == {1,2}; pnl = field²/4 capped 50 - 2  or -2

    Returns summary DataFrame: date, n_bets, n_won, total_stake, total_pnl, roi.
    """
    pending_df = conn.execute(
        "SELECT * FROM bets WHERE date = ? AND status = 'pending'",
        [date],
    ).df()

    if pending_df.empty:
        logger.info("No pending bets to resolve for {}", date)
        return pd.DataFrame(columns=["date", "n_bets", "n_won", "total_stake", "total_pnl", "roi"])

    # Fetch finish positions for all runners involved
    runner_ids_1 = pending_df["runner_id_1"].dropna().tolist()
    runner_ids_2 = pending_df["runner_id_2"].dropna().tolist()
    all_runner_ids = list(set(runner_ids_1 + runner_ids_2))

    placeholders = ", ".join(["?"] * len(all_runner_ids))
    runners_sql = f"""
        SELECT ru.runner_id, ru.race_id, ru.finish_position, ra.field_size
        FROM runners ru
        JOIN races ra ON ra.race_id = ru.race_id
        WHERE ru.runner_id IN ({placeholders})
    """
    runners_df = conn.execute(runners_sql, all_runner_ids).df()
    pos_map   = dict(zip(runners_df["runner_id"], runners_df["finish_position"]))
    field_map = dict(zip(runners_df["runner_id"], runners_df["field_size"]))

    # Final odds fallback: used when morning_odds is NULL (e.g. R7 late races)
    final_odds_sql = f"""
        SELECT runner_id, decimal_odds
        FROM odds
        WHERE runner_id IN ({placeholders}) AND odds_type = 'final'
    """
    final_odds_df = conn.execute(final_odds_sql, all_runner_ids).df()
    final_odds_map = dict(zip(final_odds_df["runner_id"], final_odds_df["decimal_odds"]))

    # Build a set of race_ids where at least one runner has a finish position
    # (i.e. results have been published). Used to detect DAI horses.
    race_id_map = dict(zip(runners_df["runner_id"], runners_df["race_id"]))
    all_race_ids = list({b["race_id"] for b in pending_df.to_dict("records")})
    finished_races: set[str] = set()
    if all_race_ids:
        ph2 = ", ".join(["?"] * len(all_race_ids))
        finished_df = conn.execute(
            f"""
            SELECT DISTINCT race_id FROM runners
            WHERE race_id IN ({ph2}) AND finish_position IS NOT NULL
            """,
            all_race_ids,
        ).df()
        finished_races = set(finished_df["race_id"].tolist())

    now = datetime.now(tz=timezone.utc)
    results = []

    for _, bet in pending_df.iterrows():
        bet_id = bet["bet_id"]
        bet_type = bet["bet_type"]
        morning_odds = bet["morning_odds"]
        runner_id_1 = bet["runner_id_1"]
        runner_id_2 = bet.get("runner_id_2")
        race_id = bet["race_id"]

        pos1 = pos_map.get(runner_id_1)
        field_size = field_map.get(runner_id_1, 8)

        if pos1 is None or pd.isna(pos1):
            if race_id not in finished_races:
                # Race results not yet published — stay pending
                continue
            # Race finished but horse has no position → DAI (disqualified
            # after integration) — counts as a loss
            logger.info("DAI detected for {} in {} — marking as lost", runner_id_1, race_id)
            stake = UNIT_STAKE * (2 if bet_type == "duo" else 1)
            hit = False
            pnl = -stake
            status = "lost"

        elif bet_type == "win":
            stake = UNIT_STAKE
            hit = int(pos1) == 1
            # Fall back to final_odds when morning_odds was not available at scrape time
            odds = morning_odds if (morning_odds is not None and not pd.isna(morning_odds)) \
                else final_odds_map.get(runner_id_1)
            if hit and odds is not None:
                pnl = (float(odds) - 1.0) * stake
            else:
                pnl = -stake
            status = "won" if hit else "lost"

        elif bet_type == "place":
            stake = UNIT_STAKE
            cutoff = 2 if field_size < 5 else 3
            hit = int(pos1) <= cutoff
            odds = morning_odds if (morning_odds is not None and not pd.isna(morning_odds)) \
                else final_odds_map.get(runner_id_1)
            if hit and odds is not None:
                pnl = (float(odds) / 4.0 - 1.0) * stake
            else:
                pnl = -stake
            status = "won" if hit else "lost"

        elif bet_type == "duo":
            stake = UNIT_STAKE * 2
            pos2 = pos_map.get(runner_id_2)
            if pos2 is None or pd.isna(pos2):
                if race_id not in finished_races:
                    continue
                # DAI on second runner
                logger.info("DAI detected for {} in {} — marking as lost", runner_id_2, race_id)
                hit = False
                pnl = -stake
                status = "lost"
            else:
                hit = set([int(pos1), int(pos2)]) == {1, 2}
                if hit:
                    raw_pnl = field_size ** 2 / 4.0
                    pnl = min(raw_pnl, 50.0) - stake
                else:
                    pnl = -stake
                status = "won" if hit else "lost"

        else:
            logger.warning("Unknown bet_type {} for bet {}", bet_type, bet_id)
            continue

        # Resolve the effective odds used for P&L (morning_odds or final_odds fallback)
        effective_odds = morning_odds if (morning_odds is not None and not pd.isna(morning_odds)) \
            else final_odds_map.get(runner_id_1)

        # Update bet in DB (INSERT OR REPLACE = upsert)
        updated_bet = {
            "bet_id": bet_id,
            "race_id": bet["race_id"],
            "date": bet["date"],
            "hippodrome": bet.get("hippodrome"),
            "bet_type": bet_type,
            "runner_id_1": runner_id_1,
            "runner_id_2": runner_id_2,
            "horse_name_1": bet.get("horse_name_1"),
            "horse_name_2": bet.get("horse_name_2"),
            "morning_odds": effective_odds,
            "model_prob": bet.get("model_prob"),
            "implied_prob": bet.get("implied_prob"),
            "ev_ratio": bet.get("ev_ratio"),
            "kelly_stake": bet.get("kelly_stake"),
            "stake": stake,
            "status": status,
            "pnl": pnl,
            "created_at": bet.get("created_at"),
            "resolved_at": now,
        }
        upsert_bet(conn, updated_bet)

        results.append({
            "bet_id": bet_id,
            "bet_type": bet_type,
            "hit": hit,
            "stake": stake,
            "pnl": pnl,
        })

    if not results:
        logger.info("No bets could be resolved for {} (results not available?)", date)
        return pd.DataFrame(columns=["date", "n_bets", "n_won", "total_stake", "total_pnl", "roi"])

    res_df = pd.DataFrame(results)
    n_bets = len(res_df)
    n_won = int(res_df["hit"].sum())
    total_stake = float(res_df["stake"].sum())
    total_pnl = float(res_df["pnl"].sum())
    roi = total_pnl / total_stake if total_stake > 0 else 0.0

    logger.info(
        "Resolved {} bets for {}: {} won, P&L={:.2f}, ROI={:.1%}",
        n_bets, date, n_won, total_pnl, roi,
    )

    return pd.DataFrame([{
        "date": date,
        "n_bets": n_bets,
        "n_won": n_won,
        "total_stake": total_stake,
        "total_pnl": total_pnl,
        "roi": roi,
    }])


def get_ledger(
    conn: duckdb.DuckDBPyConnection,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Return all bets within the optional date range, ordered by date and race_id."""
    sql = """
        SELECT * FROM bets
        WHERE (date >= ? OR ? IS NULL)
          AND (date <= ? OR ? IS NULL)
        ORDER BY date, race_id
    """
    return conn.execute(sql, [start_date, start_date, end_date, end_date]).df()
