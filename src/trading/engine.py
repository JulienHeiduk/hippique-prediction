"""Live paper trading engine: feature computation, bet generation, resolution, ledger."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable

import pandas as pd
import duckdb
from loguru import logger

from config.settings import WIN_EV_THRESHOLD, UNIT_STAKE
from src.features.pipeline import enrich_base_df
from src.scraper.client import PMUClient
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
    base_sql = """
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
          AND ra.date = ?
          AND ru.scratch = FALSE
        ORDER BY ru.race_id, ru.horse_number
    """
    base_df = conn.execute(base_sql, [date]).df()

    return enrich_base_df(conn, base_df, morning_odds_fallback=True)


def generate_bets(
    conn: duckdb.DuckDBPyConnection,
    date: str,
    ev_threshold: float = WIN_EV_THRESHOLD,
    scorer_fn: Callable[[pd.DataFrame], pd.Series] | None = None,
    bet_types: list[str] | None = None,
    min_race_time: datetime | None = None,
    model_source: str = "lgbm",
) -> list[dict]:
    """Score today's runners and log EV+ bets to the bets table.

    Steps:
    1. compute_today_features() → df
    2. Score all runners with scorer_fn (defaults to LightGBM)
    3. For each race: compute model_prob, compare to implied_prob
    4. Log 'win' bets for EV+ top-1 runners
    5. upsert_bet() each qualifying bet (idempotent via deterministic bet_id)
    7. Return list of bet dicts
    """
    if scorer_fn is None:
        from src.model.lgbm import load_lgbm_model, score_lgbm
        _model = load_lgbm_model()
        scorer_fn = lambda df, m=_model: score_lgbm(df, m)

    if bet_types is None:
        bet_types = ["win"]

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

    # Filter out races that have already started
    if min_race_time is not None and "race_datetime" in df.columns:
        # race_datetime from DuckDB is tz-naive local time; pandas 2.x requires a
        # pd.Timestamp (not a plain datetime) for dtype=datetime64[us] comparisons.
        # Strip tz if present, then wrap in pd.Timestamp.
        cutoff = pd.Timestamp(min_race_time.replace(tzinfo=None))
        race_datetimes = pd.to_datetime(
            df.groupby("race_id")["race_datetime"].first(), errors="coerce"
        )
        future_races = race_datetimes[
            race_datetimes > cutoff
        ].index
        skipped = set(df["race_id"].unique()) - set(future_races)
        if skipped:
            logger.info("Skipping {} past/in-progress race(s) in update session", len(skipped))
        df = df[df["race_id"].isin(future_races)]
        if df.empty:
            logger.info("No future races to update bets for {}", date)
            return []

    # Load existing bets for this date so we can protect them from overwrite.
    existing_rows = conn.execute(
        "SELECT bet_id, status, morning_odds, created_at FROM bets WHERE date = ?",
        [date],
    ).df()
    existing_map: dict[str, dict] = {
        row["bet_id"]: row for row in existing_rows.to_dict("records")
    }

    bets: list[dict] = []
    now = datetime.now(tz=timezone.utc)
    _sfx = f"_{model_source}"

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

            # Prefer live (final) odds for EV — reflects the current market.
            # Fall back to morning reference when live odds are not yet available.
            final_implied_top1   = top1.get("final_implied_prob_norm")
            morning_implied_top1 = top1.get("morning_implied_prob_norm")
            if final_implied_top1 is not None and not pd.isna(final_implied_top1):
                implied_prob_top1 = float(final_implied_top1)
            elif morning_implied_top1 is not None and not pd.isna(morning_implied_top1):
                implied_prob_top1 = float(morning_implied_top1)
            else:
                implied_prob_top1 = 1.0 / field_size

            model_prob_top1 = float(top1["model_prob"])
            ev_ratio = model_prob_top1 / implied_prob_top1 if implied_prob_top1 > 0 else 0.0

            if model_prob_top1 > implied_prob_top1 * ev_threshold:
                bet_id_win = f"{race_id}_win{_sfx}"
                existing_win = existing_map.get(bet_id_win, {})

                # Never overwrite a resolved bet
                if existing_win.get("status") in ("won", "lost"):
                    bets.append(existing_win)  # keep it in the returned list
                else:
                    # Best available odds: prefer live (final) over morning reference
                    final_odds_top1   = top1.get("final_odds")
                    morning_odds_top1 = top1.get("morning_odds")
                    morning_odds_val  = (
                        float(final_odds_top1)   if final_odds_top1   is not None and not pd.isna(final_odds_top1)   else
                        float(morning_odds_top1) if morning_odds_top1 is not None and not pd.isna(morning_odds_top1) else
                        None
                    )
                    # Preserve only when truly no odds available now
                    if morning_odds_val is None and existing_win.get("morning_odds") is not None:
                        morning_odds_val = existing_win["morning_odds"]
                    ks = kelly_stake(model_prob_top1, morning_odds_val or field_size)
                    bet = {
                        "bet_id": bet_id_win,
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
                        "created_at": existing_win.get("created_at") or now,
                        "resolved_at": None,
                        "model_source": model_source,
                    }
                    upsert_bet(conn, bet)
                    bets.append(bet)

                    # Mirror place bet: same horse, no extra EV filter
                    bet_id_place = f"{race_id}_place{_sfx}"
                    existing_place = existing_map.get(bet_id_place, {})
                    if existing_place.get("status") not in ("won", "lost"):
                        place_bet = {
                            **bet,
                            "bet_id": bet_id_place,
                            "bet_type": "place",
                            "created_at": existing_place.get("created_at") or now,
                        }
                        upsert_bet(conn, place_bet)
                        bets.append(place_bet)
                    else:
                        bets.append(existing_place)

                logger.info(
                    "BET win+place | {} | {} | model_prob={:.2%} implied={:.2%} EV={:.2f}",
                    race_id, top1.get("horse_name"), model_prob_top1,
                    implied_prob_top1, ev_ratio,
                )



    # ── Refresh odds for pending bets not regenerated in this run ──────────
    # When a race is processed but no horse clears the EV threshold the
    # existing pending bet stays in the DB untouched.  Its stored cote may be
    # stale (e.g. a live-odds fallback from the morning when the reference
    # price was not yet published).  Update morning_odds / implied_prob /
    # ev_ratio for those bets so the report always shows current figures.
    # Only runners whose race is still in the future are eligible — never
    # touch a race that has already started.
    cutoff_ts = pd.Timestamp(min_race_time.replace(tzinfo=None)) if min_race_time is not None else None
    runner_id_to_row: dict = {}
    for _, row in df.iterrows():
        rdt = pd.to_datetime(row.get("race_datetime"), errors="coerce")
        if cutoff_ts is not None and (pd.isna(rdt) or rdt <= cutoff_ts):
            continue
        runner_id_to_row[str(row["runner_id"])] = row

    refreshed_ids = {b.get("bet_id") for b in bets if isinstance(b, dict)}
    for _bet_id, ex in existing_map.items():
        if ex.get("status") not in (None, "pending"):
            continue
        if _bet_id in refreshed_ids:
            continue
        runner_id = str(ex.get("runner_id_1") or "")
        row = runner_id_to_row.get(runner_id)
        if row is None:
            continue
        final_odds_r   = row.get("final_odds")
        morning_odds_r = row.get("morning_odds")
        best_odds = (
            float(final_odds_r)   if final_odds_r   is not None and not pd.isna(final_odds_r)   else
            float(morning_odds_r) if morning_odds_r is not None and not pd.isna(morning_odds_r) else
            None
        )
        if best_odds is None or best_odds == ex.get("morning_odds"):
            continue
        fi = row.get("final_implied_prob_norm")
        mi = row.get("morning_implied_prob_norm")
        fresh_impl = (
            float(fi) if fi is not None and not pd.isna(fi) else
            float(mi) if mi is not None and not pd.isna(mi) else
            None
        )
        model_p = float(ex.get("model_prob") or 0.0)
        new_ev  = model_p / fresh_impl if (fresh_impl and fresh_impl > 0) else ex.get("ev_ratio", 0.0)
        new_ks  = kelly_stake(model_p, best_odds)
        upsert_bet(conn, {
            **{k: ex.get(k) for k in ex if k not in ("morning_odds", "implied_prob", "ev_ratio", "kelly_stake")},
            "morning_odds": best_odds,
            "implied_prob": fresh_impl if fresh_impl is not None else ex.get("implied_prob"),
            "ev_ratio":     new_ev,
            "kelly_stake":  new_ks,
        })
        logger.debug(
            "Refreshed odds for {} | {:.2f} → {:.2f}",
            _bet_id, ex.get("morning_odds") or 0.0, best_odds,
        )

    logger.info("{} bets generated for {}", len(bets), date)
    return bets



def _extract_place_dividend(rapports: list, horse_num: int) -> float | None:
    """Parse E_SIMPLE_PLACE dividend for a specific horse number.

    Returns gross return per 1€ staked (e.g. 2.10), or None if not found.
    """
    for entry in rapports:
        if entry.get("typePari") != "E_SIMPLE_PLACE":
            continue
        for rap in entry.get("rapports", []):
            combo = rap.get("combinaison", "")
            if combo == str(horse_num):
                div = rap.get("dividendePourUnEuro") or rap.get("dividende")
                if div and div > 0:
                    mise_base = entry.get("miseBase", 100)
                    return float(div) / float(mise_base)
    return None


def _extract_couple_gagnant_dividend(rapports: list, horse_nums: tuple[int, int] | None = None) -> float | None:
    """Parse E_COUPLE_GAGNANT dividend from rapports-définitifs response.

    Returns gross return per 1€ staked (e.g. 2.90), or None if not found.
    `horse_nums` is ignored for now — we take the first non-NP rapport entry.
    dividendePourUnEuro is already expressed per 1€ in centimes (290 → 2.90).
    """
    for entry in rapports:
        if entry.get("typePari") != "E_COUPLE_GAGNANT":
            continue
        for rap in entry.get("rapports", []):
            combo = rap.get("combinaison", "")
            if "NP" in combo:
                continue
            div = rap.get("dividendePourUnEuro") or rap.get("dividende")
            if div and div > 0:
                mise_base = entry.get("miseBase", 100)
                return float(div) / float(mise_base)
    return None


def resolve_bets(
    conn: duckdb.DuckDBPyConnection,
    date: str,
) -> pd.DataFrame:
    """Resolve pending bets for *date* using actual finish positions.

    Applies the same P&L rules as backtest.py:
      win:   hit = pos1 == 1;  pnl = morning_odds - 1  or -1
      place: hit = pos1 <= 3 (<=2 if field<5); pnl = morning_odds/4 - 1  or -1
      duo:   hit = {pos1,pos2} == {1,2}; pnl = E_COUPLE_GAGNANT_dividend * stake - stake  or -2

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
        SELECT ru.runner_id, ru.race_id, ru.finish_position
        FROM runners ru
        WHERE ru.runner_id IN ({placeholders})
          AND ru.scratch = FALSE
    """
    runners_df = conn.execute(runners_sql, all_runner_ids).df()
    pos_map   = dict(zip(runners_df["runner_id"], runners_df["finish_position"]))

    # Field size: count ALL non-scratch runners per race (not just bet runners)
    race_id_list = list(runners_df["race_id"].unique())
    if race_id_list:
        ph_races = ", ".join(["?"] * len(race_id_list))
        field_df = conn.execute(f"""
            SELECT race_id, COUNT(*) AS field_size
            FROM runners WHERE race_id IN ({ph_races}) AND scratch = FALSE
            GROUP BY race_id
        """, race_id_list).df()
        race_field_map = dict(zip(field_df["race_id"], field_df["field_size"]))
    else:
        race_field_map = {}
    # Map runner_id → field_size via its race
    runner_race = dict(zip(runners_df["runner_id"], runners_df["race_id"]))
    field_map = {rid: race_field_map.get(runner_race[rid], 8) for rid in runner_race}

    # Actual final odds (dernierRapportDirect) = real dividend used for P&L.
    # Take the latest snapshot per runner in case multiple were stored.
    final_odds_sql = f"""
        SELECT runner_id, decimal_odds
        FROM odds
        WHERE runner_id IN ({placeholders}) AND odds_type = 'final'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY runner_id ORDER BY snapshot_time DESC) = 1
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

    # Fetch rapports définitifs for races that have place or duo bets.
    # We need these for actual SIMPLE_PLACE and COUPLE_GAGNANT dividends.
    place_race_ids = set(pending_df.loc[pending_df["bet_type"] == "place", "race_id"].unique())
    duo_race_ids = set(pending_df.loc[pending_df["bet_type"] == "duo", "race_id"].unique())
    rapport_race_ids = list(place_race_ids | duo_race_ids)

    race_info_df = conn.execute(
        f"""
        SELECT race_id, date, reunion_number, course_number
        FROM races
        WHERE race_id IN ({', '.join(['?'] * len(rapport_race_ids))})
        """,
        rapport_race_ids,
    ).df() if rapport_race_ids else pd.DataFrame()

    # race_id → full rapports list (reused for both place and duo lookups)
    rapports_cache: dict[str, list] = {}
    dividend_map: dict[str, float | None] = {}
    if not race_info_df.empty:
        with PMUClient() as pmu:
            for _, ri in race_info_df.iterrows():
                rapports = pmu.fetch_rapports_definitifs(
                    ri["date"], int(ri["reunion_number"]), int(ri["course_number"])
                )
                rapports_cache[ri["race_id"]] = rapports
                if ri["race_id"] in duo_race_ids:
                    dividend_map[ri["race_id"]] = _extract_couple_gagnant_dividend(rapports)

    # Map runner_id → horse_number for place dividend lookup
    horse_num_map: dict[str, int] = {}
    if all_runner_ids:
        hn_df = conn.execute(f"""
            SELECT runner_id, horse_number FROM runners
            WHERE runner_id IN ({placeholders})
        """, all_runner_ids).df()
        horse_num_map = dict(zip(hn_df["runner_id"], hn_df["horse_number"]))


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
            # Use the actual final dividend (dernierRapportDirect); fall back to
            # the reference morning odds only if no final odds are available.
            odds = final_odds_map.get(runner_id_1) or (
                morning_odds if (morning_odds is not None and not pd.isna(morning_odds)) else None
            )
            if hit and odds is not None:
                pnl = (float(odds) - 1.0) * stake
            else:
                pnl = -stake
            status = "won" if hit else "lost"

        elif bet_type == "place":
            stake = UNIT_STAKE
            place_cutoff = 2 if field_size < 5 else 3
            hit = int(pos1) <= place_cutoff
            if hit:
                # Use actual E_SIMPLE_PLACE dividend from rapports définitifs
                horse_num = horse_num_map.get(runner_id_1)
                race_rapports = rapports_cache.get(race_id, [])
                place_div = _extract_place_dividend(race_rapports, horse_num) if horse_num else None
                if place_div is not None:
                    pnl = (place_div - 1.0) * stake
                else:
                    # Fallback: approximate as win odds / 4
                    odds = final_odds_map.get(runner_id_1) or (
                        morning_odds if (morning_odds is not None and not pd.isna(morning_odds)) else None
                    )
                    pnl = (float(odds) / 4.0 - 1.0) * stake if odds else -stake
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
                    # Use actual E_COUPLE_GAGNANT dividend when available;
                    # fall back to field²/4 approximation otherwise.
                    dividend = dividend_map.get(race_id)
                    if dividend is not None:
                        pnl = dividend * stake - stake
                    else:
                        raw_pnl = field_size ** 2 / 4.0
                        pnl = min(raw_pnl, 50.0) - stake
                else:
                    pnl = -stake
                status = "won" if hit else "lost"

        else:
            logger.warning("Unknown bet_type {} for bet {}", bet_type, bet_id)
            continue

        # Effective odds stored on the resolved bet = actual final dividend if available
        effective_odds = final_odds_map.get(runner_id_1) or (
            morning_odds if (morning_odds is not None and not pd.isna(morning_odds)) else None
        )

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
            "model_source": bet.get("model_source", "rule_based"),
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
