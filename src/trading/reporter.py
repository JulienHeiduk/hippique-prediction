"""HTML bet report generator."""
from __future__ import annotations

import base64
import io
from datetime import datetime
from pathlib import Path

import duckdb

from config.settings import ROOT


REPORTS_DIR = ROOT / "data" / "reports"

_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Paris PMU — {date_label}</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
    font-size: 14px;
    background: #f4f5f7;
    color: #1a1a2e;
    padding: 24px 16px;
  }}
  h1 {{
    font-size: 22px;
    font-weight: 700;
    margin-bottom: 6px;
    color: #1a1a2e;
  }}
  .subtitle {{
    font-size: 13px;
    color: #666;
    margin-bottom: 16px;
  }}
  /* ── Summary bar ── */
  .summary {{
    display: flex;
    gap: 12px;
    flex-wrap: wrap;
    margin-bottom: 24px;
  }}
  .summary-card {{
    background: #fff;
    border-radius: 8px;
    padding: 12px 18px;
    box-shadow: 0 1px 3px rgba(0,0,0,.08);
    min-width: 110px;
  }}
  .summary-card .val {{
    font-size: 22px;
    font-weight: 700;
    color: #1a1a2e;
  }}
  .summary-card .lbl {{
    font-size: 11px;
    color: #888;
    margin-top: 2px;
  }}
  .val-pos {{ color: #2e7d32 !important; }}
  .val-neg {{ color: #c62828 !important; }}
  /* ── Race blocks ── */
  .race-block {{
    background: #fff;
    border-radius: 10px;
    box-shadow: 0 1px 4px rgba(0,0,0,.10);
    margin-bottom: 20px;
    overflow: hidden;
  }}
  .race-header {{
    background: #1a1a2e;
    color: #fff;
    padding: 12px 16px;
    display: flex;
    align-items: center;
    gap: 14px;
    flex-wrap: wrap;
  }}
  .race-ref {{
    font-size: 18px;
    font-weight: 700;
    letter-spacing: .5px;
  }}
  .race-meta {{
    font-size: 12px;
    color: #aab;
    display: flex;
    gap: 12px;
    flex-wrap: wrap;
  }}
  .race-meta span::before {{ content: "·"; margin-right: 6px; }}
  .race-meta span:first-child::before {{ content: ""; margin-right: 0; }}
  .pmu-link {{
    margin-left: auto;
    font-size: 11px;
    font-weight: 600;
    color: #8ab4f8;
    text-decoration: none;
    white-space: nowrap;
    border: 1px solid rgba(138,180,248,.4);
    border-radius: 4px;
    padding: 3px 8px;
  }}
  .pmu-link:hover {{ background: rgba(138,180,248,.15); }}
  /* ── Bet rows ── */
  .bet-row {{
    border-top: 1px solid #eef0f4;
    padding: 14px 16px;
    display: grid;
    grid-template-columns: 90px 1fr auto;
    gap: 10px;
    align-items: start;
  }}
  .bet-row.row-won  {{ background: #f1faf2; }}
  .bet-row.row-lost {{ background: #fff8f8; }}
  .badge {{
    display: inline-block;
    padding: 3px 9px;
    border-radius: 5px;
    font-size: 11px;
    font-weight: 700;
    letter-spacing: .8px;
    text-transform: uppercase;
  }}
  .badge-win   {{ background: #e8f5e9; color: #2e7d32; }}
  .badge-duo   {{ background: #e3f2fd; color: #1565c0; }}
  .badge-place {{ background: #fff8e1; color: #e65100; }}
  .status-badge {{
    display: inline-block;
    padding: 2px 7px;
    border-radius: 4px;
    font-size: 10px;
    font-weight: 700;
    letter-spacing: .6px;
    text-transform: uppercase;
    margin-left: 6px;
    vertical-align: middle;
  }}
  .status-won     {{ background: #2e7d32; color: #fff; }}
  .status-lost    {{ background: #c62828; color: #fff; }}
  .status-pending {{ background: #757575; color: #fff; }}
  .model-badge {{
    display: inline-block;
    padding: 2px 7px;
    border-radius: 4px;
    font-size: 10px;
    font-weight: 700;
    letter-spacing: .6px;
    text-transform: uppercase;
    margin-left: 6px;
    vertical-align: middle;
  }}
  .model-rule {{ background: #fff8e1; color: #f57f17; }}
  .model-lgbm {{ background: #e8eaf6; color: #283593; }}
  .horse-line {{
    font-size: 15px;
    font-weight: 600;
    color: #1a1a2e;
    margin-bottom: 3px;
  }}
  .horse-num {{
    display: inline-block;
    background: #1a1a2e;
    color: #fff;
    border-radius: 4px;
    padding: 1px 6px;
    font-size: 13px;
    font-weight: 700;
    margin-right: 6px;
  }}
  .bet-stats {{
    font-size: 12px;
    color: #555;
    margin-top: 4px;
  }}
  .bet-stats span {{ margin-right: 12px; }}
  .result-line {{
    font-size: 12px;
    margin-top: 5px;
    color: #444;
  }}
  .finish-pos {{
    font-weight: 700;
  }}
  .winner-name {{
    color: #2e7d32;
    font-weight: 600;
  }}
  .ev {{ font-weight: 700; }}
  .ev-high {{ color: #2e7d32; }}
  .ev-mid  {{ color: #e65100; }}
  /* ── Odds completeness tag ── */
  .odds-tag {{
    font-size: 11px;
    font-weight: 600;
    padding: 3px 8px;
    border-radius: 4px;
    white-space: nowrap;
  }}
  .odds-ok      {{ background: rgba(46,125,50,.25);  color: #a5d6a7; }}
  .odds-missing {{ background: rgba(230,81,0,.30);   color: #ffcc80; }}
  /* ── Stake / P&L column ── */
  .stake-col {{
    text-align: right;
    white-space: nowrap;
  }}
  .stake-amount {{
    font-size: 18px;
    font-weight: 700;
    color: #1a1a2e;
  }}
  .pnl-pos {{ font-size: 18px; font-weight: 700; color: #2e7d32; }}
  .pnl-neg {{ font-size: 18px; font-weight: 700; color: #c62828; }}
  .stake-label {{ font-size: 11px; color: #888; }}
  /* ── Empty state ── */
  .no-bets {{
    background: #fff;
    border-radius: 10px;
    padding: 32px;
    text-align: center;
    color: #888;
    font-size: 15px;
  }}
  .footer {{
    margin-top: 28px;
    font-size: 11px;
    color: #aaa;
    text-align: center;
  }}
  @media print {{
    body {{ background: #fff; padding: 8px; }}
    .race-block {{ box-shadow: none; border: 1px solid #ccc; }}
    .race-header {{ background: #333 !important; -webkit-print-color-adjust: exact; }}
    .bet-row.row-won  {{ background: #f1faf2 !important; -webkit-print-color-adjust: exact; }}
    .bet-row.row-lost {{ background: #fff8f8 !important; -webkit-print-color-adjust: exact; }}
  }}
</style>
</head>
<body>
<h1>Paris du jour — {date_label}</h1>
<p class="subtitle">Généré le {generated_at}</p>
{summary_html}
{body}
<div class="footer">Système hippique PMU — paper trading uniquement</div>
</body>
</html>
"""

_SUMMARY_CARD = '<div class="summary-card"><div class="val {val_class}">{val}</div><div class="lbl">{lbl}</div></div>'

_RACE_BLOCK = """\
<div class="race-block">
  <div class="race-header">
    <span class="race-ref">{hippodrome} · {race_ref}</span>
    <span class="race-meta">
      {time_span}{distance_span}{field_span}
    </span>
    {odds_tag}
    {pmu_link}
  </div>
  {bet_rows}
</div>
"""

_BET_ROW = """\
  <div class="bet-row {row_class}">
    <div>
      <span class="status-badge status-{status}">{status_label}</span>
      {model_badge}
    </div>
    <div>
      <div class="horse-line">{horses}</div>
      <div class="bet-stats">
        <span>Cote : <strong>{odds}</strong></span>
        <span>Modèle : <strong>{model_pct}</strong></span>
        <span>Marché : <strong>{implied_pct}</strong></span>
        <span class="ev {ev_class}">EV {ev_ratio}</span>
      </div>
      {result_line}
    </div>
    <div class="stake-col">
      {pnl_html}
      <div class="stake-label">{amount_label}</div>
    </div>
  </div>
"""


def _pmu_url(date_yyyymmdd: str, r_num: int | None, c_num: int | None) -> str | None:
    """Build the PMU race page URL: https://www.pmu.fr/turf/DDMMYYYY/Rn/Cc/"""
    if not r_num or not c_num:
        return None
    ddmmyyyy = date_yyyymmdd[6:8] + date_yyyymmdd[4:6] + date_yyyymmdd[:4]
    return f"https://www.pmu.fr/turf/{ddmmyyyy}/R{r_num}/C{c_num}/"


def _horse_tag(horse_num: int | None, horse_name: str | None) -> str:
    num_html = f'<span class="horse-num">#{horse_num}</span>' if horse_num else ""
    name = horse_name or "?"
    return f"{num_html}{name}"


def _extract_horse_num(runner_id: str) -> int | None:
    """Extract horse number from runner_id = '{race_id}-{horse_num}'."""
    try:
        return int(runner_id.rsplit("-", 1)[-1])
    except (ValueError, IndexError):
        return None


def _ordinal(n: int) -> str:
    return "1er" if n == 1 else f"{n}e"


def export_bets_html(
    conn: duckdb.DuckDBPyConnection,
    date: str,
) -> Path:
    """Generate an HTML bet sheet for *date* and save to data/reports/bets_{date}.html.

    Queries bets fresh from the DB so the file always reflects the current
    status (pending / won / lost) and P&L when called after resolve_bets().

    Args:
        conn:  DuckDB connection.
        date:  YYYYMMDD string.

    Returns:
        Path to the generated HTML file.
    """
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    output_path = REPORTS_DIR / f"bets_{date}.html"

    # ── 1. Load bets from DB ────────────────────────────────────────────────
    bets_df = conn.execute(
        "SELECT * FROM bets WHERE date = ? AND bet_type IN ('win', 'place') ORDER BY race_id",
        [date],
    ).df()

    bets = bets_df.to_dict("records") if not bets_df.empty else []

    # ── 2. Race metadata ────────────────────────────────────────────────────
    race_ids = list({b["race_id"] for b in bets}) if bets else []
    race_meta: dict[str, dict] = {}
    if race_ids:
        placeholders = ", ".join(["?"] * len(race_ids))
        rows = conn.execute(
            f"""
            SELECT race_id, hippodrome, race_datetime,
                   distance_metres, field_size,
                   reunion_number, course_number
            FROM races WHERE race_id IN ({placeholders})
            """,
            race_ids,
        ).df()
        for _, row in rows.iterrows():
            race_meta[row["race_id"]] = row.to_dict()

    # ── 3. Missing morning odds count per race ──────────────────────────────
    missing_odds: dict[str, int] = {}
    if race_ids:
        placeholders = ", ".join(["?"] * len(race_ids))
        odds_cov = conn.execute(
            f"""
            SELECT ru.race_id,
                   COUNT(DISTINCT ru.runner_id)                                    AS total_runners,
                   COUNT(DISTINCT CASE WHEN o.odds_type IN ('morning', 'final')
                                       THEN o.runner_id END)                       AS runners_with_odds
            FROM runners ru
            LEFT JOIN odds o ON o.runner_id = ru.runner_id
            WHERE ru.race_id IN ({placeholders})
              AND ru.scratch = FALSE
            GROUP BY ru.race_id
            """,
            race_ids,
        ).df()
        for _, row in odds_cov.iterrows():
            missing_odds[row["race_id"]] = int(row["total_runners"]) - int(row["runners_with_odds"])

    # ── 4. Finish positions for all runners today ───────────────────────────
    # runner_id → {finish_position, horse_name}
    finish_info: dict[str, dict] = {}
    # race_id → winner horse_name (finish_position = 1)
    race_winners: dict[str, str] = {}
    if race_ids:
        placeholders = ", ".join(["?"] * len(race_ids))
        res = conn.execute(
            f"""
            SELECT runner_id, race_id, horse_name, finish_position
            FROM runners
            WHERE race_id IN ({placeholders}) AND scratch = FALSE
            """,
            race_ids,
        ).df()
        for _, row in res.iterrows():
            finish_info[row["runner_id"]] = {
                "finish_position": row["finish_position"],
                "horse_name": row["horse_name"],
            }
            try:
                if int(row["finish_position"]) == 1:
                    race_winners[row["race_id"]] = row["horse_name"]
            except (TypeError, ValueError):
                pass

    # ── 5. Summary stats ────────────────────────────────────────────────────
    _SCALE = 10  # display multiplier: historical bets stored at 2€, shown at 20€
    n_total   = len(bets)
    n_won     = sum(1 for b in bets if b.get("status") == "won")
    n_lost    = sum(1 for b in bets if b.get("status") == "lost")
    n_pending = sum(1 for b in bets if b.get("status") == "pending")
    resolved  = [b for b in bets if b.get("status") in ("won", "lost")]
    total_budget = sum(b.get("stake") or 0 for b in bets) * _SCALE
    total_stake = sum(b.get("stake") or 0 for b in resolved) * _SCALE
    total_pnl   = sum(b.get("pnl")   or 0 for b in resolved) * _SCALE
    roi = total_pnl / total_stake if total_stake > 0 else None

    def _card(val: str, lbl: str, val_class: str = "") -> str:
        return _SUMMARY_CARD.format(val=val, lbl=lbl, val_class=val_class)

    pnl_class = "val-pos" if total_pnl >= 0 else "val-neg"
    roi_class = "val-pos" if (roi or 0) >= 0 else "val-neg"

    summary_html = ""
    if n_total:
        cards = [
            _card(str(n_total),   "paris total"),
            _card(f"{n_won}/{n_won+n_lost}" if (n_won + n_lost) else "0/0", "gagnés"),
            _card(str(n_pending), "en attente"),
        ]
        if n_won + n_lost > 0:
            cards.append(_card(f"{total_pnl:+.1f} €", "gain du jour", pnl_class))
        if roi is not None:
            cards.append(_card(f"{roi:+.0%}", "ROI", roi_class))
        summary_html = '<div class="summary">' + "".join(cards) + "</div>"

    # ── 6. Group bets by race ───────────────────────────────────────────────
    races_seen: list[str] = []
    bets_by_race: dict[str, list[dict]] = {}
    for b in bets:
        rid = b["race_id"]
        if rid not in bets_by_race:
            races_seen.append(rid)
            bets_by_race[rid] = []
        bets_by_race[rid].append(b)

    # Sort races by start time so the HTML is ordered chronologically
    def _race_sort_key(race_id: str):
        dt = race_meta.get(race_id, {}).get("race_datetime")
        if dt is None or str(dt) in ("None", "NaT", ""):
            return ""
        return dt.strftime("%H:%M") if hasattr(dt, "strftime") else str(dt)[11:16]

    races_seen.sort(key=_race_sort_key)

    # ── 7. Build HTML body ──────────────────────────────────────────────────
    body_parts: list[str] = []

    if not bets:
        body_parts.append('<div class="no-bets">Aucun pari EV+ trouvé pour cette date.</div>')
    else:
        for race_id in races_seen:
            meta = race_meta.get(race_id, {})
            hippodrome = meta.get("hippodrome") or race_id
            r_num = meta.get("reunion_number")
            c_num = meta.get("course_number")
            race_ref = f"R{r_num}·C{c_num}" if r_num and c_num else race_id

            dt = meta.get("race_datetime")
            if dt and str(dt) not in ("None", "NaT", ""):
                try:
                    time_str = dt.strftime("%H:%M") if hasattr(dt, "strftime") else str(dt)[11:16]
                    time_span = f"<span>{time_str}</span>"
                except Exception:
                    time_span = ""
            else:
                time_span = ""

            dist  = meta.get("distance_metres")
            field = meta.get("field_size")
            distance_span = f"<span>{int(dist)} m</span>"  if dist  else ""
            field_span    = f"<span>{int(field)} partants</span>" if field else ""

            url = _pmu_url(date, r_num, c_num)
            pmu_link = (
                f'<a class="pmu-link" href="{url}" target="_blank">↗ pmu.fr</a>'
                if url else ""
            )

            n_missing = missing_odds.get(race_id, 0)
            if n_missing == 0:
                odds_tag = '<span class="odds-tag odds-ok">✓ Cotes complètes</span>'
            else:
                odds_tag = f'<span class="odds-tag odds-missing">⚠ {n_missing} cote{"s" if n_missing > 1 else ""} manquante{"s" if n_missing > 1 else ""}</span>'

            bet_rows_html = ""
            for b in bets_by_race[race_id]:
                bet_type = b.get("bet_type", "win")
                bet_type_label = {"win": "Gagnant", "place": "Placé", "duo": "Duo"}.get(bet_type, bet_type)
                status = b.get("status") or "pending"
                status_label = {"won": "✓ Gagné", "lost": "✗ Perdu", "pending": "⏳ En attente"}.get(status, status)
                row_class = {"won": "row-won", "lost": "row-lost", "pending": ""}.get(status, "")

                # Horses
                num1 = _extract_horse_num(b.get("runner_id_1") or "")
                horses_html = _horse_tag(num1, b.get("horse_name_1"))
                if bet_type == "duo" and b.get("runner_id_2"):
                    num2 = _extract_horse_num(b.get("runner_id_2") or "")
                    horses_html += f"  +  {_horse_tag(num2, b.get('horse_name_2'))}"

                model_badge = '<span class="model-badge model-lgbm">LightGBM</span>'

                morning_odds = b.get("morning_odds")
                odds_str   = f"{morning_odds:.2f}" if morning_odds else "N/A"
                model_prob = b.get("model_prob") or 0.0
                implied_prob = b.get("implied_prob") or 0.0
                ev_ratio   = b.get("ev_ratio") or 0.0
                ev_class   = "ev-high" if ev_ratio >= 1.3 else "ev-mid"

                # Result line (finish position + winner)
                result_line = ""
                if status in ("won", "lost"):
                    rid1 = b.get("runner_id_1") or ""
                    fp1  = finish_info.get(rid1, {}).get("finish_position")
                    parts = []
                    try:
                        parts.append(f'<span class="finish-pos">Arrivé {_ordinal(int(fp1))}</span>')
                    except (TypeError, ValueError):
                        pass
                    winner = race_winners.get(race_id)
                    if winner and winner != b.get("horse_name_1"):
                        parts.append(f'Vainqueur : <span class="winner-name">{winner}</span>')
                    if bet_type == "duo" and status == "lost":
                        rid2 = b.get("runner_id_2") or ""
                        fp2  = finish_info.get(rid2, {}).get("finish_position")
                        hn2  = b.get("horse_name_2") or ""
                        try:
                            parts.append(f'{hn2} → {_ordinal(int(fp2))}')
                        except (TypeError, ValueError):
                            pass
                    if parts:
                        result_line = f'<div class="result-line">{" · ".join(parts)}</div>'

                # P&L / stake column
                stake = (b.get("stake") or 0.0) * _SCALE
                pnl   = ((b.get("pnl") or 0.0) * _SCALE) if b.get("pnl") is not None else None
                if status == "won" and pnl is not None:
                    pnl_html     = f'<div class="pnl-pos">+{pnl:.1f} €</div>'
                    amount_label = "gain net"
                elif status == "lost" and pnl is not None:
                    pnl_html     = f'<div class="pnl-neg">{pnl:.1f} €</div>'
                    amount_label = "perte"
                else:
                    pnl_html     = f'<div class="stake-amount">{stake:.0f} €</div>'
                    amount_label = "mise"

                bet_rows_html += _BET_ROW.format(
                    row_class=row_class,
                    bet_type=bet_type,
                    bet_type_label=bet_type_label,
                    status=status,
                    status_label=status_label,
                    model_badge=model_badge,
                    horses=horses_html,
                    odds=odds_str,
                    model_pct=f"{model_prob:.0%}",
                    implied_pct=f"{implied_prob:.0%}",
                    ev_ratio=f"{ev_ratio:.2f}",
                    ev_class=ev_class,
                    result_line=result_line,
                    pnl_html=pnl_html,
                    amount_label=amount_label,
                )

            body_parts.append(_RACE_BLOCK.format(
                hippodrome=hippodrome.upper() if hippodrome else "?",
                race_ref=race_ref,
                time_span=time_span,
                distance_span=distance_span,
                field_span=field_span,
                odds_tag=odds_tag,
                pmu_link=pmu_link,
                bet_rows=bet_rows_html,
            ))

    date_label   = f"{date[6:8]}/{date[4:6]}/{date[:4]}"
    generated_at = datetime.now().strftime("%d/%m/%Y %H:%M")

    html = _HTML_TEMPLATE.format(
        date_label=date_label,
        generated_at=generated_at,
        summary_html=summary_html,
        body="\n".join(body_parts),
    )

    output_path.write_text(html, encoding="utf-8")
    return output_path


def export_model_report_html(conn: duckdb.DuckDBPyConnection) -> Path:
    """Generate the LightGBM model evaluation report.

    Saves to data/reports/model_report.html.
    """
    import pandas as pd
    from src.features.pipeline import compute_features
    from src.model.lgbm import load_lgbm_model, score_lgbm, train_lgbm, FEATURES

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    output_path = REPORTS_DIR / "model_report.html"
    generated_at = datetime.now().strftime("%d/%m/%Y %H:%M")

    # ── Load all historical features ─────────────────────────────────────────
    df = compute_features(conn)
    if df.empty:
        html = f"""<!DOCTYPE html><html lang="fr"><head><meta charset="UTF-8">
<title>Modèles — PMU</title></head><body>
<h2>Aucune donnée historique disponible.</h2>
<p style="color:#888">Généré le {generated_at}</p>
</body></html>"""
        output_path.write_text(html, encoding="utf-8")
        return output_path

    dates      = sorted(df["date"].unique())
    n_races    = df["race_id"].nunique()
    n_runners  = len(df)
    date_min   = f"{dates[0][6:8]}/{dates[0][4:6]}/{dates[0][:4]}"
    date_max   = f"{dates[-1][6:8]}/{dates[-1][4:6]}/{dates[-1][:4]}"

    holdout_n    = min(30, max(5, len(dates) // 5))
    holdout_dates = set(dates[-holdout_n:])
    train_df     = df[~df["date"].isin(holdout_dates)]
    holdout_df   = df[df["date"].isin(holdout_dates)]

    # ── Shared helpers ────────────────────────────────────────────────────────
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    def _ranking_metrics(eval_df: pd.DataFrame, scorer_fn):
        """top-1 acc, top-3 acc, NDCG@1, n_races."""
        scores = scorer_fn(eval_df)
        eval_df = eval_df.copy()
        eval_df["_score"] = eval_df["runner_id"].map(
            dict(zip(scores.index, scores.values))).fillna(0)
        top1, top3, ndcg1, n = 0, 0, 0.0, 0
        for _, rdf in eval_df.groupby("race_id"):
            if len(rdf) < 2:
                continue
            rdf = rdf.sort_values("_score", ascending=False).reset_index(drop=True)
            top_pick_pos   = int(rdf["finish_position"].iloc[0])
            winner_in_top3 = (rdf["finish_position"].iloc[:3] == 1).any()
            top1  += int(top_pick_pos == 1)
            top3  += int(winner_in_top3)
            ndcg1 += 1.0 if top_pick_pos == 1 else 0.0
            n     += 1
        if n == 0:
            return 0.0, 0.0, 0.0, 0
        return top1 / n, top3 / n, ndcg1 / n, n

    def _pct(v):
        color = "#2e7d32" if v >= 0.35 else ("#e65100" if v >= 0.25 else "#c62828")
        return f'<span style="color:{color};font-weight:700">{v:.1%}</span>'

    def _fig_to_b64(fig) -> str:
        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=130, bbox_inches="tight")
        plt.close(fig)
        return base64.b64encode(buf.getvalue()).decode()

    def _img(b64: str) -> str:
        return (f'<img src="data:image/png;base64,{b64}" '
                f'style="width:100%;max-width:800px;border-radius:8px;">')

    # ═════════════════════════════════════════════════════════════════════════
    # LightGBM (WIN bets)
    # ═════════════════════════════════════════════════════════════════════════
    lgbm_model = load_lgbm_model()
    lgbm_section = ""

    if lgbm_model is not None:
        n_trees = lgbm_model.num_trees()

        lgbm_scorer = lambda d, m=lgbm_model: score_lgbm(d, m)
        lgbm_is_top1, lgbm_is_top3, lgbm_is_ndcg, lgbm_n_is = _ranking_metrics(df, lgbm_scorer)

        lgbm_ho_top1, lgbm_ho_top3, lgbm_ho_ndcg, lgbm_n_ho = 0.0, 0.0, 0.0, 0
        if not holdout_df.empty and not train_df.empty:
            try:
                ho_lgbm = train_lgbm(train_df)
                ho_lgbm_scorer = lambda d, m=ho_lgbm: score_lgbm(d, m)
                lgbm_ho_top1, lgbm_ho_top3, lgbm_ho_ndcg, lgbm_n_ho = _ranking_metrics(
                    holdout_df, ho_lgbm_scorer)
            except Exception:
                pass

        # Feature importance chart
        importance = lgbm_model.feature_importance(importance_type="gain")
        feat_names = lgbm_model.feature_name()
        fi_df = pd.DataFrame({"feature": feat_names, "gain": importance})
        fi_df = fi_df.sort_values("gain", ascending=False).reset_index(drop=True)
        fi_df["gain_pct"] = fi_df["gain"] / fi_df["gain"].sum() * 100

        fi_chart_b64 = ""
        try:
            top_n  = min(20, len(fi_df))
            top_fi = fi_df.head(top_n).iloc[::-1]
            fig, ax = plt.subplots(figsize=(8, top_n * 0.38 + 1.2))
            ax.barh(top_fi["feature"], top_fi["gain_pct"], color="#1565c0", alpha=0.82)
            ax.set_xlabel("Importance (% gain)", fontsize=10)
            ax.set_title("Importance des variables — LightGBM LambdaRank",
                         fontsize=12, fontweight="bold")
            ax.spines[["top", "right"]].set_visible(False)
            ax.grid(axis="x", alpha=0.3)
            for bar, val in zip(ax.patches, top_fi["gain_pct"]):
                ax.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height() / 2,
                        f"{val:.1f}%", va="center", fontsize=8, color="#333")
            plt.tight_layout()
            fi_chart_b64 = _fig_to_b64(fig)
        except Exception:
            pass

        fi_rows = ""
        max_gain = fi_df["gain_pct"].max() or 1.0
        for i, row in fi_df.iterrows():
            bar_w = int(row["gain_pct"] / max_gain * 100)
            fi_rows += f"""
        <tr>
          <td>{i+1}</td>
          <td>{row['feature']}</td>
          <td>
            <div style="display:flex;align-items:center;gap:8px">
              <div style="background:#1565c0;height:10px;width:{bar_w}%;border-radius:3px;min-width:2px"></div>
              <span>{row['gain_pct']:.1f}%</span>
            </div>
          </td>
        </tr>"""

        lgbm_section = f"""
<div class="model-header lgbm-header">
  <span class="model-badge lgbm-badge">WIN · LightGBM</span>
  <span class="model-desc">LambdaRank · {n_trees} arbres · {len(FEATURES)} variables · réentraîné quotidiennement</span>
</div>

<div class="cards">
  <div class="card"><div class="val">{n_races:,}</div><div class="lbl">Courses (entraînement)</div></div>
  <div class="card"><div class="val">{n_runners:,}</div><div class="lbl">Chevaux</div></div>
  <div class="card"><div class="val">{n_trees}</div><div class="lbl">Arbres de décision</div></div>
  <div class="card"><div class="val">{len(FEATURES)}</div><div class="lbl">Variables</div></div>
</div>

<div class="section-title">Métriques de ranking</div>
<div class="metrics-grid">
  <div class="metric-card">
    <h3>In-sample — {lgbm_n_is:,} courses</h3>
    <div class="metric-row"><span>Top-1 accuracy</span>{_pct(lgbm_is_top1)}</div>
    <div class="metric-row"><span>Top-3 accuracy</span>{_pct(lgbm_is_top3)}</div>
    <div class="metric-row"><span>NDCG@1</span>{_pct(lgbm_is_ndcg)}</div>
    <p class="note">Modèle scoré sur ses propres données d'entraînement — optimiste.</p>
  </div>
  <div class="metric-card">
    <h3>Holdout <span class="tag">quasi out-of-sample</span> — {lgbm_n_ho:,} courses</h3>
    <div class="metric-row"><span>Top-1 accuracy</span>{_pct(lgbm_ho_top1)}</div>
    <div class="metric-row"><span>Top-3 accuracy</span>{_pct(lgbm_ho_top3)}</div>
    <div class="metric-row"><span>NDCG@1</span>{_pct(lgbm_ho_ndcg)}</div>
    <p class="note">Entraîné sur tout sauf les {holdout_n} derniers jours.</p>
  </div>
</div>

<div class="section-title">Importance des variables</div>
<div class="chart-wrap">{_img(fi_chart_b64) if fi_chart_b64 else ""}</div>

<table>
  <thead><tr><th>#</th><th>Variable</th><th>Importance (gain)</th></tr></thead>
  <tbody>{fi_rows}</tbody>
</table>
"""
    else:
        lgbm_section = """
<div class="model-header lgbm-header">
  <span class="model-badge lgbm-badge">WIN · LightGBM</span>
</div>
<p style="color:#888;padding:16px">Modèle LightGBM non disponible — lancez un entraînement à 08:00.</p>
"""

    # ═════════════════════════════════════════════════════════════════════════
    # Assemble final HTML
    # ═════════════════════════════════════════════════════════════════════════
    html = f"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Évaluation des modèles — PMU</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
          font-size: 14px; background: #f4f5f7; color: #1a1a2e; padding: 24px 16px; }}
  h1 {{ font-size: 22px; font-weight: 700; margin-bottom: 4px; }}
  .subtitle {{ font-size: 13px; color: #666; margin-bottom: 20px; }}
  .cards {{ display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 24px; }}
  .card {{ background: #fff; border-radius: 8px; padding: 14px 20px;
           box-shadow: 0 1px 3px rgba(0,0,0,.08); min-width: 120px; }}
  .card .val {{ font-size: 20px; font-weight: 700; color: #1a1a2e; }}
  .card .lbl {{ font-size: 11px; color: #888; margin-top: 2px; }}
  .section-title {{ font-size: 15px; font-weight: 700; margin: 22px 0 10px; color: #1a1a2e; }}
  .metrics-grid {{ display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 24px; }}
  .metric-card {{ background:#fff; border-radius:8px; padding:14px 20px;
                  box-shadow:0 1px 3px rgba(0,0,0,.08); flex:1; min-width:220px; }}
  .metric-card h3 {{ font-size:13px; color:#555; margin-bottom:10px; font-weight:600; }}
  .metric-row {{ display:flex; justify-content:space-between; font-size:13px;
                 margin-top:6px; padding-top:4px; border-top:1px solid #f0f0f0; }}
  .metric-row:first-of-type {{ border-top:none; margin-top:0; }}
  .chart-wrap {{ background:#fff; border-radius:10px; padding:16px;
                 box-shadow:0 1px 4px rgba(0,0,0,.1); margin-bottom:24px; }}
  table {{ width:100%; border-collapse:collapse; background:#fff; border-radius:10px;
           overflow:hidden; box-shadow:0 1px 4px rgba(0,0,0,.10); margin-bottom:24px; }}
  th {{ background:#1a1a2e; color:#fff; padding:10px 14px; text-align:left;
        font-size:12px; font-weight:600; }}
  td {{ padding:9px 14px; border-top:1px solid #eef0f4; font-size:13px; }}
  tr:hover td {{ background:#f8f9fb; }}
  .footer {{ margin-top:28px; font-size:11px; color:#aaa; text-align:center; }}
  .tag {{ display:inline-block; padding:2px 8px; border-radius:4px; font-size:11px;
          font-weight:700; background:#e8eaf6; color:#283593; margin-left:6px; }}
  .note {{ font-size:12px; color:#888; margin-top:8px; }}
  .model-header {{ display:flex; align-items:center; gap:12px; flex-wrap:wrap;
                   margin-bottom:20px; }}
  .model-badge {{ display:inline-block; padding:5px 14px; border-radius:6px;
                  font-size:13px; font-weight:700; letter-spacing:.5px; }}
  .rb-badge   {{ background:#fff3e0; color:#e65100; border:1px solid #ffcc80; }}
  .lgbm-badge {{ background:#e8eaf6; color:#283593; border:1px solid #9fa8da; }}
  .model-desc {{ font-size:13px; color:#666; }}
  .divider {{ height:2px; background:#e8eaf6; margin:32px 0 28px; border-radius:1px; }}
  code {{ background:#f0f0f0; padding:1px 5px; border-radius:3px; font-size:12px; }}
</style>
</head>
<body>
<h1>Évaluation des modèles</h1>
<p class="subtitle">Généré le {generated_at}</p>

{lgbm_section}

<div class="footer">Stratégie PMU — WIN · LightGBM</div>
</body>
</html>"""

    output_path.write_text(html, encoding="utf-8")
    return output_path


def export_performance_html(conn: duckdb.DuckDBPyConnection) -> Path:
    """Generate a cumulative performance report and save to data/reports/performance.html."""
    import pandas as pd

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    output_path = REPORTS_DIR / "performance.html"

    # ── Load all resolved WIN bets ───────────────────────────────────────────
    bets_df = conn.execute("""
        SELECT date, bet_type, model_source, stake, pnl, status
        FROM bets
        WHERE status IN ('won', 'lost') AND bet_type IN ('win', 'place')
        ORDER BY date
    """).df()

    generated_at = datetime.now().strftime("%d/%m/%Y %H:%M")

    if bets_df.empty:
        html = f"""<!DOCTYPE html><html lang="fr"><head><meta charset="UTF-8">
<title>Performance — PMU</title></head><body>
<h2>Aucune donnée résolue disponible.</h2>
<p style="color:#888">Généré le {generated_at}</p></body></html>"""
        output_path.write_text(html, encoding="utf-8")
        return output_path

    bets_df["date"] = bets_df["date"].astype(str)
    bets_df["hit"] = bets_df["status"] == "won"

    # Keep only dates that have a corresponding bet sheet HTML
    existing_dates = {
        p.stem.replace("bets_", "")
        for p in REPORTS_DIR.glob("bets_*.html")
    }
    bets_df = bets_df[bets_df["date"].isin(existing_dates)]
    if bets_df.empty:
        html = f"""<!DOCTYPE html><html lang="fr"><head><meta charset="UTF-8">
<title>Performance — PMU</title></head><body>
<h2>Aucune donnée résolue disponible.</h2>
<p style="color:#888">Généré le {generated_at}</p></body></html>"""
        output_path.write_text(html, encoding="utf-8")
        return output_path

    # ── Per-day summary ──────────────────────────────────────────────────────
    daily = (
        bets_df.groupby("date")
        .agg(n_bets=("pnl", "count"), n_won=("hit", "sum"),
             stake=("stake", "sum"), pnl=("pnl", "sum"))
        .reset_index()
    )
    daily["roi"] = daily["pnl"] / daily["stake"]
    daily["cum_pnl"] = daily["pnl"].cumsum()
    daily["date_label"] = daily["date"].apply(
        lambda d: f"{d[6:8]}/{d[4:6]}/{d[:4]}"
    )

    # ── Overall stats ────────────────────────────────────────────────────────
    _SCALE = 10  # display multiplier: historical bets stored at 2€, shown at 20€
    total_bets  = int(bets_df["pnl"].count())
    total_stake = float(bets_df["stake"].sum()) * _SCALE
    total_pnl   = float(bets_df["pnl"].sum()) * _SCALE
    total_roi   = total_pnl / total_stake if total_stake else 0.0

    # Per-type stats for the dashboard sidecar
    win_df   = bets_df[bets_df["bet_type"] == "win"]
    place_df = bets_df[bets_df["bet_type"] == "place"]

    # ── Write stats.json sidecar for the Streamlit dashboard ─────────────────
    import json
    stats_payload = {
        "pnl_total": round(total_pnl, 2),
        "n_total":   total_bets,
        "pnl_win":   round(float(win_df["pnl"].sum()) * _SCALE, 2) if not win_df.empty else None,
        "n_win":     int(win_df["pnl"].count()),
        "pnl_place": round(float(place_df["pnl"].sum()) * _SCALE, 2) if not place_df.empty else None,
        "n_place":   int(place_df["pnl"].count()),
    }
    (REPORTS_DIR / "stats.json").write_text(json.dumps(stats_payload), encoding="utf-8")

    chart_b64 = ""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        fig, ax = plt.subplots(figsize=(10, 3.5))
        x = list(range(len(daily)))
        y = (daily["cum_pnl"] * _SCALE).tolist()
        ax.plot(x, y, color="#1565c0", linewidth=2.5, zorder=3, label="WIN + Placé")
        ax.fill_between(x, y, 0,
                        where=[v >= 0 for v in y], alpha=0.12, color="#2e7d32")
        ax.fill_between(x, y, 0,
                        where=[v < 0 for v in y], alpha=0.12, color="#c62828")
        ax.axhline(0, color="#aaa", linewidth=0.8, linestyle="--")
        ax.set_xticks(x)
        ax.set_xticklabels(daily["date_label"].tolist(), rotation=30, ha="right", fontsize=9)
        ax.set_ylabel("P&L cumulé (€)", fontsize=10)
        ax.set_title("P&L cumulé — stratégie WIN + Placé · LightGBM",
                     fontsize=12, fontweight="bold")
        ax.legend(fontsize=9, loc="upper left")
        ax.grid(axis="y", alpha=0.3)
        ax.spines[["top", "right"]].set_visible(False)
        plt.tight_layout()

        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=140, bbox_inches="tight")
        plt.close(fig)
        chart_b64 = base64.b64encode(buf.getvalue()).decode()
    except Exception:
        pass

    chart_html = (
        f'<img src="data:image/png;base64,{chart_b64}" '
        f'style="width:100%;max-width:900px;border-radius:8px;">'
        if chart_b64 else ""
    )

    # ── Daily rows ───────────────────────────────────────────────────────────
    def _pnl_style(v: float) -> str:
        color = "#2e7d32" if v >= 0 else "#c62828"
        return f'style="color:{color};font-weight:700"'

    rows_html = ""
    for _, r in daily.iterrows():
        pnl_s = f"{r['pnl'] * _SCALE:+.1f}"
        cum_s = f"{r['cum_pnl'] * _SCALE:+.1f}"
        roi_s = f"{r['roi']:+.0%}"
        hit_s = f"{r['n_won']}/{int(r['n_bets'])}"
        rows_html += f"""
        <tr>
          <td>{r['date_label']}</td>
          <td>{int(r['n_bets'])}</td>
          <td>{hit_s}</td>
          <td {_pnl_style(r['pnl'])}>{pnl_s} €</td>
          <td {_pnl_style(r['roi'])}>{roi_s}</td>
          <td {_pnl_style(r['cum_pnl'])}>{cum_s} €</td>
        </tr>"""

    pnl_class = "pos" if total_pnl >= 0 else "neg"
    roi_class = "pos" if total_roi >= 0 else "neg"

    html = f"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Performance — PMU Hippique</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif;
          font-size: 14px; background: #f4f5f7; color: #1a1a2e; padding: 24px 16px; }}
  h1 {{ font-size: 22px; font-weight: 700; margin-bottom: 4px; }}
  .subtitle {{ font-size: 13px; color: #666; margin-bottom: 20px; }}
  .cards {{ display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 24px; }}
  .card {{ background: #fff; border-radius: 8px; padding: 14px 20px;
           box-shadow: 0 1px 3px rgba(0,0,0,.08); min-width: 120px; }}
  .card .val {{ font-size: 22px; font-weight: 700; }}
  .card .lbl {{ font-size: 11px; color: #888; margin-top: 2px; }}
  .pos {{ color: #2e7d32; }}
  .neg {{ color: #c62828; }}
  .chart-wrap {{ background:#fff; border-radius:10px; padding:16px;
                 box-shadow:0 1px 4px rgba(0,0,0,.1); margin-bottom:24px; }}
  table {{ width: 100%; border-collapse: collapse; background: #fff;
           border-radius: 10px; overflow: hidden;
           box-shadow: 0 1px 4px rgba(0,0,0,.10); }}
  th {{ background: #1a1a2e; color: #fff; padding: 10px 14px;
        text-align: left; font-size: 12px; font-weight: 600; }}
  td {{ padding: 9px 14px; border-top: 1px solid #eef0f4; font-size: 13px; }}
  tr:hover td {{ background: #f8f9fb; }}
  .section-title {{ font-size: 16px; font-weight: 700; margin: 24px 0 10px; }}
  .breakdown {{ display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 24px; }}
  .bk-card {{ background:#fff; border-radius:8px; padding:14px 20px;
              box-shadow:0 1px 3px rgba(0,0,0,.08); flex:1; min-width:200px; }}
  .bk-card h3 {{ font-size:13px; color:#666; margin-bottom:8px; }}
  .bk-row {{ display:flex; justify-content:space-between; font-size:13px; margin-top:4px; }}
  .footer {{ margin-top: 28px; font-size: 11px; color: #aaa; text-align: center; }}
</style>
</head>
<body>
<h1>Performance — Stratégie Hybride</h1>
<p class="subtitle">WIN + Placé · LightGBM &nbsp;|&nbsp; Généré le {generated_at}</p>

<div class="cards">
  <div class="card"><div class="val {pnl_class}">{total_pnl:+.1f} €</div><div class="lbl">P&amp;L cumulé</div></div>
  <div class="card"><div class="val {roi_class}">{total_roi:+.0%}</div><div class="lbl">ROI global</div></div>
  <div class="card"><div class="val">{total_bets}</div><div class="lbl">Paris résolus</div></div>
  <div class="card"><div class="val">{len(daily)}</div><div class="lbl">Jours actifs</div></div>
</div>

<div class="chart-wrap">{chart_html}</div>

<div class="section-title">Détail par jour</div>
<table>
  <thead><tr>
    <th>Date</th><th>Paris</th><th>Gagnés</th>
    <th>P&amp;L</th><th>ROI</th><th>P&amp;L cumulé</th>
  </tr></thead>
  <tbody>{rows_html}</tbody>
</table>

<div class="footer">Système hippique PMU — paper trading uniquement</div>
</body>
</html>"""

    output_path.write_text(html, encoding="utf-8")
    return output_path
