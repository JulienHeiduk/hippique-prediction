"""HTML bet report generator."""
from __future__ import annotations

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
      <span class="badge badge-{bet_type}">{bet_type_label}</span>
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
        "SELECT * FROM bets WHERE date = ? ORDER BY race_id, bet_type",
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
    n_total   = len(bets)
    n_won     = sum(1 for b in bets if b.get("status") == "won")
    n_lost    = sum(1 for b in bets if b.get("status") == "lost")
    n_pending = sum(1 for b in bets if b.get("status") == "pending")
    resolved  = [b for b in bets if b.get("status") in ("won", "lost")]
    total_budget = sum(b.get("stake") or 0 for b in bets)
    total_stake = sum(b.get("stake") or 0 for b in resolved)
    total_pnl   = sum(b.get("pnl")   or 0 for b in resolved)
    roi = total_pnl / total_stake if total_stake > 0 else None

    # Per-model P&L
    rb_resolved   = [b for b in resolved if b.get("model_source", "rule_based") == "rule_based"]
    lgbm_resolved = [b for b in resolved if b.get("model_source") == "lgbm"]
    rb_pnl   = sum(b.get("pnl") or 0 for b in rb_resolved)
    lgbm_pnl = sum(b.get("pnl") or 0 for b in lgbm_resolved)

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
            _card(f"{total_budget:.1f} €", "budget du jour"),
            _card(f"{total_pnl:+.1f} €", "P&amp;L", pnl_class),
        ]
        if roi is not None:
            cards.append(_card(f"{roi:+.0%}", "ROI", roi_class))
        if rb_resolved:
            cards.append(_card(
                f"{rb_pnl:+.1f} €",
                "P&amp;L WIN (Règles)",
                "val-pos" if rb_pnl >= 0 else "val-neg",
            ))
        if lgbm_resolved:
            cards.append(_card(
                f"{lgbm_pnl:+.1f} €",
                "P&amp;L DUO (LightGBM)",
                "val-pos" if lgbm_pnl >= 0 else "val-neg",
            ))
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

                model_source = b.get("model_source", "rule_based")
                if model_source == "lgbm":
                    model_badge = '<span class="model-badge model-lgbm">LightGBM</span>'
                else:
                    model_badge = '<span class="model-badge model-rule">Règles</span>'

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
                stake = b.get("stake") or 0.0
                pnl   = b.get("pnl")
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
