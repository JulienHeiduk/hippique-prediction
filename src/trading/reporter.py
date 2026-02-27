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
    margin-bottom: 28px;
  }}
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
  .bet-row {{
    border-top: 1px solid #eef0f4;
    padding: 14px 16px;
    display: grid;
    grid-template-columns: 90px 1fr auto;
    gap: 10px;
    align-items: start;
  }}
  .bet-row:first-of-type {{ border-top: none; }}
  .badge {{
    display: inline-block;
    padding: 3px 9px;
    border-radius: 5px;
    font-size: 11px;
    font-weight: 700;
    letter-spacing: .8px;
    text-transform: uppercase;
  }}
  .badge-win  {{ background: #e8f5e9; color: #2e7d32; }}
  .badge-duo  {{ background: #e3f2fd; color: #1565c0; }}
  .badge-place{{ background: #fff8e1; color: #e65100; }}
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
  .ev {{
    font-weight: 700;
  }}
  .ev-high  {{ color: #2e7d32; }}
  .ev-mid   {{ color: #e65100; }}
  .stake-col {{
    text-align: right;
    white-space: nowrap;
  }}
  .stake-amount {{
    font-size: 18px;
    font-weight: 700;
    color: #1a1a2e;
  }}
  .stake-label {{
    font-size: 11px;
    color: #888;
  }}
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
  }}
</style>
</head>
<body>
<h1>Paris du jour — {date_label}</h1>
<p class="subtitle">Généré le {generated_at} · {n_bets} pari(s) EV+</p>
{body}
<div class="footer">Système hippique PMU — paper trading uniquement</div>
</body>
</html>
"""

_RACE_BLOCK = """\
<div class="race-block">
  <div class="race-header">
    <span class="race-ref">{hippodrome} · {race_ref}</span>
    <span class="race-meta">
      {time_span}{distance_span}{field_span}
    </span>
  </div>
  {bet_rows}
</div>
"""

_BET_ROW = """\
  <div class="bet-row">
    <div><span class="badge badge-{bet_type}">{bet_type_label}</span></div>
    <div>
      <div class="horse-line">{horses}</div>
      <div class="bet-stats">
        <span>Cote : <strong>{odds}</strong></span>
        <span>Modèle : <strong>{model_pct}</strong></span>
        <span>Marché : <strong>{implied_pct}</strong></span>
        <span class="ev {ev_class}">EV {ev_ratio}</span>
      </div>
    </div>
    <div class="stake-col">
      <div class="stake-amount">{stake} €</div>
      <div class="stake-label">mise</div>
    </div>
  </div>
"""


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


def export_bets_html(
    bets: list[dict],
    conn: duckdb.DuckDBPyConnection,
    date: str,
) -> Path:
    """Generate an HTML bet sheet and save to data/reports/bets_{date}.html.

    Args:
        bets:  List of bet dicts returned by generate_bets().
        conn:  DuckDB connection (for race metadata lookups).
        date:  YYYYMMDD string.

    Returns:
        Path to the generated HTML file.
    """
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    output_path = REPORTS_DIR / f"bets_{date}.html"

    # Fetch race metadata for all races involved
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

    # Group bets by race_id
    races_seen: list[str] = []
    bets_by_race: dict[str, list[dict]] = {}
    for b in bets:
        rid = b["race_id"]
        if rid not in bets_by_race:
            races_seen.append(rid)
            bets_by_race[rid] = []
        bets_by_race[rid].append(b)

    # Build HTML body
    body_parts: list[str] = []

    if not bets:
        body_parts.append('<div class="no-bets">Aucun pari EV+ trouvé pour aujourd\'hui.</div>')
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
                    if hasattr(dt, "strftime"):
                        time_str = dt.strftime("%H:%M")
                    else:
                        time_str = str(dt)[11:16]
                    time_span = f"<span>{time_str}</span>"
                except Exception:
                    time_span = ""
            else:
                time_span = ""

            dist = meta.get("distance_metres")
            distance_span = f"<span>{int(dist)} m</span>" if dist else ""

            field = meta.get("field_size")
            field_span = f"<span>{int(field)} partants</span>" if field else ""

            # Bet rows for this race
            bet_rows_html = ""
            for b in bets_by_race[race_id]:
                bet_type = b.get("bet_type", "win")
                bet_type_label = {"win": "Gagnant", "place": "Placé", "duo": "Duo"}.get(bet_type, bet_type)

                # Horse(s)
                num1 = _extract_horse_num(b.get("runner_id_1", ""))
                horses_html = _horse_tag(num1, b.get("horse_name_1"))
                if bet_type == "duo" and b.get("runner_id_2"):
                    num2 = _extract_horse_num(b.get("runner_id_2", ""))
                    horses_html += f"  +  {_horse_tag(num2, b.get('horse_name_2'))}"

                morning_odds = b.get("morning_odds")
                odds_str = f"{morning_odds:.2f}" if morning_odds else "N/A"

                model_prob = b.get("model_prob") or 0.0
                implied_prob = b.get("implied_prob") or 0.0
                ev_ratio = b.get("ev_ratio") or 0.0

                ev_class = "ev-high" if ev_ratio >= 1.3 else "ev-mid"

                stake = b.get("stake") or 0.0

                bet_rows_html += _BET_ROW.format(
                    bet_type=bet_type,
                    bet_type_label=bet_type_label,
                    horses=horses_html,
                    odds=odds_str,
                    model_pct=f"{model_prob:.0%}",
                    implied_pct=f"{implied_prob:.0%}",
                    ev_ratio=f"{ev_ratio:.2f}",
                    ev_class=ev_class,
                    stake=f"{stake:.0f}",
                )

            body_parts.append(_RACE_BLOCK.format(
                hippodrome=hippodrome.upper() if hippodrome else "?",
                race_ref=race_ref,
                time_span=time_span,
                distance_span=distance_span,
                field_span=field_span,
                bet_rows=bet_rows_html,
            ))

    date_label = f"{date[6:8]}/{date[4:6]}/{date[:4]}"
    generated_at = datetime.now().strftime("%d/%m/%Y %H:%M")

    html = _HTML_TEMPLATE.format(
        date_label=date_label,
        generated_at=generated_at,
        n_bets=len(bets),
        body="\n".join(body_parts),
    )

    output_path.write_text(html, encoding="utf-8")
    return output_path
