"""Streamlit dashboard — PMU Hippique Paper Trading (viewer)."""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import json

import duckdb
import streamlit as st

from config.settings import DB_PATH, ROOT

REPORTS_DIR = ROOT / "data" / "reports"
MODEL_REPORT = REPORTS_DIR / "model_report.html"
STATS_FILE   = REPORTS_DIR / "stats.json"


def _get_sidebar_stats() -> dict | None:
    """Read pre-computed stats from stats.json (written by export_performance_html)."""
    if not STATS_FILE.exists():
        return None
    try:
        return json.loads(STATS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return None


# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="PMU Hippique — Paper Trading",
    page_icon="🐎",
    layout="wide",
)

# ── Collect available reports ─────────────────────────────────────────────────
html_files = sorted(REPORTS_DIR.glob("bets_*.html"), reverse=True) if REPORTS_DIR.exists() else []


def _label(p: Path) -> str:
    stem = p.stem.replace("bets_", "")
    try:
        return datetime.strptime(stem, "%Y%m%d").strftime("%d/%m/%Y")
    except ValueError:
        return p.name


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("🐎 PMU Paper Trading")

    if not html_files:
        st.warning("Aucune fiche disponible.")
        selected_path: Path | None = None
    else:
        options = {_label(f): f for f in html_files}
        selected_label = st.selectbox("Date", list(options.keys()))
        selected_path = options[selected_label]

    st.divider()
    stats = _get_sidebar_stats()
    if stats:
        col_w, col_p = st.columns(2)
        col_w.metric("P&L WIN Trot", f"{stats.get('win_pnl_total', 0):+.1f} €")
        col_p.metric("P&L Placé Trot", f"{stats.get('place_pnl_total', 0):+.1f} €")
        col_wp, col_pp = st.columns(2)
        col_wp.metric("P&L WIN Plat", f"{stats.get('win_plat_pnl_total', 0):+.1f} €")
        col_pp.metric("P&L Placé Plat", f"{stats.get('place_plat_pnl_total', 0):+.1f} €")
    st.divider()
    st.caption("Paper trading uniquement — Trot & Plat PMU")

# ── Main — tabs ───────────────────────────────────────────────────────────────
tab_bets, tab_perf, tab_model = st.tabs(["📋 Paris du jour", "📈 Performance", "🤖 Évaluation des modèles"])

def _get_selected_date(path: Path | None) -> str | None:
    """Extract YYYYMMDD date string from a bets_YYYYMMDD.html path."""
    if path is None:
        return None
    return path.stem.replace("bets_", "")


def _delete_bet(bet_id: str, date: str) -> None:
    """Delete a bet from DuckDB and regenerate the HTML report for that date."""
    conn = duckdb.connect(str(DB_PATH))
    from src.scraper.storage import init_schema
    init_schema(conn)
    conn.execute("DELETE FROM bets WHERE bet_id = ?", [bet_id])
    # Regenerate the HTML report
    from src.trading.reporter import export_bets_html
    export_bets_html(conn, date)
    conn.close()


with tab_bets:
    if selected_path is None:
        st.info(
            "Aucune fiche HTML disponible dans `data/reports/`. "
            "Le scheduler génère et pousse automatiquement les fiches chaque jour."
        )
    else:
        sel_date = _get_selected_date(selected_path)

        # ── Load bets from DuckDB for delete buttons ──────────────────────
        bets_df = None
        if sel_date and DB_PATH.exists():
            try:
                conn = duckdb.connect(str(DB_PATH), read_only=True)
                bets_df = conn.execute(
                    "SELECT bet_id, horse_name_1, bet_type, hippodrome, morning_odds, "
                    "       model_prob, ev_ratio, stake, status "
                    "FROM bets WHERE date = ? ORDER BY race_id, bet_type",
                    [sel_date],
                ).df()
                conn.close()
            except Exception:
                bets_df = None

        # ── Show HTML report ──────────────────────────────────────────────
        html_content = selected_path.read_text(encoding="utf-8")
        st.html(html_content, height=900)

        # ── Delete buttons per bet ────────────────────────────────────────
        if bets_df is not None and not bets_df.empty:
            st.subheader("Supprimer un pari")
            for _, row in bets_df.iterrows():
                bet_id = row["bet_id"]
                bet_type_label = {"win": "WIN", "place": "PLACÉ"}.get(row["bet_type"], row["bet_type"])
                odds_str = f"{row['morning_odds']:.2f}" if row["morning_odds"] else "N/A"
                label = (
                    f"{row['horse_name_1']} · {bet_type_label} · "
                    f"cote {odds_str} · EV {row['ev_ratio']:.2f} · "
                    f"{row['hippodrome']}"
                )
                col_label, col_btn = st.columns([5, 1])
                col_label.write(label)
                if col_btn.button("Supprimer", key=f"del_{bet_id}"):
                    _delete_bet(bet_id, sel_date)
                    st.rerun()

with tab_perf:
    perf_stats = _get_sidebar_stats()
    if perf_stats and perf_stats.get("daily"):
        import pandas as pd
        daily_df = pd.DataFrame(perf_stats["daily"])
        daily_df["date"] = pd.to_datetime(daily_df["date"], format="%d/%m/%Y")
        daily_df = daily_df.sort_values("date")
        daily_df = daily_df.set_index("date")
        daily_df.index.name = "Jour"
        daily_df = daily_df.rename(columns={
            "win_cum_pnl": "P&L cumulé WIN Trot (€)",
            "place_cum_pnl": "P&L cumulé Placé Trot (€)",
            "win_plat_cum_pnl": "P&L cumulé WIN Plat (€)",
            "place_plat_cum_pnl": "P&L cumulé Placé Plat (€)",
        })
        chart_cols = ["P&L cumulé WIN Trot (€)", "P&L cumulé Placé Trot (€)"]
        if "P&L cumulé WIN Plat (€)" in daily_df.columns:
            chart_cols += ["P&L cumulé WIN Plat (€)", "P&L cumulé Placé Plat (€)"]
        st.line_chart(daily_df[chart_cols])
        st.caption("P&L cumulé WIN & Placé · Trot + Plat · LightGBM")
    else:
        st.info("Aucune donnée de performance disponible.")

with tab_model:
    if not MODEL_REPORT.exists():
        st.info("Le rapport modèle sera généré après le premier entraînement quotidien (08:00).")
    else:
        model_content = MODEL_REPORT.read_text(encoding="utf-8")
        st.html(model_content, height=1100)
