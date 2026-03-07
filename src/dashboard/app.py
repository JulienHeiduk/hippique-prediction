"""Streamlit dashboard — PMU Hippique Paper Trading (viewer)."""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import duckdb
import streamlit as st
import streamlit.components.v1 as components

from config.settings import ROOT, DB_PATH

REPORTS_DIR = ROOT / "data" / "reports"
MODEL_REPORT = REPORTS_DIR / "model_report.html"


@st.cache_data(ttl=300)
def _get_pnl_stats() -> dict | None:
    """Query cumulative P&L, basic stats and daily series from resolved bets (read-only)."""
    if not DB_PATH.exists():
        return None
    try:
        import pandas as pd
        conn = duckdb.connect(str(DB_PATH), read_only=True)
        row = conn.execute("""
            SELECT
                COUNT(*)                                       AS n_bets,
                SUM(CASE WHEN status='won' THEN 1 ELSE 0 END) AS n_won,
                SUM(pnl)                                       AS total_pnl,
                SUM(stake)                                     AS total_stake
            FROM bets
            WHERE status IN ('won', 'lost')
        """).fetchone()
        daily = conn.execute("""
            SELECT date, SUM(pnl) AS pnl
            FROM bets
            WHERE status IN ('won', 'lost')
            GROUP BY date
            ORDER BY date
        """).df()
        conn.close()
        if row is None or row[0] == 0:
            return None
        n_bets, n_won, total_pnl, total_stake = row
        daily["date_label"] = daily["date"].apply(
            lambda d: f"{str(d)[6:8]}/{str(d)[4:6]}"
        )
        daily["cum_pnl"] = daily["pnl"].cumsum()
        return {
            "n_bets": int(n_bets),
            "n_won": int(n_won),
            "total_pnl": float(total_pnl),
            "roi": float(total_pnl) / float(total_stake) if total_stake else 0.0,
            "daily": daily,
        }
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
    pnl_stats = _get_pnl_stats()
    if pnl_stats:
        delta_color = "normal" if pnl_stats["total_pnl"] >= 0 else "inverse"
        st.metric(
            "P&L cumulé",
            f"{pnl_stats['total_pnl']:+.1f} €",
            delta=f"ROI {pnl_stats['roi']:+.0%}",
            delta_color=delta_color,
        )
        st.caption(f"{pnl_stats['n_won']}/{pnl_stats['n_bets']} paris gagnés")
        daily = pnl_stats["daily"]
        if len(daily) > 1:
            import pandas as pd
            chart_df = daily.set_index("date_label")[["cum_pnl"]].rename(
                columns={"cum_pnl": "P&L cumulé (€)"}
            )
            st.line_chart(chart_df, height=160)
    st.divider()
    st.caption("Paper trading uniquement — Trot PMU")

# ── Main — tabs ───────────────────────────────────────────────────────────────
tab_bets, tab_model = st.tabs(["📋 Paris du jour", "🤖 Évaluation des modèles"])

with tab_bets:
    if selected_path is None:
        st.info(
            "Aucune fiche HTML disponible dans `data/reports/`. "
            "Le scheduler génère et pousse automatiquement les fiches chaque jour."
        )
    else:
        html_content = selected_path.read_text(encoding="utf-8")
        components.html(html_content, height=900, scrolling=True)

with tab_model:
    if not MODEL_REPORT.exists():
        st.info("Le rapport modèle sera généré après le premier entraînement quotidien (08:00).")
    else:
        model_content = MODEL_REPORT.read_text(encoding="utf-8")
        components.html(model_content, height=1100, scrolling=True)
