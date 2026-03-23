"""Streamlit dashboard — PMU Hippique Paper Trading (viewer)."""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import json

import streamlit as st
import streamlit.components.v1 as components

from config.settings import ROOT

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
        st.metric("P&L cumulé", f"{stats['pnl_total']:+.1f} €", f"{stats['n_total']} paris")
    st.divider()
    st.caption("Paper trading uniquement — Trot PMU")

# ── Main — tabs ───────────────────────────────────────────────────────────────
tab_bets, tab_perf, tab_model = st.tabs(["📋 Paris du jour", "📈 Performance", "🤖 Évaluation des modèles"])

with tab_bets:
    if selected_path is None:
        st.info(
            "Aucune fiche HTML disponible dans `data/reports/`. "
            "Le scheduler génère et pousse automatiquement les fiches chaque jour."
        )
    else:
        html_content = selected_path.read_text(encoding="utf-8")
        components.html(html_content, height=900, scrolling=True)

with tab_perf:
    perf_stats = _get_sidebar_stats()
    if perf_stats and perf_stats.get("daily"):
        import pandas as pd
        daily_df = pd.DataFrame(perf_stats["daily"]).set_index("date")
        daily_df.index.name = "Jour"
        daily_df.columns = ["P&L cumulé (€)"]
        st.line_chart(daily_df, color="#1565c0")
        st.caption(f"P&L cumulé WIN · LightGBM — {perf_stats['n_total']} paris résolus")
    else:
        st.info("Aucune donnée de performance disponible.")

with tab_model:
    if not MODEL_REPORT.exists():
        st.info("Le rapport modèle sera généré après le premier entraînement quotidien (08:00).")
    else:
        model_content = MODEL_REPORT.read_text(encoding="utf-8")
        components.html(model_content, height=1100, scrolling=True)
