"""Streamlit dashboard — PMU Hippique Paper Trading (viewer)."""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import re

import streamlit as st
import streamlit.components.v1 as components

from config.settings import ROOT

REPORTS_DIR = ROOT / "data" / "reports"
MODEL_REPORT = REPORTS_DIR / "model_report.html"
PERFORMANCE_REPORT = REPORTS_DIR / "performance.html"


@st.cache_data(ttl=300)
def _get_cumulative_pnl() -> tuple[float, float, float] | None:
    """Parse total / WIN / DUO cumulative P&L from performance.html."""
    if not PERFORMANCE_REPORT.exists():
        return None
    try:
        content = PERFORMANCE_REPORT.read_text(encoding="utf-8")
        pat = re.compile(
            r'([+\-]?\d+\.?\d*)\s*€</div>\s*<div class="lbl">P&amp;L cumulé([^<]*)</div>'
        )
        values: dict[str, float] = {}
        for m in pat.finditer(content):
            label = m.group(2).strip()  # "", "WIN", "DUO"
            values[label] = float(m.group(1))
        if "" in values:
            return values.get(""), values.get("WIN", 0.0), values.get("DUO", 0.0)
    except Exception:
        pass
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
    pnl_data = _get_cumulative_pnl()
    if pnl_data is not None:
        total, win, duo = pnl_data
        st.metric("P&L cumulé", f"{total:+.1f} €")
        st.metric("↳ WIN", f"{win:+.1f} €")
        st.metric("↳ DUO", f"{duo:+.1f} €")
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
