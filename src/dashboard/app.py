"""Streamlit dashboard — PMU Hippique Paper Trading (viewer)."""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import streamlit as st
import streamlit.components.v1 as components

from config.settings import ROOT
from src.scraper import get_connection

REPORTS_DIR = ROOT / "data" / "reports"


def _get_cumulative_pnl() -> float | None:
    """Return total P&L across all resolved bets, or None on error."""
    try:
        conn = get_connection()
        result = conn.execute(
            "SELECT COALESCE(SUM(pnl), 0) FROM bets WHERE status IN ('won', 'lost')"
        ).fetchone()
        conn.close()
        return float(result[0]) if result else None
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
        cum_pnl = _get_cumulative_pnl()
        if cum_pnl is not None:
            color = "#2e7d32" if cum_pnl >= 0 else "#c62828"
            sign = "+" if cum_pnl >= 0 else ""
            st.markdown(
                f"""
                <div style="background:#fff;border-radius:8px;padding:12px 18px;
                            box-shadow:0 1px 3px rgba(0,0,0,.08);text-align:center;">
                  <div style="font-size:24px;font-weight:700;color:{color};">
                    {sign}{cum_pnl:.1f} €
                  </div>
                  <div style="font-size:11px;color:#888;margin-top:2px;">
                    Total gains / pertes
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    st.divider()
    st.caption("Paper trading uniquement — Trot PMU")

# ── Main — HTML viewer ────────────────────────────────────────────────────────
if selected_path is None:
    st.info(
        "Aucune fiche HTML disponible dans `data/reports/`. "
        "Le scheduler génère et pousse automatiquement les fiches chaque jour."
    )
else:
    html_content = selected_path.read_text(encoding="utf-8")
    components.html(html_content, height=900, scrolling=True)
