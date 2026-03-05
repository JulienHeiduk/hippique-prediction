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

REPORTS_DIR = ROOT / "data" / "reports"


def _get_cumulative_pnl() -> float | None:
    """Parse cumulative P&L from all HTML report files.

    Returns the total P&L float, or None when no resolved bets exist yet.
    """
    import re

    _VAL = r'<div class="val[^"]*">([+\-]?\d+\.?\d*)\s*€</div>\s*<div class="lbl">P&amp;L</div>'
    pat = re.compile(_VAL)

    total = 0.0
    found = False
    for html_file in sorted(REPORTS_DIR.glob("bets_*.html")):
        try:
            content = html_file.read_text(encoding="utf-8")
            m = pat.search(content)
            if m:
                total += float(m.group(1))
                found = True
        except Exception:
            pass

    return total if found else None

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
        pnl = _get_cumulative_pnl()
        if pnl is not None:
            st.metric("Total gains / pertes", f"{pnl:+.1f} €")

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
