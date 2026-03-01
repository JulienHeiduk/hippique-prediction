"""Streamlit dashboard — PMU Hippique Paper Trading."""
from __future__ import annotations

import sys
from datetime import date, datetime
from pathlib import Path

# Ensure project root is on sys.path (required on Streamlit Cloud where only
# the app's own directory is added automatically).
_ROOT = Path(__file__).parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import streamlit as st
import streamlit.components.v1 as components

from config.settings import ROOT

REPORTS_DIR = ROOT / "data" / "reports"

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="PMU Hippique — Paper Trading",
    page_icon="🐎",
    layout="wide",
)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("🐎 PMU Paper Trading")
    selected_date = st.date_input("Date", value=date.today())
    date_str: str = selected_date.strftime("%Y%m%d")
    st.caption(f"Date sélectionnée : **{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}**")
    st.divider()
    st.caption("Paper trading uniquement — Trot PMU")

# ── Helpers ───────────────────────────────────────────────────────────────────

def _run_capturing_logs(fn, *args) -> tuple[str, Exception | None]:
    """Run *fn* and capture all loguru output into a string."""
    from loguru import logger

    lines: list[str] = []

    def _sink(msg):
        lines.append(str(msg).rstrip())

    handler_id = logger.add(_sink, format="{time:HH:mm:ss} | {level:<8} | {message}")
    err: Exception | None = None
    try:
        fn(*args)
    except Exception as exc:
        err = exc
    finally:
        logger.remove(handler_id)
    return "\n".join(lines), err


def _show_result(logs: str, err: Exception | None, label: str) -> None:
    if err:
        st.error(f"❌ Erreur : {err}")
    else:
        st.success(f"✅ {label} terminé !")
    if logs:
        with st.expander("Logs", expanded=bool(err)):
            st.code(logs, language=None)


# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_pipeline, tab_html = st.tabs(["⚙️ Pipeline", "📄 Fiche HTML"])


# ── Tab 1 — Pipeline ──────────────────────────────────────────────────────────
with tab_pipeline:
    st.header("Sessions quotidiennes")
    st.markdown(
        "Sélectionnez la date dans la barre latérale puis lancez la session correspondante."
    )

    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("🌅 Session Matin")
        st.caption("Scraping programme + génération des paris EV+ + HTML")
        if st.button("Lancer la session du matin", key="btn_morning", use_container_width=True):
            with st.spinner("Scraping + génération des paris…"):
                from src.trading.scheduler import run_morning_session
                logs, err = _run_capturing_logs(run_morning_session, date_str)
            _show_result(logs, err, "Session du matin")

    with col2:
        st.subheader("🔄 Mise à jour")
        st.caption("Résultats partiels + cotes actualisées + HTML")
        if st.button("Lancer la mise à jour", key="btn_update", use_container_width=True):
            with st.spinner("Re-scraping + résolution + génération HTML…"):
                from src.scraper import get_connection, run_pipeline
                from src.trading.engine import generate_bets, resolve_bets
                from src.trading.reporter import export_bets_html

                def _midday_session(d: str) -> None:
                    run_pipeline(d)
                    conn = get_connection()
                    try:
                        resolve_bets(conn, d)
                        generate_bets(conn, d)
                        export_bets_html(conn, d)
                    finally:
                        conn.close()

                logs, err = _run_capturing_logs(_midday_session, date_str)
            _show_result(logs, err, "Mise à jour")

    with col3:
        st.subheader("🌙 Session Soir")
        st.caption("Résultats finaux + résolution des paris + P&L")
        if st.button("Lancer la session du soir", key="btn_evening", use_container_width=True):
            with st.spinner("Scraping résultats + résolution des paris…"):
                from src.trading.scheduler import run_evening_session
                logs, err = _run_capturing_logs(run_evening_session, date_str)
            _show_result(logs, err, "Session du soir")


# ── Tab 2 — Fiche HTML ────────────────────────────────────────────────────────
with tab_html:
    st.header("Fiche de paris HTML")

    html_files = sorted(REPORTS_DIR.glob("bets_*.html"), reverse=True) if REPORTS_DIR.exists() else []

    if not html_files:
        st.info(
            "Aucune fiche disponible. "
            "Lancez d'abord une session depuis l'onglet **⚙️ Pipeline** pour en générer une."
        )
    else:
        def _label(p: Path) -> str:
            stem = p.stem.replace("bets_", "")
            try:
                return datetime.strptime(stem, "%Y%m%d").strftime("%d/%m/%Y")
            except ValueError:
                return p.name

        options = {_label(f): f for f in html_files}
        selected_label = st.selectbox("Choisir une fiche", list(options.keys()))
        selected_path = options[selected_label]

        col_info, col_dl = st.columns([3, 1])
        with col_info:
            st.caption(f"Fichier : `{selected_path.name}`")
        with col_dl:
            html_bytes = selected_path.read_bytes()
            st.download_button(
                label="⬇️ Télécharger",
                data=html_bytes,
                file_name=selected_path.name,
                mime="text/html",
                use_container_width=True,
            )

        height = st.slider("Hauteur de l'aperçu (px)", min_value=400, max_value=2000, value=900, step=100)
        html_content = selected_path.read_text(encoding="utf-8")
        components.html(html_content, height=height, scrolling=True)
