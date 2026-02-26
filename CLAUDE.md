# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run Streamlit dashboard
streamlit run src/dashboard/app.py

# Run all tests
pytest tests/

# Run a single test file
pytest tests/test_scraper.py -v
```

## Project Status

Greenfield project — directory structure and `config/settings.py` are in place, but all `src/` modules are empty stubs. Implementation starts from scratch.

Full project context and domain knowledge is in `CONTEXT.md`.

## Architecture

**Pipeline flow:** `scraper/` → `features/` → `model/` → `trading/` → `dashboard/`

Each stage is a separate package under `src/`. The Streamlit app in `dashboard/` is the only user-facing entry point.

**Central config** (`config/settings.py`): all file paths, API URLs, and trading constants live here. Import from this module — never hardcode paths or magic numbers.

Key constants already defined:
- `DB_PATH` — DuckDB database at `data/processed/hippique.duckdb`
- `RAW_DIR` / `LOG_DIR` — data and log directories (both git-ignored)
- `PMU_BASE`, `PMU_REUNIONS`, `PMU_RACE`, `PMU_HORSE` — PMU API URL templates
- `KELLY_FRACTION = 0.25`, `EV_THRESHOLD = 1.0`, `UNIT_STAKE = 2.0`

## Key Technical Decisions

- **HTTP client:** `httpx` (not `requests`)
- **Logging:** `loguru` (not stdlib `logging`)
- **Storage:** DuckDB — use analytical SQL directly, avoid ORM abstractions
- **DataFrames:** Polars preferred for performance; Pandas acceptable for compatibility
- **Scheduling:** APScheduler for the daily pipeline
- **Secrets:** use `.env` file (loaded via `python-dotenv`); never hardcode credentials

## Domain Summary

French PMU trot-race prediction system. The core loop:
1. Scrape PMU API for today's races and horse data
2. Score each horse with rule-based features (form, jockey, odds drift)
3. Compare model probability vs implied pool probability
4. Log EV+ bets (where `model_prob > implied_prob * EV_THRESHOLD`) with Kelly-sized stakes
5. After races, record actual results and compute paper P&L

Only **Trot** races. Paper trading only until ROI is validated over 30+ race days.
