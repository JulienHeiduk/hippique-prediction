# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run Streamlit dashboard
streamlit run src/dashboard/app.py

# Run tests
pytest tests/

# Run a single test file
pytest tests/test_scraper.py -v
```

## Repository Structure

```
src/
├── scraper/     # PMU API + secondary source scrapers
├── features/    # Feature engineering (form, jockey, market signals)
├── model/       # Rule-based scorer → future LightGBM lambdarank
├── trading/     # EV+ detection, Kelly staking, P&L logging
└── dashboard/   # Streamlit app
data/
├── raw/         # Raw API responses (git-ignored)
└── processed/   # hippique.duckdb + cleaned datasets (git-ignored)
config/
└── settings.py  # Central config: paths, API URLs, trading constants
notebooks/       # Exploration & analysis
logs/            # Prediction logs, P&L records (git-ignored)
```

## Project Overview

A **paper-trading** horse race prediction system targeting positive expected value (EV+) bets on French PMU races (Trot only). The system compares model probability against the implied pool probability to identify mispriced combinations. No real money is involved until ROI is validated over 30+ race days.

The core challenge: PMU retains ~25-28% of the pool, so the model must outperform the crowd by enough to overcome that structural handicap.

## Planned Tech Stack

| Layer | Technology |
|-------|-----------|
| Data collection | Python (`httpx` + `BeautifulSoup`) |
| Scheduling | APScheduler or cron |
| Storage | DuckDB |
| Feature engineering | Pandas / Polars |
| Model (POC) | Rule-based scoring → LightGBM lambdarank |
| Dashboard | Streamlit |

## Data Sources

**PMU Unofficial API (primary)**
- Daily program: `https://online.pmu.fr/rest/catalog/reunions`
- Race detail: `https://online.pmu.fr/rest/catalog/reunions/{date}/R{n}/C{n}`
- Horse history: `https://online.pmu.fr/rest/horse/{horseName}`

**Secondary (scraping):** geny.com, paris-turf.com, Equidia, Zeturf

## Architecture Plan

The pipeline is structured in four sequential parts:

1. **Data pipeline** — scrape PMU API for program + runners, extract odds/stats/jockey/trainer, store raw data in DuckDB
2. **Rule-based scoring model** — recent form score (weighted positions over last 5 races), jockey win rate on distance, odds rank vs model rank divergence
3. **Paper trading simulation** — run model before races, log predictions with timestamps, compare against evening results, compute fictitious ROI on Gagnant, Placé, Duo
4. **Iteration** — error analysis, feature refinement, eventual replacement with LightGBM (lambdarank)

## Key Domain Concepts

**Bet types by priority:**
- `Duo` — 2 horses, most crowd errors → best EV+ opportunities
- `Quinté+` — high combinatorial complexity → more mispricing
- `Gagnant/Placé` — simple but highest take rate friction; only play with strong model confidence

**EV+ condition:** only bet when `model_probability > implied_pool_probability`

**Kelly criterion (conservative staking):**
```python
f = (p * b - q) / b  # p=win_prob, q=1-p, b=decimal_odds-1
```

**Key features to engineer:**
- Horse form: position in last N races (3/5/10), avg gap to winner, win rate by distance/track
- Jockey & trainer: win rates (global, by distance, by track), jockey×horse synergy
- Market signals: morning vs final odds drift, pool implied probability vs model probability
- Race context: track condition, distance fit, field size, race category

## Project Constraints

- **Trot races only** (more consistent than flat; Quinté+ is almost always trot)
- **Paper trading** until ROI is validated
- **Stack philosophy:** lightweight and fast to iterate — DuckDB over PostgreSQL, rules before ML
- **Language:** Python
