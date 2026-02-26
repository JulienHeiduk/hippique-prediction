# Horse Racing Prediction System — Project Context

## Goal

Build a data pipeline and prediction system to identify positive expected value (EV+) bets on French horse races (PMU), starting with **Trot** races. The system runs in **paper trading mode** (no real money) to validate model performance before any real betting.

---

## Core Insight: Take Rate & Value Betting

PMU retains ~25-28% of the total pool before redistributing winnings. This means:
- Every bet starts with a structural -25% handicap
- The model must outperform the "crowd" by enough to overcome this edge
- The goal is not just predicting winners, but finding **mispriced combinations** where our probability estimate > implied pool probability

This is as much a **market modelling problem** as a horse racing prediction problem.

---

## Bet Type Strategy

Not all bet types are equal. Priority:

| Bet Type | Notes |
|----------|-------|
| **Duo** | Best entry point — 2 horses, order/no order. Crowd makes more errors here than in simple bets. |
| **Quinté+** | High combinatorial complexity = more crowd errors = more value opportunities. Evening trot races. |
| **Gagnant/Placé** | Simple but highest take rate friction. Only play if strong model confidence. |

Decision rule: only place a bet when **model probability > implied pool probability** (EV+ condition).

---

## Tech Stack

```
Data Collection  → Python (httpx + BeautifulSoup)
Scheduler        → APScheduler or cron
Storage          → DuckDB (lightweight, analytical queries)
Feature Eng.     → Pandas / Polars
Model (POC)      → Rule-based scoring → then LightGBM lambdarank
Serving          → Streamlit dashboard
```

---

## Data Sources

### PMU Unofficial API (primary)
- Program of the day: `https://online.pmu.fr/rest/catalog/reunions`
- Race detail: `https://online.pmu.fr/rest/catalog/reunions/{date}/R{n}/C{n}`
- Horse history: `https://online.pmu.fr/rest/horse/{horseName}`

### Secondary sources (scraping)
- geny.com — detailed horse/jockey stats
- paris-turf.com — press comments, pronostics (useful as LLM input)
- Equidia / Zeturf — additional odds and form data

---

## Key Features to Engineer

### Horse form
- Position in last N races (3, 5, 10)
- Average gap to winner
- Win rate / top-3 rate on same distance and track type
- Days since last race (fatigue proxy)

### Jockey & Trainer
- Jockey win rate (global, on distance, on track)
- Jockey × Horse synergy (past races together)
- Trainer win rate in similar conditions

### Market signals
- Morning odds vs final odds (drift = strong signal)
- Implied probability from pool vs model probability
- Over/under-representation in the pool (crowd bias detection)

### Race context
- Track condition (sec, bon, souple, lourd)
- Distance vs horse's optimal distance
- Field size
- Race category / prize money

---

## POC Plan

### Part 1 — Data pipeline
- Scrape PMU API for today's program + runners
- Extract basic stats: odds, last races, jockey, trainer
- Store raw data in DuckDB

### Part 2 — Scoring model
- Build rule-based scoring (no ML yet):
  - Recent form score (weighted positions over last 5 races)
  - Jockey win rate on distance
  - Odds rank vs model rank divergence
- Output: ranked list of horses per race

### Part 3 — Paper trading simulation
- Run model on today's races **before** they happen
- Log predictions with timestamps
- Compare against actual results in the evening
- Compute fictitious ROI on: Gagnant, Placé, Duo

### Part 4+ — Iteration
- Analyze prediction errors
- Refine features
- Identify which bet types show the best ROI pattern
- Start replacing rule-based scoring with LightGBM (lambdarank)

---

## Paper Trading Logic

```python
# For each race:
# 1. Compute model probability for each horse
# 2. Compute implied pool probability from odds
# 3. Flag EV+ bets where model_prob > pool_implied_prob * threshold
# 4. Log recommended bets with stake (Kelly fraction)
# 5. After race: log actual result and P&L

# Kelly fraction (conservative):
# f = (p * b - q) / b
# where p = model win prob, q = 1-p, b = decimal odds - 1
```

---

## Future Extensions (post-POC)

- LLM layer to parse press comments and pronostics as additional features
- Automated daily pipeline with morning scrape + evening result logging
- Backtesting engine on historical data
- Streamlit dashboard: recommended bets, ROI tracker, model confidence visualization
- Kaggle dataset publication of collected data (side project / audience building)

---

## Project Constraints

- **Focus**: Trot races only (more consistent, less variance than flat racing, Quinté+ is almost always trot)
- **Mode**: Paper trading until ROI is validated over 30+ race days
- **Stack philosophy**: lightweight and fast to iterate — DuckDB over PostgreSQL, rule-based before ML
- **Language**: Python
