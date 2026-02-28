# Formula Trot — Horse Selection Model

This document explains exactly how the system selects horses, computes probabilities, and decides which bets to place.

---

## 1. Data collected per runner

The system covers both **Trot Attelé** (`TROT_ATTELE`) and **Trot Monté** (`TROT_MONTE`) races. All other PMU disciplines (Plat, Obstacle, Cross, etc.) are filtered out at scrape time and never enter the database. The exact discipline is stored in the `discipline` column of the `races` table.

For each qualifying trot race, the following raw data is fetched from the PMU API and stored in DuckDB:

| Field | Description |
|---|---|
| `horse_name` | Name of the horse |
| `jockey_name` | Name of the jockey (driver) |
| `musique` | Form string — encoded history of recent race results |
| `morning_odds` | Decimal odds published in the morning programme |
| `final_odds` | Decimal odds at race start (available after the race) |
| `finish_position` | Actual finishing position (available after the race) |
| `scratch` | Whether the horse was withdrawn |

Only non-scratched horses in **Trot** races with at least **4 runners** are considered.

---

## 2. Features computed

### 2.1 Form score (`form_score`)

Parsed from the `musique` string — a compact PMU notation encoding finish positions across recent races.

**Parsing rules:**
- Each token is a digit (finish position 1–9+) or `D` (disqualified)
- `0` = DNF (did not finish)
- Results annotated `(YYYY)` year markers are stripped
- Results are read **most-recent first**

**Score formula** (last 5 valid positions, excluding DNF and DQ):

```
form_score = Σ  max(0, 10 - pos_i) × 0.75^i   for i = 0..4
```

- Position 1 (win) contributes `9 × 1.0 = 9.0`
- Position 2 contributes `8 × 0.75 = 6.0`
- Position 5 contributes `5 × 0.316 = 1.58`
- Position 10+ contributes `0` (no points)
- Recency decay factor: **0.75 per race** (most recent race weighted most)
- Returns `None` if the horse has fewer than 2 valid results in its full history

### 2.2 Market features (`morning_odds_rank`, `odds_drift_pct`, `morning_implied_prob_norm`)

Derived from the morning odds within each race:

| Feature | Formula | Meaning |
|---|---|---|
| `morning_odds_rank` | rank ascending within race | 1 = favourite |
| `final_odds_rank` | rank ascending within race | 1 = favourite at start |
| `odds_drift_pct` | `(final - morning) / morning` | negative = shortening (market confidence ↑) |
| `morning_implied_prob` | `1 / morning_odds` | raw market probability |
| `morning_implied_prob_norm` | `implied_prob / Σ(implied_prob in race)` | normalised to sum to 1 per race |

The normalisation removes the bookmaker's overround so that probabilities within a race sum exactly to 1. This is the **implied probability** used for EV comparisons.

### 2.3 Jockey win rate (`jockey_win_rate`)

Rolling win rate computed **without data leakage** — only races strictly before the race date are counted:

```
jockey_win_rate = wins_before_date / starts_before_date
```

Returns `NaN` / `0` for jockeys with no prior history in the database.

---

## 3. The scoring model — M4 Combined

The active model is **M4 Combined** (`score_combined` in `src/model/scorer.py`).

It scores each horse with a **weighted sum of 4 normalised components**, all scaled 0–1 within the race.

### 3.1 Components

| Component | Normalisation within race | What it captures |
|---|---|---|
| **Odds** | `(n - rank) / (n - 1)` — favourite scores 1.0 | Market consensus |
| **Form** | `form_score / max(form_score)` | Recent finishing history |
| **Drift** | `clip(-drift, 0, 1) / max(clip(-drift, 0, 1))` | Odds shortening (money coming in) |
| **Jockey** | `jockey_win_rate / max(jockey_win_rate)` | Driver skill |

The drift component rewards horses whose odds **shortened** from morning to start (negative drift = market confidence increased). Drifters (lengthening odds) score 0.

### 3.2 Default weights

| Component | Weight | Share |
|---|---|---|
| Odds | `w_odds = 0.40` | 40% |
| Form | `w_form = 0.35` | 35% |
| Drift | `w_drift = 0.15` | 15% |
| Jockey | `w_jockey = 0.10` | 10% |

```
raw_score = (0.40 × odds_comp + 0.35 × form_comp + 0.15 × drift_comp + 0.10 × jockey_comp)
            / (0.40 + 0.35 + 0.15 + 0.10)
```

### 3.3 Weight optimisation

Weights can be optimised via a grid search (`optimize_weights` in `src/model/scorer.py`) that tests all combinations of `[0.0, 0.25, 0.50, 0.75, 1.0]` for each weight (624 valid combos). The grid search uses a fully vectorised ROI computation for speed (~13 seconds on 61 days of data).

**Empirical results on 61-day backtest (1422 races, 12685 runners):**

| Model | ROI | Hit rate | Bets |
|---|---|---|---|
| M0 Baseline (1/odds only) | 23.0% | 39.2% | 1409 |
| M4 Combined (default weights) | 123.6% | 42.3% | 1409 |
| M4 Best (grid search, in-sample) | ~268% | — | — |

---

## 4. Model probability

After scoring, each horse's **model probability** is computed by normalising scores within the race:

```
model_prob(horse) = score(horse) / Σ score(all horses in race)
```

This gives a probability distribution summing to 1 per race, representing the model's estimated win probability for each runner.

---

## 5. Implied probability

The market's estimated win probability, derived from morning odds and normalised to remove the overround:

```
implied_prob(horse) = (1 / morning_odds) / Σ (1 / morning_odds) for all horses in race
```

If morning odds are unavailable, the fallback is `1 / field_size` (uniform distribution).

---

## 6. Expected Value (EV ratio)

The EV ratio compares the model's view of a horse's chance to what the market implies:

```
EV = model_prob / implied_prob
```

| EV | Interpretation |
|---|---|
| `< 1.0` | Market prices the horse more favourably than the model — no edge |
| `= 1.0` | Model and market agree |
| `> 1.0` | Model sees more value than the market — potential edge |

A bet is placed only when:

```
model_prob > implied_prob × EV_THRESHOLD
```

with `EV_THRESHOLD = 1.0` (configurable in `config/settings.py`). In practice this means **any horse the model rates above the market's normalised probability** generates a bet.

---

## 7. Bet types

Two bet types are generated for each qualifying race:

### WIN
- **Selection:** horse ranked #1 by the model
- **Condition:** `model_prob > implied_prob`
- **Win condition:** horse finishes 1st
- **P&L:** `(morning_odds - 1) × stake` if win, `-stake` if loss

### DUO
- **Selection:** horses ranked #1 and #2 by the model
- **Condition:** `combined_model_prob > combined_implied_prob` (sum of top-2)
- **Win condition:** both horses finish 1st and 2nd in any order
- **P&L:** `min(field_size² / 4, 50) - 2×stake` if win, `-2×stake` if loss
- **Minimum field:** 4 runners

Both bet types require a minimum field of **4 runners**.

---

## 8. Stake sizing — Fractional Kelly

The Kelly criterion sizes the bet proportionally to the edge:

```
full_kelly = (p × b - q) / b

where:
  p = model_prob
  b = decimal_odds - 1   (net profit per unit staked)
  q = 1 - p              (probability of loss)
```

To reduce variance, only a **quarter Kelly** is used:

```
kelly_stake = full_kelly × KELLY_FRACTION × UNIT_STAKE
            = full_kelly × 0.25 × 2.0 €
```

The stake is **capped at 3 × UNIT_STAKE = 6.0 €** regardless of how large the Kelly fraction computes. In live paper trading, the stake recorded in the `bets` table is the flat `UNIT_STAKE` (€2 win, €4 duo); `kelly_stake` is stored as an advisory value.

| Constant | Value |
|---|---|
| `UNIT_STAKE` | 2.0 € |
| `KELLY_FRACTION` | 0.25 |
| Max stake (win) | 6.0 € |
| Max stake (duo) | 6.0 € (per leg) |

---

## 9. End-to-end decision flow

```
PMU API
  └─ Scrape races + runners + morning odds
       └─ form_score (musique parsing)
       └─ odds_features (rank, drift, implied prob)
       └─ jockey_win_rate (rolling, no leakage)
            └─ score_combined (weighted 4-component score)
                 └─ model_prob = score / Σ scores per race
                 └─ implied_prob = normalised 1/odds
                 └─ EV = model_prob / implied_prob
                      └─ EV > 1.0 → generate WIN and/or DUO bet
                           └─ kelly_stake computed
                           └─ bet upserted to DuckDB (idempotent)
                           └─ HTML bet sheet exported
```

---

## 10. Source files

| File | Role |
|---|---|
| `src/features/form.py` | `parse_musique()`, `form_score()` |
| `src/features/market.py` | `odds_features()` |
| `src/features/pipeline.py` | `compute_features()` — full feature build from DuckDB |
| `src/model/scorer.py` | `score_combined()`, `optimize_weights()` |
| `src/model/backtest.py` | `backtest()`, P&L rules, `BacktestReport` |
| `src/trading/kelly.py` | `kelly_stake()` |
| `src/trading/engine.py` | `generate_bets()`, `resolve_bets()` |
| `src/trading/scheduler.py` | Morning (09:00, 11:30) and evening (22:00) sessions |
| `config/settings.py` | All constants: `EV_THRESHOLD`, `KELLY_FRACTION`, `UNIT_STAKE` |
