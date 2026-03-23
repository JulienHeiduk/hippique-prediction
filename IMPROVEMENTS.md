# Improvement Ideas — Backlog

Ideas to improve model ROI and bet precision. Ordered by expected impact vs effort.

---

## 1. More Historical Data ⭐ Highest priority

**Status:** actionable now — just run `backfill(days=180)`

The #1 bottleneck. With only ~70 days of data:
- Sparse features (`avg_km_time_hist` 47% coverage, `horse_win_rate_at_distance` 39%, `days_since_last_win` 7%) can't signal — this is why the new features were reverted
- Walk-forward folds have high variance (only ~40 test dates)
- No seasonality is captured

With 6 months:
- All features in `/information/NEW_FEATURES_CLAUDE.md` can be re-enabled
- Walk-forward estimates become reliable
- LightGBM ROI estimates will stabilise

---

## 2. EV Threshold Tuning ✅ Done

**Status:** implemented — `WIN_EV_THRESHOLD = 1.1` in `config/settings.py`

Walk-forward grid search results (181 days, rules scorer for WIN):

| EV threshold | WIN ROI | Bets |
|---|---|---|
| 1.0 (was default) | 111.7% | 3110 |
| **1.1 (current)** | **111.4%** | **3006** |
| 1.2 | 112.8% | 2905 |
| 1.5 | 117.0% | 2621 |
| 2.0 | 120.8% | 2122 |

**Decision:** `WIN_EV_THRESHOLD = 1.1` — live data suggested higher thresholds cut too many bets. Revisit after 3+ months of live results.

---

## 3. LightGBM Hyperparameter Tuning ✅ Done

**Status:** implemented — Optuna, 60 trials, 3-fold time-series CV, GPU (OpenCL)

Results (DUO walk-forward, 181 days):

| Config | DUO ROI | Hit | P&L |
|---|---|---|---|
| Fixed params (n_est=300, lr=0.05) | 119.6% | 21.4% | 8036u |
| **Tuned params** | **141.3%** | **22.9%** | **9496u** |

Best params found (`data/models/lgbm_params.json`):

| Param | Before | After | Effect |
|---|---|---|---|
| `n_estimators` | 300 | 104 | Fewer trees → less overfitting |
| `learning_rate` | 0.05 | 0.013 | Slower, more conservative |
| `min_child_samples` | 10 | 48 | Stronger leaf regularisation |
| `reg_alpha` | 0.0 | 0.14 | L1 added |
| `reg_lambda` | 0.0 | 1.94 | L2 added |

The model was overfitting with 300 estimators. `train_lgbm()` auto-loads params from `lgbm_params.json` when present. Re-run `scripts/tune_lgbm_hyperparams.py` after more data.

---

## 4. DUO and Place Bets ✅ Removed — WIN only

**Status:** DUO and Place bet types removed from production. Only WIN bets are generated.

Walk-forward benchmark (181 days, `bet_type=duo`):

| Model | Label | DUO ROI | Hit | Bets |
|---|---|---|---|---|
| WIN model | 2=1st, 1=top3, 0=rest | **119.6%** | 21.4% | 3361 |
| DUO model | 2=top2, 0=rest | 113.0% | 21.3% | 3361 |

**Conclusion:** after live testing, Place and DUO bets were dropped. The single WIN strategy with LightGBM is cleaner and easier to monitor. `train_lgbm_duo()` is preserved in `src/model/lgbm.py` but not used. Revisit with >6 months of data if needed.

---

## 5. Live Odds for EV Calculation ✅ Done

**Status:** implemented — `generate_bets()` now uses `final_implied_prob_norm` (live odds) instead of `morning_implied_prob_norm` for EV computation, with fallback to morning odds when live odds are not yet available.

Also: pending bets have their odds refreshed on every hourly update so the bet sheet always shows current figures.

---

## 6. 30-Minute Odds Refresh ✅ Done

**Status:** implemented — scheduler runs `run_hourly_update` at `:00` and `:30` of every hour from 10:00 to 22:30 (26 runs/day instead of 13).

---

## 7. Probability Calibration

**Status:** medium effort

`model_prob` is derived from raw LightGBM scores via softmax normalisation. These are not well-calibrated probabilities — the EV ratio `model_prob / implied_prob` is therefore approximate.

Adding **Platt scaling** or **isotonic regression** (fitted on a held-out fold) would make the EV filter more precise and reduce false positives.

Libraries: `sklearn.calibration.CalibratedClassifierCV`.

---

## 8. CatBoost with Raw Categoricals

**Status:** medium effort — new model to evaluate

CatBoost natively handles categorical features without manual encoding:
- `hippodrome` — track-specific effects
- `jockey_name` — direct jockey encoding (instead of derived win rate)
- `trainer_name` — direct trainer encoding

This would capture track/jockey/trainer effects more richly than rolling win rates, and CatBoost works well even on small datasets (handles high-cardinality categories via ordered target encoding internally).

---

## 9. Race-Level Filtering

**Status:** low effort — filter in `generate_bets()`

Not all races are equally predictable. Filtering out specific races before betting could improve precision:

| Filter | Rationale |
|---|---|
| `field_size > 12` | Very large fields → high variance, lower prediction reliability |
| `odds_drift_pct` near 0 for all runners | Market hasn't moved → low information |
| Many debutants (no musique) | Unpredictable field |
| `race_hour < 10` | Early morning races often have thinner markets |

Can be tested via a simple boolean column in the features and added as a condition in `generate_bets()`.

---

## 10. New Features (revisit after more data)

See `NEW_FEATURES_CLAUDE.md` for the full backlog.

Features already computed in `pipeline.py` and `engine.py` but currently excluded from `FEATURES` in `lgbm.py` due to sparsity (< 6 months of data):

| Feature | Coverage now | Re-enable when |
|---|---|---|
| `avg_km_time_hist` | 47% | > 90% (6+ months) |
| `horse_win_rate_at_distance` | 39% | > 70% |
| `jockey_win_rate_at_track` | 58% | Stable at 3+ months |
| `days_since_last_win` | 7.6% | > 50% (6+ months) |
| `horse_jockey_win_rate` | 25.6% | > 60% |
| `avg_position_last3/5` | 93% | Ready now — re-test in isolation |

> `avg_km_time_hist` is the most promising: km_time is the primary metric in trot racing and will have strong signal once coverage is sufficient.

---

## 11. Sequential Form Analysis

**Status:** low effort — extend `form.py`

Instead of just win rate and average position, model the **trend** more richly:
- Streak: consecutive wins/losses/top-3
- Acceleration: is the horse getting better or worse each race?
- Consistency score: standard deviation of finish positions

These are derivable from `parse_musique()` with no new data.

---

## 12. Plat (Flat/Gallop) Race Support

**Status:** not started — significant effort

Currently the scraper filters out all non-trot races (`PLAT`, `OBSTACLE`, etc.) at parse time. Adding Plat support would require:

- New feature set: Plat has no `musique` encoding or `km_time` — form must be derived differently (finish positions, margins, official times)
- Separate LightGBM model trained on Plat data (different dynamics, field sizes, jockey importance)
- Separate EV threshold calibrated on Plat races
- Parser changes: remove the `_is_trot()` filter or make it configurable per discipline

Lower priority than trot improvements — start only once the trot strategy is stable and has 6+ months of data.

---

## Summary Table

| Idea | Impact | Effort | Status |
|---|---|---|---|
| More historical data (6 months) | ⭐⭐⭐ | Low | Ongoing |
| EV threshold tuning | ⭐⭐ | Low | ✅ Done (WIN=1.1) |
| LightGBM hyperparameter tuning | ⭐⭐ | Medium | ✅ Done (+21.7% ROI) |
| DUO / Place bets | ⭐⭐ | Medium | ✅ Removed — WIN only |
| Live odds for EV | ⭐⭐ | Low | ✅ Done |
| 30-min odds refresh | ⭐ | Low | ✅ Done |
| Probability calibration | ⭐ | Medium | After more data |
| CatBoost with categoricals | ⭐⭐ | Medium | After more data |
| Race-level filtering | ⭐ | Low | Now |
| Re-enable new features (km_time etc.) | ⭐⭐⭐ | Zero | After 6 months data |
| Sequential form analysis | ⭐ | Low | Now |
| Plat race support | ⭐⭐ | High | After trot is stable |
