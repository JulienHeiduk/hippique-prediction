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
- LightGBM DUO ROI will stabilise

---

## 2. EV Threshold Tuning ✅ Done

**Status:** implemented — `WIN_EV_THRESHOLD = 1.5` in `config/settings.py`

Walk-forward grid search results (181 days, rules scorer for WIN):

| EV threshold | WIN ROI | Bets |
|---|---|---|
| 1.0 (was default) | 111.7% | 3110 |
| 1.1 | 111.4% | 3006 |
| 1.2 | 112.8% | 2905 |
| **1.5 (new)** | **117.0%** | **2621** |
| 2.0 | 120.8% | 2122 |

**Decision:** `WIN_EV_THRESHOLD = 1.5` — best ROI/volume trade-off (+5.3% ROI, -16% bets vs 1.0).

**DUO:** EV filter in backtest uses wrong formula (top-1 only instead of combined_model_prob). Production code already uses `combined_model_prob > combined_implied_prob * threshold` — kept at 1.0.

---

## 3. LightGBM Hyperparameter Tuning

**Status:** medium effort — use Optuna or manual grid search inside walk-forward

Current fixed params: `n_estimators=300, num_leaves=31, learning_rate=0.05, min_child_samples=10`.

Key params to tune:
- `min_child_samples` — controls overfitting on small training folds
- `num_leaves` — model complexity (try 15, 31, 63)
- `learning_rate` + `n_estimators` jointly (try 0.02/500, 0.05/300, 0.1/150)
- `subsample` / `colsample_bytree` — regularisation

A simple 3-fold time-series CV inside `train_lgbm()` could find better params automatically.

---

## 4. DUO-Specific Model

**Status:** medium effort — modify training label

The current LightGBM is trained with WIN-focused relevance (2=1st, 1=top3, 0=rest), but DUO needs the **pair** (1st + 2nd) correct.

Option A — Change label for DUO training: `2 = 1st or 2nd, 0 = rest`

Option B — Train two separate models:
- `lgbm_win.txt` optimised for WIN (current)
- `lgbm_duo.txt` optimised for DUO (new label)

This would likely recover the DUO ROI drop seen when adding new features.

---

## 5. Probability Calibration

**Status:** medium effort

`model_prob` is derived from raw LightGBM scores via softmax normalisation. These are not well-calibrated probabilities — the EV ratio `model_prob / implied_prob` is therefore approximate.

Adding **Platt scaling** or **isotonic regression** (fitted on a held-out fold) would make the EV filter more precise and reduce false positives.

Libraries: `sklearn.calibration.CalibratedClassifierCV`.

---

## 6. CatBoost with Raw Categoricals

**Status:** medium effort — new model to evaluate

CatBoost natively handles categorical features without manual encoding:
- `hippodrome` — track-specific effects
- `jockey_name` — direct jockey encoding (instead of derived win rate)
- `trainer_name` — direct trainer encoding

This would capture track/jockey/trainer effects more richly than rolling win rates, and CatBoost works well even on small datasets (handles high-cardinality categories via ordered target encoding internally).

---

## 7. Race-Level Filtering

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

## 8. New Features (revisit after more data)

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

## 9. Sequential Form Analysis

**Status:** low effort — extend `form.py`

Instead of just win rate and average position, model the **trend** more richly:
- Streak: consecutive wins/losses/top-3
- Acceleration: is the horse getting better or worse each race?
- Consistency score: standard deviation of finish positions

These are derivable from `parse_musique()` with no new data.

---

## Summary Table

| Idea | Impact | Effort | When |
|---|---|---|---|
| More historical data (6 months) | ⭐⭐⭐ | Low | Now |
| EV threshold tuning | ⭐⭐ | Low | ✅ Done (WIN=1.5) |
| LightGBM hyperparameter tuning | ⭐⭐ | Medium | After more data |
| DUO-specific model | ⭐⭐ | Medium | Now |
| Probability calibration | ⭐ | Medium | After more data |
| CatBoost with categoricals | ⭐⭐ | Medium | After more data |
| Race-level filtering | ⭐ | Low | Now |
| Re-enable new features (km_time etc.) | ⭐⭐⭐ | Zero | After 6 months data |
| Sequential form analysis | ⭐ | Low | Now |
