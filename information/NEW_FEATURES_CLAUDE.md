# New Features — Backlog for Future Iterations

Cross-reference with `NEW_FEATURES.md`. Only features **not yet implemented** and
judged **worth adding** are listed here. Features already in the model or not
applicable to French PMU trot are excluded.

---

## Already Implemented (excluded from backlog)

| NEW_FEATURES.md item | Implemented as |
|---|---|
| Recent Form | `form_score`, `win_rate_last5`, `top3_rate_last5`, `form_trend`, `best_position_last5`, `n_valid_runs` |
| Implied Probability from Odds | `morning_implied_prob_norm` |
| Days Since Last Race | `days_since_last_race` |
| Jockey Performance (overall) | `jockey_win_rate` |
| Trainer Performance (overall) | `trainer_win_rate` |
| Starting Gate Position | `draw_position` |
| Market Movement | `odds_drift_pct`, `odds_rank_change`, `is_favorite`, `field_entropy` |
| Distance | `distance_metres` |

---

## Backlog — High Priority

### F1. Relative Speed Rating (km_time)
**Relevant to**: LightGBM
**PMU data available**: yes — `km_time` is stored in the `runners` table for past races.

Trot-specific metric: how fast a horse performed relative to others in the same race.

```
speed_z = (km_time_horse - avg_km_time_race) / std_km_time_race
```

Also useful over last N races:
- `avg_km_time_last5` — horse's average km_time over last 5 races
- `best_km_time_last5` — best (fastest) km_time over last 5 races
- `km_time_vs_race_avg` — relative speed in the current race (for historical training)

> Note: lower km_time = faster. Need to handle NULL km_times (retired horses, etc.)

---

### F2. Average Finishing Position (last 3 / last 5)
**Relevant to**: LightGBM, rule-based
**PMU data available**: yes — derivable from `musique`.

We have `win_rate_last5` and `top3_rate_last5` but not the raw average position,
which captures more granular information (e.g. a horse that always finishes 4th vs 8th).

- `avg_position_last3`
- `avg_position_last5`

---

### F3. Distance Performance
**Relevant to**: LightGBM
**PMU data available**: yes — `distance_metres` and `finish_position` in history.

Win rate and avg finishing position split by distance band (±500 m around current race):
- `horse_win_rate_at_distance` — win rate in races at similar distance (no leakage)
- `horse_avg_position_at_distance`

---

### F4. Jockey & Trainer Stats at Track
**Relevant to**: LightGBM
**PMU data available**: yes — `hippodrome` on `races`, joinable with runner history.

Jockey and trainer performance is highly track-dependent in France (some specialize
on certain hippodromes).

- `jockey_win_rate_at_track`
- `trainer_win_rate_at_track`

Same SQL pattern as current `jockey_win_rate`, filtered to `hr.hippodrome = ra.hippodrome`.

---

### F5. Days Since Last Win
**Relevant to**: LightGBM
**PMU data available**: yes — derivable from runner history.

Complements `days_since_last_race`. A horse on a long losing streak behaves
differently from one that just won.

- `days_since_last_win` — NULL if never won in history

---

### F6. Horse–Jockey Pair Stats
**Relevant to**: LightGBM
**PMU data available**: yes.

Chemistry between a specific horse and its regular jockey. Derivable from
`runners` history filtered on `horse_name + jockey_name` pairs.

- `horse_jockey_win_rate` — win rate for this horse+jockey combination
- `horse_jockey_n_races` — number of races together (confidence weight)

---

## Backlog — Medium Priority

### F7. Gate Win Rate at Track
**Relevant to**: LightGBM
**PMU data available**: yes — `draw_position` + `hippodrome`.

Historical win rate for a given gate number on a given hippodrome. Certain inside
gates are advantageous on tight tracks.

- `gate_win_rate_at_track`

---

### F8. Jockey & Trainer Win Rate at Distance
**Relevant to**: LightGBM
**PMU data available**: yes.

Extends F4 with distance banding (same ±500 m logic as F3).

- `jockey_win_rate_at_distance`
- `trainer_win_rate_at_distance`

---

### F9. Class Rating
**Relevant to**: LightGBM
**PMU data available**: partial — PMU programmes include race category codes
(e.g. `categorieParticularite`, `conditionSexe`, `conditionAge`). Not currently
parsed/stored.

Measures the level of competition faced previously:
- `class_level` — numeric encoding of race category
- `class_change` — step up / step down vs previous race

> Requires extending the scraper/parser to capture race condition fields.

---

## Excluded (not applicable to French PMU trot)

| Feature | Reason excluded |
|---|---|
| Surface Performance | Harness trot: track surface is standardised; no turf/dirt split |
| Weight Carried | Not a key variable in harness trot; `handicap_distance` already covers the handicap aspect |
| Pace Profile / Race Pace Projection | Would require split-time data not available from the PMU API |
| Elo Ratings (horse/jockey/trainer) | High implementation complexity; useful only with large history (100+ days). Revisit after 6 months of data. |
| Monte Carlo Finish Probability | Can be derived from model scores once ranking is stable; adds complexity without clear marginal gain at this stage. |
