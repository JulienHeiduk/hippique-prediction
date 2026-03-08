# Horse Racing Ranking Model Features

## Core Features

### 1. Recent Form
Measures the horse’s recent performance trend.

Examples:
- average finishing position over the last 3 races
- average finishing position over the last 5 races
- number of top-3 finishes in recent races

---

### 2. Relative Speed Rating
Measures how fast the horse performed relative to others in the same race.

Example:
speed_rating - average_speed_rating_of_race

---

### 3. Implied Probability from Odds
Information extracted from the betting market.

Formula:
p_i = (1 / odds_i) / Σ(1 / odds_j)

---

### 4. Days Since Last Race
Captures the horse’s freshness.

Examples:
- days since last race
- days since last win

---

### 5. Distance Performance
How well the horse performs at the race distance.

Examples:
- win rate at similar distances
- average finishing position at this distance

---

### 6. Surface Performance
Horse’s ability on the track surface.

Examples:
- win rate on turf
- win rate on dirt
- win rate on synthetic tracks

---

### 7. Jockey Performance
Measures the jockey’s historical performance.

Examples:
- jockey overall win rate
- jockey win rate at this racecourse
- jockey win rate at this distance

---

### 8. Trainer Performance
Measures the trainer’s effectiveness.

Examples:
- trainer overall win rate
- trainer win rate for this race type
- trainer win rate at this racecourse

---

### 9. Weight Carried
Important in handicap races.

Examples:
- weight carried by the horse
- weight difference vs race average
- weight change compared to last race

---

### 10. Starting Gate Position
The starting stall number can influence the outcome depending on the track layout.

Examples:
- gate number
- historical win rate of this gate on the track

---

# Advanced Features (Used in Professional Models)

### 11. Horse Elo Rating
Dynamic rating representing the strength of a horse based on past results.

Examples:
- horse Elo rating
- Elo change since last race
- Elo percentile within the race

---

### 12. Jockey Elo Rating
Measures jockey strength using an Elo system.

Examples:
- jockey Elo rating
- jockey Elo vs race average

---

### 13. Trainer Elo Rating
Captures the trainer’s historical competitiveness.

Examples:
- trainer Elo rating
- trainer Elo trend

---

### 14. Pace Profile
Captures the typical running style of the horse.

Examples:
- early speed index
- average position at first call
- closing speed index

---

### 15. Race Pace Projection
Estimates how fast the race will be early.

Examples:
- number of front runners
- expected early pace pressure
- predicted pace rank

---

### 16. Class Rating
Measures the level of competition faced previously.

Examples:
- average class of last races
- class change vs previous race
- relative class score

---

### 17. Field Strength
Measures the overall difficulty of the race.

Examples:
- mean Elo rating of competitors
- standard deviation of ratings
- number of high-rated horses

---

### 18. Horse–Jockey Interaction
Captures chemistry between a horse and jockey.

Examples:
- win rate for this horse-jockey pair
- average finishing position together
- number of races together

---

### 19. Market Movement
Captures information from changes in betting odds.

Examples:
- opening odds
- closing odds
- odds drift or steam

---

### 20. Monte Carlo Finish Probability
Estimated finish probabilities from simulated race outcomes.

Examples:
- probability of finishing 1st
- probability of finishing top 3
- probability of finishing top 5