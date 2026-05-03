"""Central configuration — paths, API endpoints, model constants."""
from pathlib import Path

ROOT = Path(__file__).parent.parent

# Storage
DB_PATH = ROOT / "data" / "processed" / "hippique.duckdb"
RAW_DIR = ROOT / "data" / "raw"
LOG_DIR = ROOT / "logs"
MODEL_DIR = ROOT / "data" / "models"
LGBM_MODEL_PATH = MODEL_DIR / "lgbm_ranker.txt"       # Trot WIN model (label: 2=1st, 1=top3, 0=rest)
LGBM_MEDIANS_PATH = MODEL_DIR / "lgbm_medians.json"   # Trot training-time medians for NaN imputation
LGBM_PLAT_MODEL_PATH = MODEL_DIR / "plat_ranker.txt"  # Plat WIN model
LGBM_PLAT_MEDIANS_PATH = MODEL_DIR / "plat_medians.json"  # Plat training-time medians
LGBM_PLAT_PARAMS_PATH = MODEL_DIR / "lgbm_plat_params.json"  # Plat-specific hyperparameters
LGBM_PLAT_CALIBRATOR_PATH = MODEL_DIR / "plat_calibrator.joblib"  # Isotonic calibrator
LGBM_TROT_CALIBRATOR_PATH = MODEL_DIR / "trot_calibrator.joblib"  # Isotonic calibrator (trot, when re-enabled)

# PMU API  (date format in URLs: DDMMYYYY)
PMU_BASE = "https://offline.turfinfo.api.pmu.fr/rest/client/7"
PMU_REUNIONS = f"{PMU_BASE}/programme/{{date}}"          # GET /programme/DDMMYYYY
PMU_RACE = f"{PMU_BASE}/programme/{{date}}/R{{reunion}}/C{{course}}/participants?specialisation=INTERNET"
PMU_ONLINE_BASE = "https://online.turfinfo.api.pmu.fr/rest/client/7"
PMU_RAPPORTS = f"{PMU_ONLINE_BASE}/programme/{{date}}/R{{reunion}}/C{{course}}/rapports-definitifs?specialisation=INTERNET"

# Paper trading
KELLY_FRACTION = 0.25     # conservative Kelly multiplier
EV_THRESHOLD = 1.0        # default EV threshold (model_prob > implied_prob * threshold)
WIN_EV_THRESHOLD = 1.0    # WIN bets: lgbm scorer
UNIT_STAKE = 20.0         # € per unit (paper mode)
DAILY_STOP_LOSS = 200.0   # max cumulative realized loss per (date, model) before new bets are frozen

# Plat model: weight on model vs market when blending probabilities.
# 1.0 = pure model, 0.0 = pure market. Walk-forward sweep on resolved
# plat history showed pure model (1.0) gave best ROI (+6.3%); blending
# diluted the signal because the binary classifier is already calibrated.
PLAT_MARKET_BLEND_W = 1.0

# Sanity cap for "morning_odds" used at bet time. Real betting prices
# very rarely exceed 100; values above this threshold (e.g. 999) are
# typically post-race non-runner sentinels or scraper placeholders that
# leaked into the morning_odds fallback path. Bets with effective odds
# above the cap are skipped to avoid phantom EV.
MAX_BETTABLE_ODDS = 100.0

# Per-discipline EV thresholds. Walk-forward analysis on the last 30 days
# showed plat win bets are profitable only at EV ratio >= 1.5 (+21% ROI on
# the restricted set). Trot win is paused (threshold = +inf) until the trot
# model is retrained as a calibrated binary classifier.
EV_THRESHOLDS: dict[str, float] = {
    "trot": float("inf"),
    "plat": 1.5,
}
