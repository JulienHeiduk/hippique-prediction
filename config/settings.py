"""Central configuration — paths, API endpoints, model constants."""
from pathlib import Path

ROOT = Path(__file__).parent.parent

# Storage
DB_PATH = ROOT / "data" / "processed" / "hippique.duckdb"
RAW_DIR = ROOT / "data" / "raw"
LOG_DIR = ROOT / "logs"
MODEL_DIR = ROOT / "data" / "models"
LGBM_MODEL_PATH = MODEL_DIR / "lgbm_ranker.txt"       # WIN model (label: 2=1st, 1=top3, 0=rest)
LGBM_MEDIANS_PATH = MODEL_DIR / "lgbm_medians.json"   # Training-time medians for NaN imputation

# PMU API  (date format in URLs: DDMMYYYY)
PMU_BASE = "https://offline.turfinfo.api.pmu.fr/rest/client/7"
PMU_REUNIONS = f"{PMU_BASE}/programme/{{date}}"          # GET /programme/DDMMYYYY
PMU_RACE = f"{PMU_BASE}/programme/{{date}}/R{{reunion}}/C{{course}}/participants?specialisation=INTERNET"
PMU_ONLINE_BASE = "https://online.turfinfo.api.pmu.fr/rest/client/7"
PMU_RAPPORTS = f"{PMU_ONLINE_BASE}/programme/{{date}}/R{{reunion}}/C{{course}}/rapports-definitifs?specialisation=INTERNET"

# Paper trading
KELLY_FRACTION = 0.25     # conservative Kelly multiplier
EV_THRESHOLD = 1.0        # default EV threshold (model_prob > implied_prob * threshold)
WIN_EV_THRESHOLD = 0.8    # WIN bets: lgbm scorer
UNIT_STAKE = 20.0         # € per unit (paper mode)
