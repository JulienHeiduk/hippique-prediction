"""Central configuration — paths, API endpoints, model constants."""
from pathlib import Path

ROOT = Path(__file__).parent.parent

# Storage
DB_PATH = ROOT / "data" / "processed" / "hippique.duckdb"
RAW_DIR = ROOT / "data" / "raw"
LOG_DIR = ROOT / "logs"

# PMU API  (date format in URLs: DDMMYYYY)
PMU_BASE = "https://offline.turfinfo.api.pmu.fr/rest/client/7"
PMU_REUNIONS = f"{PMU_BASE}/programme/{{date}}"          # GET /programme/DDMMYYYY
PMU_RACE = f"{PMU_BASE}/programme/{{date}}/R{{reunion}}/C{{course}}/participants?specialisation=INTERNET"

# Paper trading
KELLY_FRACTION = 0.25   # conservative Kelly multiplier
EV_THRESHOLD = 1.0      # model_prob must exceed implied_prob * threshold to bet
UNIT_STAKE = 2.0        # € per unit (paper mode)
