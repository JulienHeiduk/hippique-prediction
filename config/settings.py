"""Central configuration — paths, API endpoints, model constants."""
from pathlib import Path

ROOT = Path(__file__).parent.parent

# Storage
DB_PATH = ROOT / "data" / "processed" / "hippique.duckdb"
RAW_DIR = ROOT / "data" / "raw"
LOG_DIR = ROOT / "logs"

# PMU API
PMU_BASE = "https://online.pmu.fr/rest"
PMU_REUNIONS = f"{PMU_BASE}/catalog/reunions"
PMU_RACE = f"{PMU_BASE}/catalog/reunions/{{date}}/R{{reunion}}/C{{course}}"
PMU_HORSE = f"{PMU_BASE}/horse/{{name}}"

# Paper trading
KELLY_FRACTION = 0.25   # conservative Kelly multiplier
EV_THRESHOLD = 1.0      # model_prob must exceed implied_prob * threshold to bet
UNIT_STAKE = 2.0        # € per unit (paper mode)
