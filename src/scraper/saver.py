"""Raw JSON persistence to data/raw/YYYYMMDD/."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from loguru import logger

from config.settings import RAW_DIR


def get_raw_dir(date: str) -> Path:
    """Return and create data/raw/YYYYMMDD/ directory."""
    raw_dir = RAW_DIR / date
    raw_dir.mkdir(parents=True, exist_ok=True)
    return raw_dir


def save_raw(data: Any, date: str, filename: str) -> Path:
    """Write *data* as JSON to data/raw/<date>/<filename>. Warns on failure, never raises."""
    path = get_raw_dir(date) / filename
    try:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
        logger.debug("Saved raw file: {}", path)
    except Exception as exc:
        logger.warning("Failed to save raw file {}: {}", path, exc)
    return path


def load_raw(date: str, filename: str) -> dict | None:
    """Load a previously saved raw JSON file. Returns None if missing."""
    path = RAW_DIR / date / filename
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as exc:
        logger.warning("Failed to load raw file {}: {}", path, exc)
        return None
