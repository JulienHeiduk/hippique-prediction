"""Public API for the scraper package."""
from src.scraper.pipeline import run as run_pipeline
from src.scraper.storage import get_connection, init_schema

__all__ = ["run_pipeline", "get_connection", "init_schema"]
