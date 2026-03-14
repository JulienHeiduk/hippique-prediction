"""HTTP layer: PMUClient with retry/backoff."""
from __future__ import annotations

import time
from datetime import date as date_type
from typing import Any

import httpx
from loguru import logger

from config.settings import PMU_RACE, PMU_REUNIONS, PMU_RAPPORTS


class PipelineError(Exception):
    """Raised when all HTTP retries are exhausted."""


_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.pmu.fr/",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "fr-FR,fr;q=0.9",
}

_POLITE_DELAY = 0.5    # seconds between requests
_RATE_LIMIT_SLEEP = 30  # seconds to sleep on HTTP 429


def _to_api_date(date_yyyymmdd: str) -> str:
    """Convert YYYYMMDD → DDMMYYYY (PMU URL date format)."""
    return date_yyyymmdd[6:8] + date_yyyymmdd[4:6] + date_yyyymmdd[0:4]


class PMUClient:
    def __init__(
        self,
        timeout: float = 10.0,
        max_retries: int = 3,
        backoff_base: float = 2.0,
    ) -> None:
        self._timeout = timeout
        self._max_retries = max_retries
        self._backoff_base = backoff_base
        self._client = httpx.Client(headers=_HEADERS, timeout=timeout, follow_redirects=True)

    def __enter__(self) -> PMUClient:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch_reunions(self, date: str | None = None) -> dict:
        """Fetch the full race programme for *date* (YYYYMMDD). Defaults to today."""
        if date is None:
            date = date_type.today().strftime("%Y%m%d")
        api_date = _to_api_date(date)
        url = PMU_REUNIONS.format(date=api_date)
        return self._get_with_retry(url)

    def fetch_race(self, date: str, reunion: int, course: int) -> dict:
        """Fetch participants for a single race."""
        api_date = _to_api_date(date)
        url = PMU_RACE.format(date=api_date, reunion=reunion, course=course)
        return self._get_with_retry(url)

    def fetch_rapports_definitifs(self, date: str, reunion: int, course: int) -> list:
        """Fetch final dividends for a single race (rapports-définitifs).

        Returns the raw list of pari objects, or [] on 404 (race not finished yet).
        """
        api_date = _to_api_date(date)
        url = PMU_RAPPORTS.format(date=api_date, reunion=reunion, course=course)
        try:
            return self._get_with_retry(url)
        except PipelineError:
            return []

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _get_with_retry(self, url: str) -> dict:
        last_exc: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                time.sleep(_POLITE_DELAY)
                response = self._client.get(url)

                if response.status_code == 429:
                    logger.warning("Rate limited (429) on {}. Sleeping {}s", url, _RATE_LIMIT_SLEEP)
                    time.sleep(_RATE_LIMIT_SLEEP)
                    last_exc = PipelineError(f"HTTP 429 on {url}")
                    continue

                if response.status_code == 404:
                    raise PipelineError(f"HTTP 404 on {url}")

                response.raise_for_status()
                return response.json()

            except PipelineError:
                raise
            except Exception as exc:
                last_exc = exc
                wait = self._backoff_base ** attempt
                logger.warning(
                    "Attempt {}/{} failed for {}: {}. Retrying in {}s",
                    attempt, self._max_retries, url, exc, wait,
                )
                time.sleep(wait)

        raise PipelineError(
            f"All {self._max_retries} retries exhausted for {url}"
        ) from last_exc
