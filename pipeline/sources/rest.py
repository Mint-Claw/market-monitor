"""REST API data source using aiohttp."""

from __future__ import annotations

import logging
from typing import Any

import aiohttp

from .base import DataSource, SourceError

logger = logging.getLogger(__name__)


class RESTSource(DataSource):
    """Fetch records from a REST API endpoint.

    Args:
        name: Human-readable source identifier.
        url: The HTTP(S) endpoint to GET.
        headers: Optional request headers (e.g. auth tokens).
        params: Optional query parameters.
        results_key: JSON key that holds the list of records.
            ``None`` means the top-level response is the list.
        timeout: Request timeout in seconds.
    """

    def __init__(
        self,
        name: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
        results_key: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        super().__init__(name)
        self.url = url
        self.headers = headers or {}
        self.params = params or {}
        self.results_key = results_key
        self.timeout = aiohttp.ClientTimeout(total=timeout)

    async def fetch(self) -> list[dict[str, Any]]:
        """GET the endpoint and return parsed JSON records."""
        try:
            async with aiohttp.ClientSession(
                headers=self.headers, timeout=self.timeout
            ) as session:
                async with session.get(self.url, params=self.params) as resp:
                    resp.raise_for_status()
                    data = await resp.json()

            if self.results_key:
                data = data[self.results_key]

            if not isinstance(data, list):
                data = [data]

            logger.info("REST source %s fetched %d records", self.name, len(data))
            return data

        except aiohttp.ClientError as exc:
            raise SourceError(self.name, f"HTTP error: {exc}") from exc
        except KeyError as exc:
            raise SourceError(
                self.name, f"Missing results key {self.results_key!r}: {exc}"
            ) from exc
