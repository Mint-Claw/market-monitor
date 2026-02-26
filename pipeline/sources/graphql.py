"""GraphQL query data source."""

from __future__ import annotations

import logging
from typing import Any

import aiohttp

from .base import DataSource, SourceError

logger = logging.getLogger(__name__)


class GraphQLSource(DataSource):
    """Execute a GraphQL query against an HTTP endpoint.

    Args:
        name: Human-readable source identifier.
        url: The GraphQL HTTP(S) endpoint.
        query: The GraphQL query string.
        variables: Optional query variables.
        headers: Optional request headers.
        results_path: Dot-separated path into the ``data`` object to reach
            the list of records (e.g. ``"market.items"``).
        timeout: Request timeout in seconds.
    """

    def __init__(
        self,
        name: str,
        url: str,
        query: str,
        *,
        variables: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        results_path: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        super().__init__(name)
        self.url = url
        self.query = query
        self.variables = variables or {}
        self.headers = headers or {}
        self.results_path = results_path
        self.timeout = aiohttp.ClientTimeout(total=timeout)

    async def fetch(self) -> list[dict[str, Any]]:
        """POST the GraphQL query and return parsed records."""
        payload = {"query": self.query, "variables": self.variables}
        try:
            async with aiohttp.ClientSession(
                headers=self.headers, timeout=self.timeout
            ) as session:
                async with session.post(self.url, json=payload) as resp:
                    resp.raise_for_status()
                    body = await resp.json()

            if "errors" in body and body["errors"]:
                raise SourceError(
                    self.name,
                    f"GraphQL errors: {body['errors']}",
                )

            data = body.get("data", {})
            if self.results_path:
                for key in self.results_path.split("."):
                    data = data[key]

            if not isinstance(data, list):
                data = [data]

            logger.info("GraphQL source %s fetched %d records", self.name, len(data))
            return data

        except aiohttp.ClientError as exc:
            raise SourceError(self.name, f"HTTP error: {exc}") from exc
        except KeyError as exc:
            raise SourceError(
                self.name, f"Missing results path {self.results_path!r}: {exc}"
            ) from exc
