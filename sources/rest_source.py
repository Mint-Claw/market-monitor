"""REST API source — fetches JSON from any REST endpoint."""

from __future__ import annotations

import logging
from typing import Any

import aiohttp

from sources.base import BaseSource, register_source

logger = logging.getLogger(__name__)


@register_source("rest")
class RestSource(BaseSource):
    """Fetch records from a REST (JSON) API endpoint."""

    async def fetch(self) -> list[dict[str, Any]]:
        url = self.config["url"]
        method = self.config.get("method", "GET").upper()
        params = self.config.get("params", {})
        headers = self.config.get("headers", {})
        max_records = self.config.get("max_records")

        logger.info("REST  %s %s", method, url)
        async with aiohttp.ClientSession() as session:
            async with session.request(
                method,
                url,
                params=params,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()

        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            records = data.get("data", data.get("results", [data]))
        else:
            records = [data]

        if max_records:
            records = records[:max_records]

        logger.info("REST  %s -> %d records", self.name, len(records))
        return records
