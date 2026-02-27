"""CSV URL source — streams a remote CSV and converts rows to dicts."""

from __future__ import annotations

import csv
import io
import logging
from typing import Any

import aiohttp

from sources.base import BaseSource, register_source

logger = logging.getLogger(__name__)


@register_source("csv")
class CsvSource(BaseSource):
    """Fetch and parse a CSV file from a URL."""

    async def fetch(self) -> list[dict[str, Any]]:
        url = self.config["url"]
        max_records = self.config.get("max_records")

        logger.info("CSV   GET %s", url)
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                resp.raise_for_status()
                text = await resp.text()

        reader = csv.DictReader(io.StringIO(text))
        records = [dict(row) for row in reader]

        if max_records:
            records = records[:max_records]

        logger.info("CSV   %s -> %d records", self.name, len(records))
        return records
