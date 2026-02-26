"""CSV file data source."""

from __future__ import annotations

import csv
import io
import logging
from pathlib import Path
from typing import Any

import aiohttp

from .base import DataSource, SourceError

logger = logging.getLogger(__name__)


class CSVSource(DataSource):
    """Read records from a local or remote CSV file.

    Args:
        name: Human-readable source identifier.
        path: Local file path *or* HTTP(S) URL to a CSV.
        delimiter: CSV delimiter character.
        encoding: File encoding.
    """

    def __init__(
        self,
        name: str,
        path: str,
        *,
        delimiter: str = ",",
        encoding: str = "utf-8",
    ) -> None:
        super().__init__(name)
        self.path = path
        self.delimiter = delimiter
        self.encoding = encoding

    async def fetch(self) -> list[dict[str, Any]]:
        """Parse the CSV and return rows as dicts."""
        try:
            text = await self._read_text()
            reader = csv.DictReader(
                io.StringIO(text), delimiter=self.delimiter
            )
            records = [dict(row) for row in reader]
            logger.info("CSV source %s fetched %d records", self.name, len(records))
            return records
        except Exception as exc:
            raise SourceError(self.name, f"CSV parse error: {exc}") from exc

    async def _read_text(self) -> str:
        if self.path.startswith(("http://", "https://")):
            async with aiohttp.ClientSession() as session:
                async with session.get(self.path) as resp:
                    resp.raise_for_status()
                    return await resp.text(encoding=self.encoding)
        return Path(self.path).read_text(encoding=self.encoding)
