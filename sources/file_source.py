"""Local file source — reads JSON or CSV files from disk."""

from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from typing import Any

from sources.base import BaseSource, register_source

logger = logging.getLogger(__name__)

# Project root is one directory up from sources/
_PROJECT_ROOT = Path(__file__).resolve().parent.parent


@register_source("file")
class FileSource(BaseSource):
    """Read records from a local JSON or CSV file."""

    async def fetch(self) -> list[dict[str, Any]]:
        raw_path = self.config["path"]
        path = Path(raw_path)
        if not path.is_absolute():
            path = _PROJECT_ROOT / raw_path

        if not path.exists():
            raise FileNotFoundError(f"Local file not found: {path}")

        logger.info("FILE  READ %s", path)
        text = path.read_text(encoding="utf-8")
        max_records = self.config.get("max_records")

        if path.suffix.lower() == ".csv":
            reader = csv.DictReader(text.splitlines())
            records: list[dict[str, Any]] = [dict(row) for row in reader]
        else:
            data = json.loads(text)
            records = data if isinstance(data, list) else [data]

        if max_records:
            records = records[:max_records]

        logger.info("FILE  %s -> %d records", self.name, len(records))
        return records
