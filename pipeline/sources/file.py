"""Local file data source supporting JSON and XML."""

from __future__ import annotations

import json
import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

from .base import DataSource, SourceError

logger = logging.getLogger(__name__)


class FileSource(DataSource):
    """Read records from a local JSON or XML file.

    Args:
        name: Human-readable source identifier.
        path: Path to the file.
        format: ``"json"`` or ``"xml"``. Auto-detected from extension if omitted.
        results_key: For JSON, a top-level key holding the records list.
        xml_record_tag: For XML, the tag name of each record element.
    """

    def __init__(
        self,
        name: str,
        path: str,
        *,
        format: str | None = None,
        results_key: str | None = None,
        xml_record_tag: str = "record",
    ) -> None:
        super().__init__(name)
        self.path = Path(path)
        self.format = format or self._detect_format()
        self.results_key = results_key
        self.xml_record_tag = xml_record_tag

    def _detect_format(self) -> str:
        suffix = self.path.suffix.lower()
        if suffix == ".xml":
            return "xml"
        return "json"

    async def fetch(self) -> list[dict[str, Any]]:
        """Read and parse the local file."""
        try:
            text = self.path.read_text(encoding="utf-8")
        except OSError as exc:
            raise SourceError(self.name, f"Cannot read file: {exc}") from exc

        try:
            if self.format == "xml":
                records = self._parse_xml(text)
            else:
                records = self._parse_json(text)
            logger.info("File source %s loaded %d records", self.name, len(records))
            return records
        except Exception as exc:
            raise SourceError(self.name, f"Parse error: {exc}") from exc

    def _parse_json(self, text: str) -> list[dict[str, Any]]:
        data = json.loads(text)
        if self.results_key:
            data = data[self.results_key]
        if isinstance(data, dict):
            data = [data]
        return data

    def _parse_xml(self, text: str) -> list[dict[str, Any]]:
        root = ET.fromstring(text)
        records: list[dict[str, Any]] = []
        for elem in root.iter(self.xml_record_tag):
            record: dict[str, Any] = dict(elem.attrib)
            for child in elem:
                record[child.tag] = child.text
            records.append(record)
        return records
