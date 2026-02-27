"""
Base source class and plugin registry.

All source types inherit from BaseSource and register themselves with
@register_source("type_name"). The pipeline discovers sources purely
through this registry — no core code changes needed for new sources.
"""

from __future__ import annotations

import abc
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Plugin registry
# ---------------------------------------------------------------------------
SOURCE_REGISTRY: dict[str, type["BaseSource"]] = {}


def register_source(type_name: str):
    """Class decorator that registers a source type by name."""

    def decorator(cls: type[BaseSource]):
        SOURCE_REGISTRY[type_name] = cls
        cls.source_type = type_name
        return cls

    return decorator


def get_source_class(type_name: str) -> type["BaseSource"]:
    """Look up a registered source class by its config ``type`` field."""
    if type_name not in SOURCE_REGISTRY:
        available = ", ".join(sorted(SOURCE_REGISTRY))
        raise KeyError(
            f"Unknown source type '{type_name}'. Registered: {available}"
        )
    return SOURCE_REGISTRY[type_name]


# ---------------------------------------------------------------------------
# Base class
# ---------------------------------------------------------------------------
class BaseSource(abc.ABC):
    """Abstract base for every data-source plugin."""

    source_type: str = "base"

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.name: str = config["name"]
        self.unit: str = config.get("unit", "record")

    # ---- public API -------------------------------------------------------

    @abc.abstractmethod
    async def fetch(self) -> list[dict[str, Any]]:
        """Fetch raw records from the upstream source.  Must be async."""
        ...

    def normalize(self, raw_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Convert raw records into the common pipeline schema.

        Schema::

            {
                "source":    str,   # e.g. "rest_jsonplaceholder"
                "timestamp": str,   # ISO-8601
                "value":     Any,   # the actual data payload
                "unit":      str,   # e.g. "record", "trade", "row"
            }

        Subclasses may override this for source-specific timestamp
        extraction or value reshaping.
        """
        now = datetime.now(timezone.utc).isoformat()
        return [
            {
                "source": self.name,
                "timestamp": self._extract_timestamp(rec) or now,
                "value": rec,
                "unit": self.unit,
            }
            for rec in raw_records
        ]

    # ---- helpers ----------------------------------------------------------

    @staticmethod
    def _extract_timestamp(record: dict[str, Any]) -> str | None:
        """Best-effort extraction of an ISO timestamp from a record."""
        for key in ("timestamp", "date", "Date", "created_at", "time", "T"):
            val = record.get(key)
            if val is None:
                continue
            # Epoch milliseconds (e.g. Binance trade time)
            if isinstance(val, (int, float)) and val > 1e12:
                return datetime.fromtimestamp(val / 1000, tz=timezone.utc).isoformat()
            if isinstance(val, (int, float)):
                return datetime.fromtimestamp(val, tz=timezone.utc).isoformat()
            # Date-only strings like "2020-01-22"
            s = str(val)
            if len(s) == 10 and s[4] == "-":
                return s + "T00:00:00+00:00"
            return s
        return None
