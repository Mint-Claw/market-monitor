"""Schema normalization for multi-source data aggregation."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


REQUIRED_FIELDS = {"source", "timestamp", "value", "unit"}


def normalize_record(raw: dict[str, Any], source_name: str) -> dict[str, Any] | None:
    """Normalize a raw record to {source, timestamp, value, unit}.

    Returns None if the record cannot be normalized.
    """
    try:
        record = {
            "source": source_name,
            "timestamp": _parse_timestamp(raw),
            "value": _parse_value(raw),
            "unit": _parse_unit(raw),
        }
        return record
    except (KeyError, ValueError, TypeError):
        return None


def _parse_timestamp(raw: dict[str, Any]) -> str:
    """Extract and normalize timestamp to ISO-8601 UTC string."""
    for key in ("timestamp", "time", "date", "datetime", "ts", "created_at"):
        if key in raw and raw[key] is not None:
            val = raw[key]
            if isinstance(val, (int, float)):
                # Unix epoch — seconds or milliseconds
                if val > 1e12:
                    val = val / 1000.0
                dt = datetime.fromtimestamp(val, tz=timezone.utc)
                return dt.isoformat()
            if isinstance(val, str):
                # Try ISO parse
                for fmt in (
                    "%Y-%m-%dT%H:%M:%S%z",
                    "%Y-%m-%dT%H:%M:%SZ",
                    "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%d",
                ):
                    try:
                        dt = datetime.strptime(val, fmt)
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        return dt.isoformat()
                    except ValueError:
                        continue
                raise ValueError(f"Unparseable timestamp: {val}")
    raise KeyError("No timestamp field found")


def _parse_value(raw: dict[str, Any]) -> float:
    """Extract numeric value."""
    for key in ("value", "price", "amount", "val", "close"):
        if key in raw and raw[key] is not None:
            return float(raw[key])
    raise KeyError("No value field found")


def _parse_unit(raw: dict[str, Any]) -> str:
    """Extract unit string."""
    for key in ("unit", "currency", "symbol", "asset"):
        if key in raw and raw[key] is not None:
            return str(raw[key])
    return "USD"
