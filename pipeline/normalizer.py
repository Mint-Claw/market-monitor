"""Normalize raw records from various sources into NormalizedRecord."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .models import NormalizedRecord, SourceConfig


def _parse_timestamp(raw: Any) -> datetime:
    """Best-effort timestamp parsing."""
    if isinstance(raw, datetime):
        return raw
    if isinstance(raw, (int, float)):
        # Unix timestamp — seconds or milliseconds
        if raw > 1e12:
            return datetime.fromtimestamp(raw / 1000, tz=timezone.utc)
        return datetime.fromtimestamp(raw, tz=timezone.utc)
    if isinstance(raw, str):
        # Try ISO format first
        for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ",
                    "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z",
                    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(raw, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue
    return datetime.now(timezone.utc)


def _extract_value(record: dict, config: SourceConfig) -> float:
    """Extract numeric value from record using config hints."""
    # Check config for explicit field mapping
    value_field = config.params.get("value_field", "value")
    price_fields = [value_field, "value", "price", "amount", "close", "last", "rate", "count"]
    
    for field in price_fields:
        if field in record:
            try:
                return float(record[field])
            except (ValueError, TypeError):
                continue
    
    # Try first numeric value
    for v in record.values():
        try:
            return float(v)
        except (ValueError, TypeError):
            continue
    return 0.0


def _extract_unit(record: dict, config: SourceConfig) -> str:
    """Extract unit from record or config."""
    unit_field = config.params.get("unit_field", "unit")
    if unit_field in record:
        return str(record[unit_field])
    return config.params.get("default_unit", "USD")


def _extract_timestamp(record: dict, config: SourceConfig) -> datetime:
    """Extract timestamp from record using config hints."""
    ts_field = config.params.get("timestamp_field", "timestamp")
    ts_fields = [ts_field, "timestamp", "time", "date", "created_at", "ts", "datetime"]
    
    for field in ts_fields:
        if field in record:
            return _parse_timestamp(record[field])
    return datetime.now(timezone.utc)


def normalize_record(record: dict, config: SourceConfig) -> NormalizedRecord:
    """Normalize a single raw record."""
    return NormalizedRecord(
        source=config.name,
        timestamp=_extract_timestamp(record, config),
        value=_extract_value(record, config),
        unit=_extract_unit(record, config),
        metadata={k: v for k, v in record.items()
                  if k not in ("timestamp", "time", "date", "value", "price", "unit")},
    )


def normalize_records(records: list[dict], config: SourceConfig) -> list[NormalizedRecord]:
    """Normalize a batch of raw records."""
    return [normalize_record(r, config) for r in records]
