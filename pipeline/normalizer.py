"""Format-specific normalizers that convert raw dicts to NormalizedRecord."""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from typing import Any, Callable

from .schema import NormalizedRecord, RecordType

logger = logging.getLogger(__name__)

# Type alias for a normalizer function
NormalizerFn = Callable[[dict[str, Any], str], NormalizedRecord | None]


def _safe_float(val: Any) -> float | None:
    """Try to cast *val* to float, return ``None`` on failure."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_datetime(val: Any) -> datetime:
    """Best-effort parse of a timestamp value."""
    if isinstance(val, datetime):
        return val
    if val is None:
        return datetime.now(timezone.utc)
    s = str(val).strip()
    # Try epoch seconds / milliseconds
    try:
        num = float(s)
        if num > 1e12:
            num /= 1000.0
        return datetime.fromtimestamp(num, tz=timezone.utc)
    except ValueError:
        pass
    # Try ISO format
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return datetime.now(timezone.utc)


def _make_id(source: str, raw: dict[str, Any]) -> str:
    """Deterministic ID from source name + record content."""
    for key in ("id", "ID", "Id", "record_id", "uuid"):
        if key in raw:
            return f"{source}_{raw[key]}"
    digest = hashlib.md5(str(sorted(raw.items())).encode()).hexdigest()[:12]
    return f"{source}_{digest}"


def _detect_type(raw: dict[str, Any]) -> RecordType:
    """Heuristic record-type detection."""
    keys = {k.lower() for k in raw}
    if keys & {"price", "ask", "bid", "close", "open"}:
        return RecordType.PRICE
    if keys & {"trade", "side", "buy", "sell"}:
        return RecordType.TRADE
    if keys & {"metric", "measurement", "gauge"}:
        return RecordType.METRIC
    if keys & {"event", "action", "type"}:
        return RecordType.EVENT
    return RecordType.OTHER


def default_normalizer(raw: dict[str, Any], source: str) -> NormalizedRecord | None:
    """Generic normalizer that maps common field names heuristically."""
    try:
        record_id = _make_id(source, raw)
        ts_val = raw.get("timestamp") or raw.get("time") or raw.get("date") or raw.get("datetime")
        symbol = raw.get("symbol") or raw.get("ticker") or raw.get("asset") or raw.get("name")
        value = _safe_float(
            raw.get("price") or raw.get("value") or raw.get("close") or raw.get("amount")
        )
        volume = _safe_float(raw.get("volume") or raw.get("quantity") or raw.get("qty"))
        currency = raw.get("currency") or raw.get("ccy") or raw.get("quote_currency")
        label = raw.get("label") or raw.get("title") or raw.get("description")

        # Everything that wasn't mapped goes into metadata
        mapped_keys = {
            "id", "ID", "Id", "record_id", "uuid",
            "timestamp", "time", "date", "datetime",
            "symbol", "ticker", "asset", "name",
            "price", "value", "close", "amount",
            "volume", "quantity", "qty",
            "currency", "ccy", "quote_currency",
            "label", "title", "description",
        }
        metadata = {k: v for k, v in raw.items() if k not in mapped_keys}

        return NormalizedRecord(
            id=record_id,
            source=source,
            record_type=_detect_type(raw),
            symbol=str(symbol) if symbol else None,
            timestamp=_safe_datetime(ts_val),
            value=value,
            volume=volume,
            currency=str(currency) if currency else None,
            label=str(label) if label else None,
            metadata=metadata,
        )
    except Exception:
        logger.exception("Failed to normalize record from %s: %s", source, raw)
        return None


class Normalizer:
    """Registry of per-source normalizer functions.

    Falls back to :func:`default_normalizer` when no custom function is
    registered for a source.
    """

    def __init__(self) -> None:
        self._registry: dict[str, NormalizerFn] = {}

    def register(self, source_name: str, fn: NormalizerFn) -> None:
        """Register a custom normalizer for *source_name*."""
        self._registry[source_name] = fn

    def normalize(
        self, raw_records: list[dict[str, Any]], source_name: str
    ) -> list[NormalizedRecord]:
        """Normalize a batch of raw records."""
        fn = self._registry.get(source_name, default_normalizer)
        results: list[NormalizedRecord] = []
        for raw in raw_records:
            rec = fn(raw, source_name)
            if rec is not None:
                results.append(rec)
        logger.info(
            "Normalized %d/%d records from %s",
            len(results), len(raw_records), source_name,
        )
        return results
