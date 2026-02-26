"""Common normalized schema using Pydantic models."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class RecordType(str, Enum):
    """Broad classification of a data record."""
    PRICE = "price"
    TRADE = "trade"
    METRIC = "metric"
    EVENT = "event"
    OTHER = "other"


class NormalizedRecord(BaseModel):
    """Canonical record every source is normalised into.

    Fields are intentionally generic so they can represent prices,
    trades, metrics, or arbitrary events from any source.
    """

    id: str = Field(..., description="Unique record identifier (source-prefixed).")
    source: str = Field(..., description="Name of the originating data source.")
    record_type: RecordType = Field(
        default=RecordType.OTHER, description="Broad record classification."
    )
    symbol: str | None = Field(
        default=None, description="Ticker / asset symbol if applicable."
    )
    timestamp: datetime = Field(
        default_factory=datetime.utcnow,
        description="Record timestamp (UTC).",
    )
    value: float | None = Field(
        default=None, description="Primary numeric value (price, amount, …)."
    )
    volume: float | None = Field(
        default=None, description="Secondary numeric value (volume, quantity, …)."
    )
    currency: str | None = Field(default=None, description="Currency code.")
    label: str | None = Field(
        default=None, description="Human-readable label or title."
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Catch-all for extra fields that don't map elsewhere.",
    )

    model_config = ConfigDict(frozen=True)

    def merge_key(self) -> str:
        """Key used for deduplication / merging."""
        return f"{self.source}:{self.id}"


class PipelineResult(BaseModel):
    """Container returned by the pipeline engine after a run."""

    records: list[NormalizedRecord] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)
    sources_ok: int = 0
    sources_failed: int = 0
    total_raw: int = 0
    total_normalized: int = 0
    total_merged: int = 0
