"""Common data models for the pipeline."""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any


@dataclass
class NormalizedRecord:
    """Unified record schema: {source, timestamp, value, unit}."""
    source: str
    timestamp: datetime
    value: float
    unit: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["timestamp"] = self.timestamp.isoformat()
        return d


@dataclass
class SourceConfig:
    """Config-driven source definition."""
    name: str
    kind: str  # rest | csv | websocket | graphql | file
    url: str | None = None
    path: str | None = None
    query: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    timeout: float = 10.0
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineStats:
    """Stats about a pipeline run."""
    total_records: int = 0
    sources_ok: int = 0
    sources_failed: int = 0
    fetch_time_ms: float = 0
    normalize_time_ms: float = 0
    total_time_ms: float = 0
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)
