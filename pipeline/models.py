"""Data models for the pipeline."""
from __future__ import annotations
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any


@dataclass
class Record:
    source: str
    timestamp: str
    value: float
    unit: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class SourceConfig:
    name: str
    kind: str  # rest, csv, websocket, graphql, file
    url: str | None = None
    path: str | None = None
    query: str | None = None
    headers: dict[str, str] | None = None
    timeout: float = 10.0
    field_mapping: dict[str, str] | None = None  # bonus: custom field mapping


@dataclass
class PipelineStats:
    total_records: int = 0
    sources_ok: int = 0
    sources_failed: int = 0
    elapsed_seconds: float = 0.0
    per_source: dict[str, int] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class PipelineResult:
    records: list[Record]
    stats: PipelineStats

    def to_dict(self) -> dict[str, Any]:
        return {
            "records": [r.to_dict() for r in self.records],
            "stats": self.stats.to_dict(),
        }
