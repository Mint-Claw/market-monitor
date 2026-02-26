"""Main pipeline: fetch → normalize → merge → output."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any

import aiohttp

from .models import NormalizedRecord, PipelineStats, SourceConfig
from .fetchers import fetch_source
from .normalizer import normalize_records

logger = logging.getLogger(__name__)


class Pipeline:
    """Config-driven multi-source data aggregation pipeline."""

    def __init__(self, sources: list[SourceConfig], state_path: str | None = None):
        self.sources = sources
        self.state_path = Path(state_path) if state_path else None
        self._state: dict[str, str] = {}  # source_name -> last_timestamp (for incremental)
        if self.state_path and self.state_path.exists():
            self._state = json.loads(self.state_path.read_text())

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> Pipeline:
        """Create pipeline from a config dict."""
        sources = []
        for src in config.get("sources", []):
            sources.append(SourceConfig(
                name=src["name"],
                kind=src["kind"],
                url=src.get("url"),
                path=src.get("path"),
                query=src.get("query"),
                headers=src.get("headers", {}),
                timeout=src.get("timeout", 10.0),
                params=src.get("params", {}),
            ))
        return cls(sources, state_path=config.get("state_path"))

    @classmethod
    def from_config_file(cls, path: str | Path) -> Pipeline:
        """Load pipeline config from a JSON file."""
        config = json.loads(Path(path).read_text())
        return cls.from_config(config)

    async def _fetch_one(
        self, session: aiohttp.ClientSession, config: SourceConfig, stats: PipelineStats
    ) -> list[NormalizedRecord]:
        """Fetch and normalize one source with error handling."""
        try:
            raw = await fetch_source(session, config)
            records = normalize_records(raw, config)

            # Incremental: filter records newer than last seen
            last_ts = self._state.get(config.name)
            if last_ts:
                from datetime import datetime, timezone
                cutoff = datetime.fromisoformat(last_ts)
                records = [r for r in records if r.timestamp > cutoff]

            if records:
                newest = max(r.timestamp for r in records)
                self._state[config.name] = newest.isoformat()

            stats.sources_ok += 1
            return records
        except Exception as e:
            stats.sources_failed += 1
            stats.errors.append(f"{config.name}: {type(e).__name__}: {e}")
            logger.warning("Source %s failed: %s", config.name, e)
            return []

    async def run(self) -> tuple[list[NormalizedRecord], PipelineStats]:
        """Execute the full pipeline: concurrent fetch → normalize → merge → sort."""
        stats = PipelineStats()
        t0 = time.perf_counter()

        connector = aiohttp.TCPConnector(limit=20)
        async with aiohttp.ClientSession(connector=connector) as session:
            t_fetch_start = time.perf_counter()
            tasks = [self._fetch_one(session, src, stats) for src in self.sources]
            results = await asyncio.gather(*tasks)
            stats.fetch_time_ms = (time.perf_counter() - t_fetch_start) * 1000

        # Merge + sort
        t_norm_start = time.perf_counter()
        all_records: list[NormalizedRecord] = []
        for batch in results:
            all_records.extend(batch)
        all_records.sort(key=lambda r: r.timestamp)
        stats.normalize_time_ms = (time.perf_counter() - t_norm_start) * 1000

        stats.total_records = len(all_records)
        stats.total_time_ms = (time.perf_counter() - t0) * 1000

        # Save incremental state
        if self.state_path:
            self.state_path.write_text(json.dumps(self._state, indent=2))

        return all_records, stats

    def run_sync(self) -> tuple[list[NormalizedRecord], PipelineStats]:
        """Synchronous wrapper for run()."""
        return asyncio.run(self.run())

    @staticmethod
    def to_json(records: list[NormalizedRecord], stats: PipelineStats, path: str | None = None) -> str:
        """Output records + stats as JSON."""
        output = {
            "records": [r.to_dict() for r in records],
            "stats": stats.to_dict(),
        }
        text = json.dumps(output, indent=2, default=str)
        if path:
            Path(path).write_text(text)
        return text
