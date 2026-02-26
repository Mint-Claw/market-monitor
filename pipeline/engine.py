"""Main pipeline: concurrent fetch, normalize, merge, sort, output."""
from __future__ import annotations
import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any

from .adapters import create_adapter
from .models import PipelineResult, PipelineStats, Record, SourceConfig

logger = logging.getLogger(__name__)


def load_config(path: str | Path) -> list[SourceConfig]:
    """Load source configs from a JSON file."""
    with open(path) as f:
        raw = json.load(f)
    sources = raw if isinstance(raw, list) else raw.get("sources", [])
    return [SourceConfig(**s) for s in sources]


async def _fetch_one(config: SourceConfig) -> tuple[str, list[Record] | Exception]:
    adapter = create_adapter(config)
    try:
        records = await adapter.fetch()
        return config.name, records
    except Exception as e:
        logger.warning("Source %s failed: %s", config.name, e)
        return config.name, e


async def run_pipeline(
    sources: list[SourceConfig],
    output_path: str | Path | None = None,
    incremental_state: dict[str, str] | None = None,
) -> PipelineResult:
    """Run the full pipeline: fetch concurrently, normalize, merge, sort."""
    t0 = time.monotonic()

    tasks = [asyncio.create_task(_fetch_one(s)) for s in sources]
    results = await asyncio.gather(*tasks)

    all_records: list[Record] = []
    per_source: dict[str, int] = {}
    ok = 0
    failed = 0

    for name, result in results:
        if isinstance(result, Exception):
            failed += 1
            per_source[name] = 0
        else:
            # Incremental: skip records with timestamp <= last seen
            if incremental_state and name in incremental_state:
                last_ts = incremental_state[name]
                result = [r for r in result if r.timestamp > last_ts]
            ok += 1
            per_source[name] = len(result)
            all_records.extend(result)

    # Sort by timestamp
    all_records.sort(key=lambda r: r.timestamp)

    elapsed = time.monotonic() - t0

    stats = PipelineStats(
        total_records=len(all_records),
        sources_ok=ok,
        sources_failed=failed,
        elapsed_seconds=round(elapsed, 4),
        per_source=per_source,
    )

    pipeline_result = PipelineResult(records=all_records, stats=stats)

    if output_path:
        with open(output_path, "w") as f:
            json.dump(pipeline_result.to_dict(), f, indent=2)

    # Update incremental state
    if incremental_state is not None:
        for name, result in results:
            if not isinstance(result, Exception) and result:
                latest = max(r.timestamp for r in result)
                incremental_state[name] = latest

    return pipeline_result
