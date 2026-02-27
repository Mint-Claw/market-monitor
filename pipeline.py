#!/usr/bin/env python3
"""
Multi-Source Data Aggregation Pipeline
=======================================

Fetches data from 5+ concurrent sources, normalises every record to a
common schema, merges into a single sorted dataset, and persists through
DLT to a local DuckDB warehouse **and** a human-readable JSON file.

Usage::

    python pipeline.py                  # run with default config.json
    python pipeline.py --config my.json # custom config
    python pipeline.py --dry-run        # validate config, don't fetch

Architecture::

    config.json
        -> SourceRegistry discovers source classes
        -> asyncio.gather fetches all sources concurrently
        -> each source.normalize() -> common schema
        -> merge + sort by timestamp
        -> dlt.pipeline.run()  (DuckDB)
        -> JSON output + stats
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import dlt

# Importing sources triggers @register_source decorators
from sources import get_source_class, SOURCE_REGISTRY

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-5s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("pipeline")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent


def load_config(path: str | Path) -> dict[str, Any]:
    """Load and validate the pipeline configuration file."""
    p = Path(path)
    if not p.is_absolute():
        p = _PROJECT_ROOT / p
    if not p.exists():
        raise FileNotFoundError(f"Config file not found: {p}")
    with open(p) as f:
        cfg = json.load(f)
    assert "sources" in cfg, "Config must contain a 'sources' list"
    assert "pipeline" in cfg, "Config must contain a 'pipeline' section"
    return cfg


# ---------------------------------------------------------------------------
# Single-source fetch + normalise
# ---------------------------------------------------------------------------
async def fetch_one(
    source_cfg: dict[str, Any],
    timeout: float,
) -> tuple[str, list[dict[str, Any]], str | None]:
    """Instantiate the right source plugin, fetch, and normalise.

    Returns (source_name, records, error_or_None).
    On failure / timeout returns an empty list instead of raising.
    """
    name = source_cfg["name"]
    stype = source_cfg["type"]

    try:
        cls = get_source_class(stype)
        src = cls(source_cfg)
        raw = await asyncio.wait_for(src.fetch(), timeout=timeout)
        return name, src.normalize(raw), None
    except asyncio.TimeoutError:
        logger.warning("TIMEOUT  %s (%.1fs limit)", name, timeout)
        return name, [], f"Timeout after {timeout}s"
    except Exception as exc:
        logger.error("FAIL     %s: %s", name, exc)
        return name, [], str(exc)


# ---------------------------------------------------------------------------
# Pipeline orchestration
# ---------------------------------------------------------------------------
async def run_pipeline(config: dict[str, Any]) -> dict[str, Any]:
    """Run the full aggregation pipeline and return stats."""
    t0 = time.monotonic()
    pcfg = config["pipeline"]
    timeout = pcfg.get("timeout_seconds", 10)

    # 1. Concurrent fetch from all enabled sources
    enabled = [s for s in config["sources"] if s.get("enabled", True)]
    logger.info("Starting %d source(s) concurrently ...", len(enabled))

    tasks = [fetch_one(s, timeout) for s in enabled]
    results = await asyncio.gather(*tasks)

    # 2. Merge all records into one list
    merged: list[dict[str, Any]] = []
    source_stats: dict[str, int] = {}
    errors: dict[str, str] = {}

    for name, records, error in results:
        source_stats[name] = len(records)
        if error:
            errors[name] = error
        merged.extend(records)

    # 3. Sort by timestamp
    merged.sort(key=lambda r: r.get("timestamp", ""))

    elapsed = time.monotonic() - t0
    logger.info(
        "Aggregated %d records from %d sources in %.2fs",
        len(merged), len(enabled), elapsed,
    )

    # 4. Persist via DLT into DuckDB
    pipeline_name = pcfg.get("name", "market_monitor")
    output_dir = _PROJECT_ROOT / pcfg.get("output_dir", "output")
    output_dir.mkdir(parents=True, exist_ok=True)

    dlt_load_id = None
    try:
        dlt_pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination="duckdb",
            dataset_name=f"{pipeline_name}_data",
            pipelines_dir=str(output_dir / ".dlt"),
        )

        @dlt.resource(name="aggregated_records", write_disposition="replace")
        def aggregated_resource():
            yield from merged

        load_info = dlt_pipeline.run(aggregated_resource())
        dlt_load_id = load_info.loads_ids[0] if load_info.loads_ids else None
        logger.info("DLT load complete: %s", dlt_load_id)
    except Exception as exc:
        logger.warning("DLT pipeline step failed (non-critical): %s", exc)

    # 5. Write JSON output + stats file
    stats = {
        "pipeline": pipeline_name,
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": round(elapsed, 3),
        "total_records": len(merged),
        "sources_attempted": len(enabled),
        "sources_succeeded": len(enabled) - len(errors),
        "sources_failed": len(errors),
        "source_record_counts": source_stats,
        "errors": errors,
    }
    if dlt_load_id:
        stats["dlt_load_id"] = dlt_load_id

    output_file = output_dir / pcfg.get("output_file", "aggregated.json")
    payload = {"stats": stats, "records": merged}
    with open(output_file, "w") as f:
        json.dump(payload, f, indent=2, default=str)
    logger.info("Output written to %s", output_file)

    stats_file = output_dir / "stats.json"
    with open(stats_file, "w") as f:
        json.dump(stats, f, indent=2)
    logger.info("Stats  written to %s", stats_file)

    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Multi-source data aggregation pipeline",
    )
    parser.add_argument(
        "--config", default="config.json", help="Path to config file",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Validate config only",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    logger.info("Loaded config with %d source(s)", len(config["sources"]))
    logger.info("Registered source types: %s", ", ".join(sorted(SOURCE_REGISTRY)))

    if args.dry_run:
        for s in config["sources"]:
            status = "enabled" if s.get("enabled", True) else "disabled"
            try:
                get_source_class(s["type"])
                logger.info(
                    "  [OK]   %-25s  type=%-10s  %s",
                    s["name"], s["type"], status,
                )
            except KeyError as e:
                logger.error("  [ERR]  %-25s  %s", s["name"], e)
        sys.exit(0)

    stats = asyncio.run(run_pipeline(config))

    # Pretty-print summary
    print("\n" + "=" * 60)
    print("  Pipeline run complete")
    print("=" * 60)
    print(f"  Total records : {stats['total_records']}")
    print(f"  Elapsed       : {stats['elapsed_seconds']}s")
    if stats.get("dlt_load_id"):
        print(f"  DLT load ID   : {stats['dlt_load_id']}")
    print(f"  Sources       : {stats['sources_succeeded']}/{stats['sources_attempted']} succeeded")
    print("  Per source:")
    for name, count in stats["source_record_counts"].items():
        tag = "OK" if name not in stats.get("errors", {}) else "FAIL"
        print(f"    {name:30s} {count:>6d}  [{tag}]")
    if stats.get("errors"):
        print("  Errors:")
        for name, err in stats["errors"].items():
            print(f"    {name}: {err}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
