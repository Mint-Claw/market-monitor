"""
Multi-Source Data Aggregation & Normalization Pipeline
======================================================
Async pipeline fetching from 5+ concurrent sources (REST, CSV, WebSocket,
GraphQL, file), normalizing to a common schema and merging to sorted JSON.

Schema: {source, timestamp, value, unit}

Bounty: Mint-Claw/market-monitor#1
Author: Pipeline (autonomous AI data engineering agent)
"""
from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp
import yaml

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema & Normalization
# ---------------------------------------------------------------------------

class Record:
    """Normalized record with common schema."""
    __slots__ = ("source", "timestamp", "value", "unit")

    def __init__(self, source: str, timestamp: str, value: float, unit: str):
        self.source = source
        self.timestamp = timestamp
        self.value = value
        self.unit = unit

    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "timestamp": self.timestamp,
            "value": self.value,
            "unit": self.unit,
        }


def _resolve_field(raw: dict, field_spec: str) -> Any:
    """Resolve a field value from raw data using a field spec.

    - Quoted strings like "'rest_crypto'" return the literal.
    - Dotted paths like 'data.price' traverse nested dicts.
    - Plain keys like 'price' do a direct lookup.
    """
    if field_spec.startswith("'") and field_spec.endswith("'"):
        return field_spec[1:-1]
    parts = field_spec.split(".")
    val = raw
    for p in parts:
        if isinstance(val, dict):
            val = val.get(p)
        else:
            return None
    return val


def normalize(raw: dict, field_map: dict) -> Record | None:
    """Normalize a raw dict to a Record using the field mapping."""
    try:
        source = str(_resolve_field(raw, field_map["source"]))
        ts_raw = _resolve_field(raw, field_map["timestamp"])
        value_raw = _resolve_field(raw, field_map["value"])
        unit = str(_resolve_field(raw, field_map["unit"]) or "")

        # Parse value
        if value_raw is None:
            return None
        value = float(value_raw)

        # Normalize timestamp to ISO-8601
        if isinstance(ts_raw, (int, float)):
            ts = datetime.fromtimestamp(ts_raw, tz=timezone.utc).isoformat()
        elif isinstance(ts_raw, str):
            ts = ts_raw  # assume already ISO
        else:
            ts = datetime.now(timezone.utc).isoformat()

        return Record(source=source, timestamp=ts, value=value, unit=unit)
    except (ValueError, TypeError, KeyError) as exc:
        logger.warning("Normalization failed for %s: %s", raw, exc)
        return None


# ---------------------------------------------------------------------------
# Source Fetchers
# ---------------------------------------------------------------------------

async def fetch_rest(
    session: aiohttp.ClientSession, cfg: dict, sem: asyncio.Semaphore
) -> list[dict]:
    """Fetch from a REST API endpoint."""
    async with sem:
        try:
            headers = cfg.get("headers", {})
            timeout = aiohttp.ClientTimeout(total=cfg.get("timeout_s", 10))
            async with session.get(cfg["url"], headers=headers, timeout=timeout) as resp:
                resp.raise_for_status()
                data = await resp.json()
                if isinstance(data, list):
                    return data
                if isinstance(data, dict) and "items" in data:
                    return data["items"]
                if isinstance(data, dict) and "data" in data:
                    return data["data"] if isinstance(data["data"], list) else [data]
                return [data]
        except Exception as exc:
            logger.error("REST fetch failed for %s: %s", cfg["id"], exc)
            return []


async def fetch_csv(cfg: dict, sem: asyncio.Semaphore) -> list[dict]:
    """Read records from a CSV file."""
    async with sem:
        try:
            path = Path(cfg["path"])
            if not path.exists():
                logger.error("CSV file not found: %s", path)
                return []
            text = path.read_text(encoding=cfg.get("encoding", "utf-8"))
            reader = csv.DictReader(io.StringIO(text))
            return list(reader)
        except Exception as exc:
            logger.error("CSV fetch failed for %s: %s", cfg["id"], exc)
            return []


async def fetch_websocket(cfg: dict, sem: asyncio.Semaphore) -> list[dict]:
    """Collect messages from a WebSocket endpoint."""
    async with sem:
        try:
            import websockets
        except ImportError:
            # Fallback: use aiohttp websocket
            return await _fetch_ws_aiohttp(cfg)

        try:
            messages = []
            max_msgs = cfg.get("max_messages", 50)
            timeout_s = cfg.get("timeout_s", 5)

            async with websockets.connect(cfg["url"]) as ws:
                try:
                    async with asyncio.timeout(timeout_s):
                        while len(messages) < max_msgs:
                            msg = await ws.recv()
                            data = json.loads(msg)
                            if isinstance(data, list):
                                messages.extend(data)
                            else:
                                messages.append(data)
                except (asyncio.TimeoutError, TimeoutError):
                    pass  # normal — collect what we got
            return messages
        except Exception as exc:
            logger.error("WebSocket fetch failed for %s: %s", cfg["id"], exc)
            return []


async def _fetch_ws_aiohttp(cfg: dict) -> list[dict]:
    """Fallback WebSocket fetcher using aiohttp."""
    messages = []
    max_msgs = cfg.get("max_messages", 50)
    timeout_s = cfg.get("timeout_s", 5)

    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(cfg["url"]) as ws:
                try:
                    async with asyncio.timeout(timeout_s):
                        while len(messages) < max_msgs:
                            msg = await ws.receive()
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                messages.append(data) if isinstance(data, dict) else messages.extend(data)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                except (asyncio.TimeoutError, TimeoutError):
                    pass
    except Exception as exc:
        logger.error("aiohttp WS fetch failed for %s: %s", cfg["id"], exc)
    return messages


async def fetch_graphql(
    session: aiohttp.ClientSession, cfg: dict, sem: asyncio.Semaphore
) -> list[dict]:
    """Fetch from a GraphQL endpoint."""
    async with sem:
        try:
            timeout = aiohttp.ClientTimeout(total=cfg.get("timeout_s", 10))
            payload = {"query": cfg["query"]}
            async with session.post(
                cfg["url"],
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=timeout,
            ) as resp:
                resp.raise_for_status()
                data = await resp.json()
                # Navigate to items using items_path
                items_path = cfg.get("items_path", "")
                result = data
                for key in items_path.split("."):
                    if key and isinstance(result, dict):
                        result = result.get(key, [])
                return result if isinstance(result, list) else [result]
        except Exception as exc:
            logger.error("GraphQL fetch failed for %s: %s", cfg["id"], exc)
            return []


async def fetch_file(cfg: dict, sem: asyncio.Semaphore) -> list[dict]:
    """Read records from a JSON file."""
    async with sem:
        try:
            path = Path(cfg["path"])
            if not path.exists():
                logger.error("File not found: %s", path)
                return []
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            # Navigate to items using items_path
            items_path = cfg.get("items_path", "")
            result = data
            for key in items_path.split("."):
                if key and isinstance(result, dict):
                    result = result.get(key, data)
            return result if isinstance(result, list) else [result]
        except Exception as exc:
            logger.error("File fetch failed for %s: %s", cfg["id"], exc)
            return []


# ---------------------------------------------------------------------------
# Incremental State
# ---------------------------------------------------------------------------

def load_state(state_file: str) -> dict:
    """Load incremental pipeline state (last-seen timestamps per source)."""
    path = Path(state_file)
    if path.exists():
        with open(path, "r") as f:
            return json.load(f)
    return {}


def save_state(state_file: str, state: dict) -> None:
    """Save incremental pipeline state."""
    path = Path(state_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(state, f, indent=2)


def filter_incremental(records: list[Record], last_ts: str | None) -> list[Record]:
    """Filter records newer than last_ts for incremental mode."""
    if not last_ts:
        return records
    return [r for r in records if r.timestamp > last_ts]


# ---------------------------------------------------------------------------
# Pipeline Orchestrator
# ---------------------------------------------------------------------------

FETCHER_MAP = {
    "rest": "fetch_rest",
    "csv": "fetch_csv",
    "websocket": "fetch_websocket",
    "graphql": "fetch_graphql",
    "file": "fetch_file",
}


async def run_pipeline(config_path: str = "pipeline_config.yaml") -> dict:
    """Run the full aggregation pipeline.

    Returns: {records: [...], stats: {...}}
    """
    t0 = time.monotonic()

    # Load config
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    pipeline_cfg = config["pipeline"]
    sources = config["sources"]
    max_concurrent = pipeline_cfg.get("max_concurrent", 10)
    incremental = pipeline_cfg.get("incremental", False)
    state_file = pipeline_cfg.get("state_file", "output/.pipeline_state.json")

    sem = asyncio.Semaphore(max_concurrent)
    state = load_state(state_file) if incremental else {}

    # Dispatch concurrent fetches
    async with aiohttp.ClientSession() as session:
        tasks = []
        for src in sources:
            src_type = src["type"]
            if src_type == "rest":
                tasks.append(("rest", src, fetch_rest(session, src, sem)))
            elif src_type == "csv":
                tasks.append(("csv", src, fetch_csv(src, sem)))
            elif src_type == "websocket":
                tasks.append(("websocket", src, fetch_websocket(src, sem)))
            elif src_type == "graphql":
                tasks.append(("graphql", src, fetch_graphql(session, src, sem)))
            elif src_type == "file":
                tasks.append(("file", src, fetch_file(src, sem)))
            else:
                logger.warning("Unknown source type: %s", src_type)

        # Gather all fetches concurrently
        raw_results = await asyncio.gather(
            *[t[2] for t in tasks], return_exceptions=True
        )

    # Normalize all records
    all_records: list[Record] = []
    source_stats: dict[str, dict] = {}

    for (src_type, src_cfg, _), raw in zip(tasks, raw_results):
        src_id = src_cfg["id"]
        field_map = src_cfg["field_map"]

        if isinstance(raw, Exception):
            logger.error("Source %s raised: %s", src_id, raw)
            source_stats[src_id] = {
                "status": "error",
                "error": str(raw),
                "records": 0,
            }
            continue

        if not isinstance(raw, list):
            raw = [raw] if raw else []

        records = []
        for item in raw:
            rec = normalize(item, field_map)
            if rec:
                records.append(rec)

        # Incremental filtering
        if incremental:
            last_ts = state.get(src_id, {}).get("last_timestamp")
            records = filter_incremental(records, last_ts)
            if records:
                max_ts = max(r.timestamp for r in records)
                state.setdefault(src_id, {})["last_timestamp"] = max_ts

        all_records.extend(records)

        values = [r.value for r in records]
        source_stats[src_id] = {
            "status": "ok",
            "type": src_type,
            "records": len(records),
            "raw_fetched": len(raw),
            "min": min(values) if values else None,
            "max": max(values) if values else None,
            "mean": sum(values) / len(values) if values else None,
        }

    # Sort by timestamp
    all_records.sort(key=lambda r: r.timestamp)

    elapsed = time.monotonic() - t0

    # Build output
    output = {
        "pipeline": pipeline_cfg["name"],
        "version": pipeline_cfg["version"],
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": round(elapsed, 3),
        "total_records": len(all_records),
        "records": [r.to_dict() for r in all_records],
    }

    stats = {
        "pipeline": pipeline_cfg["name"],
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": round(elapsed, 3),
        "total_records": len(all_records),
        "sources": source_stats,
        "incremental": incremental,
    }

    # Write output files
    out_dir = Path(pipeline_cfg.get("output_file", "output/aggregated.json")).parent
    out_dir.mkdir(parents=True, exist_ok=True)

    with open(pipeline_cfg["output_file"], "w") as f:
        json.dump(output, f, indent=2)

    with open(pipeline_cfg.get("stats_file", "output/stats.json"), "w") as f:
        json.dump(stats, f, indent=2)

    if incremental:
        save_state(state_file, state)

    logger.info(
        "Pipeline complete: %d records from %d sources in %.2fs",
        len(all_records),
        len(sources),
        elapsed,
    )

    return {"records": output, "stats": stats}


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Multi-Source Aggregation Pipeline")
    parser.add_argument(
        "-c", "--config", default="pipeline_config.yaml", help="Config file path"
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Debug logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )

    result = asyncio.run(run_pipeline(args.config))
    print(f"Done: {result['stats']['total_records']} records in {result['stats']['elapsed_seconds']}s")
