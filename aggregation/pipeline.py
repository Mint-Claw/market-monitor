"""Async multi-source data aggregation & normalization pipeline."""

from __future__ import annotations

import asyncio
import csv
import io
import json
import os
import time
from pathlib import Path
from typing import Any

import aiohttp
import yaml

from .normalizer import normalize_record


async def fetch_rest(session: aiohttp.ClientSession, src: dict) -> list[dict]:
    """Fetch data from a REST API endpoint."""
    params = src.get("params", {})
    timeout = aiohttp.ClientTimeout(total=src.get("timeout", 10))
    async with session.get(src["url"], params=params, timeout=timeout) as resp:
        resp.raise_for_status()
        data = await resp.json()
        return data if isinstance(data, list) else data.get("data", data.get("results", []))


async def fetch_csv(src: dict) -> list[dict]:
    """Read records from a CSV file or URL."""
    path = src.get("path", "")
    encoding = src.get("encoding", "utf-8")

    if path.startswith(("http://", "https://")):
        async with aiohttp.ClientSession() as session:
            timeout = aiohttp.ClientTimeout(total=src.get("timeout", 10))
            async with session.get(path, timeout=timeout) as resp:
                resp.raise_for_status()
                text = await resp.text()
    else:
        text = Path(path).read_text(encoding=encoding)

    reader = csv.DictReader(io.StringIO(text))
    return list(reader)


async def fetch_websocket(session: aiohttp.ClientSession, src: dict) -> list[dict]:
    """Collect messages from a WebSocket until timeout or max_messages."""
    timeout = src.get("timeout", 5)
    max_msgs = src.get("max_messages", 500)
    records: list[dict] = []

    async def _collect():
        async with session.ws_connect(src["url"]) as ws:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    if isinstance(data, list):
                        records.extend(data)
                    else:
                        records.append(data)
                    if len(records) >= max_msgs:
                        break
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    break

    try:
        await asyncio.wait_for(_collect(), timeout=timeout)
    except asyncio.TimeoutError:
        pass  # Expected — collect what we got
    return records


async def fetch_graphql(session: aiohttp.ClientSession, src: dict) -> list[dict]:
    """Fetch data from a GraphQL endpoint."""
    timeout = aiohttp.ClientTimeout(total=src.get("timeout", 10))
    payload = {"query": src["query"]}
    async with session.post(src["url"], json=payload, timeout=timeout) as resp:
        resp.raise_for_status()
        body = await resp.json()
        data = body.get("data", {})
        # Return the first list found in the response data
        for v in data.values():
            if isinstance(v, list):
                return v
        return []


async def fetch_file(src: dict) -> list[dict]:
    """Read records from a local JSON file."""
    path = Path(src["path"])
    text = path.read_text(encoding=src.get("encoding", "utf-8"))
    data = json.loads(text)
    return data if isinstance(data, list) else data.get("data", data.get("results", []))


FETCHERS = {
    "rest": lambda session, src: fetch_rest(session, src),
    "csv": lambda session, src: fetch_csv(src),
    "websocket": lambda session, src: fetch_websocket(session, src),
    "graphql": lambda session, src: fetch_graphql(session, src),
    "file": lambda session, src: fetch_file(src),
}


async def _fetch_source(
    session: aiohttp.ClientSession, src: dict
) -> tuple[str, list[dict], str | None]:
    """Fetch a single source, returning (name, records, error)."""
    name = src["name"]
    src_type = src["type"]
    fetcher = FETCHERS.get(src_type)
    if not fetcher:
        return name, [], f"Unknown source type: {src_type}"
    try:
        raw_records = await fetcher(session, src)
        normalized = []
        for raw in raw_records:
            rec = normalize_record(raw, name)
            if rec is not None:
                normalized.append(rec)
        return name, normalized, None
    except Exception as e:
        return name, [], f"{type(e).__name__}: {e}"


def load_config(config_path: str) -> dict:
    """Load pipeline configuration from YAML."""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


async def run_pipeline(config_path: str) -> dict[str, Any]:
    """Run the full aggregation pipeline. Returns merged data + stats."""
    config = load_config(config_path)
    sources = config.get("sources", [])
    pipeline_cfg = config.get("pipeline", {})
    global_timeout = pipeline_cfg.get("global_timeout", 15)
    output_path = pipeline_cfg.get("output_path", "output/merged.json")
    sort_by = pipeline_cfg.get("sort_by", "timestamp")
    sort_order = pipeline_cfg.get("sort_order", "asc")

    start = time.monotonic()
    all_records: list[dict] = []
    stats: dict[str, Any] = {"sources": {}, "errors": []}

    async with aiohttp.ClientSession() as session:
        try:
            tasks = [_fetch_source(session, src) for src in sources]
            results = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=global_timeout,
            )
        except asyncio.TimeoutError:
            stats["errors"].append("Global timeout exceeded")
            results = []

    for result in results:
        if isinstance(result, Exception):
            stats["errors"].append(str(result))
            continue
        name, records, error = result
        stats["sources"][name] = {"records": len(records)}
        if error:
            stats["sources"][name]["error"] = error
            stats["errors"].append(f"{name}: {error}")
        all_records.extend(records)

    # Sort
    reverse = sort_order == "desc"
    all_records.sort(key=lambda r: r.get(sort_by, ""), reverse=reverse)

    elapsed = time.monotonic() - start
    stats["total_records"] = len(all_records)
    stats["elapsed_seconds"] = round(elapsed, 3)

    # Write output
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(all_records, indent=2))

    return {"records": all_records, "stats": stats}


def main(config_path: str = "aggregation/config.yaml") -> None:
    """CLI entry point."""
    result = asyncio.run(run_pipeline(config_path))
    stats = result["stats"]
    print(f"Pipeline complete: {stats['total_records']} records in {stats['elapsed_seconds']}s")
    for name, info in stats["sources"].items():
        err = f" (ERROR: {info['error']})" if "error" in info else ""
        print(f"  {name}: {info['records']} records{err}")
    if stats["errors"]:
        print(f"  Errors: {len(stats['errors'])}")


if __name__ == "__main__":
    import sys
    cfg = sys.argv[1] if len(sys.argv) > 1 else "aggregation/config.yaml"
    main(cfg)
