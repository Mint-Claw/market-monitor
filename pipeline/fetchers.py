"""Async fetchers for each source type."""
from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
from pathlib import Path
from typing import Any

import aiohttp

from .models import SourceConfig

logger = logging.getLogger(__name__)


async def fetch_rest(session: aiohttp.ClientSession, config: SourceConfig) -> list[dict]:
    """Fetch from a REST API endpoint."""
    async with session.get(
        config.url,
        headers=config.headers,
        timeout=aiohttp.ClientTimeout(total=config.timeout),
        params=config.params.get("query_params", {}),
    ) as resp:
        resp.raise_for_status()
        return await resp.json()


async def fetch_csv(session: aiohttp.ClientSession, config: SourceConfig) -> list[dict]:
    """Fetch and parse a CSV source (URL or local path)."""
    if config.path:
        text = Path(config.path).read_text()
    else:
        async with session.get(
            config.url,
            headers=config.headers,
            timeout=aiohttp.ClientTimeout(total=config.timeout),
        ) as resp:
            resp.raise_for_status()
            text = await resp.text()

    reader = csv.DictReader(io.StringIO(text))
    return list(reader)


async def fetch_websocket(session: aiohttp.ClientSession, config: SourceConfig) -> list[dict]:
    """Connect to WebSocket, collect messages for a duration, return list."""
    import aiohttp as _aio

    results: list[dict] = []
    duration = config.params.get("duration_seconds", 5)
    subscribe_msg = config.params.get("subscribe_message")

    try:
        async with session.ws_connect(
            config.url,
            timeout=config.timeout,
            headers=config.headers,
        ) as ws:
            if subscribe_msg:
                await ws.send_json(subscribe_msg)

            deadline = asyncio.get_event_loop().time() + duration
            while asyncio.get_event_loop().time() < deadline:
                try:
                    msg = await asyncio.wait_for(ws.receive(), timeout=1.0)
                    if msg.type == _aio.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        if isinstance(data, list):
                            results.extend(data)
                        else:
                            results.append(data)
                    elif msg.type in (_aio.WSMsgType.CLOSED, _aio.WSMsgType.ERROR):
                        break
                except asyncio.TimeoutError:
                    continue
    except Exception as e:
        logger.warning("WebSocket fetch error for %s: %s", config.name, e)
        raise

    return results


async def fetch_graphql(session: aiohttp.ClientSession, config: SourceConfig) -> list[dict]:
    """Fetch from a GraphQL endpoint."""
    payload = {
        "query": config.query or config.params.get("query", ""),
        "variables": config.params.get("variables", {}),
    }
    async with session.post(
        config.url,
        json=payload,
        headers={**config.headers, "Content-Type": "application/json"},
        timeout=aiohttp.ClientTimeout(total=config.timeout),
    ) as resp:
        resp.raise_for_status()
        body = await resp.json()

    # Extract the data — handle nested response
    data = body.get("data", body)
    # If data is a dict with one key containing a list, unwrap it
    if isinstance(data, dict) and len(data) == 1:
        val = next(iter(data.values()))
        if isinstance(val, list):
            return val
    if isinstance(data, list):
        return data
    return [data]


async def fetch_file(session: aiohttp.ClientSession, config: SourceConfig) -> list[dict]:
    """Read from a local file (JSON or JSONL)."""
    path = Path(config.path or config.url)
    text = path.read_text()

    if path.suffix == ".jsonl":
        return [json.loads(line) for line in text.strip().splitlines() if line.strip()]
    else:
        data = json.loads(text)
        return data if isinstance(data, list) else [data]


FETCHER_MAP = {
    "rest": fetch_rest,
    "csv": fetch_csv,
    "websocket": fetch_websocket,
    "graphql": fetch_graphql,
    "file": fetch_file,
}


async def fetch_source(session: aiohttp.ClientSession, config: SourceConfig) -> list[dict]:
    """Dispatch to the correct fetcher based on source kind."""
    fetcher = FETCHER_MAP.get(config.kind)
    if not fetcher:
        raise ValueError(f"Unknown source kind: {config.kind}")
    return await fetcher(session, config)
