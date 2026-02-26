"""Source adapters for fetching data from various source types."""
from __future__ import annotations
import asyncio
import csv
import io
import json
import logging
from abc import ABC, abstractmethod
from typing import AsyncGenerator

import aiohttp

from .models import Record, SourceConfig

logger = logging.getLogger(__name__)


def _apply_mapping(raw: dict, mapping: dict[str, str] | None, source_name: str) -> Record:
    """Normalize a raw dict to a Record using optional field mapping."""
    if mapping:
        raw = {target: raw.get(src, raw.get(target)) for target, src in mapping.items()}
    return Record(
        source=raw.get("source", source_name),
        timestamp=str(raw.get("timestamp", "")),
        value=float(raw.get("value", 0)),
        unit=str(raw.get("unit", "")),
    )


class SourceAdapter(ABC):
    def __init__(self, config: SourceConfig):
        self.config = config

    @abstractmethod
    async def fetch(self) -> list[Record]:
        ...


class RestAdapter(SourceAdapter):
    async def fetch(self) -> list[Record]:
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(self.config.url, headers=self.config.headers) as resp:
                resp.raise_for_status()
                data = await resp.json()
                if isinstance(data, dict):
                    data = data.get("data", data.get("records", [data]))
                return [_apply_mapping(r, self.config.field_mapping, self.config.name) for r in data]


class CsvAdapter(SourceAdapter):
    async def fetch(self) -> list[Record]:
        if self.config.url:
            timeout = aiohttp.ClientTimeout(total=self.config.timeout)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(self.config.url, headers=self.config.headers) as resp:
                    resp.raise_for_status()
                    text = await resp.text()
        elif self.config.path:
            loop = asyncio.get_event_loop()
            text = await loop.run_in_executor(None, lambda: open(self.config.path).read())
        else:
            raise ValueError(f"CSV source '{self.config.name}' needs url or path")
        reader = csv.DictReader(io.StringIO(text))
        return [_apply_mapping(dict(row), self.config.field_mapping, self.config.name) for row in reader]


class WebSocketAdapter(SourceAdapter):
    async def fetch(self) -> list[Record]:
        records = []
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.ws_connect(self.config.url) as ws:
                try:
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            if data.get("type") == "done":
                                break
                            records.append(_apply_mapping(data, self.config.field_mapping, self.config.name))
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
                except asyncio.TimeoutError:
                    pass
        return records


class GraphQLAdapter(SourceAdapter):
    async def fetch(self) -> list[Record]:
        query = self.config.query or '{ records { source timestamp value unit } }'
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                self.config.url,
                json={"query": query},
                headers=self.config.headers,
            ) as resp:
                resp.raise_for_status()
                body = await resp.json()
                data = body.get("data", {})
                # grab first list found in data
                items = []
                for v in data.values():
                    if isinstance(v, list):
                        items = v
                        break
                return [_apply_mapping(r, self.config.field_mapping, self.config.name) for r in items]


class FileAdapter(SourceAdapter):
    async def fetch(self) -> list[Record]:
        path = self.config.path
        if not path:
            raise ValueError(f"File source '{self.config.name}' needs path")
        loop = asyncio.get_event_loop()
        text = await loop.run_in_executor(None, lambda: open(path).read())
        data = json.loads(text)
        if isinstance(data, dict):
            data = data.get("data", data.get("records", [data]))
        return [_apply_mapping(r, self.config.field_mapping, self.config.name) for r in data]


ADAPTERS: dict[str, type[SourceAdapter]] = {
    "rest": RestAdapter,
    "csv": CsvAdapter,
    "websocket": WebSocketAdapter,
    "graphql": GraphQLAdapter,
    "file": FileAdapter,
}


def create_adapter(config: SourceConfig) -> SourceAdapter:
    cls = ADAPTERS.get(config.kind)
    if not cls:
        raise ValueError(f"Unknown source kind: {config.kind}")
    return cls(config)
