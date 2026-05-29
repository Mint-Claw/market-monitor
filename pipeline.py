#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
market-monitor-pipeline
=======================
Async multi-source data aggregation & normalization pipeline.
Fetches from 5+ concurrent sources (REST, CSV, WebSocket, GraphQL, file),
normalizes to common schema, merges + sorts, outputs JSON + stats.

Config-driven source definitions.
"""
__version__ = "1.0.0"

import asyncio
import json
import logging
import os
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import aiohttp
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("pipeline")


# ── Data Schema ──────────────────────────────────────────────────────────────

@dataclass
class Record:
    """Normalized record schema."""
    source: str
    timestamp: str       # ISO 8601 string
    value: float
    unit: str
    extra: dict = field(default_factory=dict)  # custom field mapping space

    def to_dict(self):
        return asdict(self)


# ── Source Definition ────────────────────────────────────────────────────────

@dataclass
class SourceDef:
    """Config-driven source definition."""
    name: str
    type: str            # rest | csv | websocket | graphql | file
    endpoint: str        # URL or file path
    params: dict = field(default_factory=dict)
    timeout: float = 10.0
    field_mapping: dict = field(default_factory=dict)
    incremental: bool = False
    last_timestamp: Optional[str] = None


# ── Abstract Source Fetcher ─────────────────────────────────────────────────

class BaseFetcher(ABC):
    """Abstract base for all source fetchers."""

    def __init__(self, source_def: SourceDef, session: aiohttp.ClientSession):
        self.defn = source_def
        self.session = session
        self.log = logging.getLogger(f"fetcher.{source_def.name}")

    @abstractmethod
    async def fetch(self) -> list[Record]:
        ...

    def normalize(self, raw: Any) -> list[Record]:
        """Override for custom field mapping."""
        raise NotImplementedError

    def apply_mapping(self, record: dict) -> dict:
        """Apply custom field mapping if configured."""
        if not self.defn.field_mapping:
            return record
        mapped = {}
        for target, source_key in self.defn.field_mapping.items():
            if source_key in record:
                mapped[target] = record[source_key]
        return mapped


# ── Concrete Fetchers ───────────────────────────────────────────────────────

class RestFetcher(BaseFetcher):
    async def fetch(self) -> list[Record]:
        self.log.info(f"Fetching REST: {self.defn.endpoint}")
        try:
            async with self.session.get(
                self.defn.endpoint,
                params=self.defn.params,
                timeout=aiohttp.ClientTimeout(total=self.defn.timeout)
            ) as resp:
                data = await resp.json()
                if isinstance(data, list):
                    return [self._to_record(item) for item in data]
                elif isinstance(data, dict) and 'data' in data:
                    return [self._to_record(item) for item in data['data']]
                return []
        except asyncio.TimeoutError:
            self.log.warning(f"REST timeout: {self.defn.endpoint}")
            return []
        except Exception as e:
            self.log.error(f"REST error: {e}")
            return []

    def _to_record(self, item: dict) -> Record:
        mapped = self.apply_mapping(item) if self.defn.field_mapping else item
        return Record(
            source=self.defn.name,
            timestamp=mapped.get('timestamp', datetime.now(timezone.utc).isoformat()),
            value=float(mapped.get('value', 0)),
            unit=mapped.get('unit', 'USD'),
            extra={k: v for k, v in mapped.items() if k not in ('timestamp', 'value', 'unit')}
        )


class CsvFetcher(BaseFetcher):
    async def fetch(self) -> list[Record]:
        self.log.info(f"Fetching CSV: {self.defn.endpoint}")
        import csv
        import io
        try:
            async with self.session.get(
                self.defn.endpoint,
                timeout=aiohttp.ClientTimeout(total=self.defn.timeout)
            ) as resp:
                text = await resp.text()
                reader = csv.DictReader(io.StringIO(text))
                records = []
                for row in reader:
                    mapped = self.apply_mapping(row) if self.defn.field_mapping else row
                    records.append(Record(
                        source=self.defn.name,
                        timestamp=mapped.get('timestamp', datetime.now(timezone.utc).isoformat()),
                        value=float(mapped.get('value', 0)),
                        unit=mapped.get('unit', 'USD'),
                        extra={k: v for k, v in mapped.items() if k not in ('timestamp', 'value', 'unit')}
                    ))
                return records
        except Exception as e:
            self.log.error(f"CSV error: {e}")
            return []


class WebSocketFetcher(BaseFetcher):
    async def fetch(self) -> list[Record]:
        self.log.info(f"Fetching WebSocket: {self.defn.endpoint}")
        records = []
        try:
            async with self.session.ws_connect(
                self.defn.endpoint,
                timeout=self.defn.timeout
            ) as ws:
                for _ in range(20):  # collect up to 20 messages
                    try:
                        msg = await ws.receive(timeout=self.defn.timeout)
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            mapped = self.apply_mapping(data) if self.defn.field_mapping else data
                            records.append(Record(
                                source=self.defn.name,
                                timestamp=mapped.get('timestamp', datetime.now(timezone.utc).isoformat()),
                                value=float(mapped.get('value', 0)),
                                unit=mapped.get('unit', 'USD'),
                                extra={k: v for k, v in mapped.items() if k not in ('timestamp', 'value', 'unit')}
                            ))
                        elif msg.type == aiohttp.WSMsgType.CLOSED:
                            break
                    except asyncio.TimeoutError:
                        break
                    except Exception:
                        break
            return records
        except Exception as e:
            self.log.warning(f"WS connection error (mock may not be running): {e}")
            return []


class GraphQLFetcher(BaseFetcher):
    async def fetch(self) -> list[Record]:
        self.log.info(f"Fetching GraphQL: {self.defn.endpoint}")
        query = self.defn.params.get('query', '{ data { timestamp value unit } }')
        try:
            async with self.session.post(
                self.defn.endpoint,
                json={'query': query},
                timeout=aiohttp.ClientTimeout(total=self.defn.timeout)
            ) as resp:
                result = await resp.json()
                items = result.get('data', {}).get('data', result.get('data', {}))
                if isinstance(items, dict):
                    items = [items]
                records = []
                for item in items:
                    mapped = self.apply_mapping(item) if self.defn.field_mapping else item
                    records.append(Record(
                        source=self.defn.name,
                        timestamp=mapped.get('timestamp', datetime.now(timezone.utc).isoformat()),
                        value=float(mapped.get('value', 0)),
                        unit=mapped.get('unit', 'USD'),
                        extra={k: v for k, v in mapped.items() if k not in ('timestamp', 'value', 'unit')}
                    ))
                return records
        except Exception as e:
            self.log.error(f"GraphQL error: {e}")
            return []


class FileFetcher(BaseFetcher):
    async def fetch(self) -> list[Record]:
        path = self.defn.endpoint
        self.log.info(f"Fetching File: {path}")
        try:
            loop = asyncio.get_event_loop()
            with open(path, 'r') as f:
                text = await loop.run_in_executor(None, f.read)
            if path.endswith('.json'):
                data = json.loads(text)
                if isinstance(data, list):
                    return [self._to_record(item) for item in data]
                return [self._to_record(data)]
            elif path.endswith('.csv'):
                import csv, io
                reader = csv.DictReader(io.StringIO(text))
                records = []
                for row in reader:
                    mapped = self.apply_mapping(row) if self.defn.field_mapping else row
                    records.append(Record(
                        source=self.defn.name,
                        timestamp=mapped.get('timestamp', datetime.now(timezone.utc).isoformat()),
                        value=float(mapped.get('value', 0)),
                        unit=mapped.get('unit', 'USD'),
                        extra={k: v for k, v in mapped.items() if k not in ('timestamp', 'value', 'unit')}
                    ))
                return records
            return []
        except Exception as e:
            self.log.error(f"File error: {e}")
            return []

    def _to_record(self, item: dict) -> Record:
        mapped = self.apply_mapping(item) if self.defn.field_mapping else item
        return Record(
            source=self.defn.name,
            timestamp=mapped.get('timestamp', datetime.now(timezone.utc).isoformat()),
            value=float(mapped.get('value', 0)),
            unit=mapped.get('unit', 'USD'),
            extra={k: v for k, v in mapped.items() if k not in ('timestamp', 'value', 'unit')}
        )


# ── Fetcher Registry ────────────────────────────────────────────────────────

FETCHER_MAP = {
    'rest': RestFetcher,
    'csv': CsvFetcher,
    'websocket': WebSocketFetcher,
    'graphql': GraphQLFetcher,
    'file': FileFetcher,
}


# ── Pipeline Core ────────────────────────────────────────────────────────────

class Pipeline:
    """Orchestrates concurrent fetching, normalization, merge, and output."""

    def __init__(self, config_path: str = "config.yaml"):
        self.config_path = config_path
        self.sources: list[SourceDef] = []
        self.records: list[Record] = []
        self.stats: dict = {}
        self._load_config()

    def _load_config(self):
        with open(self.config_path, 'r') as f:
            cfg = yaml.safe_load(f)
        pipeline_cfg = cfg.get('pipeline', cfg)
        for src in pipeline_cfg.get('sources', []):
            self.sources.append(SourceDef(**src))
        if not self.sources:
            raise ValueError("No sources defined in config")

    async def run(self) -> dict:
        """Execute the pipeline: fetch -> normalize -> merge -> sort -> output."""
        log.info(f"Pipeline starting with {len(self.sources)} sources")
        t0 = time.time()

        async with aiohttp.ClientSession() as session:
            tasks = []
            for src_def in self.sources:
                fetcher_cls = FETCHER_MAP.get(src_def.type)
                if not fetcher_cls:
                    log.warning(f"Unknown source type: {src_def.type}, skipping")
                    continue
                fetcher = fetcher_cls(src_def, session)
                tasks.append(self._fetch_with_metrics(fetcher))

            results = await asyncio.gather(*tasks, return_exceptions=True)

        all_records = []
        for i, result in enumerate(results):
            src_name = self.sources[i].name if i < len(self.sources) else f"source_{i}"
            if isinstance(result, Exception):
                log.error(f"Fatal error from {src_name}: {result}")
                continue
            all_records.extend(result)

        # Merge + sort by timestamp
        all_records.sort(key=lambda r: r.timestamp)
        self.records = all_records

        # Compute stats
        elapsed = time.time() - t0
        if all_records:
            values = [r.value for r in all_records]
            self.stats = {
                'total_records': len(all_records),
                'sources_queried': len(self.sources),
                'sources_responded': len(set(r.source for r in all_records)),
                'elapsed_seconds': round(elapsed, 3),
                'min_value': round(min(values), 4),
                'max_value': round(max(values), 4),
                'mean_value': round(sum(values) / len(values), 4),
                'source_breakdown': {},
            }
            for src in self.sources:
                count = sum(1 for r in all_records if r.source == src.name)
                self.stats['source_breakdown'][src.name] = count
        else:
            self.stats = {
                'total_records': 0,
                'elapsed_seconds': round(elapsed, 3),
                'note': 'No records collected'
            }

        log.info(f"Pipeline done: {self.stats['total_records']} records in {elapsed:.2f}s")
        return self.stats

    async def _fetch_with_metrics(self, fetcher: BaseFetcher) -> list[Record]:
        t0 = time.time()
        records = await fetcher.fetch()
        elapsed = time.time() - t0
        self.log = logging.getLogger("pipeline.metrics")
        self.log.info(f"{fetcher.defn.name}: {len(records)} records in {elapsed:.2f}s")
        return records

    def output_json(self, path: str = "output.json"):
        """Write sorted records to JSON file."""
        data = {
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'stats': self.stats,
            'records': [r.to_dict() for r in self.records],
        }
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)
        log.info(f"Output written to {path}")

    def print_stats(self):
        print(json.dumps(self.stats, indent=2))


# ── CLI Entry Point ─────────────────────────────────────────────────────────

async def main():
    import argparse
    parser = argparse.ArgumentParser(description="Multi-source data aggregation pipeline")
    parser.add_argument('--config', default='config.yaml', help='Config file path')
    parser.add_argument('--output', default='output.json', help='Output JSON path')
    args = parser.parse_args()

    pipeline = Pipeline(config_path=args.config)
    stats = await pipeline.run()
    pipeline.output_json(args.output)
    pipeline.print_stats()

    # Acceptance check
    if stats['total_records'] >= 1000 and stats['elapsed_seconds'] < 15:
        print("\n✅ ACCEPTANCE: Pipeline processed 1000+ records in <15s")
    elif stats['total_records'] < 1000:
        print(f"\n⚠️  Acceptance: Only {stats['total_records']} records (need 1000+)")
    else:
        print(f"\n⚠️  Acceptance: {stats['elapsed_seconds']:.2f}s (need <15s)")

    return pipeline


if __name__ == '__main__':
    asyncio.run(main())
