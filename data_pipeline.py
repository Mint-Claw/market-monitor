"""
Multi-Source Data Aggregation & Normalization Pipeline

Async Python pipeline fetching from 5+ concurrent sources 
(REST, CSV, WebSocket, GraphQL, file), normalizing to common schema
and merging to sorted JSON.
"""

import asyncio
import aiohttp
import json
import csv
import time
from dataclasses import dataclass, asdict
from typing import Any, Optional, Callable, AsyncIterator
from enum import Enum
from pathlib import Path
from datetime import datetime
import io


class SourceType(Enum):
    REST = "rest"
    CSV = "csv"
    WEBSOCKET = "websocket"
    GRAPHQL = "graphql"
    FILE = "file"


@dataclass
class SourceConfig:
    """Configuration for a data source"""
    name: str
    type: SourceType
    url: Optional[str] = None
    path: Optional[str] = None
    query: Optional[str] = None  # For GraphQL
    headers: Optional[dict] = None
    timeout: float = 10.0
    field_mapping: Optional[dict] = None  # Map source fields to normalized schema


@dataclass 
class NormalizedRecord:
    """Normalized output schema"""
    source: str
    timestamp: str
    value: Any
    unit: Optional[str] = None
    metadata: Optional[dict] = None
    
    def to_dict(self) -> dict:
        return {k: v for k, v in asdict(self).items() if v is not None}


@dataclass
class PipelineStats:
    """Statistics for pipeline run"""
    total_records: int
    sources_succeeded: int
    sources_failed: int
    duration_ms: float
    errors: list[str]


class DataPipeline:
    """Async data aggregation pipeline"""
    
    def __init__(self, sources: list[SourceConfig], timeout: float = 15.0):
        self.sources = sources
        self.timeout = timeout
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        self._session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, *args):
        if self._session:
            await self._session.close()
    
    async def _fetch_rest(self, config: SourceConfig) -> list[dict]:
        """Fetch from REST API"""
        async with self._session.get(
            config.url,
            headers=config.headers,
            timeout=aiohttp.ClientTimeout(total=config.timeout)
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
            return data if isinstance(data, list) else [data]
    
    async def _fetch_csv(self, config: SourceConfig) -> list[dict]:
        """Fetch from CSV (URL or file path)"""
        if config.url:
            async with self._session.get(
                config.url,
                timeout=aiohttp.ClientTimeout(total=config.timeout)
            ) as resp:
                text = await resp.text()
                reader = csv.DictReader(io.StringIO(text))
                return list(reader)
        elif config.path:
            with open(config.path, 'r') as f:
                reader = csv.DictReader(f)
                return list(reader)
        return []
    
    async def _fetch_websocket(self, config: SourceConfig, max_messages: int = 10) -> list[dict]:
        """Fetch from WebSocket (collect N messages)"""
        records = []
        try:
            async with self._session.ws_connect(
                config.url,
                timeout=config.timeout
            ) as ws:
                count = 0
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            records.append(json.loads(msg.data))
                            count += 1
                            if count >= max_messages:
                                break
                        except json.JSONDecodeError:
                            continue
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
        except asyncio.TimeoutError:
            pass  # Return what we got
        return records
    
    async def _fetch_graphql(self, config: SourceConfig) -> list[dict]:
        """Fetch from GraphQL endpoint"""
        payload = {"query": config.query}
        async with self._session.post(
            config.url,
            json=payload,
            headers=config.headers or {"Content-Type": "application/json"},
            timeout=aiohttp.ClientTimeout(total=config.timeout)
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
            # GraphQL response is usually {data: {field: [...]}}
            if "data" in data:
                for key, value in data["data"].items():
                    if isinstance(value, list):
                        return value
                return [data["data"]]
            return [data]
    
    async def _fetch_file(self, config: SourceConfig) -> list[dict]:
        """Fetch from local file (JSON or CSV)"""
        path = Path(config.path)
        if not path.exists():
            return []
        
        content = path.read_text()
        if path.suffix == '.json':
            data = json.loads(content)
            return data if isinstance(data, list) else [data]
        elif path.suffix == '.csv':
            reader = csv.DictReader(io.StringIO(content))
            return list(reader)
        else:
            # Try JSON first, then treat as single record
            try:
                data = json.loads(content)
                return data if isinstance(data, list) else [data]
            except json.JSONDecodeError:
                return [{"raw": content}]
    
    async def _fetch_source(self, config: SourceConfig) -> tuple[str, list[dict], Optional[str]]:
        """Fetch from a single source, return (name, records, error)"""
        try:
            if config.type == SourceType.REST:
                records = await self._fetch_rest(config)
            elif config.type == SourceType.CSV:
                records = await self._fetch_csv(config)
            elif config.type == SourceType.WEBSOCKET:
                records = await self._fetch_websocket(config)
            elif config.type == SourceType.GRAPHQL:
                records = await self._fetch_graphql(config)
            elif config.type == SourceType.FILE:
                records = await self._fetch_file(config)
            else:
                return config.name, [], f"Unknown source type: {config.type}"
            
            return config.name, records, None
        except asyncio.TimeoutError:
            return config.name, [], f"Timeout after {config.timeout}s"
        except Exception as e:
            return config.name, [], str(e)[:100]
    
    def _normalize_record(
        self, 
        record: dict, 
        source_name: str,
        field_mapping: Optional[dict] = None
    ) -> NormalizedRecord:
        """Normalize a record to common schema"""
        mapping = field_mapping or {}
        
        # Extract timestamp
        ts_field = mapping.get("timestamp", "timestamp")
        timestamp = record.get(ts_field) or record.get("time") or record.get("date")
        if not timestamp:
            timestamp = datetime.utcnow().isoformat()
        elif isinstance(timestamp, (int, float)):
            timestamp = datetime.fromtimestamp(timestamp).isoformat()
        
        # Extract value
        value_field = mapping.get("value", "value")
        value = record.get(value_field) or record.get("price") or record.get("amount")
        if value is None:
            # Use first numeric field
            for k, v in record.items():
                if isinstance(v, (int, float)):
                    value = v
                    break
        if value is None:
            value = record  # Store whole record
        
        # Extract unit
        unit_field = mapping.get("unit", "unit")
        unit = record.get(unit_field) or record.get("currency")
        
        # Collect remaining fields as metadata
        core_fields = {ts_field, value_field, unit_field, "timestamp", "time", "date", "value", "price", "amount", "unit", "currency"}
        metadata = {k: v for k, v in record.items() if k not in core_fields}
        
        return NormalizedRecord(
            source=source_name,
            timestamp=str(timestamp),
            value=value,
            unit=unit,
            metadata=metadata if metadata else None
        )
    
    async def run(self) -> tuple[list[dict], PipelineStats]:
        """
        Run the pipeline: fetch all sources concurrently, normalize, merge & sort.
        Returns (records, stats)
        """
        start = time.time()
        errors = []
        
        # Fetch all sources concurrently
        tasks = [self._fetch_source(config) for config in self.sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        all_records = []
        succeeded = 0
        failed = 0
        
        for i, result in enumerate(results):
            config = self.sources[i]
            if isinstance(result, Exception):
                failed += 1
                errors.append(f"{config.name}: {str(result)[:50]}")
                continue
            
            name, records, error = result
            if error:
                failed += 1
                errors.append(f"{name}: {error}")
            else:
                succeeded += 1
                # Normalize each record
                for rec in records:
                    normalized = self._normalize_record(rec, name, config.field_mapping)
                    all_records.append(normalized.to_dict())
        
        # Sort by timestamp
        all_records.sort(key=lambda x: x.get("timestamp", ""))
        
        duration = (time.time() - start) * 1000
        
        stats = PipelineStats(
            total_records=len(all_records),
            sources_succeeded=succeeded,
            sources_failed=failed,
            duration_ms=round(duration, 2),
            errors=errors
        )
        
        return all_records, stats


async def run_pipeline(sources: list[SourceConfig]) -> tuple[list[dict], PipelineStats]:
    """Convenience function to run pipeline"""
    async with DataPipeline(sources) as pipeline:
        return await pipeline.run()


# Example configurations
EXAMPLE_SOURCES = [
    SourceConfig(
        name="crypto_prices",
        type=SourceType.REST,
        url="https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd",
        field_mapping={"value": "usd"}
    ),
    SourceConfig(
        name="local_data",
        type=SourceType.FILE,
        path="./latest_scan.json"
    ),
]


if __name__ == "__main__":
    import sys
    
    async def main():
        print("=== Multi-Source Data Pipeline ===\n")
        
        # Demo with file source (since we have local data)
        sources = [
            SourceConfig(
                name="local_scan",
                type=SourceType.FILE,
                path="./latest_scan.json"
            ),
        ]
        
        records, stats = await run_pipeline(sources)
        
        print(f"Stats:")
        print(f"  - Total records: {stats.total_records}")
        print(f"  - Sources succeeded: {stats.sources_succeeded}")
        print(f"  - Sources failed: {stats.sources_failed}")
        print(f"  - Duration: {stats.duration_ms}ms")
        
        if stats.errors:
            print(f"  - Errors: {stats.errors}")
        
        print(f"\nSample output (first 3 records):")
        for rec in records[:3]:
            print(f"  {json.dumps(rec, default=str)[:100]}...")
        
        # Output full JSON
        output = {
            "records": records,
            "stats": asdict(stats)
        }
        
        with open("pipeline_output.json", "w") as f:
            json.dump(output, f, indent=2, default=str)
        
        print(f"\nFull output written to pipeline_output.json")
    
    asyncio.run(main())
