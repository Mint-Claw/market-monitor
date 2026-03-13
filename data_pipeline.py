"""
Multi-Source Data Aggregation & Normalization Pipeline

Config-driven async pipeline that fetches from multiple sources,
normalizes to common schema, and outputs merged sorted JSON.
"""

import asyncio
import json
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any
import aiohttp
import aiofiles


@dataclass
class NormalizedRecord:
    """Common schema for normalized data."""
    source: str
    timestamp: str  # ISO 8601 format
    value: float
    unit: str
    metadata: dict = None

    def to_dict(self) -> dict:
        data = {
            "source": self.source,
            "timestamp": self.timestamp,
            "value": self.value,
            "unit": self.unit
        }
        if self.metadata:
            data["metadata"] = self.metadata
        return data


class DataSource(ABC):
    """Abstract base class for data sources."""
    
    def __init__(self, name: str, config: dict):
        self.name = name
        self.config = config
        self.timeout = config.get("timeout", 10)
    
    @abstractmethod
    async def fetch(self) -> list[NormalizedRecord]:
        """Fetch and normalize data from this source."""
        pass
    
    async def _fetch_with_timeout(self, coro) -> list[NormalizedRecord]:
        """Execute fetch with timeout and error handling."""
        try:
            return await asyncio.wait_for(coro, timeout=self.timeout)
        except asyncio.TimeoutError:
            print(f"[{self.name}] Timeout after {self.timeout}s")
            return []
        except Exception as e:
            print(f"[{self.name}] Error: {e}")
            return []


class RESTDataSource(DataSource):
    """REST API data source."""
    
    async def fetch(self) -> list[NormalizedRecord]:
        async def _fetch():
            records = []
            url = self.config["url"]
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        # Handle both list and dict responses
                        items = data if isinstance(data, list) else data.get("data", [])
                        for item in items:
                            records.append(NormalizedRecord(
                                source=self.name,
                                timestamp=item.get("timestamp", datetime.now().isoformat()),
                                value=float(item.get("value", 0)),
                                unit=item.get("unit", "USD"),
                                metadata=item.get("metadata", {})
                            ))
            return records
        
        return await self._fetch_with_timeout(_fetch())


class CSVDataSource(DataSource):
    """CSV file data source."""
    
    async def fetch(self) -> list[NormalizedRecord]:
        async def _fetch():
            records = []
            filepath = self.config["path"]
            
            # Handle both local and remote CSV
            if filepath.startswith("http"):
                async with aiohttp.ClientSession() as session:
                    async with session.get(filepath) as response:
                        if response.status == 200:
                            text = await response.text()
                            lines = text.strip().split("\n")
                        else:
                            return []
            else:
                async with aiofiles.open(filepath, 'r') as f:
                    content = await f.read()
                    lines = content.strip().split("\n")
            
            if not lines:
                return []
            
            # Parse CSV (simple implementation)
            headers = lines[0].split(",")
            for line in lines[1:]:
                if not line.strip():
                    continue
                values = line.split(",")
                if len(values) >= 3:
                    records.append(NormalizedRecord(
                        source=self.name,
                        timestamp=values[0].strip() or datetime.now().isoformat(),
                        value=float(values[1].strip()),
                        unit=values[2].strip(),
                        metadata=dict(zip(headers[3:], values[3:])) if len(values) > 3 else {}
                    ))
            return records
        
        return await self._fetch_with_timeout(_fetch())


class WebSocketDataSource(DataSource):
    """WebSocket data source."""
    
    async def fetch(self) -> list[NormalizedRecord]:
        async def _fetch():
            records = []
            ws_url = self.config["url"]
            max_messages = self.config.get("max_messages", 10)
            
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(ws_url, timeout=self.timeout) as ws:
                        for _ in range(max_messages):
                            msg = await ws.receive_json()
                            records.append(NormalizedRecord(
                                source=self.name,
                                timestamp=msg.get("timestamp", datetime.now().isoformat()),
                                value=float(msg.get("value", 0)),
                                unit=msg.get("unit", "USD"),
                                metadata=msg.get("metadata", {})
                            ))
            except Exception as e:
                print(f"[{self.name}] WebSocket error: {e}")
            
            return records
        
        return await self._fetch_with_timeout(_fetch())


class GraphQLDataSource(DataSource):
    """GraphQL data source."""
    
    async def fetch(self) -> list[NormalizedRecord]:
        async def _fetch():
            records = []
            url = self.config["url"]
            query = self.config.get("query", "{ data { timestamp value unit } }")
            
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json={"query": query}) as response:
                    if response.status == 200:
                        result = await response.json()
                        data = result.get("data", {}).get("data", [])
                        for item in data:
                            records.append(NormalizedRecord(
                                source=self.name,
                                timestamp=item.get("timestamp", datetime.now().isoformat()),
                                value=float(item.get("value", 0)),
                                unit=item.get("unit", "USD"),
                                metadata=item.get("metadata", {})
                            ))
            return records
        
        return await self._fetch_with_timeout(_fetch())


class FileDataSource(DataSource):
    """Local/remote file data source (JSON)."""
    
    async def fetch(self) -> list[NormalizedRecord]:
        async def _fetch():
            records = []
            filepath = self.config["path"]
            
            if filepath.startswith("http"):
                async with aiohttp.ClientSession() as session:
                    async with session.get(filepath) as response:
                        if response.status == 200:
                            data = await response.json()
                        else:
                            return []
            else:
                async with aiofiles.open(filepath, 'r') as f:
                    content = await f.read()
                    data = json.loads(content)
            
            items = data if isinstance(data, list) else data.get("data", [])
            for item in items:
                records.append(NormalizedRecord(
                    source=self.name,
                    timestamp=item.get("timestamp", datetime.now().isoformat()),
                    value=float(item.get("value", 0)),
                    unit=item.get("unit", "USD"),
                    metadata=item.get("metadata", {})
                ))
            return records
        
        return await self._fetch_with_timeout(_fetch())


class DataPipeline:
    """Main data aggregation pipeline."""
    
    SOURCE_TYPES = {
        "rest": RESTDataSource,
        "csv": CSVDataSource,
        "websocket": WebSocketDataSource,
        "graphql": GraphQLDataSource,
        "file": FileDataSource,
    }
    
    def __init__(self, config: dict):
        self.config = config
        self.sources: list[DataSource] = []
        self._init_sources()
    
    def _init_sources(self):
        """Initialize data sources from config."""
        for source_config in self.config.get("sources", []):
            source_type = source_config.get("type", "rest")
            source_class = self.SOURCE_TYPES.get(source_type)
            if source_class:
                self.sources.append(source_class(
                    source_config["name"],
                    source_config
                ))
    
    async def fetch_all(self) -> list[NormalizedRecord]:
        """Fetch from all sources concurrently."""
        tasks = [source.fetch() for source in self.sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_records = []
        for result in results:
            if isinstance(result, list):
                all_records.extend(result)
            elif isinstance(result, Exception):
                print(f"Source error: {result}")
        
        return all_records
    
    def merge_and_sort(self, records: list[NormalizedRecord]) -> list[dict]:
        """Merge records and sort by timestamp."""
        # Sort by timestamp (newest first)
        sorted_records = sorted(
            records,
            key=lambda r: r.timestamp,
            reverse=True
        )
        return [r.to_dict() for r in sorted_records]
    
    def generate_stats(self, records: list[NormalizedRecord]) -> dict:
        """Generate statistics from records."""
        if not records:
            return {
                "total_records": 0,
                "sources": [],
                "value_range": {"min": 0, "max": 0},
                "units": {}
            }
        
        sources = list(set(r.source for r in records))
        units = {}
        for r in records:
            units[r.unit] = units.get(r.unit, 0) + 1
        
        values = [r.value for r in records]
        
        return {
            "total_records": len(records),
            "sources": sources,
            "value_range": {
                "min": min(values) if values else 0,
                "max": max(values) if values else 0,
                "avg": sum(values) / len(values) if values else 0
            },
            "units": units,
            "fetch_time": datetime.now().isoformat()
        }
    
    async def run(self, output_path: str = None) -> dict:
        """Run the full pipeline."""
        # Fetch from all sources
        records = await self.fetch_all()
        
        # Merge and sort
        merged = self.merge_and_sort(records)
        
        # Generate stats
        stats = self.generate_stats(records)
        
        result = {
            "records": merged,
            "stats": stats
        }
        
        # Write to output if path provided
        if output_path:
            async with aiofiles.open(output_path, 'w') as f:
                await f.write(json.dumps(result, indent=2))
        
        return result


async def run_pipeline(config_path: str = None, output_path: str = None):
    """Run pipeline with config file."""
    if config_path is None:
        # Default config with mock sources
        config = {
            "sources": [
                {
                    "name": "mock_rest",
                    "type": "rest",
                    "url": "http://localhost:8080/api/data",
                    "timeout": 5
                },
                {
                    "name": "mock_csv",
                    "type": "csv",
                    "path": "http://localhost:8080/data.csv",
                    "timeout": 5
                },
                {
                    "name": "mock_websocket",
                    "type": "websocket",
                    "url": "ws://localhost:8080/ws",
                    "timeout": 5,
                    "max_messages": 10
                },
                {
                    "name": "mock_graphql",
                    "type": "graphql",
                    "url": "http://localhost:8080/graphql",
                    "timeout": 5
                },
                {
                    "name": "mock_file",
                    "type": "file",
                    "path": "http://localhost:8080/data.json",
                    "timeout": 5
                }
            ]
        }
    else:
        async with aiofiles.open(config_path, 'r') as f:
            content = await f.read()
            config = json.loads(content)
    
    pipeline = DataPipeline(config)
    result = await pipeline.run(output_path)
    return result


if __name__ == "__main__":
    import sys
    config_path = sys.argv[1] if len(sys.argv) > 1 else None
    output_path = sys.argv[2] if len(sys.argv) > 2 else "output.json"
    
    result = asyncio.run(run_pipeline(config_path, output_path))
    print(json.dumps(result["stats"], indent=2))
