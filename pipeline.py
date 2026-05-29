#!/usr/bin/env python3
"""
Multi-Source Data Aggregation & Normalization Pipeline

An async Python pipeline that fetches from 5+ concurrent sources
(REST, CSV, WebSocket, GraphQL, file), normalizes to a common schema,
and merges to sorted JSON.

Usage:
    python pipeline.py [--config config.json] [--output output.json]
"""

import asyncio
import json
import csv
import time
import sys
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
import aiohttp
import websockets

# Common schema for normalized data
@dataclass
class NormalizedRecord:
    source: str
    timestamp: str
    value: float
    unit: str
    metadata: Optional[Dict[str, Any]] = None

class DataSource:
    """Base class for data sources."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.name = config.get("name", "unknown")
        self.timeout = config.get("timeout", 10)
    
    async def fetch(self) -> List[NormalizedRecord]:
        raise NotImplementedError

class RESTSource(DataSource):
    """Fetch data from REST API."""
    
    async def fetch(self) -> List[NormalizedRecord]:
        url = self.config["url"]
        headers = self.config.get("headers", {})
        params = self.config.get("params", {})
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params, timeout=self.timeout) as resp:
                data = await resp.json()
                return self._normalize(data)
    
    def _normalize(self, data: Any) -> List[NormalizedRecord]:
        records = []
        items = data if isinstance(data, list) else data.get("data", [data])
        
        for item in items:
            records.append(NormalizedRecord(
                source=self.name,
                timestamp=item.get("timestamp", datetime.now(timezone.utc).isoformat()),
                value=float(item.get("value", 0)),
                unit=item.get("unit", "unknown"),
                metadata=item.get("metadata")
            ))
        
        return records

class CSVSource(DataSource):
    """Fetch data from CSV file."""
    
    async def fetch(self) -> List[NormalizedRecord]:
        file_path = self.config["path"]
        timestamp_col = self.config.get("timestamp_col", "timestamp")
        value_col = self.config.get("value_col", "value")
        unit_col = self.config.get("unit_col", "unit")
        
        records = []
        with open(file_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                records.append(NormalizedRecord(
                    source=self.name,
                    timestamp=row.get(timestamp_col, datetime.now(timezone.utc).isoformat()),
                    value=float(row.get(value_col, 0)),
                    unit=row.get(unit_col, "unknown"),
                    metadata={k: v for k, v in row.items() if k not in [timestamp_col, value_col, unit_col]}
                ))
        
        return records

class WebSocketSource(DataSource):
    """Fetch data from WebSocket."""
    
    async def fetch(self) -> List[NormalizedRecord]:
        url = self.config["url"]
        message = self.config.get("message", {})
        max_messages = self.config.get("max_messages", 10)
        
        records = []
        async with websockets.connect(url) as ws:
            if message:
                await ws.send(json.dumps(message))
            
            for _ in range(max_messages):
                try:
                    data = await asyncio.wait_for(ws.recv(), timeout=self.timeout)
                    parsed = json.loads(data)
                    records.extend(self._normalize(parsed))
                except asyncio.TimeoutError:
                    break
        
        return records
    
    def _normalize(self, data: Any) -> List[NormalizedRecord]:
        records = []
        items = data if isinstance(data, list) else [data]
        
        for item in items:
            records.append(NormalizedRecord(
                source=self.name,
                timestamp=item.get("timestamp", datetime.now(timezone.utc).isoformat()),
                value=float(item.get("value", 0)),
                unit=item.get("unit", "unknown"),
                metadata=item.get("metadata")
            ))
        
        return records

class GraphQLSource(DataSource):
    """Fetch data from GraphQL API."""
    
    async def fetch(self) -> List[NormalizedRecord]:
        url = self.config["url"]
        query = self.config["query"]
        variables = self.config.get("variables", {})
        headers = self.config.get("headers", {})
        
        async with aiohttp.ClientSession() as session:
            payload = {"query": query, "variables": variables}
            async with session.post(url, json=payload, headers=headers, timeout=self.timeout) as resp:
                data = await resp.json()
                return self._normalize(data)
    
    def _normalize(self, data: Any) -> List[NormalizedRecord]:
        records = []
        items = data.get("data", {}).get("items", [data])
        
        for item in items:
            records.append(NormalizedRecord(
                source=self.name,
                timestamp=item.get("timestamp", datetime.now(timezone.utc).isoformat()),
                value=float(item.get("value", 0)),
                unit=item.get("unit", "unknown"),
                metadata=item.get("metadata")
            ))
        
        return records

class FileSource(DataSource):
    """Fetch data from local file (JSON)."""
    
    async def fetch(self) -> List[NormalizedRecord]:
        file_path = self.config["path"]
        
        with open(file_path, "r") as f:
            data = json.load(f)
        
        return self._normalize(data)
    
    def _normalize(self, data: Any) -> List[NormalizedRecord]:
        records = []
        items = data if isinstance(data, list) else [data]
        
        for item in items:
            records.append(NormalizedRecord(
                source=self.name,
                timestamp=item.get("timestamp", datetime.now(timezone.utc).isoformat()),
                value=float(item.get("value", 0)),
                unit=item.get("unit", "unknown"),
                metadata=item.get("metadata")
            ))
        
        return records

# Source factory
SOURCE_TYPES = {
    "rest": RESTSource,
    "csv": CSVSource,
    "websocket": WebSocketSource,
    "graphql": GraphQLSource,
    "file": FileSource
}

def create_source(config: Dict[str, Any]) -> DataSource:
    """Create a data source from config."""
    source_type = config.get("type")
    if source_type not in SOURCE_TYPES:
        raise ValueError(f"Unknown source type: {source_type}")
    return SOURCE_TYPES[source_type](config)

class Pipeline:
    """Main pipeline for data aggregation."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.sources = [create_source(s) for s in config.get("sources", [])]
        self.timeout = config.get("pipeline_timeout", 15)
    
    async def run(self) -> Dict[str, Any]:
        """Run the pipeline and return results."""
        start_time = time.time()
        
        # Fetch from all sources concurrently
        tasks = [self._fetch_with_timeout(source) for source in self.sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Collect all records
        all_records = []
        errors = []
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                errors.append({
                    "source": self.sources[i].name,
                    "error": str(result)
                })
            else:
                all_records.extend(result)
        
        # Sort by timestamp
        all_records.sort(key=lambda r: r.timestamp)
        
        # Calculate stats
        elapsed = time.time() - start_time
        stats = {
            "total_records": len(all_records),
            "sources_attempted": len(self.sources),
            "sources_succeeded": len(self.sources) - len(errors),
            "sources_failed": len(errors),
            "elapsed_seconds": round(elapsed, 3),
            "errors": errors
        }
        
        # Convert to dict
        records_dict = [asdict(r) for r in all_records]
        
        return {
            "records": records_dict,
            "stats": stats
        }
    
    async def _fetch_with_timeout(self, source: DataSource) -> List[NormalizedRecord]:
        """Fetch from a source with timeout."""
        try:
            return await asyncio.wait_for(source.fetch(), timeout=self.timeout)
        except asyncio.TimeoutError:
            raise TimeoutError(f"Source {source.name} timed out after {self.timeout}s")
        except Exception as e:
            raise Exception(f"Source {source.name} failed: {str(e)}")

async def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Multi-Source Data Aggregation Pipeline")
    parser.add_argument("--config", default="config.json", help="Config file path")
    parser.add_argument("--output", default="output.json", help="Output file path")
    args = parser.parse_args()
    
    # Load config
    with open(args.config, "r") as f:
        config = json.load(f)
    
    # Run pipeline
    pipeline = Pipeline(config)
    result = await pipeline.run()
    
    # Save output
    with open(args.output, "w") as f:
        json.dump(result, f, indent=2)
    
    # Print stats
    print(f"Pipeline completed in {result['stats']['elapsed_seconds']}s")
    print(f"Total records: {result['stats']['total_records']}")
    print(f"Sources: {result['stats']['sources_succeeded']}/{result['stats']['sources_attempted']} succeeded")
    
    if result['stats']['errors']:
        print(f"Errors: {len(result['stats']['errors'])}")
        for error in result['stats']['errors']:
            print(f"  - {error['source']}: {error['error']}")

if __name__ == "__main__":
    asyncio.run(main())
