import asyncio
import aiohttp
import aiofiles
import websockets
import csv
import json
import time
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from abc import ABC, abstractmethod

class DataNormalizer:
    @staticmethod
    def normalize(record: dict, mapping: dict, source_name: str) -> Optional[dict]:
        try:
            timestamp_val = record.get(mapping.get("timestamp", "timestamp"))
            if timestamp_val is None:
                return None
            
            if isinstance(timestamp_val, (int, float)):
                # Ensure it's not in milliseconds
                if timestamp_val > 1000000000000:
                    timestamp_val = timestamp_val / 1000.0
                timestamp = datetime.fromtimestamp(timestamp_val, tz=timezone.utc).isoformat()
            else:
                # Keep as string, assuming it's valid ISO 8601 or similar
                timestamp = str(timestamp_val)
            
            value_raw = record.get(mapping.get("value", "value"), 0.0)
            value = float(value_raw) if value_raw is not None else 0.0
            
            unit = str(record.get(mapping.get("unit", "unit"), ""))
            
            # Custom field mapping (Bonus Feature)
            custom_fields = {}
            if "custom" in mapping:
                for k, v in mapping["custom"].items():
                    custom_fields[k] = record.get(v)
            
            result = {
                "source": source_name,
                "timestamp": timestamp,
                "value": value,
                "unit": unit
            }
            if custom_fields:
                result.update(custom_fields)
                
            return result
        except Exception as e:
            return None

class DataSource(ABC):
    def __init__(self, name: str, config: dict):
        self.name = name
        self.config = config
        self.mapping = config.get("mapping", {})
        self.timeout = config.get("timeout", 5.0)

    @abstractmethod
    async def fetch(self) -> List[Dict[str, Any]]:
        pass

class RESTSource(DataSource):
    async def fetch(self) -> List[Dict[str, Any]]:
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(self.config["url"], timeout=self.timeout) as response:
                    data = await response.json()
                    if "data_path" in self.config:
                        for key in self.config["data_path"].split('.'):
                            if data:
                                data = data.get(key, [])
                    if not isinstance(data, list):
                        data = [data] if data else []
                    return [r for r in (DataNormalizer.normalize(rec, self.mapping, self.name) for rec in data) if r is not None]
            except Exception as e:
                print(f"Error fetching REST source {self.name}: {e}")
                return []

class GraphQLSource(DataSource):
    async def fetch(self) -> List[Dict[str, Any]]:
        async with aiohttp.ClientSession() as session:
            try:
                payload = {"query": self.config["query"]}
                async with session.post(self.config["url"], json=payload, timeout=self.timeout) as response:
                    data = await response.json()
                    data = data.get("data", {})
                    if "data_path" in self.config:
                        for key in self.config["data_path"].split('.'):
                            if data:
                                data = data.get(key, [])
                    if not isinstance(data, list):
                        data = [data] if data else []
                    return [r for r in (DataNormalizer.normalize(rec, self.mapping, self.name) for rec in data) if r is not None]
            except Exception as e:
                print(f"Error fetching GraphQL source {self.name}: {e}")
                return []

class WebSocketSource(DataSource):
    async def fetch(self) -> List[Dict[str, Any]]:
        records = []
        try:
            async with websockets.connect(self.config["url"], close_timeout=self.timeout) as ws:
                message = await asyncio.wait_for(ws.recv(), timeout=self.timeout)
                data = json.loads(message)
                if "data_path" in self.config:
                    for key in self.config["data_path"].split('.'):
                        if data:
                            data = data.get(key, [])
                if not isinstance(data, list):
                    data = [data] if data else []
                records = [r for r in (DataNormalizer.normalize(rec, self.mapping, self.name) for rec in data) if r is not None]
        except Exception as e:
            print(f"Error fetching WebSocket source {self.name}: {e}")
        return records

class CSVSource(DataSource):
    async def fetch(self) -> List[Dict[str, Any]]:
        url = self.config["url"]
        records = []
        try:
            if url.startswith("http"):
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=self.timeout) as response:
                        text = await response.text()
                        reader = csv.DictReader(text.splitlines())
                        for row in reader:
                            res = DataNormalizer.normalize(row, self.mapping, self.name)
                            if res:
                                records.append(res)
            else:
                async with aiofiles.open(url, mode='r') as f:
                    text = await f.read()
                    reader = csv.DictReader(text.splitlines())
                    for row in reader:
                        res = DataNormalizer.normalize(row, self.mapping, self.name)
                        if res:
                            records.append(res)
        except Exception as e:
            print(f"Error fetching CSV source {self.name}: {e}")
        return records

class LocalFileSource(DataSource):
    async def fetch(self) -> List[Dict[str, Any]]:
        records = []
        try:
            async with aiofiles.open(self.config["path"], mode='r') as f:
                data = json.loads(await f.read())
                if "data_path" in self.config:
                    for key in self.config["data_path"].split('.'):
                        if data:
                            data = data.get(key, [])
                if not isinstance(data, list):
                    data = [data] if data else []
                records = [r for r in (DataNormalizer.normalize(rec, self.mapping, self.name) for rec in data) if r is not None]
        except Exception as e:
            print(f"Error fetching Local File source {self.name}: {e}")
        return records

class Pipeline:
    def __init__(self, config: List[Dict[str, Any]]):
        self.sources = []
        # Support for incremental update (Bonus Feature)
        self.last_fetch_time = {}
        for src_conf in config:
            src_type = src_conf["type"]
            name = src_conf["name"]
            self.last_fetch_time[name] = None
            if src_type == "rest":
                self.sources.append(RESTSource(name, src_conf))
            elif src_type == "graphql":
                self.sources.append(GraphQLSource(name, src_conf))
            elif src_type == "websocket":
                self.sources.append(WebSocketSource(name, src_conf))
            elif src_type == "csv":
                self.sources.append(CSVSource(name, src_conf))
            elif src_type == "file":
                self.sources.append(LocalFileSource(name, src_conf))

    async def run(self, incremental=False):
        start_time = time.time()
        
        tasks = [source.fetch() for source in self.sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_records = []
        for i, res in enumerate(results):
            source_name = self.sources[i].name
            if isinstance(res, list):
                if incremental and self.last_fetch_time[source_name] is not None:
                    # Incremental filtering: keep only records newer than last fetch
                    filtered = [r for r in res if r["timestamp"] > self.last_fetch_time[source_name]]
                    all_records.extend(filtered)
                else:
                    all_records.extend(res)
                
                if res:
                    max_ts = max([r["timestamp"] for r in res])
                    if self.last_fetch_time[source_name] is None or max_ts > self.last_fetch_time[source_name]:
                        self.last_fetch_time[source_name] = max_ts
            else:
                print(f"Source {source_name} failed: {res}")
        
        # Merge and sort
        all_records.sort(key=lambda x: x.get("timestamp", ""))
        
        end_time = time.time()
        stats = {
            "total_records": len(all_records),
            "execution_time_seconds": round(end_time - start_time, 4),
            "sources_processed": len(self.sources)
        }
        
        return {
            "stats": stats,
            "data": all_records
        }

if __name__ == "__main__":
    pass
