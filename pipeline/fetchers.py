"""Async fetchers for different source types."""
import asyncio
import aiohttp
import csv
import json
from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path
from .config import SourceConfig

class BaseFetcher:
    """Base class for all fetchers."""
    
    def __init__(self, config: SourceConfig):
        self.config = config
    
    async def fetch(self) -> List[Dict[str, Any]]:
        """Fetch data from source. Must be implemented by subclasses."""
        raise NotImplementedError
    
    def normalize(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Normalize data to standard schema."""
        normalized = []
        mapping = self.config.field_mapping or {}
        
        for item in raw_data:
            try:
                normalized_item = {
                    "source": self.config.name,
                    "timestamp": self._extract_field(item, mapping.get("timestamp", "timestamp")),
                    "value": self._extract_field(item, mapping.get("value", "value")),
                    "unit": self._extract_field(item, mapping.get("unit", "unit"), default="")
                }
                normalized.append(normalized_item)
            except Exception as e:
                # Skip malformed records
                continue
        
        return normalized
    
    def _extract_field(self, item: Dict, field_path: str, default: Any = None) -> Any:
        """Extract nested field using dot notation."""
        keys = field_path.split(".")
        value = item
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return default
            if value is None:
                return default
        return value


class RESTFetcher(BaseFetcher):
    """Fetch data from REST API."""
    
    async def fetch(self) -> List[Dict[str, Any]]:
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                self.config.url,
                params=self.config.params,
                headers=self.config.headers
            ) as response:
                response.raise_for_status()
                data = await response.json()
                # Handle both list and dict responses
                if isinstance(data, list):
                    return data
                elif isinstance(data, dict) and "data" in data:
                    return data["data"] if isinstance(data["data"], list) else [data["data"]]
                else:
                    return [data]


class CSVFetcher(BaseFetcher):
    """Fetch data from CSV file or URL."""
    
    async def fetch(self) -> List[Dict[str, Any]]:
        if self.config.url.startswith("http"):
            return await self._fetch_remote()
        else:
            return await self._fetch_local()
    
    async def _fetch_remote(self) -> List[Dict[str, Any]]:
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(self.config.url, headers=self.config.headers) as response:
                response.raise_for_status()
                text = await response.text()
                return self._parse_csv(text)
    
    async def _fetch_local(self) -> List[Dict[str, Any]]:
        path = Path(self.config.url)
        text = await asyncio.to_thread(path.read_text)
        return self._parse_csv(text)
    
    def _parse_csv(self, text: str) -> List[Dict[str, Any]]:
        reader = csv.DictReader(text.splitlines())
        return list(reader)


class WebSocketFetcher(BaseFetcher):
    """Fetch data from WebSocket (collect messages for duration)."""
    
    async def fetch(self) -> List[Dict[str, Any]]:
        import aiohttp
        messages = []
        duration = self.config.params.get("duration", 5)  # seconds
        
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(self.config.url) as ws:
                # Send subscription if provided
                if "subscribe" in self.config.params:
                    await ws.send_json(self.config.params["subscribe"])
                
                # Collect messages for specified duration
                end_time = asyncio.get_event_loop().time() + duration
                while asyncio.get_event_loop().time() < end_time:
                    try:
                        msg = await asyncio.wait_for(ws.receive(), timeout=1.0)
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            messages.append(data)
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
                    except asyncio.TimeoutError:
                        continue
        
        return messages


class GraphQLFetcher(BaseFetcher):
    """Fetch data from GraphQL endpoint."""
    
    async def fetch(self) -> List[Dict[str, Any]]:
        query = self.config.params.get("query", "")
        variables = self.config.params.get("variables", {})
        
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                self.config.url,
                json={"query": query, "variables": variables},
                headers=self.config.headers
            ) as response:
                response.raise_for_status()
                result = await response.json()
                
                # Extract data from GraphQL response
                if "data" in result:
                    data = result["data"]
                    # Find first list in the response
                    for value in data.values():
                        if isinstance(value, list):
                            return value
                    return [data]
                return []


class FileFetcher(BaseFetcher):
    """Fetch data from local JSON file."""
    
    async def fetch(self) -> List[Dict[str, Any]]:
        path = Path(self.config.url)
        text = await asyncio.to_thread(path.read_text)
        data = json.loads(text)
        
        if isinstance(data, list):
            return data
        elif isinstance(data, dict) and "data" in data:
            return data["data"] if isinstance(data["data"], list) else [data["data"]]
        else:
            return [data]


def get_fetcher(config: SourceConfig) -> BaseFetcher:
    """Factory function to get appropriate fetcher."""
    fetchers = {
        "rest": RESTFetcher,
        "csv": CSVFetcher,
        "websocket": WebSocketFetcher,
        "graphql": GraphQLFetcher,
        "file": FileFetcher
    }
    
    fetcher_class = fetchers.get(config.type)
    if not fetcher_class:
        raise ValueError(f"Unknown source type: {config.type}")
    
    return fetcher_class(config)
