"""
Multi-Source Data Aggregation & Normalization Pipeline
Fetches from 5+ concurrent sources (REST, CSV, WebSocket, GraphQL, File),
normalizes to a common schema, and merges to sorted JSON.
"""

import asyncio
import aiohttp
import csv
import json
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime

# Common Schema: {source: str, timestamp: float, value: float, unit: str}

class SourceFetcher:
    """Base class for source fetchers"""
    async def fetch(self, config: Dict) -> List[Dict]:
        raise NotImplementedError

class RESTFetcher(SourceFetcher):
    async def fetch(self, config: Dict) -> List[Dict]:
        url = config['url']
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return [{
                        'source': config['name'],
                        'timestamp': item.get('timestamp', time.time()),
                        'value': float(item.get('value', 0)),
                        'unit': config.get('unit', 'unknown')
                    } for item in data]
        return []

class CSVFetcher(SourceFetcher):
    async def fetch(self, config: Dict) -> List[Dict]:
        path = Path(config['path'])
        if not path.exists():
            return []
        results = []
        with open(path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                results.append({
                    'source': config['name'],
                    'timestamp': float(row.get('timestamp', time.time())),
                    'value': float(row.get('value', 0)),
                    'unit': config.get('unit', 'unknown')
                })
        return results

class WebSocketFetcher(SourceFetcher):
    async def fetch(self, config: Dict) -> List[Dict]:
        # Simulated WS connection for demo (real WS would use websockets lib)
        # For mock, we'll treat it like a REST call that returns WS-style data
        url = config['url'].replace('ws://', 'http://').replace('wss://', 'https://')
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return [{
                            'source': config['name'],
                            'timestamp': item.get('timestamp', time.time()),
                            'value': float(item.get('value', 0)),
                            'unit': config.get('unit', 'unknown')
                        } for item in data]
            except:
                return []
        return []

class GraphQLFetcher(SourceFetcher):
    async def fetch(self, config: Dict) -> List[Dict]:
        url = config['url']
        query = config.get('query', '{ data { timestamp value } }')
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(url, json={'query': query}, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        return []
                    
                    # Safely parse JSON response
                    try:
                        data = await resp.json()
                    except (aiohttp.ContentTypeError, json.JSONDecodeError):
                        return []

                    # Extract from GraphQL response structure
                    try:
                        records = data.get('data', {}).get('data', []) or data.get('data', [])
                    except AttributeError:
                        return []

                    # Fix: Check type BEFORE iterating
                    if not isinstance(records, list):
                        return []

                    result: List[Dict] = []
                    for item in records:
                        if not isinstance(item, dict):
                            continue
                        try:
                            result.append({
                                'source': config['name'],
                                'timestamp': item.get('timestamp', time.time()),
                                'value': float(item.get('value', 0)),
                                'unit': config.get('unit', 'unknown')
                            })
                        except (ValueError, TypeError):
                            continue  # Skip malformed records
                    return result
            except Exception:
                return []
        return []


class FileFetcher(SourceFetcher):
    async def fetch(self, config: Dict) -> List[Dict]:
        path = Path(config['path'])
        if not path.exists():
            return []
        content = path.read_text()
        data = json.loads(content)
        if isinstance(data, dict):
            data = [data]
        return [{
            'source': config['name'],
            'timestamp': item.get('timestamp', time.time()),
            'value': float(item.get('value', 0)),
            'unit': config.get('unit', 'unknown')
        } for item in data]

FETCHERS = {
    'rest': RESTFetcher(),
    'csv': CSVFetcher(),
    'websocket': WebSocketFetcher(),
    'graphql': GraphQLFetcher(),
    'file': FileFetcher()
}

async def fetch_all_sources(sources: List[Dict]) -> List[Dict]:
    """Fetch from all sources concurrently"""
    tasks = []
    for source in sources:
        source_type = source.get('type', 'rest')
        fetcher = FETCHERS.get(source_type)
        if fetcher:
            tasks.append(fetcher.fetch(source))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Flatten results and handle errors
    all_records = []
    for result in results:
        if isinstance(result, list):
            all_records.extend(result)
    return all_records

def normalize_and_sort(records: List[Dict]) -> List[Dict]:
    """Sort by timestamp and ensure schema compliance"""
    normalized = []
    for record in records:
        try:
            normalized.append({
                'source': str(record.get('source', 'unknown')),
                'timestamp': float(record.get('timestamp', time.time())),
                'value': float(record.get('value', 0)),
                'unit': str(record.get('unit', 'unknown'))
            })
        except (ValueError, TypeError):
            continue  # Skip malformed records
    
    return sorted(normalized, key=lambda x: x['timestamp'])

async def run_pipeline(config_path: str = 'config.json') -> Dict:
    """Main pipeline execution"""
    start_time = time.time()
    
    # Load config
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    sources = config.get('sources', [])
    
    # Fetch concurrently
    records = await fetch_all_sources(sources)
    
    # Normalize and sort
    normalized = normalize_and_sort(records)
    
    elapsed = time.time() - start_time
    
    # Stats
    stats = {
        'total_records': len(normalized),
        'sources_used': len(set(r['source'] for r in normalized)),
        'execution_time_ms': round(elapsed * 1000, 2),
        'timestamp': datetime.now().isoformat()
    }
    
    return {
        'data': normalized,
        'stats': stats
    }

if __name__ == '__main__':
    result = asyncio.run(run_pipeline())
    print(json.dumps(result, indent=2))
