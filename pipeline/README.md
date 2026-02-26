# Multi-Source Data Aggregation & Normalization Pipeline

Async Python pipeline that fetches data from 5+ concurrent sources (REST, CSV, WebSocket, GraphQL, local files), normalizes everything to a common Pydantic schema, and merges/deduplicates the results.

## Architecture

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  REST API   │  │  CSV File   │  │  WebSocket  │  │  GraphQL    │  │  JSON/XML   │
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │                │                │                │                │
       └────────────────┴────────────────┴────────────────┴────────────────┘
                                         │
                              ┌──────────▼──────────┐
                              │   PipelineEngine    │
                              │  (async orchestrator)│
                              └──────────┬──────────┘
                                         │
                    ┌────────────────────┬┴┬────────────────────┐
                    ▼                    ▼ ▼                    ▼
             ┌─────────────┐    ┌──────────────┐    ┌──────────────┐
             │   Fetch     │    │  Normalize   │    │    Merge     │
             │ (concurrent)│───▶│ (per-source) │───▶│ (deduplicate)│
             └─────────────┘    └──────────────┘    └──────┬───────┘
                                                           │
                                                    ┌──────▼───────┐
                                                    │PipelineResult│
                                                    └──────────────┘
```

## Installation

```bash
pip install -r requirements.txt
```

## Quick Start

```python
import asyncio
from pipeline.engine import PipelineEngine
from pipeline.sources import RESTSource, CSVSource, FileSource

async def main():
    engine = PipelineEngine()

    engine.add_source(RESTSource(
        "crypto_prices",
        url="https://api.example.com/v1/prices",
        results_key="data",
    ))
    engine.add_source(CSVSource(
        "historical",
        path="data/history.csv",
    ))
    engine.add_source(FileSource(
        "local_data",
        path="data/snapshot.json",
        results_key="items",
    ))

    result = await engine.run()

    print(f"Sources OK: {result.sources_ok}, Failed: {result.sources_failed}")
    print(f"Records: {result.total_raw} raw → {result.total_normalized} normalized → {result.total_merged} merged")

    for record in result.records[:5]:
        print(f"  {record.source}: {record.symbol} = {record.value} ({record.timestamp})")

asyncio.run(main())
```

## Data Sources

| Source | Class | Description |
|--------|-------|-------------|
| REST API | `RESTSource` | Async HTTP GET with configurable headers, params, results key |
| CSV | `CSVSource` | Local or remote CSV files with configurable delimiter |
| WebSocket | `WebSocketSource` | Streaming WebSocket with optional subscribe message |
| GraphQL | `GraphQLSource` | GraphQL POST queries with dot-path result extraction |
| File | `FileSource` | Local JSON or XML files with auto-format detection |

## Custom Normalizers

Register per-source normalizer functions to handle non-standard field mappings:

```python
from pipeline.normalizer import Normalizer
from pipeline.schema import NormalizedRecord

def my_normalizer(raw: dict, source: str) -> NormalizedRecord:
    return NormalizedRecord(
        id=f"{source}_{raw['uid']}",
        source=source,
        symbol=raw["asset_code"],
        value=float(raw["last_price"]),
    )

engine = PipelineEngine()
engine.normalizer.register("custom_api", my_normalizer)
```

## Custom Merge Strategy

```python
from pipeline.merger import Merger
from pipeline.schema import NormalizedRecord

def prefer_higher_value(a: NormalizedRecord, b: NormalizedRecord) -> NormalizedRecord:
    return a if (a.value or 0) >= (b.value or 0) else b

engine = PipelineEngine(merger=Merger(conflict_resolver=prefer_higher_value))
```

## Testing

```bash
pytest tests/ -v
```

## Project Structure

```
pipeline/
├── __init__.py
├── schema.py          # Pydantic models (NormalizedRecord, PipelineResult)
├── normalizer.py      # Format-specific normalizers
├── merger.py          # Merge & deduplicate logic
├── engine.py          # Async pipeline orchestrator
└── sources/
    ├── __init__.py
    ├── base.py        # Abstract DataSource interface
    ├── rest.py        # REST API source
    ├── csv_source.py  # CSV file source
    ├── websocket.py   # WebSocket streaming source
    ├── graphql.py     # GraphQL query source
    └── file.py        # Local JSON/XML file source
tests/
└── test_pipeline.py   # Comprehensive test suite
```

## Dependencies

- `aiohttp` — async HTTP client (REST, CSV remote, GraphQL)
- `pydantic` — data validation and schema
- `websockets` — WebSocket client
- `pytest` / `pytest-asyncio` — testing
