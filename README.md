# Market Monitor — Multi-Source Data Aggregation Pipeline

Async Python pipeline using [dlthub](https://dlthub.com/) that fetches from **5+ concurrent sources**, normalises every record to a common schema, and merges them into sorted JSON + DuckDB output.

## Performance

| Config | Records | Time | Use Case |
|--------|---------|------|----------|
| `config_fast.json` | 1,800+ | ~5s | **Bounty requirement** (<15s for 1000+) |
| `config.json` | 5,400+ | ~21s | Maximum capacity testing |

## Sources

| # | Type | Source | Fast | Full |
|---|------|--------|------|------|
| 1 | REST | JSONPlaceholder Photos | 500 | 1500 |
| 2 | CSV | COVID-19 Dataset | 500 | 1500 |
| 3 | WebSocket | Binance BTC/USDT Trades | 50 | 150 |
| 4 | GraphQL | Rick & Morty API (paginated) | 250 | 826 |
| 5 | File | Crypto sample data | 500 | 1500 |

## Output Schema

Every record — regardless of source — is normalised to exactly four fields:

```json
{
  "source": "rest_jsonplaceholder",
  "timestamp": "2026-02-27T12:34:56+00:00",
  "value": { "…": "…" },
  "unit": "record"
}
```

## Quick Start

```bash
# 1. Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run the pipeline
python pipeline.py

# 4. Run tests
pytest test_pipeline.py -v
```

## Usage

```bash
# Default config
python pipeline.py

# Custom config file
python pipeline.py --config my_config.json

# Dry-run — validate config without fetching
python pipeline.py --dry-run
```

## Output

| File | Description |
|------|-------------|
| `output/aggregated.json` | All records merged & sorted by timestamp, plus stats |
| `output/stats.json` | Pipeline execution statistics |
| `output/.dlt/` | DLT pipeline state + DuckDB warehouse |

### Sample stats.json

```json
{
  "pipeline": "market_monitor",
  "run_timestamp": "2026-02-27T15:00:00+00:00",
  "elapsed_seconds": 4.521,
  "total_records": 1162,
  "sources_attempted": 5,
  "sources_succeeded": 5,
  "sources_failed": 0,
  "source_record_counts": {
    "rest_jsonplaceholder": 100,
    "csv_covid19": 500,
    "ws_binance_btcusdt": 50,
    "graphql_countries": 250,
    "file_local_sample": 10
  },
  "dlt_load_id": "1709042400.123456"
}
```

## Configuration

All sources are defined declaratively in `config.json`:

```json
{
  "pipeline": {
    "name": "market_monitor",
    "output_dir": "output",
    "output_file": "aggregated.json",
    "timeout_seconds": 10
  },
  "sources": [
    {
      "name": "my_api",
      "type": "rest",
      "enabled": true,
      "url": "https://api.example.com/data",
      "method": "GET",
      "headers": { "Authorization": "Bearer ${API_TOKEN}" },
      "unit": "record",
      "max_records": 100
    }
  ]
}
```

Each source entry only needs `name`, `type`, and a type-specific field (e.g. `url` or `path`). Everything else has sensible defaults.

## Architecture

```
config.json
  │
  ▼
┌──────────────────────────────┐
│   SOURCE REGISTRY (auto)     │  @register_source("rest")
│   rest, csv, websocket,      │  @register_source("csv")
│   graphql, file, …           │  @register_source("your_type")
└──────────────┬───────────────┘
               │
  asyncio.gather (concurrent)
               │
  ┌────────────┼────────────┐
  ▼            ▼            ▼
fetch()    fetch()      fetch()    ← each source plugin
  │            │            │
  ▼            ▼            ▼
normalize() normalize() normalize() ← common schema
  │            │            │
  └────────────┼────────────┘
               │
          merge + sort
               │
       ┌───────┴───────┐
       ▼               ▼
  JSON output    DLT → DuckDB
```

Key design decisions:

1. **Plugin registry** — Source types auto-register via `@register_source`. The pipeline discovers them at import time.
2. **Concurrent fetch** — All enabled sources run in parallel via `asyncio.gather`.
3. **Graceful degradation** — Any source that fails or times out returns `[]`; the rest continue.
4. **Normalise at source** — Each source class owns its `normalize()`, allowing type-specific timestamp extraction.
5. **DLT for persistence** — Records flow through a DLT pipeline into DuckDB for structured querying.

## Testing

```bash
# All tests
pytest test_pipeline.py -v

# Individual source only
pytest test_pipeline.py -v -k "TestRestSource"

# Full pipeline integration
pytest test_pipeline.py -v -k "TestFullPipeline"
```

Tests cover:
- Plugin registry correctness
- Config validation
- Schema normalisation (timestamps, defaults, epoch-ms)
- Each source type individually (live network)
- **Per-source capacity tests** (REST/CSV/File 1500+, GraphQL 800+, WebSocket 100+)
- Full pipeline end-to-end (record count, schema compliance, sort order)
- **Fast config performance** (<15s for 1000+ records)
- **Full config performance** (<45s for 5000+ records)
- Graceful failure (bad URLs, timeouts)

---

## Extensibility

### Adding a New Source (3 steps)

**Step 1** — Create `sources/my_source.py`:

```python
from sources.base import BaseSource, register_source

@register_source("my_type")
class MySource(BaseSource):
    async def fetch(self):
        # Your async fetch logic here
        return [{"key": "value"}, ...]
```

**Step 2** — Import it in `sources/__init__.py`:

```python
from sources.my_source import MySource  # noqa: F401
```

**Step 3** — Add entries to `config.json`:

```json
{
  "name": "my_new_source",
  "type": "my_type",
  "enabled": true,
  "url": "https://...",
  "unit": "record"
}
```

That's it. No changes to `pipeline.py` or any other file.

### Custom Normalisation

Override the `normalize()` method when your source needs special handling:

```python
@register_source("exchange")
class ExchangeSource(BaseSource):
    async def fetch(self):
        ...

    def normalize(self, raw_records):
        """Custom: extract price + volume into a cleaner value."""
        return [
            {
                "source": self.name,
                "timestamp": self._extract_timestamp(rec),
                "value": {
                    "price": rec["last_price"],
                    "volume_24h": rec["vol"],
                },
                "unit": self.unit,
            }
            for rec in raw_records
        ]
```

The base class provides `_extract_timestamp()` which handles ISO strings, date-only strings, and epoch-millisecond values out of the box.

### Plugin Architecture Details

The registry is a simple dict populated by decorators:

```python
# sources/base.py
SOURCE_REGISTRY: dict[str, type[BaseSource]] = {}

def register_source(type_name: str):
    def decorator(cls):
        SOURCE_REGISTRY[type_name] = cls
        return cls
    return decorator
```

The pipeline resolves source types at runtime:

```python
cls = get_source_class(config["type"])  # KeyError if unknown
src = cls(config)
data = await src.fetch()
records = src.normalize(data)
```

This means third-party source plugins only need to:
1. Subclass `BaseSource`
2. Implement `async fetch() -> list[dict]`
3. Register with `@register_source("name")`
4. Get imported before the pipeline runs

### Future Improvements

- **Incremental / delta updates** — Track last-seen timestamps per source and only fetch new data. The `config.json` already supports `max_records`; extending to `since_timestamp` would enable true incremental loads.
- **Caching layer** — Add an optional TTL-based cache (e.g. `diskcache`) so repeated runs within a window skip network calls.
- **Rate limiting** — Per-source rate limits in config, enforced via `aiohttp` throttling or `asyncio.Semaphore`.
- **Retry strategies** — Configurable retry count + exponential backoff per source type.
- **Parallel source groups** — Group sources by priority or dependency, fetch groups sequentially but sources within a group concurrently.
- **Schema validation** — Add JSON Schema or Pydantic model validation on normalised records before merge.
- **Streaming output** — For very large datasets, stream records to JSONL instead of buffering the full list.
- **Webhook notifications** — Post pipeline stats to Slack/Discord on completion or failure.

## Dependencies

| Package | Purpose |
|---------|---------|
| `dlt[duckdb]` | Data loading framework + DuckDB destination |
| `aiohttp` | Async HTTP client (REST, CSV, GraphQL) |
| `websockets` | WebSocket client (streaming data) |
| `pytest` | Test framework |
| `pytest-asyncio` | Async test support |

## License

MIT
