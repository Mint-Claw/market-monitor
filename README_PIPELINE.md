# Multi-Source Data Aggregation Pipeline

Async Python pipeline for fetching data from 5+ concurrent sources (REST, CSV, WebSocket, GraphQL, file), normalizing to a common schema, and merging to sorted JSON.

## Features

- ✅ **5 Source Types**: REST API, CSV, WebSocket, GraphQL, local files
- ✅ **Async & Concurrent**: Fetch from multiple sources simultaneously
- ✅ **Normalized Schema**: All data standardized to `{source, timestamp, value, unit}`
- ✅ **Performance**: 1000+ records in <15s
- ✅ **Error Handling**: Graceful timeout/failure handling with retries
- ✅ **Bonus Features**: 
  - Custom field mapping for different source schemas
  - Incremental updates support (fetch only new records)

## Installation

```bash
pip install -r requirements.txt
```

## Quick Start

### 1. Start Mock Server

```bash
python mock_server/server.py
```

The mock server runs on `http://127.0.0.1:8888` and provides test endpoints for all 5 source types.

### 2. Generate Test Data

```bash
python tests/generate_test_data.py
```

### 3. Run Pipeline

```bash
python run_pipeline.py example_config.json -v
```

## Configuration

Create a JSON config file with your data sources:

```json
{
  "sources": [
    {
      "name": "rest_api",
      "type": "rest",
      "url": "https://api.example.com/data",
      "timeout": 10.0,
      "params": {"limit": 100},
      "headers": {"Authorization": "Bearer token"}
    },
    {
      "name": "csv_file",
      "type": "csv",
      "url": "https://example.com/data.csv"
    },
    {
      "name": "websocket_stream",
      "type": "websocket",
      "url": "ws://example.com/stream",
      "params": {"duration": 5}
    },
    {
      "name": "graphql_api",
      "type": "graphql",
      "url": "https://api.example.com/graphql",
      "params": {
        "query": "{ metrics { timestamp value unit } }"
      }
    },
    {
      "name": "local_data",
      "type": "file",
      "url": "data/local.json"
    }
  ],
  "output_path": "output.json",
  "max_concurrent": 10,
  "retry_attempts": 2
}
```

### Custom Field Mapping

If your source uses different field names, use `field_mapping`:

```json
{
  "name": "custom_api",
  "type": "rest",
  "url": "https://api.example.com/data",
  "field_mapping": {
    "timestamp": "created_at",
    "value": "price",
    "unit": "currency"
  }
}
```

## Testing

Run the full test suite:

```bash
pytest tests/test_pipeline.py -v
```

Tests include:
- Full pipeline with all 5 source types
- Error handling and graceful failures
- Custom field mapping
- Performance validation (<15s for 1000+ records)

## Output Format

```json
{
  "data": [
    {
      "source": "rest_api",
      "timestamp": "2026-02-27T07:00:00Z",
      "value": 100.5,
      "unit": "USD"
    },
    ...
  ],
  "stats": {
    "total_records": 1250,
    "sources_succeeded": 5,
    "sources_failed": 0,
    "duration_seconds": 8.42,
    "errors": []
  }
}
```

## Architecture

```
pipeline/
├── config.py      # Configuration schema
├── fetchers.py    # Source-specific fetchers
└── engine.py      # Main pipeline orchestration

mock_server/
└── server.py      # Test server with all source types

tests/
├── test_pipeline.py       # Integration tests
└── generate_test_data.py  # Test data generator
```

## Performance

- Concurrent fetching with configurable semaphore
- Async I/O for all network operations
- Retry logic with exponential backoff
- Tested with 1000+ records in <15s

## Error Handling

- Graceful timeout handling per source
- Configurable retry attempts
- Failed sources don't block pipeline
- Detailed error reporting in stats

## License

MIT

## Bounty

This implementation fulfills the requirements for [Mint-Claw/market-monitor Issue #1](https://github.com/Mint-Claw/market-monitor/issues/1) - $15 bounty for Multi-Source Data Aggregation Pipeline.
