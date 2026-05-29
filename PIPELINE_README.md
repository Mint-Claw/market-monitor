# Multi-Source Data Aggregation Pipeline

An async Python pipeline that fetches from 5+ concurrent sources (REST, CSV, WebSocket, GraphQL, file), normalizes to a common schema, and merges to sorted JSON.

## Features

- **5 Source Types**: REST API, CSV files, WebSocket feeds, GraphQL APIs, local JSON files
- **Concurrent Fetching**: All sources are fetched asynchronously for maximum performance
- **Common Schema**: All data is normalized to `{source, timestamp, value, unit}`
- **Sorted Output**: Records are merged and sorted by timestamp
- **Graceful Error Handling**: Timeouts and failures are captured without stopping the pipeline
- **Config-Driven**: Sources are defined in a JSON configuration file
- **Statistics**: Pipeline reports total records, success/failure counts, and elapsed time

## Installation

```bash
pip install aiohttp websockets pytest pytest-asyncio
```

## Usage

### Basic Usage

```bash
python pipeline.py --config config.json --output output.json
```

### Configuration

Create a `config.json` file with your sources:

```json
{
  "pipeline_timeout": 15,
  "sources": [
    {
      "name": "rest-api",
      "type": "rest",
      "url": "https://api.example.com/data",
      "timeout": 5,
      "headers": {"Authorization": "Bearer token"},
      "params": {"limit": 100}
    },
    {
      "name": "csv-file",
      "type": "csv",
      "path": "data/market_data.csv",
      "timestamp_col": "date",
      "value_col": "price",
      "unit_col": "currency"
    },
    {
      "name": "websocket-feed",
      "type": "websocket",
      "url": "wss://ws.example.com/feed",
      "timeout": 5,
      "max_messages": 50
    },
    {
      "name": "graphql-api",
      "type": "graphql",
      "url": "https://api.example.com/graphql",
      "timeout": 5,
      "query": "{ items { timestamp value unit } }"
    },
    {
      "name": "local-json",
      "type": "file",
      "path": "data/historical.json"
    }
  ]
}
```

### Source Types

| Type | Description | Required Fields |
|------|-------------|-----------------|
| `rest` | REST API endpoint | `url` |
| `csv` | CSV file | `path`, `timestamp_col`, `value_col`, `unit_col` |
| `websocket` | WebSocket feed | `url` |
| `graphql` | GraphQL API | `url`, `query` |
| `file` | Local JSON file | `path` |

### Output Format

```json
{
  "records": [
    {
      "source": "rest-api",
      "timestamp": "2026-01-01T00:00:00Z",
      "value": 100.50,
      "unit": "USD",
      "metadata": {"index": 0}
    }
  ],
  "stats": {
    "total_records": 1000,
    "sources_attempted": 5,
    "sources_succeeded": 5,
    "sources_failed": 0,
    "elapsed_seconds": 2.456,
    "errors": []
  }
}
```

## Testing

### Run Tests

```bash
# Run all tests
pytest tests/ -v

# Run specific test
pytest tests/test_pipeline.py::test_file_source -v
```

### Run Mock Server

```bash
# Start mock server for testing
python mock_server.py
```

The mock server provides:
- REST API: http://localhost:8080/api/data
- GraphQL: http://localhost:8080/graphql
- WebSocket: ws://localhost:8081

## Performance

The pipeline is designed to complete within 15 seconds for 1000+ records:

- Concurrent fetching from all sources
- Async I/O for non-blocking operations
- Configurable timeouts per source and for the entire pipeline

## Error Handling

- **Timeouts**: Each source has a configurable timeout
- **Failures**: Failed sources are logged but don't stop the pipeline
- **Statistics**: Error details are included in the output stats

## License

MIT
