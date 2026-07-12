import pytest
import asyncio
from pipeline import Pipeline
from mock_server import create_app
import json

@pytest.fixture
def test_data_file(tmp_path):
    d = tmp_path / "data"
    d.mkdir()
    p = d / "data.json"
    p.write_text(json.dumps([
        {"time_field": "2024-01-01T00:04:00Z", "val_field": 1000.0, "unit_field": "XRP"}
    ]))
    return str(p)

@pytest.fixture
def pipeline_config(test_data_file):
    return [
        {
            "name": "rest_source",
            "type": "rest",
            "url": "http://127.0.0.1:8080/api/rest",
            "data_path": "items",
            "mapping": {
                "timestamp": "t",
                "value": "p",
                "unit": "c",
                "custom": {"extra_field": "extra"}
            }
        },
        {
            "name": "graphql_source",
            "type": "graphql",
            "url": "http://127.0.0.1:8080/api/graphql",
            "query": "{ market { timestamp value symbol } }",
            "data_path": "market",
            "mapping": {
                "timestamp": "timestamp",
                "value": "value",
                "unit": "symbol"
            }
        },
        {
            "name": "csv_source",
            "type": "csv",
            "url": "http://127.0.0.1:8080/api/csv",
            "mapping": {
                "timestamp": "date",
                "value": "amount",
                "unit": "type"
            }
        },
        {
            "name": "ws_source",
            "type": "websocket",
            "url": "ws://127.0.0.1:8080/api/ws",
            "data_path": "events",
            "mapping": {
                "timestamp": "ts",
                "value": "val",
                "unit": "coin"
            }
        },
        {
            "name": "file_source",
            "type": "file",
            "path": test_data_file,
            "mapping": {
                "timestamp": "time_field",
                "value": "val_field",
                "unit": "unit_field"
            }
        }
    ]

@pytest.mark.asyncio
async def test_full_pipeline(aiohttp_server, pipeline_config):
    app = create_app()
    server = await aiohttp_server(app, port=8080)
    
    pipeline = Pipeline(pipeline_config)
    result = await pipeline.run()
    
    assert result["stats"]["sources_processed"] == 5
    assert result["stats"]["total_records"] == 7
    
    data = result["data"]
    # Check that data is sorted by timestamp
    timestamps = [r["timestamp"] for r in data]
    assert timestamps == sorted(timestamps)
    
    # Check custom field mapping
    rest_data = [r for r in data if r["source"] == "rest_source"]
    assert len(rest_data) == 2
    assert "extra_field" in rest_data[0]
    
    # Test incremental
    result2 = await pipeline.run(incremental=True)
    assert result2["stats"]["total_records"] == 0  # No new records since last fetch
