"""Integration tests for pipeline."""
import asyncio
import pytest
import json
from pathlib import Path
import sys
import time

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.config import PipelineConfig, SourceConfig
from pipeline.engine import run_pipeline
from mock_server.server import start_server

@pytest.fixture
def test_data_path():
    """Create test data file."""
    test_dir = Path(__file__).parent / "test_data"
    test_dir.mkdir(exist_ok=True)
    
    data_file = test_dir / "data.json"
    data = [
        {
            "timestamp": f"2026-02-27T11:{i:02d}:00Z",
            "value": 500 + i * 0.6,
            "unit": "CNY"
        }
        for i in range(100)
    ]
    data_file.write_text(json.dumps(data))
    
    yield str(data_file)

@pytest.mark.asyncio
async def test_full_pipeline_with_mock(test_data_path, tmp_path):
    """Test complete pipeline with all 5 source types using embedded mock server."""
    output_path = tmp_path / "output.json"
    
    # Start mock server
    runner = await start_server(port=8889)
    
    try:
        await asyncio.sleep(0.5)  # Let server start
        
        config = PipelineConfig(
            sources=[
                SourceConfig(
                    name="rest_source",
                    type="rest",
                    url="http://127.0.0.1:8889/api/rest",
                    params={"count": "250"}
                ),
                SourceConfig(
                    name="csv_source",
                    type="csv",
                    url="http://127.0.0.1:8889/api/csv",
                    params={"count": "250"}
                ),
                SourceConfig(
                    name="websocket_source",
                    type="websocket",
                    url="ws://127.0.0.1:8889/ws",
                    params={"duration": 3}
                ),
                SourceConfig(
                    name="graphql_source",
                    type="graphql",
                    url="http://127.0.0.1:8889/api/graphql",
                    params={"query": "{ metrics { timestamp value unit } }"}
                ),
                SourceConfig(
                    name="file_source",
                    type="file",
                    url=test_data_path
                )
            ],
            output_path=str(output_path),
            max_concurrent=5
        )
        
        start = time.time()
        result = await run_pipeline(config)
        duration = time.time() - start
        
        # Assertions
        assert result["stats"]["sources_succeeded"] == 5
        assert result["stats"]["sources_failed"] == 0
        assert result["stats"]["total_records"] >= 500  # At least 500 records
        assert duration < 15  # Must complete in <15s
        
        # Verify output file
        assert output_path.exists()
        output_data = json.loads(output_path.read_text())
        assert "data" in output_data
        assert "stats" in output_data
        
        # Verify data structure
        for record in output_data["data"][:10]:
            assert "source" in record
            assert "timestamp" in record
            assert "value" in record
            assert "unit" in record
        
        print(f"\n✓ Pipeline completed in {duration:.2f}s")
        print(f"✓ Total records: {result['stats']['total_records']}")
        print(f"✓ All 5 source types working")
        
    finally:
        await runner.cleanup()

@pytest.mark.asyncio
async def test_error_handling(tmp_path):
    """Test graceful error handling."""
    output_path = tmp_path / "output.json"
    
    config = PipelineConfig(
        sources=[
            SourceConfig(
                name="invalid_rest",
                type="rest",
                url="http://127.0.0.1:9999/nonexistent",
                timeout=2.0
            ),
            SourceConfig(
                name="invalid_file",
                type="file",
                url="/nonexistent/path/data.json"
            )
        ],
        output_path=str(output_path),
        retry_attempts=1
    )
    
    result = await run_pipeline(config)
    
    # Should handle errors gracefully
    assert result["stats"]["sources_failed"] == 2
    assert len(result["stats"]["errors"]) == 2
    assert output_path.exists()
    
    print(f"\n✓ Error handling working")
    print(f"✓ Failed sources: {result['stats']['sources_failed']}")

@pytest.mark.asyncio
async def test_field_mapping(tmp_path):
    """Test custom field mapping (bonus feature)."""
    output_path = tmp_path / "output.json"
    
    # Create data with different field names
    custom_data_path = tmp_path / "custom.json"
    custom_data = [
        {
            "ts": f"2026-02-27T12:{i:02d}:00Z",
            "price": 100 + i,
            "currency": "USD"
        }
        for i in range(50)
    ]
    custom_data_path.write_text(json.dumps(custom_data))
    
    config = PipelineConfig(
        sources=[
            SourceConfig(
                name="custom_source",
                type="file",
                url=str(custom_data_path),
                field_mapping={
                    "timestamp": "ts",
                    "value": "price",
                    "unit": "currency"
                }
            )
        ],
        output_path=str(output_path)
    )
    
    result = await run_pipeline(config)
    
    assert result["stats"]["total_records"] == 50
    # Verify mapping worked
    first_record = result["data"][0]
    assert first_record["timestamp"] == "2026-02-27T12:00:00Z"
    assert first_record["value"] == 100
    assert first_record["unit"] == "USD"
    
    print(f"\n✓ Field mapping working")

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
