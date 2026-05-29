#!/usr/bin/env python3
"""
Integration tests for the multi-source data aggregation pipeline.
"""

import asyncio
import json
import os
import sys
import tempfile
import pytest
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline import Pipeline, NormalizedRecord

# Test data
TEST_CSV = """timestamp,value,unit
2026-01-01T00:00:00Z,100.50,USD
2026-01-01T01:00:00Z,101.25,USD
2026-01-01T02:00:00Z,99.75,USD
"""

TEST_JSON = [
    {"timestamp": "2026-01-01T00:00:00Z", "value": 150.25, "unit": "EUR"},
    {"timestamp": "2026-01-01T01:00:00Z", "value": 151.00, "unit": "EUR"}
]

@pytest.fixture
def temp_dir():
    """Create temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir

@pytest.fixture
def csv_file(temp_dir):
    """Create test CSV file."""
    path = os.path.join(temp_dir, "test.csv")
    with open(path, "w") as f:
        f.write(TEST_CSV)
    return path

@pytest.fixture
def json_file(temp_dir):
    """Create test JSON file."""
    path = os.path.join(temp_dir, "test.json")
    with open(path, "w") as f:
        json.dump(TEST_JSON, f)
    return path

@pytest.mark.asyncio
async def test_file_source(csv_file, json_file):
    """Test file source types."""
    config = {
        "pipeline_timeout": 5,
        "sources": [
            {
                "name": "csv-source",
                "type": "csv",
                "path": csv_file,
                "timestamp_col": "timestamp",
                "value_col": "value",
                "unit_col": "unit"
            },
            {
                "name": "json-source",
                "type": "file",
                "path": json_file
            }
        ]
    }
    
    pipeline = Pipeline(config)
    result = await pipeline.run()
    
    assert result["stats"]["total_records"] == 5  # 3 CSV + 2 JSON
    assert result["stats"]["sources_succeeded"] == 2
    assert result["stats"]["sources_failed"] == 0
    assert len(result["records"]) == 5
    
    # Check records are sorted by timestamp
    timestamps = [r["timestamp"] for r in result["records"]]
    assert timestamps == sorted(timestamps)

@pytest.mark.asyncio
async def test_timeout_handling(csv_file):
    """Test timeout handling."""
    config = {
        "pipeline_timeout": 0.001,  # Very short timeout
        "sources": [
            {
                "name": "csv-source",
                "type": "csv",
                "path": csv_file,
                "timestamp_col": "timestamp",
                "value_col": "value",
                "unit_col": "unit"
            }
        ]
    }
    
    pipeline = Pipeline(config)
    result = await pipeline.run()
    
    # CSV should still work even with short timeout
    assert result["stats"]["total_records"] == 3

@pytest.mark.asyncio
async def test_error_handling(temp_dir):
    """Test error handling for invalid files."""
    config = {
        "pipeline_timeout": 5,
        "sources": [
            {
                "name": "invalid-csv",
                "type": "csv",
                "path": os.path.join(temp_dir, "nonexistent.csv"),
                "timestamp_col": "timestamp",
                "value_col": "value",
                "unit_col": "unit"
            }
        ]
    }
    
    pipeline = Pipeline(config)
    result = await pipeline.run()
    
    assert result["stats"]["total_records"] == 0
    assert result["stats"]["sources_failed"] == 1
    assert len(result["stats"]["errors"]) == 1

@pytest.mark.asyncio
async def test_normalized_schema(csv_file):
    """Test that records follow normalized schema."""
    config = {
        "pipeline_timeout": 5,
        "sources": [
            {
                "name": "csv-source",
                "type": "csv",
                "path": csv_file,
                "timestamp_col": "timestamp",
                "value_col": "value",
                "unit_col": "unit"
            }
        ]
    }
    
    pipeline = Pipeline(config)
    result = await pipeline.run()
    
    for record in result["records"]:
        assert "source" in record
        assert "timestamp" in record
        assert "value" in record
        assert "unit" in record
        assert isinstance(record["value"], float)

@pytest.mark.asyncio
async def test_multiple_sources(csv_file, json_file):
    """Test multiple sources of different types."""
    config = {
        "pipeline_timeout": 5,
        "sources": [
            {
                "name": "csv-source",
                "type": "csv",
                "path": csv_file,
                "timestamp_col": "timestamp",
                "value_col": "value",
                "unit_col": "unit"
            },
            {
                "name": "json-source",
                "type": "file",
                "path": json_file
            }
        ]
    }
    
    pipeline = Pipeline(config)
    result = await pipeline.run()
    
    # Check sources are recorded
    sources = set(r["source"] for r in result["records"])
    assert "csv-source" in sources
    assert "json-source" in sources

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
