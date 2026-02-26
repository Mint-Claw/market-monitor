"""Tests for data_pipeline module"""

import asyncio
import json
import tempfile
import pytest
from pathlib import Path
from data_pipeline import (
    DataPipeline, SourceConfig, SourceType, 
    NormalizedRecord, run_pipeline, load_config,
    IncrementalPipeline
)


class TestSourceConfig:
    def test_rest_config(self):
        config = SourceConfig(
            name="test_api",
            type=SourceType.REST,
            url="https://api.example.com/data"
        )
        assert config.name == "test_api"
        assert config.type == SourceType.REST
    
    def test_file_config(self):
        config = SourceConfig(
            name="local_file",
            type=SourceType.FILE,
            path="./data.json"
        )
        assert config.path == "./data.json"


class TestNormalizedRecord:
    def test_to_dict(self):
        record = NormalizedRecord(
            source="test",
            timestamp="2026-01-01T00:00:00",
            value=100,
            unit="USD"
        )
        d = record.to_dict()
        assert d["source"] == "test"
        assert d["value"] == 100
        assert "metadata" not in d  # None values excluded
    
    def test_with_metadata(self):
        record = NormalizedRecord(
            source="test",
            timestamp="2026-01-01T00:00:00",
            value=50,
            metadata={"extra": "info"}
        )
        d = record.to_dict()
        assert d["metadata"] == {"extra": "info"}


class TestFileSource:
    @pytest.mark.asyncio
    async def test_json_file(self, tmp_path):
        # Create test JSON file
        test_data = [
            {"timestamp": "2026-01-01", "value": 100},
            {"timestamp": "2026-01-02", "value": 200}
        ]
        json_file = tmp_path / "test.json"
        json_file.write_text(json.dumps(test_data))
        
        sources = [
            SourceConfig(
                name="json_source",
                type=SourceType.FILE,
                path=str(json_file)
            )
        ]
        
        records, stats = await run_pipeline(sources)
        
        assert stats.sources_succeeded == 1
        assert stats.sources_failed == 0
        assert len(records) == 2
    
    @pytest.mark.asyncio
    async def test_csv_file(self, tmp_path):
        # Create test CSV file
        csv_content = "timestamp,value,unit\n2026-01-01,100,USD\n2026-01-02,200,EUR"
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)
        
        sources = [
            SourceConfig(
                name="csv_source",
                type=SourceType.FILE,
                path=str(csv_file)
            )
        ]
        
        records, stats = await run_pipeline(sources)
        
        assert stats.sources_succeeded == 1
        assert len(records) == 2
        assert records[0]["unit"] == "USD"
    
    @pytest.mark.asyncio
    async def test_missing_file(self):
        sources = [
            SourceConfig(
                name="missing",
                type=SourceType.FILE,
                path="./nonexistent_file.json"
            )
        ]
        
        records, stats = await run_pipeline(sources)
        
        # Should succeed but return empty
        assert stats.sources_succeeded == 1
        assert len(records) == 0


class TestNormalization:
    @pytest.mark.asyncio
    async def test_auto_timestamp(self, tmp_path):
        # Data without timestamp
        test_data = [{"price": 100}]
        json_file = tmp_path / "no_ts.json"
        json_file.write_text(json.dumps(test_data))
        
        sources = [
            SourceConfig(
                name="no_ts",
                type=SourceType.FILE,
                path=str(json_file)
            )
        ]
        
        records, stats = await run_pipeline(sources)
        
        assert len(records) == 1
        assert "timestamp" in records[0]  # Should auto-generate
    
    @pytest.mark.asyncio
    async def test_field_mapping(self, tmp_path):
        test_data = [{"custom_time": "2026-01-01", "custom_val": 999}]
        json_file = tmp_path / "custom.json"
        json_file.write_text(json.dumps(test_data))
        
        sources = [
            SourceConfig(
                name="custom",
                type=SourceType.FILE,
                path=str(json_file),
                field_mapping={"timestamp": "custom_time", "value": "custom_val"}
            )
        ]
        
        records, stats = await run_pipeline(sources)
        
        assert records[0]["value"] == 999
        assert records[0]["timestamp"] == "2026-01-01"


class TestConcurrency:
    @pytest.mark.asyncio
    async def test_multiple_sources(self, tmp_path):
        # Create multiple test files
        for i in range(5):
            data = [{"value": i * 100, "timestamp": f"2026-01-0{i+1}"}]
            (tmp_path / f"source{i}.json").write_text(json.dumps(data))
        
        sources = [
            SourceConfig(
                name=f"source_{i}",
                type=SourceType.FILE,
                path=str(tmp_path / f"source{i}.json")
            )
            for i in range(5)
        ]
        
        records, stats = await run_pipeline(sources)
        
        assert stats.sources_succeeded == 5
        assert stats.total_records == 5
        # Check sorted by timestamp
        timestamps = [r["timestamp"] for r in records]
        assert timestamps == sorted(timestamps)


class TestPerformance:
    @pytest.mark.asyncio
    async def test_1000_records_under_15s(self, tmp_path):
        """Requirement: 1000+ records in <15s"""
        # Create large dataset
        data = [
            {"timestamp": f"2026-01-{(i % 28) + 1:02d}", "value": i}
            for i in range(1000)
        ]
        large_file = tmp_path / "large.json"
        large_file.write_text(json.dumps(data))
        
        sources = [
            SourceConfig(
                name="large_source",
                type=SourceType.FILE,
                path=str(large_file)
            )
        ]
        
        records, stats = await run_pipeline(sources)
        
        assert stats.total_records >= 1000
        assert stats.duration_ms < 15000  # Under 15 seconds


class TestErrorHandling:
    @pytest.mark.asyncio
    async def test_graceful_failure(self, tmp_path):
        # Mix of valid and invalid sources
        valid_data = [{"value": 100}]
        (tmp_path / "valid.json").write_text(json.dumps(valid_data))
        
        sources = [
            SourceConfig(
                name="valid",
                type=SourceType.FILE,
                path=str(tmp_path / "valid.json")
            ),
            SourceConfig(
                name="invalid",
                type=SourceType.FILE,
                path="./does_not_exist.json"
            )
        ]
        
        records, stats = await run_pipeline(sources)
        
        # Pipeline should complete, returning data from valid source
        assert stats.sources_succeeded >= 1
        assert len(records) >= 1


class TestOutputFormat:
    @pytest.mark.asyncio
    async def test_json_output(self, tmp_path):
        test_data = [{"timestamp": "2026-01-01", "value": 42, "currency": "BTC"}]
        json_file = tmp_path / "out.json"
        json_file.write_text(json.dumps(test_data))
        
        sources = [
            SourceConfig(
                name="test",
                type=SourceType.FILE,
                path=str(json_file)
            )
        ]
        
        records, stats = await run_pipeline(sources)
        
        # Verify JSON serializable
        output = json.dumps({"records": records, "stats": {
            "total": stats.total_records,
            "succeeded": stats.sources_succeeded
        }})
        assert isinstance(output, str)


class TestConfigLoader:
    def test_load_config(self, tmp_path):
        config = {
            "pipeline": {"timeout": 10.0},
            "sources": [
                {"name": "test", "type": "file", "path": "./data.json"}
            ]
        }
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(config))
        
        sources, settings = load_config(str(config_file))
        
        assert len(sources) == 1
        assert sources[0].name == "test"
        assert sources[0].type == SourceType.FILE
        assert settings["timeout"] == 10.0
    
    def test_config_with_all_source_types(self, tmp_path):
        config = {
            "sources": [
                {"name": "rest", "type": "rest", "url": "http://api.test.com"},
                {"name": "csv", "type": "csv", "path": "./data.csv"},
                {"name": "ws", "type": "websocket", "url": "ws://stream.test.com"},
                {"name": "gql", "type": "graphql", "url": "http://gql.test.com", "query": "{ data }"},
                {"name": "file", "type": "file", "path": "./data.json"}
            ]
        }
        config_file = tmp_path / "config.json"
        config_file.write_text(json.dumps(config))
        
        sources, _ = load_config(str(config_file))
        
        assert len(sources) == 5
        assert {s.type for s in sources} == {
            SourceType.REST, SourceType.CSV, SourceType.WEBSOCKET,
            SourceType.GRAPHQL, SourceType.FILE
        }


class TestIncrementalUpdates:
    @pytest.mark.asyncio
    async def test_incremental_pipeline(self, tmp_path):
        # Create test data
        test_data = [
            {"timestamp": "2026-01-01T00:00:00", "value": 100},
            {"timestamp": "2026-01-02T00:00:00", "value": 200}
        ]
        json_file = tmp_path / "data.json"
        json_file.write_text(json.dumps(test_data))
        state_file = tmp_path / ".state.json"
        
        sources = [
            SourceConfig(
                name="test",
                type=SourceType.FILE,
                path=str(json_file)
            )
        ]
        
        # First run - get all records
        async with IncrementalPipeline(sources, state_file=str(state_file)) as pipeline:
            records, stats = await pipeline.run(incremental=True)
        
        assert len(records) == 2
        assert state_file.exists()
        
        # Second run with same data - should be filtered (no new records)
        async with IncrementalPipeline(sources, state_file=str(state_file)) as pipeline:
            records, stats = await pipeline.run(incremental=True)
        
        assert len(records) == 0  # All records already seen
    
    @pytest.mark.asyncio
    async def test_incremental_new_records(self, tmp_path):
        state_file = tmp_path / ".state.json"
        json_file = tmp_path / "data.json"
        
        sources = [
            SourceConfig(
                name="test",
                type=SourceType.FILE,
                path=str(json_file)
            )
        ]
        
        # First run
        json_file.write_text(json.dumps([
            {"timestamp": "2026-01-01", "value": 100}
        ]))
        async with IncrementalPipeline(sources, state_file=str(state_file)) as pipeline:
            records1, _ = await pipeline.run(incremental=True)
        
        assert len(records1) == 1
        
        # Second run with newer data
        json_file.write_text(json.dumps([
            {"timestamp": "2026-01-01", "value": 100},  # Old
            {"timestamp": "2026-01-02", "value": 200}   # New
        ]))
        async with IncrementalPipeline(sources, state_file=str(state_file)) as pipeline:
            records2, _ = await pipeline.run(incremental=True)
        
        assert len(records2) == 1  # Only the new record
        assert records2[0]["timestamp"] == "2026-01-02"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
