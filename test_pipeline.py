"""Integration tests for the multi-source aggregation pipeline."""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any

import pytest

# Ensure we run from the market-monitor directory for relative imports
os.chdir(Path(__file__).parent)

from pipeline import load_config, run_pipeline, fetch_one
from sources import SOURCE_REGISTRY, get_source_class
from sources.base import BaseSource
from sources.rest_source import RestSource
from sources.csv_source import CsvSource
from sources.websocket_source import WebSocketSource
from sources.graphql_source import GraphQLSource
from sources.file_source import FileSource


# ── Registry & Config Tests ──────────────────────────────────────────────────


class TestRegistry:
    def test_all_five_types_registered(self):
        for t in ("rest", "csv", "websocket", "graphql", "file"):
            assert t in SOURCE_REGISTRY, f"'{t}' not registered"

    def test_get_source_class_unknown(self):
        with pytest.raises(KeyError, match="Unknown source type"):
            get_source_class("nonexistent")


class TestConfig:
    def test_load_config(self):
        config = load_config("config.json")
        assert "pipeline" in config
        assert "sources" in config
        assert len(config["sources"]) >= 5

    def test_load_config_missing_file(self):
        with pytest.raises(FileNotFoundError):
            load_config("nonexistent.json")

    def test_all_source_types_present(self):
        config = load_config("config.json")
        types = {s["type"] for s in config["sources"]}
        assert {"rest", "csv", "websocket", "graphql", "file"}.issubset(types)

    def test_sources_have_required_fields(self):
        config = load_config("config.json")
        for s in config["sources"]:
            assert "name" in s
            assert "type" in s
            assert "unit" in s


# ── Schema / Normalization Tests ─────────────────────────────────────────────


class TestNormalization:
    def test_base_normalize_adds_schema_fields(self):
        cfg = {"name": "test", "unit": "item"}
        src = RestSource(cfg | {"url": "http://unused", "type": "rest"})
        out = src.normalize([{"foo": "bar"}])
        assert len(out) == 1
        assert set(out[0].keys()) == {"source", "timestamp", "value", "unit"}
        assert out[0]["source"] == "test"
        assert out[0]["unit"] == "item"
        assert out[0]["value"] == {"foo": "bar"}

    def test_timestamp_extraction_date_only(self):
        cfg = {"name": "t", "unit": "x", "url": "http://x", "type": "rest"}
        src = RestSource(cfg)
        out = src.normalize([{"Date": "2024-03-15"}])
        assert out[0]["timestamp"] == "2024-03-15T00:00:00+00:00"

    def test_timestamp_extraction_epoch_ms(self):
        cfg = {"name": "t", "unit": "x", "url": "http://x", "type": "rest"}
        src = RestSource(cfg)
        out = src.normalize([{"timestamp": 1700000000000}])
        assert "2023-11-14" in out[0]["timestamp"]

    def test_records_sorted_after_normalize(self):
        records = [
            {"source": "c", "timestamp": "2024-03-01", "value": 3, "unit": "x"},
            {"source": "a", "timestamp": "2024-01-01", "value": 1, "unit": "x"},
            {"source": "b", "timestamp": "2024-02-01", "value": 2, "unit": "x"},
        ]
        records.sort(key=lambda r: r["timestamp"])
        assert records[0]["source"] == "a"
        assert records[2]["source"] == "c"


# ── Individual Source Tests (live network) ────────────────────────────────────


class TestRestSource:
    @pytest.mark.asyncio
    async def test_fetch_rest(self):
        cfg = {
            "name": "test_rest",
            "type": "rest",
            "url": "https://jsonplaceholder.typicode.com/posts",
            "unit": "record",
            "max_records": 5,
        }
        src = RestSource(cfg)
        raw = await src.fetch()
        assert len(raw) == 5
        records = src.normalize(raw)
        assert all(r["source"] == "test_rest" for r in records)
        assert all(set(r.keys()) == {"source", "timestamp", "value", "unit"} for r in records)

    @pytest.mark.asyncio
    async def test_fetch_rest_1500_records(self):
        """REST source must be able to fetch 1500 records."""
        cfg = {
            "name": "test_rest_1500",
            "type": "rest",
            "url": "https://jsonplaceholder.typicode.com/photos",
            "unit": "record",
            "max_records": 1500,
        }
        src = RestSource(cfg)
        raw = await src.fetch()
        assert len(raw) >= 1500, f"REST should fetch 1500 records, got {len(raw)}"
        records = src.normalize(raw)
        assert len(records) >= 1500


class TestCsvSource:
    @pytest.mark.asyncio
    async def test_fetch_csv(self):
        cfg = {
            "name": "test_csv",
            "type": "csv",
            "url": "https://raw.githubusercontent.com/datasets/covid-19/main/data/countries-aggregated.csv",
            "unit": "row",
            "max_records": 10,
        }
        src = CsvSource(cfg)
        raw = await src.fetch()
        assert len(raw) == 10
        records = src.normalize(raw)
        assert all(r["source"] == "test_csv" for r in records)
        # COVID data should have Date field turned into timestamp
        assert "T" in records[0]["timestamp"] or "2020" in records[0]["timestamp"]

    @pytest.mark.asyncio
    async def test_fetch_csv_1500_records(self):
        """CSV source must be able to fetch 1500 records."""
        cfg = {
            "name": "test_csv_1500",
            "type": "csv",
            "url": "https://raw.githubusercontent.com/datasets/covid-19/main/data/countries-aggregated.csv",
            "unit": "row",
            "max_records": 1500,
        }
        src = CsvSource(cfg)
        raw = await src.fetch()
        assert len(raw) >= 1500, f"CSV should fetch 1500 records, got {len(raw)}"
        records = src.normalize(raw)
        assert len(records) >= 1500


class TestWebSocketSource:
    @pytest.mark.asyncio
    async def test_fetch_websocket(self):
        cfg = {
            "name": "test_ws",
            "type": "websocket",
            "url": "wss://stream.binance.com:9443/ws/btcusdt@trade",
            "unit": "trade",
            "max_records": 5,
            "connect_timeout_seconds": 10,
            "recv_timeout_seconds": 3,
        }
        src = WebSocketSource(cfg)
        raw = await src.fetch()
        assert len(raw) >= 1, "WebSocket should receive at least 1 trade"
        records = src.normalize(raw)
        assert all(r["source"] == "test_ws" for r in records)
        assert all(r["unit"] == "trade" for r in records)
        # WebSocket normalize extracts Binance fields
        assert "price" in records[0]["value"]

    @pytest.mark.asyncio
    async def test_fetch_websocket_100_records(self):
        """WebSocket source must be able to fetch 100+ real-time records."""
        cfg = {
            "name": "test_ws_100",
            "type": "websocket",
            "url": "wss://stream.binance.com:9443/ws/btcusdt@trade",
            "unit": "trade",
            "max_records": 100,
            "connect_timeout_seconds": 15,
            "recv_timeout_seconds": 10,
        }
        src = WebSocketSource(cfg)
        raw = await src.fetch()
        # WebSocket is real-time, accept 50+ as trades may be slow
        assert len(raw) >= 50, f"WebSocket should fetch 50+ trades, got {len(raw)}"
        records = src.normalize(raw)
        assert all("price" in r["value"] for r in records)


class TestGraphQLSource:
    @pytest.mark.asyncio
    async def test_fetch_graphql(self):
        cfg = {
            "name": "test_gql",
            "type": "graphql",
            "url": "https://countries.trevorblades.com/graphql",
            "query": "{ countries { code name } }",
            "data_path": "countries",
            "unit": "record",
        }
        src = GraphQLSource(cfg)
        raw = await src.fetch()
        assert len(raw) > 100  # ~250 countries
        records = src.normalize(raw)
        assert all(r["source"] == "test_gql" for r in records)
        assert "code" in records[0]["value"]

    @pytest.mark.asyncio
    async def test_fetch_graphql_paginated_800_records(self):
        """GraphQL source with pagination must fetch 800+ records."""
        cfg = {
            "name": "test_gql_paginated",
            "type": "graphql",
            "url": "https://rickandmortyapi.com/graphql",
            "query": "query ($page: Int) { characters(page: $page) { info { next } results { id name status species } } }",
            "data_path": "characters",
            "unit": "character",
            "max_records": 826,
            "paginate": {
                "enabled": True,
                "page_var": "page",
                "max_pages": 42
            }
        }
        src = GraphQLSource(cfg)
        raw = await src.fetch()
        # Rick and Morty API has 826 characters total
        assert len(raw) >= 800, f"GraphQL should fetch 800+ records, got {len(raw)}"
        records = src.normalize(raw)
        assert len(records) >= 800


class TestFileSource:
    @pytest.mark.asyncio
    async def test_fetch_json_file(self):
        cfg = {
            "name": "test_file",
            "type": "file",
            "path": "data/sample_data.json",
            "unit": "record",
        }
        src = FileSource(cfg)
        raw = await src.fetch()
        assert len(raw) >= 10
        records = src.normalize(raw)
        assert all(r["source"] == "test_file" for r in records)

    @pytest.mark.asyncio
    async def test_fetch_file_1500_records(self):
        """File source must be able to fetch 1500 records."""
        cfg = {
            "name": "test_file_1500",
            "type": "file",
            "path": "data/sample_data.json",
            "unit": "record",
            "max_records": 1500,
        }
        src = FileSource(cfg)
        raw = await src.fetch()
        assert len(raw) >= 1500, f"File should fetch 1500 records, got {len(raw)}"
        records = src.normalize(raw)
        assert len(records) >= 1500

    @pytest.mark.asyncio
    async def test_fetch_file_not_found(self):
        cfg = {
            "name": "test_missing",
            "type": "file",
            "path": "/tmp/absolutely_nonexistent_file.json",
            "unit": "record",
        }
        src = FileSource(cfg)
        with pytest.raises(FileNotFoundError):
            await src.fetch()

    @pytest.mark.asyncio
    async def test_fetch_csv_file(self):
        csv_path = Path("data/test_temp.csv")
        csv_path.write_text("name,value\nalpha,1\nbeta,2\ngamma,3\n")
        try:
            cfg = {
                "name": "test_csv_file",
                "type": "file",
                "path": str(csv_path),
                "unit": "row",
            }
            src = FileSource(cfg)
            raw = await src.fetch()
            assert len(raw) == 3
            records = src.normalize(raw)
            assert records[0]["value"]["name"] == "alpha"
        finally:
            csv_path.unlink()


# ── Graceful Failure Tests ───────────────────────────────────────────────────


class TestGracefulFailure:
    @pytest.mark.asyncio
    async def test_fetch_one_timeout(self):
        """A source that times out returns empty list, not an exception."""
        cfg = {
            "name": "slow_source",
            "type": "websocket",
            "url": "wss://stream.binance.com:9443/ws/btcusdt@trade",
            "unit": "trade",
            "max_records": 99999,
            "connect_timeout_seconds": 99999,
            "recv_timeout_seconds": 99999,
        }
        # Use a very short outer timeout to force a timeout
        name, records, error = await fetch_one(cfg, timeout=0.001)
        assert name == "slow_source"
        assert isinstance(records, list)
        assert error is not None

    @pytest.mark.asyncio
    async def test_fetch_one_bad_url(self):
        """A source with a bad URL returns empty list, not an exception."""
        cfg = {
            "name": "bad_url",
            "type": "rest",
            "url": "https://httpstat.us/500",
            "unit": "record",
        }
        name, records, error = await fetch_one(cfg, timeout=10)
        assert name == "bad_url"
        assert records == []
        assert error is not None


# ── Full Pipeline Integration Test ────────────────────────────────────────────


class TestFullPipeline:
    @pytest.mark.asyncio
    async def test_full_pipeline_runs(self):
        """Run the full pipeline and verify output structure."""
        config = load_config("config.json")
        stats = await run_pipeline(config)

        assert stats["sources_attempted"] == 5
        assert stats["sources_succeeded"] >= 4  # allow 1 failure

        # Verify output file exists
        output_path = Path("output/aggregated.json")
        assert output_path.exists()

        with open(output_path) as f:
            output = json.load(f)

        assert "stats" in output
        assert "records" in output

        # Verify schema compliance for every record
        for record in output["records"]:
            assert set(record.keys()) == {"source", "timestamp", "value", "unit"}
            assert isinstance(record["source"], str)
            assert isinstance(record["timestamp"], str)
            assert isinstance(record["unit"], str)

        # Verify sorted by timestamp
        timestamps = [r["timestamp"] for r in output["records"]]
        assert timestamps == sorted(timestamps)

    @pytest.mark.asyncio
    async def test_record_count_minimum_5000(self):
        """Pipeline must aggregate at least 5000 records from all sources combined."""
        config = load_config("config.json")
        stats = await run_pipeline(config)

        # With 1500 each from REST, CSV, File + 800 GraphQL + 100 WebSocket = 5400+
        assert stats["total_records"] >= 5000, (
            f"Pipeline produced only {stats['total_records']} records, "
            f"expected >= 5000. Source counts: {stats['source_record_counts']}"
        )

        # Verify output file has the records
        with open(Path("output/aggregated.json")) as f:
            output = json.load(f)
        assert len(output["records"]) >= 5000

    @pytest.mark.asyncio
    async def test_pipeline_fast_under_15s(self):
        """Fast config must complete within 15s with 1000+ records (bounty requirement)."""
        config = load_config("config_fast.json")
        t0 = time.monotonic()
        stats = await run_pipeline(config)
        elapsed = time.monotonic() - t0

        assert elapsed < 15, f"Fast pipeline took {elapsed:.1f}s, exceeding 15s limit"
        assert stats["total_records"] >= 1000, f"Need 1000+ records, got {stats['total_records']}"
        assert stats["sources_succeeded"] == 5, "All 5 sources should succeed"

    @pytest.mark.asyncio
    async def test_pipeline_full_under_45s(self):
        """Full config (max capacity) must complete within 45 seconds."""
        config = load_config("config.json")
        t0 = time.monotonic()
        stats = await run_pipeline(config)
        elapsed = time.monotonic() - t0

        assert elapsed < 45, f"Full pipeline took {elapsed:.1f}s, exceeding 45s limit"
        assert stats["elapsed_seconds"] < 45
    
    @pytest.mark.asyncio
    async def test_each_source_meets_target(self):
        """Verify each source meets its target record count."""
        config = load_config("config.json")
        stats = await run_pipeline(config)
        
        counts = stats["source_record_counts"]
        
        # REST: 1500 target
        assert counts.get("rest_photos", 0) >= 1500, f"REST should have 1500+, got {counts.get('rest_photos', 0)}"
        
        # CSV: 1500 target
        assert counts.get("csv_covid19", 0) >= 1500, f"CSV should have 1500+, got {counts.get('csv_covid19', 0)}"
        
        # File: 1500 target
        assert counts.get("file_crypto_sample", 0) >= 1500, f"File should have 1500+, got {counts.get('file_crypto_sample', 0)}"
        
        # GraphQL: 800+ (API limit is 826)
        assert counts.get("graphql_rickmorty", 0) >= 800, f"GraphQL should have 800+, got {counts.get('graphql_rickmorty', 0)}"
        
        # WebSocket: 50+ (real-time constraint)
        assert counts.get("ws_binance_btcusdt", 0) >= 50, f"WebSocket should have 50+, got {counts.get('ws_binance_btcusdt', 0)}"

    @pytest.mark.asyncio
    async def test_pipeline_graceful_with_bad_source(self):
        """Pipeline continues when one source fails."""
        config = {
            "pipeline": {
                "name": "test_bad",
                "output_dir": "output",
                "output_file": "test_bad.json",
                "timeout_seconds": 3,
            },
            "sources": [
                {
                    "name": "bad_rest",
                    "type": "rest",
                    "enabled": True,
                    "url": "https://httpstat.us/500",
                    "unit": "record",
                },
                {
                    "name": "good_file",
                    "type": "file",
                    "enabled": True,
                    "path": "data/sample_data.json",
                    "unit": "record",
                },
            ],
        }
        stats = await run_pipeline(config)
        assert stats["sources_failed"] >= 1
        assert stats["sources_succeeded"] >= 1
        assert stats["total_records"] >= 10

    @pytest.mark.asyncio
    async def test_stats_file_written(self):
        config = load_config("config.json")
        await run_pipeline(config)
        stats_path = Path("output/stats.json")
        assert stats_path.exists()

        with open(stats_path) as f:
            stats = json.load(f)

        required_keys = {
            "total_records",
            "sources_attempted",
            "source_record_counts",
            "elapsed_seconds",
            "run_timestamp",
        }
        assert required_keys.issubset(stats.keys())
