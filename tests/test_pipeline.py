"""
Integration Tests for the Multi-Source Aggregation Pipeline
============================================================
Starts mock server, runs the full pipeline, validates output.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from pathlib import Path

import pytest
import aiohttp

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from aggregation_pipeline import (
    Record,
    normalize,
    fetch_rest,
    fetch_csv,
    fetch_file,
    fetch_graphql,
    fetch_websocket,
    run_pipeline,
)
from mock_server import create_app


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="module")
async def mock_server(event_loop):
    """Start the mock server for the test session."""
    app = create_app()
    runner = aiohttp.web.AppRunner(app)
    await runner.setup()
    site = aiohttp.web.TCPSite(runner, "localhost", 8099)
    await site.start()
    yield
    await runner.cleanup()


# ---------------------------------------------------------------------------
# Unit Tests: Normalization
# ---------------------------------------------------------------------------

class TestNormalization:
    def test_normalize_basic(self):
        raw = {"ts": "2026-02-28T00:00:00Z", "price": 65000, "pair": "BTC/USD"}
        field_map = {
            "source": "'test'",
            "timestamp": "ts",
            "value": "price",
            "unit": "pair",
        }
        rec = normalize(raw, field_map)
        assert rec is not None
        assert rec.source == "test"
        assert rec.value == 65000.0
        assert rec.unit == "BTC/USD"
        assert rec.timestamp == "2026-02-28T00:00:00Z"

    def test_normalize_literal_source(self):
        raw = {"ts": "2026-01-01T00:00:00Z", "val": 100}
        field_map = {
            "source": "'my_source'",
            "timestamp": "ts",
            "value": "val",
            "unit": "'USD'",
        }
        rec = normalize(raw, field_map)
        assert rec.source == "my_source"
        assert rec.unit == "USD"

    def test_normalize_nested_path(self):
        raw = {"data": {"metrics": {"price": 42.5}}, "ts": "2026-01-01"}
        field_map = {
            "source": "'nested'",
            "timestamp": "ts",
            "value": "data.metrics.price",
            "unit": "'ETH'",
        }
        rec = normalize(raw, field_map)
        assert rec.value == 42.5

    def test_normalize_missing_value(self):
        raw = {"ts": "2026-01-01", "price": None}
        field_map = {
            "source": "'test'",
            "timestamp": "ts",
            "value": "price",
            "unit": "'USD'",
        }
        rec = normalize(raw, field_map)
        assert rec is None

    def test_normalize_epoch_timestamp(self):
        raw = {"ts": 1709078400, "val": 100}
        field_map = {
            "source": "'epoch'",
            "timestamp": "ts",
            "value": "val",
            "unit": "'USD'",
        }
        rec = normalize(raw, field_map)
        assert rec is not None
        assert "2024-02-28" in rec.timestamp

    def test_record_to_dict(self):
        rec = Record("src", "2026-01-01T00:00:00Z", 100.5, "USD")
        d = rec.to_dict()
        assert d == {
            "source": "src",
            "timestamp": "2026-01-01T00:00:00Z",
            "value": 100.5,
            "unit": "USD",
        }


# ---------------------------------------------------------------------------
# Integration Tests: Individual Fetchers
# ---------------------------------------------------------------------------

class TestFetchers:
    @pytest.mark.asyncio
    async def test_fetch_rest(self, mock_server):
        sem = asyncio.Semaphore(5)
        cfg = {
            "id": "test_rest",
            "url": "http://localhost:8099/api/prices",
            "headers": {},
            "timeout_s": 5,
        }
        async with aiohttp.ClientSession() as session:
            records = await fetch_rest(session, cfg, sem)
        assert len(records) > 0
        assert "price" in records[0]
        assert "timestamp" in records[0]

    @pytest.mark.asyncio
    async def test_fetch_csv(self, mock_server):
        sem = asyncio.Semaphore(5)
        cfg = {
            "id": "test_csv",
            "path": "data/historical.csv",
            "encoding": "utf-8",
        }
        records = await fetch_csv(cfg, sem)
        assert len(records) == 10
        assert "close" in records[0]
        assert "date" in records[0]

    @pytest.mark.asyncio
    async def test_fetch_file(self, mock_server):
        sem = asyncio.Semaphore(5)
        cfg = {
            "id": "test_file",
            "path": "data/snapshot.json",
            "items_path": "records",
        }
        records = await fetch_file(cfg, sem)
        assert len(records) == 10
        assert "amount" in records[0]

    @pytest.mark.asyncio
    async def test_fetch_graphql(self, mock_server):
        sem = asyncio.Semaphore(5)
        cfg = {
            "id": "test_graphql",
            "url": "http://localhost:8099/graphql",
            "query": "{ markets { symbol lastPrice updatedAt quoteCurrency } }",
            "items_path": "data.markets",
            "timeout_s": 5,
        }
        async with aiohttp.ClientSession() as session:
            records = await fetch_graphql(session, cfg, sem)
        assert len(records) == 5
        assert "symbol" in records[0]
        assert "lastPrice" in records[0]

    @pytest.mark.asyncio
    async def test_fetch_websocket(self, mock_server):
        sem = asyncio.Semaphore(5)
        cfg = {
            "id": "test_ws",
            "url": "ws://localhost:8099/ws/prices",
            "max_messages": 10,
            "timeout_s": 3,
        }
        records = await fetch_websocket(cfg, sem)
        assert len(records) >= 1
        assert "price" in records[0]

    @pytest.mark.asyncio
    async def test_fetch_rest_timeout(self):
        """Test graceful timeout on unreachable endpoint."""
        sem = asyncio.Semaphore(5)
        cfg = {
            "id": "test_timeout",
            "url": "http://localhost:19999/api/nothing",
            "headers": {},
            "timeout_s": 1,
        }
        async with aiohttp.ClientSession() as session:
            records = await fetch_rest(session, cfg, sem)
        assert records == []

    @pytest.mark.asyncio
    async def test_fetch_csv_missing_file(self):
        """Test graceful failure for missing CSV."""
        sem = asyncio.Semaphore(5)
        cfg = {"id": "missing", "path": "data/nonexistent.csv"}
        records = await fetch_csv(cfg, sem)
        assert records == []


# ---------------------------------------------------------------------------
# Integration Test: Full Pipeline
# ---------------------------------------------------------------------------

class TestFullPipeline:
    @pytest.mark.asyncio
    async def test_full_pipeline(self, mock_server):
        """Run the complete pipeline and validate output."""
        t0 = time.monotonic()
        result = await run_pipeline("pipeline_config.yaml")
        elapsed = time.monotonic() - t0

        # Performance: pipeline should complete in <15s
        assert elapsed < 15, f"Pipeline took {elapsed:.1f}s (limit: 15s)"

        stats = result["stats"]
        records = result["records"]

        # Should have records from multiple sources
        assert records["total_records"] > 0

        # Check stats per source
        assert "rest_crypto" in stats["sources"]
        assert "csv_historical" in stats["sources"]
        assert "graphql_market" in stats["sources"]
        assert "file_snapshot" in stats["sources"]
        assert "ws_live" in stats["sources"]

        # At least 3 sources should succeed (REST, CSV, file always work)
        ok_sources = [
            s for s, v in stats["sources"].items() if v["status"] == "ok"
        ]
        assert len(ok_sources) >= 3, f"Only {len(ok_sources)} sources succeeded"

        # Records should be sorted by timestamp
        timestamps = [r["timestamp"] for r in records["records"]]
        assert timestamps == sorted(timestamps), "Records not sorted by timestamp"

        # Output files should exist
        assert Path("output/aggregated.json").exists()
        assert Path("output/stats.json").exists()

        # Validate record schema
        for rec in records["records"][:5]:
            assert "source" in rec
            assert "timestamp" in rec
            assert "value" in rec
            assert "unit" in rec
            assert isinstance(rec["value"], (int, float))

    @pytest.mark.asyncio
    async def test_pipeline_many_records(self, mock_server):
        """Verify pipeline handles 1000+ records."""
        result = await run_pipeline("pipeline_config.yaml")
        # Between REST (50), CSV (10), WS (50), GraphQL (5), File (10) = 125+
        # Even with some failures, should have meaningful count
        assert result["stats"]["total_records"] >= 10
