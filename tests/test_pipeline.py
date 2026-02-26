"""Integration tests for the multi-source pipeline."""
from __future__ import annotations
import asyncio
import json
import os
import tempfile
import time

import pytest
import pytest_asyncio
from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase, TestServer

from tests.mock_server import create_app
from pipeline.models import SourceConfig
from pipeline.engine import run_pipeline


@pytest.fixture
def file_source(tmp_path):
    """Create a JSON file source with test data."""
    records = [
        {"source": "file_source", "timestamp": f"2026-02-26T{i // 60:02d}:{i % 60:02d}:00Z",
         "value": round(50 + i * 0.3, 2), "unit": "EUR"}
        for i in range(200)
    ]
    p = tmp_path / "data.json"
    p.write_text(json.dumps(records))
    return str(p)


@pytest.fixture
def csv_file_source(tmp_path):
    """Create a CSV file source."""
    lines = ["source,timestamp,value,unit"]
    for i in range(200):
        lines.append(f"csv_file,2026-02-26T{i // 60:02d}:{i % 60:02d}:00Z,{round(70 + i * 0.2, 2)},GBP")
    p = tmp_path / "data.csv"
    p.write_text("\n".join(lines))
    return str(p)


@pytest_asyncio.fixture
async def mock_server():
    """Start the mock aiohttp server."""
    app = create_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 18932)
    await site.start()
    yield "http://127.0.0.1:18932"
    await runner.cleanup()


@pytest.mark.asyncio
async def test_all_five_sources(mock_server, file_source):
    """Test pipeline with all 5 source types concurrently."""
    sources = [
        SourceConfig(name="rest", kind="rest", url=f"{mock_server}/api/rest"),
        SourceConfig(name="csv", kind="csv", url=f"{mock_server}/api/csv"),
        SourceConfig(name="ws", kind="websocket", url=f"{mock_server.replace('http','ws')}/api/ws"),
        SourceConfig(name="gql", kind="graphql", url=f"{mock_server}/api/graphql"),
        SourceConfig(name="file", kind="file", path=file_source),
    ]
    result = await run_pipeline(sources)

    assert result.stats.sources_ok == 5
    assert result.stats.sources_failed == 0
    assert result.stats.total_records == 1000  # 200 * 5
    # Verify sorted
    timestamps = [r.timestamp for r in result.records]
    assert timestamps == sorted(timestamps)


@pytest.mark.asyncio
async def test_performance_1000_records(mock_server, file_source):
    """Pipeline must handle 1000+ records in <15s."""
    sources = [
        SourceConfig(name="rest", kind="rest", url=f"{mock_server}/api/rest"),
        SourceConfig(name="csv", kind="csv", url=f"{mock_server}/api/csv"),
        SourceConfig(name="ws", kind="websocket", url=f"{mock_server.replace('http','ws')}/api/ws"),
        SourceConfig(name="gql", kind="graphql", url=f"{mock_server}/api/graphql"),
        SourceConfig(name="file", kind="file", path=file_source),
    ]
    t0 = time.monotonic()
    result = await run_pipeline(sources)
    elapsed = time.monotonic() - t0

    assert result.stats.total_records >= 1000
    assert elapsed < 15.0, f"Pipeline took {elapsed:.2f}s, exceeds 15s limit"


@pytest.mark.asyncio
async def test_graceful_failure():
    """Pipeline should handle source failures gracefully."""
    sources = [
        SourceConfig(name="bad", kind="rest", url="http://127.0.0.1:19999/nope", timeout=2),
        SourceConfig(name="also_bad", kind="graphql", url="http://127.0.0.1:19999/nope", timeout=2),
    ]
    result = await run_pipeline(sources)

    assert result.stats.sources_failed == 2
    assert result.stats.sources_ok == 0
    assert result.stats.total_records == 0


@pytest.mark.asyncio
async def test_json_output(mock_server, file_source, tmp_path):
    """Test JSON file output."""
    out = str(tmp_path / "output.json")
    sources = [
        SourceConfig(name="rest", kind="rest", url=f"{mock_server}/api/rest"),
        SourceConfig(name="file", kind="file", path=file_source),
    ]
    result = await run_pipeline(sources, output_path=out)

    with open(out) as f:
        data = json.load(f)
    assert len(data["records"]) == 400
    assert data["stats"]["sources_ok"] == 2


@pytest.mark.asyncio
async def test_incremental_updates(mock_server):
    """Bonus: incremental mode only returns new records."""
    sources = [
        SourceConfig(name="rest", kind="rest", url=f"{mock_server}/api/rest"),
    ]
    state: dict[str, str] = {}

    # First run: get all
    r1 = await run_pipeline(sources, incremental_state=state)
    assert r1.stats.total_records == 200
    assert "rest" in state

    # Second run: same data, should get 0 new (all <= last seen)
    r2 = await run_pipeline(sources, incremental_state=state)
    assert r2.stats.total_records == 0


@pytest.mark.asyncio
async def test_custom_field_mapping(mock_server):
    """Bonus: custom field mapping renames fields."""
    sources = [
        SourceConfig(
            name="mapped",
            kind="rest",
            url=f"{mock_server}/api/rest",
            field_mapping={"source": "source", "timestamp": "timestamp", "value": "value", "unit": "unit"},
        ),
    ]
    result = await run_pipeline(sources)
    assert result.stats.total_records == 200
    assert all(r.source == "rest_source" for r in result.records)


@pytest.mark.asyncio
async def test_record_schema(mock_server):
    """All records must have {source, timestamp, value, unit}."""
    sources = [
        SourceConfig(name="rest", kind="rest", url=f"{mock_server}/api/rest"),
    ]
    result = await run_pipeline(sources)
    for r in result.records:
        assert isinstance(r.source, str) and r.source
        assert isinstance(r.timestamp, str) and r.timestamp
        assert isinstance(r.value, float)
        assert isinstance(r.unit, str) and r.unit
