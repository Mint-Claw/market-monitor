"""Integration tests for the multi-source aggregation pipeline."""

import asyncio
import csv
import json
import multiprocessing
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest
import yaml
from aiohttp import web

from aggregation.mock_server import create_app
from aggregation.pipeline import run_pipeline, load_config
from aggregation.normalizer import normalize_record


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run_mock_server(port: int, ready_event):
    """Run mock server in a separate process."""
    app = create_app()
    runner = web.AppRunner(app)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(runner.setup())
    site = web.TCPSite(runner, "127.0.0.1", port)
    loop.run_until_complete(site.start())
    ready_event.set()
    loop.run_forever()


def _create_test_data(base: Path):
    """Create CSV and JSON test data files."""
    # CSV file — 200 records
    csv_path = base / "historical.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["timestamp", "value", "unit"])
        writer.writeheader()
        t0 = datetime(2026, 1, 4, tzinfo=timezone.utc)
        for i in range(200):
            ts = t0 + timedelta(minutes=i * 5)
            writer.writerow({
                "timestamp": ts.isoformat(),
                "value": round(40000 + i * 1.5, 2),
                "unit": "USD",
            })

    # JSON file — 200 records
    json_path = base / "snapshot.json"
    t1 = datetime(2026, 1, 5, tzinfo=timezone.utc)
    records = []
    for i in range(200):
        ts = t1 + timedelta(minutes=i * 5)
        records.append({
            "timestamp": ts.isoformat(),
            "value": round(39000 + i * 2.0, 2),
            "unit": "USD",
        })
    json_path.write_text(json.dumps(records))
    return csv_path, json_path


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

MOCK_PORT = 19753


@pytest.fixture(scope="session")
def test_data_dir(tmp_path_factory):
    base = tmp_path_factory.mktemp("data")
    _create_test_data(base)
    return base


@pytest.fixture(scope="session")
def mock_server():
    ready = multiprocessing.Event()
    proc = multiprocessing.Process(
        target=_run_mock_server, args=(MOCK_PORT, ready), daemon=True
    )
    proc.start()
    ready.wait(timeout=10)
    time.sleep(0.3)  # extra settle time
    yield f"http://127.0.0.1:{MOCK_PORT}"
    proc.terminate()
    proc.join(timeout=5)


@pytest.fixture()
def pipeline_config(mock_server, test_data_dir, tmp_path):
    output_path = str(tmp_path / "merged.json")
    config = {
        "sources": [
            {
                "name": "rest_market_data",
                "type": "rest",
                "url": f"{mock_server}/api/market",
                "timeout": 10,
            },
            {
                "name": "csv_historical",
                "type": "csv",
                "path": str(test_data_dir / "historical.csv"),
            },
            {
                "name": "ws_live_feed",
                "type": "websocket",
                "url": f"ws://127.0.0.1:{MOCK_PORT}/ws/feed",
                "timeout": 5,
                "max_messages": 500,
            },
            {
                "name": "graphql_analytics",
                "type": "graphql",
                "url": f"{mock_server}/graphql",
                "timeout": 10,
                "query": "{ analytics { source timestamp value unit } }",
            },
            {
                "name": "json_file_snapshot",
                "type": "file",
                "path": str(test_data_dir / "snapshot.json"),
            },
        ],
        "pipeline": {
            "global_timeout": 15,
            "output_path": output_path,
            "sort_by": "timestamp",
            "sort_order": "asc",
        },
    }
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(yaml.dump(config))
    return str(cfg_path)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestNormalizer:
    def test_normalize_basic(self):
        raw = {"timestamp": "2026-01-01T00:00:00Z", "value": 42000.5, "unit": "USD"}
        rec = normalize_record(raw, "test")
        assert rec is not None
        assert rec["source"] == "test"
        assert rec["value"] == 42000.5
        assert rec["unit"] == "USD"

    def test_normalize_unix_epoch(self):
        raw = {"ts": 1767225600, "price": 41000, "currency": "BTC"}
        rec = normalize_record(raw, "epoch_src")
        assert rec is not None
        assert rec["value"] == 41000.0
        assert rec["unit"] == "BTC"

    def test_normalize_missing_value(self):
        raw = {"timestamp": "2026-01-01T00:00:00Z"}
        rec = normalize_record(raw, "bad")
        assert rec is None

    def test_normalize_default_unit(self):
        raw = {"timestamp": "2026-01-01T00:00:00Z", "value": 100}
        rec = normalize_record(raw, "no_unit")
        assert rec is not None
        assert rec["unit"] == "USD"


class TestPipeline:
    def test_full_pipeline(self, pipeline_config):
        """All 5 sources, 1000+ records, under 15s."""
        result = asyncio.run(run_pipeline(pipeline_config))
        stats = result["stats"]

        # Must have records from all 5 sources
        assert len(stats["sources"]) == 5
        for name, info in stats["sources"].items():
            assert info["records"] > 0, f"{name} returned 0 records"

        # Total > 1000
        assert stats["total_records"] >= 1000

        # Under 15 seconds
        assert stats["elapsed_seconds"] < 15

        # Records are sorted by timestamp ascending
        records = result["records"]
        timestamps = [r["timestamp"] for r in records]
        assert timestamps == sorted(timestamps)

    def test_output_file_written(self, pipeline_config):
        """Verify JSON output file is created."""
        result = asyncio.run(run_pipeline(pipeline_config))
        config = load_config(pipeline_config)
        out_path = Path(config["pipeline"]["output_path"])
        assert out_path.exists()
        data = json.loads(out_path.read_text())
        assert len(data) == result["stats"]["total_records"]

    def test_graceful_failure(self, tmp_path, test_data_dir):
        """Pipeline handles a bad source gracefully."""
        config = {
            "sources": [
                {
                    "name": "bad_rest",
                    "type": "rest",
                    "url": "http://127.0.0.1:1/nope",
                    "timeout": 2,
                },
                {
                    "name": "good_file",
                    "type": "file",
                    "path": str(test_data_dir / "snapshot.json"),
                },
            ],
            "pipeline": {
                "global_timeout": 10,
                "output_path": str(tmp_path / "partial.json"),
                "sort_by": "timestamp",
                "sort_order": "asc",
            },
        }
        cfg_path = tmp_path / "fail_config.yaml"
        cfg_path.write_text(yaml.dump(config))

        result = asyncio.run(run_pipeline(str(cfg_path)))
        stats = result["stats"]

        # bad_rest should have an error
        assert stats["sources"]["bad_rest"]["records"] == 0
        assert "error" in stats["sources"]["bad_rest"]

        # good_file should succeed
        assert stats["sources"]["good_file"]["records"] == 200
        assert stats["total_records"] == 200
