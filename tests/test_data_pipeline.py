#!/usr/bin/env python3
"""
Unit and Integration Tests for Multi-Source Data Aggregation & Normalization Pipeline.
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import tempfile
import time
import unittest

import pytest

from data_pipeline import DataPipeline, normalize_timestamp, normalize_value
from mock_server import MockDataServer


class TestDataNormalization(unittest.TestCase):
    def test_normalize_timestamp_unix_seconds(self):
        # 1708992000 = 2024-02-27T00:00:00Z
        res = normalize_timestamp(1708992000)
        self.assertEqual(res, "2024-02-27T00:00:00Z")

    def test_normalize_timestamp_unix_milliseconds(self):
        # 1708992000000 ms
        res = normalize_timestamp(1708992000000)
        self.assertEqual(res, "2024-02-27T00:00:00Z")

    def test_normalize_timestamp_iso_string(self):
        res = normalize_timestamp("2026-02-26T14:30:00Z")
        self.assertEqual(res, "2026-02-26T14:30:00Z")

        res_offset = normalize_timestamp("2026-02-26T16:30:00+02:00")
        self.assertEqual(res_offset, "2026-02-26T14:30:00Z")

        res_space = normalize_timestamp("2026-02-26 14:30:00")
        self.assertEqual(res_space, "2026-02-26T14:30:00Z")

    def test_normalize_value(self):
        self.assertEqual(normalize_value(123), 123)
        self.assertEqual(normalize_value(123.45), 123.45)
        self.assertEqual(normalize_value("45000.5"), 45000.5)
        self.assertEqual(normalize_value("1,200.50"), 1200.50)
        self.assertEqual(normalize_value("inactive"), "inactive")


class TestPipelineEndToEnd(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = MockDataServer()
        cls.server.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.temp_dir.name)

        # Create temporary CSV file
        self.csv_file = self.temp_path / "test.csv"
        self.csv_file.write_text(
            "time,val,unit\n"
            "2026-02-26T01:00:00Z,100.0,USD\n"
            "2026-02-26T01:30:00Z,105.5,USD\n",
            encoding="utf-8",
        )

        # Create temporary JSON file
        self.json_file = self.temp_path / "test.json"
        self.json_file.write_text(
            json.dumps([
                {"timestamp": "2026-02-26T02:00:00Z", "price": 200.0, "unit": "USD"},
                {"timestamp": "2026-02-26T02:30:00Z", "price": 205.0, "unit": "USD"},
            ]),
            encoding="utf-8",
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_all_five_sources_concurrent_fetch(self):
        config = {
            "timeout": 5.0,
            "sources": [
                {
                    "name": "src_rest",
                    "type": "rest",
                    "url": self.server.rest_url,
                    "mapping": {"timestamp": "timestamp", "value": "price"},
                    "unit": "USD",
                },
                {
                    "name": "src_csv_remote",
                    "type": "csv",
                    "url": self.server.csv_url,
                    "mapping": {"timestamp": "ts", "value": "value"},
                    "unit": "USD",
                },
                {
                    "name": "src_graphql",
                    "type": "graphql",
                    "url": self.server.graphql_url,
                    "query": "{ marketData { time amount currency } }",
                    "data_path": "marketData",
                    "mapping": {"timestamp": "time", "value": "amount", "unit": "currency"},
                    "unit": "ETH",
                },
                {
                    "name": "src_ws",
                    "type": "websocket",
                    "url": self.server.ws_url,
                    "max_messages": 3,
                    "unit": "USD",
                },
                {
                    "name": "src_file_csv",
                    "type": "file",
                    "path": str(self.csv_file),
                    "mapping": {"timestamp": "time", "value": "val"},
                    "unit": "USD",
                },
                {
                    "name": "src_file_json",
                    "type": "file",
                    "path": str(self.json_file),
                    "mapping": {"timestamp": "timestamp", "value": "price"},
                    "unit": "USD",
                },
            ],
        }

        output_file = self.temp_path / "output.json"
        pipeline = DataPipeline(config=config)
        res = asyncio.run(pipeline.run(output_path=str(output_file)))

        records = res["records"]
        stats = res["stats"]

        # Validate counts
        self.assertGreaterEqual(stats["total_records"], 12)
        self.assertEqual(stats["failed_sources"], 0)
        self.assertEqual(stats["successful_sources"], 6)

        # Validate schema for all items
        for record in records:
            self.assertIn("source", record)
            self.assertIn("timestamp", record)
            self.assertIn("value", record)
            self.assertIn("unit", record)
            self.assertTrue(record["timestamp"].endswith("Z"))
            self.assertIsInstance(record["value"], (int, float))

        # Validate chronological order
        timestamps = [r["timestamp"] for r in records]
        self.assertEqual(timestamps, sorted(timestamps))

        # Validate output file persisted
        self.assertTrue(output_file.exists())
        saved_data = json.loads(output_file.read_text(encoding="utf-8"))
        self.assertEqual(len(saved_data["records"]), len(records))

    def test_fault_tolerance_and_timeouts(self):
        config = {
            "timeout": 0.5,
            "max_retries": 1,
            "retry_backoff": 0.05,
            "sources": [
                {
                    "name": "src_healthy",
                    "type": "file",
                    "path": str(self.csv_file),
                    "mapping": {"timestamp": "time", "value": "val"},
                },
                {
                    "name": "src_slow_timeout",
                    "type": "rest",
                    "url": self.server.slow_url,
                    "timeout": 0.2,
                },
                {
                    "name": "src_http_500_error",
                    "type": "rest",
                    "url": self.server.error_url,
                },
            ],
        }

        pipeline = DataPipeline(config=config)
        res = asyncio.run(pipeline.run())

        stats = res["stats"]
        # Pipeline should continue and aggregate healthy source
        self.assertGreater(stats["total_records"], 0)
        self.assertEqual(stats["successful_sources"], 1)
        self.assertEqual(stats["failed_sources"], 2)
        self.assertEqual(len(stats["error_details"]), 2)

    def test_incremental_updates(self):
        state_file = self.temp_path / "incremental_state.json"
        config = {
            "incremental_store": str(state_file),
            "sources": [
                {
                    "name": "src_file",
                    "type": "file",
                    "path": str(self.csv_file),
                    "mapping": {"timestamp": "time", "value": "val"},
                }
            ],
        }

        # First run: should yield 2 records
        p1 = DataPipeline(config=config)
        res1 = asyncio.run(p1.run())
        self.assertEqual(len(res1["records"]), 2)
        self.assertTrue(state_file.exists())

        # Second run without new records: should yield 0 records
        p2 = DataPipeline(config=config)
        res2 = asyncio.run(p2.run())
        self.assertEqual(len(res2["records"]), 0)

        # Append new record with newer timestamp
        with open(self.csv_file, "a", encoding="utf-8") as f:
            f.write("2026-02-26T02:00:00Z,110.0,USD\n")

        # Third run: should yield only the newly added record
        p3 = DataPipeline(config=config)
        res3 = asyncio.run(p3.run())
        self.assertEqual(len(res3["records"]), 1)
        self.assertEqual(res3["records"][0]["timestamp"], "2026-02-26T02:00:00Z")

    def test_performance_benchmark_1000_records(self):
        # Generate 1500 records
        large_json = self.temp_path / "large_data.json"
        large_records = [
            {"ts": 1709000000 + i * 10, "val": 100.0 + (i % 50), "currency": "USD"}
            for i in range(1500)
        ]
        large_json.write_text(json.dumps(large_records), encoding="utf-8")

        config = {
            "timeout": 15.0,
            "sources": [
                {
                    "name": "large_file_source",
                    "type": "file",
                    "path": str(large_json),
                    "mapping": {"timestamp": "ts", "value": "val", "unit": "currency"},
                }
            ],
        }

        pipeline = DataPipeline(config=config)
        start = time.perf_counter()
        res = asyncio.run(pipeline.run())
        duration = time.perf_counter() - start

        self.assertEqual(res["stats"]["total_records"], 1500)
        # Bounty requirement: full pipeline <15s for 1000+ records
        self.assertLess(duration, 15.0)
        self.assertLess(res["stats"]["duration_seconds"], 15.0)


if __name__ == "__main__":
    unittest.main()
