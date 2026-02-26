#!/usr/bin/env python3
import asyncio
import json
import os
import tempfile
import time
import unittest

from bounty_pipeline import run_pipeline
from mock_server import start_http_server, start_ws_server


class PipelineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.http_port = 8765
        cls.ws_port = 8766
        cls.http = start_http_server(cls.http_port)
        start_ws_server(cls.ws_port)
        cls.tmp = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
        json.dump([{"timestamp": 5, "value": 50, "unit": "usd"}], cls.tmp)
        cls.tmp.close()
        time.sleep(0.2)

    @classmethod
    def tearDownClass(cls):
        cls.http.shutdown()
        os.unlink(cls.tmp.name)

    def test_pipeline_end_to_end(self):
        cfg = [
            {"name": "ws", "type": "websocket", "url": f"ws://127.0.0.1:{self.ws_port}/stream"},
            {"name": "csv", "type": "csv", "url": f"http://127.0.0.1:{self.http_port}/csv"},
            {"name": "rest", "type": "rest", "url": f"http://127.0.0.1:{self.http_port}/rest"},
            {"name": "graphql", "type": "graphql", "url": f"http://127.0.0.1:{self.http_port}/graphql", "query": "{ readings { timestamp value unit } }"},
            {"name": "file", "type": "file", "path": self.tmp.name},
        ]
        out = asyncio.run(run_pipeline(cfg))
        self.assertEqual(out["stats"]["source_count"], 5)
        self.assertEqual(out["stats"]["failed_sources"], 0)
        self.assertEqual(len(out["records"]), 5)
        timestamps = [r["timestamp"] for r in out["records"]]
        self.assertEqual(timestamps, sorted(timestamps))


if __name__ == "__main__":
    unittest.main()
