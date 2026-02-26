#!/usr/bin/env python3
"""Quick benchmark for bounty acceptance check (<15s for 1000+ records)."""

import asyncio
import json
import tempfile
import time

from bounty_pipeline import run_pipeline
from mock_server import start_http_server, start_ws_server


def main():
    http_port = 8865
    ws_port = 8866
    srv = start_http_server(http_port)
    start_ws_server(ws_port)

    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump([{"timestamp": i + 1000, "value": i, "unit": "usd"} for i in range(996)], f)
        fp = f.name

    cfg = [
        {"name": "ws", "type": "websocket", "url": f"ws://127.0.0.1:{ws_port}/stream"},
        {"name": "csv", "type": "csv", "url": f"http://127.0.0.1:{http_port}/csv"},
        {"name": "rest", "type": "rest", "url": f"http://127.0.0.1:{http_port}/rest"},
        {"name": "graphql", "type": "graphql", "url": f"http://127.0.0.1:{http_port}/graphql", "query": "{ readings { timestamp value unit } }"},
        {"name": "file", "type": "file", "path": fp},
    ]

    start = time.time()
    out = asyncio.run(run_pipeline(cfg))
    elapsed = time.time() - start

    srv.shutdown()

    print(json.dumps({
        "elapsed_seconds": round(elapsed, 4),
        "total_records": out["stats"]["total_records"],
        "passes_under_15s": elapsed < 15,
    }, indent=2))


if __name__ == "__main__":
    main()
