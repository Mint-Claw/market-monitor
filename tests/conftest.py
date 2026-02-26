"""Shared fixtures for pipeline tests."""
import asyncio
import json
import random
import threading
from pathlib import Path

import pytest
from aiohttp import web
from tests.mock_server import create_app, FILE_RECORDS


def _run_server(app, port, ready_event):
    """Run the server in a background thread with its own event loop."""
    loop = asyncio.new_event_loop()

    async def _start():
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", port)
        await site.start()
        ready_event.set()
        # Keep running until thread is stopped
        try:
            while True:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        finally:
            await runner.cleanup()

    loop.run_until_complete(_start())


@pytest.fixture(scope="session")
def mock_server():
    port = random.randint(19100, 19999)
    app = create_app()
    ready = threading.Event()
    t = threading.Thread(target=_run_server, args=(app, port, ready), daemon=True)
    t.start()
    ready.wait(timeout=5)
    yield f"http://127.0.0.1:{port}"


@pytest.fixture
def file_source(tmp_path):
    p = tmp_path / "data.json"
    p.write_text(json.dumps(FILE_RECORDS))
    return str(p)


@pytest.fixture
def config(mock_server, file_source):
    return {
        "sources": [
            {"name": "rest-source", "kind": "rest", "url": f"{mock_server}/api/rest?limit=200"},
            {"name": "csv-source", "kind": "csv", "url": f"{mock_server}/api/csv"},
            {"name": "ws-source", "kind": "websocket",
             "url": f"{mock_server.replace('http', 'ws')}/api/ws",
             "params": {"duration_seconds": 2}, "timeout": 10},
            {"name": "graphql-source", "kind": "graphql", "url": f"{mock_server}/api/graphql",
             "params": {"query": "{ records { timestamp value unit id } }"}},
            {"name": "file-source", "kind": "file", "path": file_source},
        ]
    }
