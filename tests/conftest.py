import json
import subprocess
import sys
import time
import pytest
import urllib.request

from tests.mock_server import FILE_RECORDS

PORT = 18935


@pytest.fixture(scope="session")
def server():
    """Start mock server as subprocess."""
    proc = subprocess.Popen(
        [sys.executable, "-m", "tests.mock_server"],
        cwd="/tmp/market-monitor",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    # Wait for server to be ready
    url = f"http://127.0.0.1:{PORT}"
    for _ in range(30):
        try:
            urllib.request.urlopen(f"{url}/api/rest?limit=1", timeout=1)
            break
        except Exception:
            time.sleep(0.2)
    yield url
    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture
def file_source(tmp_path):
    p = tmp_path / "data.json"
    p.write_text(json.dumps(FILE_RECORDS))
    return str(p)


@pytest.fixture
def config(server, file_source):
    return {
        "sources": [
            {"name": "rest-source", "kind": "rest", "url": f"{server}/api/rest?limit=200"},
            {"name": "csv-source", "kind": "csv", "url": f"{server}/api/csv"},
            {"name": "ws-source", "kind": "websocket",
             "url": f"{server.replace('http', 'ws')}/api/ws",
             "params": {"duration_seconds": 3}},
            {"name": "graphql-source", "kind": "graphql", "url": f"{server}/api/graphql",
             "params": {"query": "{ records { timestamp value unit id } }"}},
            {"name": "file-source", "kind": "file", "path": file_source},
        ]
    }
