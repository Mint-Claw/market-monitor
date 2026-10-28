#!/usr/bin/env python3
"""
Mock Server for Multi-Source Data Aggregation Pipeline.
Provides mock HTTP endpoints (REST, CSV, GraphQL, Slow, Error)
and WebSocket streaming server.
"""

from __future__ import annotations

import argparse
import asyncio
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
from threading import Thread
import time
from typing import Any, Dict, List, Optional
import websockets

logger = logging.getLogger("mock_server")


class _MockHTTPHandler(BaseHTTPRequestHandler):
    rest_data: List[Dict[str, Any]] = [
        {"timestamp": "2026-02-26T10:00:00Z", "price": 50100.5, "unit": "USD"},
        {"timestamp": "2026-02-26T10:05:00Z", "price": 50250.0, "unit": "USD"},
        {"timestamp": "2026-02-26T10:10:00Z", "price": 50180.25, "unit": "USD"},
    ]

    csv_data: List[Dict[str, Any]] = [
        {"ts": "2026-02-26T09:00:00Z", "value": 49950.0, "unit": "USD"},
        {"ts": "2026-02-26T09:30:00Z", "value": 50020.5, "unit": "USD"},
    ]

    graphql_data: List[Dict[str, Any]] = [
        {"time": "2026-02-26T08:15:00Z", "amount": 105.4, "currency": "ETH"},
        {"time": "2026-02-26T08:45:00Z", "amount": 210.8, "currency": "ETH"},
    ]

    slow_delay: float = 3.0

    def _write_response(self, code: int, content_type: str, body: bytes):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802
        if self.path in ("/rest", "/api/rest"):
            body = json.dumps(self.rest_data).encode("utf-8")
            self._write_response(200, "application/json", body)
            return

        if self.path in ("/csv", "/api/csv"):
            lines = ["ts,value,unit"]
            for row in self.csv_data:
                lines.append(f"{row['ts']},{row['value']},{row['unit']}")
            body = "\n".join(lines).encode("utf-8")
            self._write_response(200, "text/csv", body)
            return

        if self.path in ("/slow", "/api/slow"):
            time.sleep(self.slow_delay)
            body = json.dumps([{"timestamp": "2026-02-26T12:00:00Z", "value": 1.0}]).encode("utf-8")
            self._write_response(200, "application/json", body)
            return

        if self.path in ("/error", "/api/error"):
            self._write_response(500, "text/plain", b"Internal Server Error")
            return

        self._write_response(404, "text/plain", b"Not Found")

    def do_POST(self):  # noqa: N802
        if self.path in ("/graphql", "/api/graphql"):
            content_length = int(self.headers.get("Content-Length", 0))
            _ = self.rfile.read(content_length)
            resp = {
                "data": {
                    "marketData": self.graphql_data
                }
            }
            body = json.dumps(resp).encode("utf-8")
            self._write_response(200, "application/json", body)
            return

        self._write_response(404, "text/plain", b"Not Found")

    def log_message(self, format, *args):  # noqa: A003
        pass


class MockDataServer:
    def __init__(
        self,
        http_host: str = "127.0.0.1",
        http_port: int = 0,
        ws_host: str = "127.0.0.1",
        ws_port: int = 0,
        ws_messages: Optional[List[Dict[str, Any]]] = None,
    ):
        self.http_host = http_host
        self.http_port = http_port
        self.ws_host = ws_host
        self.ws_port = ws_port

        self.ws_messages = ws_messages or [
            {"timestamp": "2026-02-26T11:00:00Z", "value": 50300.0, "unit": "USD"},
            {"timestamp": "2026-02-26T11:01:00Z", "value": 50320.5, "unit": "USD"},
            {"timestamp": "2026-02-26T11:02:00Z", "value": 50290.0, "unit": "USD"},
        ]

        self._http_server: Optional[ThreadingHTTPServer] = None
        self._http_thread: Optional[Thread] = None

        self._ws_loop: Optional[asyncio.AbstractEventLoop] = None
        self._ws_server = None
        self._ws_thread: Optional[Thread] = None
        self._ws_stop_event = None

    def set_data(
        self,
        rest_data: Optional[List[Dict[str, Any]]] = None,
        csv_data: Optional[List[Dict[str, Any]]] = None,
        graphql_data: Optional[List[Dict[str, Any]]] = None,
        ws_messages: Optional[List[Dict[str, Any]]] = None,
        slow_delay: Optional[float] = None,
    ):
        if rest_data is not None:
            _MockHTTPHandler.rest_data = rest_data
        if csv_data is not None:
            _MockHTTPHandler.csv_data = csv_data
        if graphql_data is not None:
            _MockHTTPHandler.graphql_data = graphql_data
        if ws_messages is not None:
            self.ws_messages = ws_messages
        if slow_delay is not None:
            _MockHTTPHandler.slow_delay = slow_delay

    def start(self):
        # 1. Start HTTP server
        self._http_server = ThreadingHTTPServer((self.http_host, self.http_port), _MockHTTPHandler)
        self.http_port = self._http_server.server_port
        self._http_thread = Thread(target=self._http_server.serve_forever, daemon=True)
        self._http_thread.start()

        # 2. Start WebSocket server in separate thread
        ready_event = asyncio.Event()

        def run_ws():
            self._ws_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._ws_loop)

            async def ws_handler(websocket):
                try:
                    for msg in self.ws_messages:
                        await websocket.send(json.dumps(msg))
                        await asyncio.sleep(0.01)
                except Exception:
                    pass

            async def main_ws():
                server = await websockets.serve(ws_handler, self.ws_host, self.ws_port)
                self._ws_server = server
                sockets = server.sockets
                if sockets:
                    self.ws_port = sockets[0].getsockname()[1]
                ready_event.set()
                await asyncio.Future()  # run forever

            try:
                self._ws_loop.run_until_complete(main_ws())
            except Exception:
                pass

        self._ws_thread = Thread(target=run_ws, daemon=True)
        self._ws_thread.start()

        # Wait until ws is bound
        timeout = 5.0
        start = time.time()
        while self.ws_port == 0 and time.time() - start < timeout:
            time.sleep(0.02)

    def stop(self):
        if self._http_server:
            self._http_server.shutdown()
            self._http_server.server_close()
        if self._ws_loop and self._ws_server:
            self._ws_loop.call_soon_threadsafe(self._ws_server.close)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    @property
    def rest_url(self) -> str:
        return f"http://{self.http_host}:{self.http_port}/rest"

    @property
    def csv_url(self) -> str:
        return f"http://{self.http_host}:{self.http_port}/csv"

    @property
    def graphql_url(self) -> str:
        return f"http://{self.http_host}:{self.http_port}/graphql"

    @property
    def slow_url(self) -> str:
        return f"http://{self.http_host}:{self.http_port}/slow"

    @property
    def error_url(self) -> str:
        return f"http://{self.http_host}:{self.http_port}/error"

    @property
    def ws_url(self) -> str:
        return f"ws://{self.ws_host}:{self.ws_port}/ws"


def main():
    parser = argparse.ArgumentParser(description="Mock Server for Data Pipeline Testing")
    parser.add_argument("--http-port", type=int, default=8080, help="HTTP server port")
    parser.add_argument("--ws-port", type=int, default=8765, help="WebSocket server port")
    args = parser.parse_args()

    server = MockDataServer(http_port=args.http_port, ws_port=args.ws_port)
    server.start()
    print(f"Mock HTTP Server running at http://127.0.0.1:{server.http_port}")
    print(f"  REST:    {server.rest_url}")
    print(f"  CSV:     {server.csv_url}")
    print(f"  GraphQL: {server.graphql_url}")
    print(f"Mock WebSocket Server running at {server.ws_url}")
    print("Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        server.stop()
        print("Server stopped.")


if __name__ == "__main__":
    main()
