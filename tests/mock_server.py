"""Mock server providing all 5 source types for testing."""
from __future__ import annotations

import asyncio
import json
import random
import time
from datetime import datetime, timezone, timedelta
from aiohttp import web


def _generate_records(n: int, source_name: str = "mock") -> list[dict]:
    """Generate N test records."""
    base_time = datetime(2026, 2, 26, 12, 0, 0, tzinfo=timezone.utc)
    records = []
    for i in range(n):
        records.append({
            "timestamp": (base_time + timedelta(seconds=i * 10)).isoformat(),
            "value": round(100 + random.uniform(-10, 10), 4),
            "unit": "USD",
            "id": f"{source_name}-{i}",
        })
    return records


# --- REST ---
async def rest_handler(request: web.Request) -> web.Response:
    n = int(request.query.get("limit", "200"))
    return web.json_response(_generate_records(n, "rest-api"))


# --- CSV ---
async def csv_handler(request: web.Request) -> web.Response:
    records = _generate_records(200, "csv-feed")
    lines = ["timestamp,value,unit,id"]
    for r in records:
        lines.append(f"{r['timestamp']},{r['value']},{r['unit']},{r['id']}")
    return web.Response(text="\n".join(lines), content_type="text/csv")


# --- WebSocket ---
async def ws_handler(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    records = _generate_records(200, "ws-stream")
    # Send in batches of 20
    for i in range(0, len(records), 20):
        batch = records[i:i+20]
        await ws.send_json(batch)
        await asyncio.sleep(0.1)
    await ws.close()
    return ws


# --- GraphQL ---
async def graphql_handler(request: web.Request) -> web.Response:
    body = await request.json()
    # Simple mock — always return records regardless of query
    records = _generate_records(200, "graphql-api")
    return web.json_response({"data": {"records": records}})


# --- File source (served as JSON for test setup) ---
FILE_RECORDS = _generate_records(200, "file-local")


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/api/rest", rest_handler)
    app.router.add_get("/api/csv", csv_handler)
    app.router.add_get("/api/ws", ws_handler)
    app.router.add_post("/api/graphql", graphql_handler)
    return app


async def start_server(port: int = 18932) -> tuple[web.AppRunner, str]:
    """Start the mock server, return (runner, base_url)."""
    app = create_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", port)
    await site.start()
    return runner, f"http://127.0.0.1:{port}"


if __name__ == "__main__":
    async def main():
        runner, url = await start_server(18935)
        print(f"Mock server running at {url}", flush=True)
        try:
            await asyncio.Event().wait()
        finally:
            await runner.cleanup()

    asyncio.run(main())
