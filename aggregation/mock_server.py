"""Mock server providing REST, WebSocket, GraphQL, and CSV endpoints for testing."""

import asyncio
import json
import random
from datetime import datetime, timezone, timedelta

from aiohttp import web


def _generate_records(n: int, source: str, base_time: datetime | None = None) -> list[dict]:
    """Generate n mock market records."""
    if base_time is None:
        base_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    records = []
    for i in range(n):
        ts = base_time + timedelta(minutes=i * 5)
        records.append({
            "timestamp": ts.isoformat(),
            "value": round(40000 + random.uniform(-2000, 2000), 2),
            "unit": "USD",
            "source": source,
        })
    return records


REST_DATA = _generate_records(250, "rest_market_data")
GQL_DATA = _generate_records(200, "graphql_analytics",
                              datetime(2026, 1, 2, tzinfo=timezone.utc))
WS_DATA = _generate_records(200, "ws_live_feed",
                             datetime(2026, 1, 3, tzinfo=timezone.utc))


async def handle_rest(request: web.Request) -> web.Response:
    return web.json_response(REST_DATA)


async def handle_graphql(request: web.Request) -> web.Response:
    return web.json_response({"data": {"analytics": GQL_DATA}})


async def handle_websocket(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    # Send data in batches of 50
    for i in range(0, len(WS_DATA), 50):
        batch = WS_DATA[i:i + 50]
        await ws.send_json(batch)
        await asyncio.sleep(0.05)
    await ws.close()
    return ws


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/api/market", handle_rest)
    app.router.add_post("/graphql", handle_graphql)
    app.router.add_get("/ws/feed", handle_websocket)
    return app


def run_server(host: str = "0.0.0.0", port: int = 9753) -> None:
    app = create_app()
    web.run_app(app, host=host, port=port, print=lambda *a: None)


if __name__ == "__main__":
    run_server()
