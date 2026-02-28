"""
Mock Server for Pipeline Integration Tests
===========================================
Provides all 5 source types: REST, CSV (via file), WebSocket, GraphQL, and File.
Uses aiohttp for HTTP and WebSocket serving.
"""
from __future__ import annotations

import asyncio
import json
import random
import time
from datetime import datetime, timezone, timedelta

from aiohttp import web

# ---------------------------------------------------------------------------
# Sample Data Generation
# ---------------------------------------------------------------------------

COINS = ["BTC", "ETH", "SOL", "AVAX", "MATIC"]
BASE_PRICES = {"BTC": 65000, "ETH": 1900, "SOL": 140, "AVAX": 35, "MATIC": 0.85}


def _gen_price_record(coin: str, idx: int) -> dict:
    base = BASE_PRICES.get(coin, 100)
    jitter = random.uniform(-0.05, 0.05) * base
    ts = datetime.now(timezone.utc) - timedelta(minutes=idx * 5)
    return {
        "symbol": coin,
        "price": round(base + jitter, 4),
        "currency": "USD",
        "timestamp": ts.isoformat(),
        "volume_24h": round(random.uniform(1e6, 1e9), 2),
    }


def _gen_records(count: int = 200) -> list[dict]:
    records = []
    for i in range(count):
        coin = COINS[i % len(COINS)]
        records.append(_gen_price_record(coin, i))
    return records


ALL_RECORDS = _gen_records(200)

# ---------------------------------------------------------------------------
# REST Endpoint
# ---------------------------------------------------------------------------

async def handle_rest_prices(request: web.Request) -> web.Response:
    """GET /api/prices -> list of price records."""
    limit = int(request.query.get("limit", 50))
    data = ALL_RECORDS[:limit]
    return web.json_response(data)

# ---------------------------------------------------------------------------
# GraphQL Endpoint
# ---------------------------------------------------------------------------

async def handle_graphql(request: web.Request) -> web.Response:
    """POST /graphql -> markets query."""
    body = await request.json()
    query = body.get("query", "")

    if "markets" in query:
        markets = []
        for coin in COINS:
            base = BASE_PRICES[coin]
            markets.append({
                "symbol": coin,
                "lastPrice": round(base + random.uniform(-100, 100), 4),
                "updatedAt": datetime.now(timezone.utc).isoformat(),
                "quoteCurrency": "USD",
            })
        return web.json_response({"data": {"markets": markets}})

    return web.json_response({"errors": [{"message": "Unknown query"}]}, status=400)

# ---------------------------------------------------------------------------
# WebSocket Endpoint
# ---------------------------------------------------------------------------

async def handle_websocket(request: web.Request) -> web.WebSocketResponse:
    """WS /ws/prices -> stream price updates."""
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    try:
        for i in range(100):
            coin = COINS[i % len(COINS)]
            msg = {
                "pair": f"{coin}/USD",
                "price": round(BASE_PRICES[coin] + random.uniform(-200, 200), 4),
                "ts": datetime.now(timezone.utc).isoformat(),
                "seq": i,
            }
            await ws.send_json(msg)
            await asyncio.sleep(0.02)  # 50 msgs/sec
    except (ConnectionError, asyncio.CancelledError):
        pass

    await ws.close()
    return ws

# ---------------------------------------------------------------------------
# App Setup
# ---------------------------------------------------------------------------

def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/api/prices", handle_rest_prices)
    app.router.add_post("/graphql", handle_graphql)
    app.router.add_get("/ws/prices", handle_websocket)
    return app


if __name__ == "__main__":
    app = create_app()
    web.run_app(app, host="localhost", port=8099)
