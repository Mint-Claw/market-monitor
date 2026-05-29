#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mock servers for testing the pipeline.
Serves REST, CSV, GraphQL endpoints and WebSocket on localhost:8899.
"""
import asyncio
import csv
import io
import json
import logging
import os
import random
from datetime import datetime, timezone

from aiohttp import web, WSMsgType

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("mockserver")

HOST = "0.0.0.0"
PORT = 8899

# ── Generate test data ──────────────────────────────────────────────────────

def gen_prices(count=200):
    records = []
    base = datetime.now(timezone.utc)
    for i in range(count):
        ts = base.timestamp() - i * 60
        records.append({
            "timestamp": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
            "value": round(50000 + random.uniform(-5000, 5000), 2),
            "unit": "USD",
            "symbol": random.choice(["BTC", "ETH", "SOL", "ADA", "DOT"])
        })
    return records


def gen_csv_rows(count=250):
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["timestamp", "value", "unit", "market"])
    base = datetime.now(timezone.utc)
    for i in range(count):
        ts = base.timestamp() - i * 120
        writer.writerow([
            datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
            round(random.uniform(1, 50000), 2),
            "USD",
            random.choice(["spot", "futures", "options"])
        ])
    return output.getvalue()


def gen_snapshot(count=300):
    records = []
    base = datetime.now(timezone.utc)
    for i in range(count):
        ts = base.timestamp() - i * 30
        records.append({
            "timestamp": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
            "value": round(random.uniform(100, 100000), 2),
            "unit": "USD",
            "source": "snapshot"
        })
    return records


def gen_ws_messages(count=100):
    msgs = []
    base = datetime.now(timezone.utc)
    for i in range(count):
        ts = base.timestamp() - i * 10
        msgs.append({
            "timestamp": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
            "value": round(random.uniform(45000, 55000), 2),
            "unit": "USD",
            "trade_id": i + 1
        })
    return msgs


# Write local data files
os.makedirs("mock_data", exist_ok=True)

snapshot = gen_snapshot(300)
with open("mock_data/snapshot.json", "w") as f:
    json.dump(snapshot, f, indent=2)
log.info(f"Wrote mock_data/snapshot.json ({len(snapshot)} records)")

csv_content = gen_csv_rows(250)
with open("mock_data/historical.csv", "w") as f:
    f.write(csv_content)
log.info(f"Wrote mock_data/historical.csv")

# ── HTTP Routes ─────────────────────────────────────────────────────────────

async def handle_prices(request):
    prices = gen_prices(200)
    return web.json_response(prices)


async def handle_csv(request):
    return web.Response(text=csv_content, content_type="text/csv")


async def handle_graphql(request):
    body = await request.json()
    query = body.get("query", "")
    data = gen_prices(200)
    result = {
        "data": {
            "data": data
        }
    }
    return web.json_response(result)


async def handle_ws(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    messages = gen_ws_messages(100)
    for msg in messages:
        await ws.send_json(msg)
        await asyncio.sleep(0.01)  # small delay between messages
    await ws.close()
    return ws


async def handle_health(request):
    return web.json_response({"status": "ok", "service": "mock-server"})


# ── Main ────────────────────────────────────────────────────────────────────

def create_app():
    app = web.Application()
    app.router.add_get("/api/prices", handle_prices)
    app.router.add_get("/data/market.csv", handle_csv)
    app.router.add_post("/graphql", handle_graphql)
    app.router.add_get("/ws/trades", handle_ws)
    app.router.add_get("/health", handle_health)
    return app


if __name__ == "__main__":
    app = create_app()
    log.info(f"Mock server starting on {HOST}:{PORT}")
    web.run_app(app, host=HOST, port=PORT, print=lambda *a: None)
