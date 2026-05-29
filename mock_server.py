#!/usr/bin/env python3
"""
Mock server for testing the pipeline.
Provides REST, WebSocket, and GraphQL endpoints.
"""

import asyncio
import json
import random
from datetime import datetime, timezone
from aiohttp import web
import websockets

# Generate mock data
def generate_records(count=100):
    records = []
    base_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    
    for i in range(count):
        timestamp = base_time.timestamp() + i * 3600  # hourly
        records.append({
            "timestamp": datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat(),
            "value": round(random.uniform(50, 200), 2),
            "unit": random.choice(["USD", "EUR", "GBP"]),
            "metadata": {"index": i}
        })
    
    return records

# REST handler
async def rest_handler(request):
    limit = int(request.query.get("limit", 100))
    records = generate_records(limit)
    return web.json_response({"data": records})

# GraphQL handler
async def graphql_handler(request):
    body = await request.json()
    query = body.get("query", "")
    
    # Simple mock GraphQL response
    records = generate_records(50)
    return web.json_response({
        "data": {
            "items": records
        }
    })

# WebSocket handler
async def websocket_handler(websocket):
    # Send initial data
    records = generate_records(10)
    for record in records:
        await websocket.send(json.dumps(record))
        await asyncio.sleep(0.1)

# Start servers
async def start_servers():
    # REST server
    rest_app = web.Application()
    rest_app.router.add_get("/api/data", rest_handler)
    rest_app.router.add_post("/graphql", graphql_handler)
    
    rest_runner = web.AppRunner(rest_app)
    await rest_runner.setup()
    rest_site = web.TCPSite(rest_runner, "localhost", 8080)
    await rest_site.start()
    
    # WebSocket server
    ws_server = await websockets.serve(websocket_handler, "localhost", 8081)
    
    print("Mock servers started:")
    print("  REST: http://localhost:8080/api/data")
    print("  GraphQL: http://localhost:8080/graphql")
    print("  WebSocket: ws://localhost:8081")
    
    # Keep running
    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(start_servers())
