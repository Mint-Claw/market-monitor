"""Mock server providing REST, CSV, WebSocket, and GraphQL endpoints for testing."""
from __future__ import annotations
import json
from aiohttp import web


def _generate_records(source: str, count: int = 50) -> list[dict]:
    return [
        {"source": source, "timestamp": f"2026-02-26T{i // 60:02d}:{i % 60:02d}:00Z",
         "value": round(100 + i * 0.5, 2), "unit": "USD"}
        for i in range(count)
    ]


REST_DATA = _generate_records("rest_source", 200)
CSV_DATA = _generate_records("csv_source", 200)
GQL_DATA = _generate_records("graphql_source", 200)
WS_DATA = _generate_records("ws_source", 200)


async def handle_rest(request: web.Request) -> web.Response:
    return web.json_response({"data": REST_DATA})


async def handle_csv(request: web.Request) -> web.Response:
    lines = ["source,timestamp,value,unit"]
    for r in CSV_DATA:
        lines.append(f"{r['source']},{r['timestamp']},{r['value']},{r['unit']}")
    return web.Response(text="\n".join(lines), content_type="text/csv")


async def handle_graphql(request: web.Request) -> web.Response:
    body = await request.json()
    return web.json_response({"data": {"records": GQL_DATA}})


async def handle_websocket(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    for r in WS_DATA:
        await ws.send_json(r)
    await ws.send_json({"type": "done"})
    await ws.close()
    return ws


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/api/rest", handle_rest)
    app.router.add_get("/api/csv", handle_csv)
    app.router.add_post("/api/graphql", handle_graphql)
    app.router.add_get("/api/ws", handle_websocket)
    return app


if __name__ == "__main__":
    web.run_app(create_app(), port=18932)
