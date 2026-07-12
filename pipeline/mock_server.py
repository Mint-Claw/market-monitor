from aiohttp import web
import json
import asyncio

async def handle_rest(request):
    data = {
        "items": [
            {"t": 1700000000, "p": 100.5, "c": "USD", "extra": "A"},
            {"t": 1700000010, "p": 101.0, "c": "USD", "extra": "B"}
        ]
    }
    return web.json_response(data)

async def handle_graphql(request):
    data = {
        "data": {
            "market": [
                {"timestamp": "2024-01-01T00:00:00Z", "value": 500.0, "symbol": "BTC"}
            ]
        }
    }
    return web.json_response(data)

async def handle_csv(request):
    text = "date,amount,type\n2024-01-01T00:01:00Z,10.5,ETH\n2024-01-01T00:02:00Z,11.0,ETH"
    return web.Response(text=text, content_type="text/csv")

async def handle_websocket(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    data = {"events": [{"ts": "2024-01-01T00:03:00Z", "val": 42.0, "coin": "SOL"}]}
    await ws.send_json(data)
    await ws.close()
    return ws

def create_app():
    app = web.Application()
    app.router.add_get('/api/rest', handle_rest)
    app.router.add_post('/api/graphql', handle_graphql)
    app.router.add_get('/api/csv', handle_csv)
    app.router.add_get('/api/ws', handle_websocket)
    return app

if __name__ == '__main__':
    app = create_app()
    web.run_app(app, port=8080)
