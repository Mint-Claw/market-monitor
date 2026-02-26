"""Mock server for testing pipeline."""
import asyncio
import json
from aiohttp import web
import aiohttp

# Mock data generators
def generate_rest_data(count=100):
    """Generate mock REST API data."""
    return [
        {
            "timestamp": f"2026-02-27T07:{i:02d}:00Z",
            "value": 100 + i * 0.5,
            "unit": "USD"
        }
        for i in range(count)
    ]

def generate_csv_data(count=100):
    """Generate mock CSV data."""
    lines = ["timestamp,value,unit"]
    for i in range(count):
        lines.append(f"2026-02-27T08:{i:02d}:00Z,{200 + i * 0.3},EUR")
    return "\n".join(lines)

def generate_graphql_data(count=100):
    """Generate mock GraphQL data."""
    return {
        "data": {
            "metrics": [
                {
                    "timestamp": f"2026-02-27T09:{i:02d}:00Z",
                    "value": 300 + i * 0.7,
                    "unit": "GBP"
                }
                for i in range(count)
            ]
        }
    }

# HTTP handlers
async def rest_handler(request):
    """REST API endpoint."""
    count = int(request.query.get('count', 100))
    return web.json_response(generate_rest_data(count))

async def csv_handler(request):
    """CSV endpoint."""
    count = int(request.query.get('count', 100))
    return web.Response(text=generate_csv_data(count), content_type='text/csv')

async def graphql_handler(request):
    """GraphQL endpoint."""
    body = await request.json()
    # Simple mock - ignore actual query
    return web.json_response(generate_graphql_data(100))

async def websocket_handler(request):
    """WebSocket endpoint."""
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    
    # Send 50 messages
    for i in range(50):
        data = {
            "timestamp": f"2026-02-27T10:{i:02d}:00Z",
            "value": 400 + i * 0.4,
            "unit": "JPY"
        }
        await ws.send_json(data)
        await asyncio.sleep(0.05)
    
    await ws.close()
    return ws

def create_app():
    """Create mock server app."""
    app = web.Application()
    app.router.add_get('/api/rest', rest_handler)
    app.router.add_get('/api/csv', csv_handler)
    app.router.add_post('/api/graphql', graphql_handler)
    app.router.add_get('/ws', websocket_handler)
    return app

async def start_server(host='127.0.0.1', port=8888):
    """Start mock server."""
    app = create_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    print(f"Mock server running on http://{host}:{port}")
    return runner

if __name__ == '__main__':
    async def main():
        runner = await start_server()
        try:
            await asyncio.Event().wait()
        except KeyboardInterrupt:
            await runner.cleanup()
    
    asyncio.run(main())
