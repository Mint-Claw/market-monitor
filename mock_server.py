"""
Mock Server for Data Pipeline Testing

Provides mock REST, CSV, WebSocket, GraphQL, and JSON endpoints
for testing the multi-source data aggregation pipeline.
"""

import asyncio
import json
from datetime import datetime, timedelta
from aiohttp import web
import aiofiles


# Generate mock data
def generate_mock_records(count: int = 100):
    """Generate mock records with varying timestamps and values."""
    records = []
    base_time = datetime.now()
    for i in range(count):
        records.append({
            "timestamp": (base_time - timedelta(seconds=i*10)).isoformat(),
            "value": round(100 + i * 1.5 + (i % 10) * 5, 2),
            "unit": "USD" if i % 3 != 0 else "EUR",
            "metadata": {
                "id": f"rec_{i}",
                "category": f"cat_{i % 5}"
            }
        })
    return records


MOCK_DATA = generate_mock_records(200)


class MockServer:
    """Mock server with all endpoint types."""
    
    def __init__(self, host: str = "localhost", port: int = 8080):
        self.host = host
        self.port = port
        self.app = web.Application()
        self._setup_routes()
        self.runner = None
    
    def _setup_routes(self):
        """Setup all mock endpoints."""
        # REST endpoint
        self.app.router.add_get('/api/data', self.handle_rest)
        
        # CSV endpoint
        self.app.router.add_get('/data.csv', self.handle_csv)
        
        # JSON file endpoint
        self.app.router.add_get('/data.json', self.handle_json)
        
        # GraphQL endpoint
        self.app.router.add_post('/graphql', self.handle_graphql)
        
        # WebSocket endpoint
        self.app.router.add_get('/ws', self.handle_websocket)
    
    async def handle_rest(self, request):
        """REST API endpoint returning JSON array."""
        return web.json_response(MOCK_DATA[:100])
    
    async def handle_csv(self, request):
        """CSV file endpoint."""
        csv_content = "timestamp,value,unit,category\n"
        for rec in MOCK_DATA[:100]:
            csv_content += f"{rec['timestamp']},{rec['value']},{rec['unit']},{rec['metadata']['category']}\n"
        return web.Response(text=csv_content, content_type='text/csv')
    
    async def handle_json(self, request):
        """JSON file endpoint."""
        return web.json_response({"data": MOCK_DATA[:100]})
    
    async def handle_graphql(self, request):
        """GraphQL endpoint."""
        data = await request.json()
        query = data.get("query", "")
        
        # Simple mock GraphQL response
        response = {
            "data": {
                "data": MOCK_DATA[:50]
            }
        }
        return web.json_response(response)
    
    async def handle_websocket(self, request):
        """WebSocket endpoint for streaming data."""
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        
        for rec in MOCK_DATA[:10]:
            await ws.send_json(rec)
            await asyncio.sleep(0.1)  # Simulate streaming
        
        await ws.close()
        return ws
    
    async def start(self):
        """Start the mock server."""
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, self.host, self.port)
        await site.start()
        print(f"Mock server running at http://{self.host}:{self.port}")
    
    async def stop(self):
        """Stop the mock server."""
        if self.runner:
            await self.runner.cleanup()


async def run_server():
    """Run the mock server."""
    server = MockServer()
    await server.start()
    
    # Keep server running
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        await server.stop()


if __name__ == "__main__":
    asyncio.run(run_server())
