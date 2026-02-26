"""
Mock Server for testing the data aggregation pipeline.
Simulates REST, CSV, WebSocket, GraphQL, and File sources.
"""

import asyncio
import json
import time
from aiohttp import web
from pathlib import Path

def generate_mock_data(count=200):
    """Generate mock records"""
    base_time = time.time()
    return [
        {
            'timestamp': base_time - (count - i) * 60,  # 1 minute intervals
            'value': 100.0 + (i % 10) * 0.5,
            'id': i
        }
        for i in range(count)
    ]

async def handle_rest(request):
    """REST API endpoint"""
    data = generate_mock_data(200)
    return web.json_response(data)

async def handle_ws(request):
    """WebSocket endpoint (simulated for HTTP)"""
    data = generate_mock_data(150)
    return web.json_response(data)

async def handle_graphql(request):
    """GraphQL endpoint"""
    if request.method == 'POST':
        body = await request.json()
        query = body.get('query', '')
        
        # Simple GraphQL parser
        if 'data' in query or query.strip() == '':
            data = generate_mock_data(180)
            return web.json_response({'data': data})
    
    return web.json_response({'errors': ['Invalid query']})

async def handle_health(request):
    """Health check"""
    return web.json_response({'status': 'ok'})

def create_app():
    app = web.Application()
    app.router.add_get('/api/data', handle_rest)
    app.router.add_get('/ws/data', handle_ws)
    app.router.add_post('/graphql', handle_graphql)
    app.router.add_get('/health', handle_health)
    return app

if __name__ == '__main__':
    print("Starting mock server on http://localhost:8080")
    web.run_app(create_app(), port=8080)
