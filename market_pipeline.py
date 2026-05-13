import asyncio
import aiohttp
import json
import csv
import time
from datetime import datetime

class DataNormalizer:
      @staticmethod
      def normalize(source_name, raw_data, mapping):
                try:
                              return {
                                                "source": source_name,
                                                "timestamp": int(time.time()),
                                                "value": raw_data.get(mapping.get("value"), 0) if isinstance(raw_data, dict) else raw_data,
                                                "unit": mapping.get("unit", "unknown")
                              }
except Exception as e:
            return {"error": str(e), "source": source_name}

class MarketPipeline:
      def __init__(self, config_path):
                with open(config_path, 'r') as f:
                              self.config = json.load(f)
                          self.results = []

      async def fetch_rest(self, session, source):
                try:
                              async with session.get(source['url']) as response:
                                                data = await response.json()
                                                normalized = DataNormalizer.normalize(source['name'], data, source['mapping'])
                                                self.results.append(normalized)
                                                print(f"Fetched: {source['name']} (REST)")
                except Exception as e:
                              print(f"Error {source['name']}: {e}")

            async def fetch_graphql(self, session, source):
                      query = {"query": "{ price(symbol: \"BTC\") { value unit } }"}
                      try:
                                    async with session.post(source['url'], json=query) as response:
                                                      data = await response.json()
                                                      normalized = DataNormalizer.normalize(source['name'], data['data']['price'], source['mapping'])
                                                      self.results.append(normalized)
                                                      print(f"Fetched: {source['name']} (GraphQL)")
                      except Exception as e:
                                    print(f"Error {source['name']} (GraphQL): {e}")

                  async def fetch_websocket(self, source):
                            try:
                                          await asyncio.sleep(0.5) 
            mock_ws_data = {"data": 68000.5}
            normalized = DataNormalizer.normalize(source['name'], mock_ws_data, source['mapping'])
            self.results.append(normalized)
            print(f"Streamed: {source['name']} (WebSocket)")
except Exception as e:
            print(f"Error {source['name']} (WS): {e}")

    async def read_local_file(self, source):
              try:
                            mock_data = {"timestamp": int(time.time()), "value": 24.5}
                            normalized = DataNormalizer.normalize(source['name'], mock_data, source['mapping'])
                            self.results.append(normalized)
                            print(f"Read: {source['name']} (File)")
except Exception as e:
            print(f"Error {source['name']}: {e}")

    async def run(self):
              async with aiohttp.ClientSession() as session:
                            tasks = []
                            for source in self.config['sources']:
                                              if source['type'] == 'rest':
                                                                    tasks.append(self.fetch_rest(session, source))
elif source['type'] == 'graphql':
                    tasks.append(self.fetch_graphql(session, source))
elif source['type'] == 'file':
                    tasks.append(self.read_local_file(source))
elif source['type'] == 'websocket':
                    tasks.append(self.fetch_websocket(source))
            await asyncio.gather(*tasks)
        with open(self.config['output'], 'w') as f:
                      json.dump(self.results, f, indent=4)

if __name__ == '__main__':
      pipeline = MarketPipeline('config.json')
      asyncio.run(pipeline.run())
  
