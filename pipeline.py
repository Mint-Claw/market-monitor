import asyncio
import json
import csv
import time
from datetime import datetime
import random

class DataPipeline:
    def __init__(self):
        self.results = []

    async def fetch_rest(self):
        # Mock REST fetch
        await asyncio.sleep(0.5)
        return [{"ts": time.time(), "val": random.random(), "u": "USD"} for _ in range(200)]

    async def fetch_csv(self):
        # Mock CSV fetch
        await asyncio.sleep(0.3)
        return [{"timestamp": time.time(), "price": random.random(), "currency": "ETH"} for _ in range(200)]

    async def fetch_websocket(self):
        # Mock WebSocket stream capture
        await asyncio.sleep(0.8)
        return [{"time": datetime.now().isoformat(), "amount": random.random(), "unit": "BTC"} for _ in range(200)]

    async def fetch_graphql(self):
        # Mock GraphQL query
        await asyncio.sleep(0.6)
        return {"data": {"history": [{"observedAt": int(time.time()), "metric": random.random(), "measure": "SOL"} for _ in range(200)]}}

    async def fetch_file(self):
        # Mock local file read
        await asyncio.sleep(0.1)
        return [{"unix": time.time(), "data_point": random.random(), "label": "NEAR"} for _ in range(200)]

    def normalize(self, source_name, raw_data):
        normalized = []
        if source_name == "rest":
            for item in raw_data:
                normalized.append({"source": "REST", "timestamp": item["ts"], "value": item["val"], "unit": item["u"]})
        elif source_name == "csv":
            for item in raw_data:
                normalized.append({"source": "CSV", "timestamp": item["timestamp"], "value": item["price"], "unit": item["currency"]})
        elif source_name == "ws":
            for item in raw_data:
                ts = datetime.fromisoformat(item["time"]).timestamp()
                normalized.append({"source": "WebSocket", "timestamp": ts, "value": item["amount"], "unit": item["unit"]})
        elif source_name == "graphql":
            for item in raw_data["data"]["history"]:
                normalized.append({"source": "GraphQL", "timestamp": item["observedAt"], "value": item["metric"], "unit": item["measure"]})
        elif source_name == "file":
            for item in raw_data:
                normalized.append({"source": "File", "timestamp": item["unix"], "value": item["data_point"], "unit": item["label"]})
        return normalized

    async def run(self):
        start_time = time.time()
        print("Starting concurrent fetch...")
        
        tasks = {
            "rest": self.fetch_rest(),
            "csv": self.fetch_csv(),
            "ws": self.fetch_websocket(),
            "graphql": self.fetch_graphql(),
            "file": self.fetch_file()
        }
        
        responses = await asyncio.gather(*tasks.values())
        raw_map = dict(zip(tasks.keys(), responses))
        
        all_normalized = []
        for src, data in raw_map.items():
            all_normalized.extend(self.normalize(src, data))
            
        # Merge and Sort
        all_normalized.sort(key=lambda x: x["timestamp"])
        
        end_time = time.time()
        duration = end_time - start_time
        
        stats = {
            "total_records": len(all_normalized),
            "execution_time_s": round(duration, 4),
            "records_per_second": round(len(all_normalized) / duration, 2)
        }
        
        return all_normalized, stats

if __name__ == "__main__":
    pipeline = DataPipeline()
    data, stats = asyncio.run(pipeline.run())
    print(json.dumps(stats, indent=2))
    print(f"Sample record: {data[0]}")
