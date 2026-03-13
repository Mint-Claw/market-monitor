# aggregator.py

import requests
import json

class DataAggregator:
    def __init__(self, sources: list):
        self.sources = sources

    def fetch_data(self):
        data = []
        for source in self.sources:
            response = requests.get(source)
            if response.status_code == 200:
                data.append(response.json())
        return data

    def normalize_data(self, data):
        normalized_data = []
        for entry in data:
            normalized_entry = {
                'id': entry.get('id'),
                'name': entry.get('name'),
                'value': entry.get('value', 0),
                'timestamp': entry.get('timestamp', None)
            }
            normalized_data.append(normalized_entry)
        return normalized_data

    def aggregate(self):
        raw_data = self.fetch_data()
        normalized_data = self.normalize_data(raw_data)
        return normalized_data

# Example usage
sources = [
    'https://api.example.com/data1',
    'https://api.example.com/data2'
]
aggregator = DataAggregator(sources)
aggregated_data = aggregator.aggregate()
print(json.dumps(aggregated_data, indent=4))