# test_aggregator.py

import unittest
from aggregator import DataAggregator
from unittest.mock import patch

class TestDataAggregator(unittest.TestCase):

    @patch('requests.get')
    def test_fetch_data(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = [{'id': 1, 'name': 'Test', 'value': 100, 'timestamp': '2026-03-13T12:00:00'}]

        aggregator = DataAggregator(['https://api.example.com/data1'])
        data = aggregator.fetch_data()

        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['id'], 1)
        self.assertEqual(data[0]['name'], 'Test')

    def test_normalize_data(self):
        raw_data = [{'id': 1, 'name': 'Test', 'value': 100, 'timestamp': '2026-03-13T12:00:00'}]

        aggregator = DataAggregator([])
        normalized_data = aggregator.normalize_data(raw_data)

        self.assertEqual(len(normalized_data), 1)
        self.assertEqual(normalized_data[0]['id'], 1)
        self.assertEqual(normalized_data[0]['value'], 100)

    @patch('requests.get')
    def test_aggregate(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = [{'id': 1, 'name': 'Test', 'value': 100, 'timestamp': '2026-03-13T12:00:00'}]

        aggregator = DataAggregator(['https://api.example.com/data1'])
        aggregated_data = aggregator.aggregate()

        self.assertEqual(len(aggregated_data), 1)
        self.assertEqual(aggregated_data[0]['name'], 'Test')

if __name__ == '__main__':
    unittest.main()