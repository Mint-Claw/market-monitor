# Data Aggregation & Normalization Pipeline

This project provides a data aggregation and normalization pipeline that fetches data from multiple APIs, processes it into a unified format, and returns aggregated results.

## Features
- Fetches data from multiple API sources.
- Normalizes the data into a unified format.
- Allows easy integration with other systems.

## Requirements
- Python 3.x
- Requests library (can be installed using `pip install requests`)

## Usage
1. Import the `DataAggregator` class from `aggregator.py`.
2. Initialize the aggregator with a list of API sources.
3. Call `aggregate()` method to fetch and normalize data.

### Example
```python
from aggregator import DataAggregator

sources = [
    'https://api.example.com/data1',
    'https://api.example.com/data2'
]
aggregator = DataAggregator(sources)
aggregated_data = aggregator.aggregate()
print(aggregated_data)
```

## Testing
Run the following command to execute tests:

```bash
python -m unittest test_aggregator.py
```