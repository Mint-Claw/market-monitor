"""Test data files for pipeline."""
import json
from pathlib import Path

# Create test data directory
test_data_dir = Path(__file__).parent / "test_data"
test_data_dir.mkdir(exist_ok=True)

# Generate file source data
file_data = [
    {
        "timestamp": f"2026-02-27T11:{i:02d}:00Z",
        "value": 500 + i * 0.6,
        "unit": "CNY"
    }
    for i in range(100)
]

(test_data_dir / "data.json").write_text(json.dumps(file_data, indent=2))

print(f"Test data created in {test_data_dir}")
