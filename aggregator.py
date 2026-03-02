import json
def aggregate_data(sources):
    result = {"total": 0, "entities": []}
    for s in sources:
        result["total"] += 1
        result["entities"].append(s)
    return result
# Atlas: High-performance data normalization pipeline
