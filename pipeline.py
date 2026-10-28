#!/usr/bin/env python3
"""
Pipeline entrypoint for Multi-Source Data Aggregation & Normalization.
Exports Pipeline, DataPipeline, DataNormalizer, DataSource, and CLI interface.
"""

from data_pipeline import (
    DataPipeline,
    DataNormalizer,
    DataSource,
    normalize_timestamp,
    normalize_value,
    main,
)

# Alias Pipeline to DataPipeline for flexible import
Pipeline = DataPipeline

__all__ = [
    "Pipeline",
    "DataPipeline",
    "DataNormalizer",
    "DataSource",
    "normalize_timestamp",
    "normalize_value",
    "main",
]

if __name__ == "__main__":
    main()
