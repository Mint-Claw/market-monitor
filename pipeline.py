#!/usr/bin/env python3
"""
Pipeline entrypoint for Multi-Source Data Aggregation & Normalization.
Exports Pipeline, DataPipeline, and CLI interface.
"""

from data_pipeline import DataPipeline, main

# Alias Pipeline to DataPipeline for flexible import
Pipeline = DataPipeline

if __name__ == "__main__":
    main()
