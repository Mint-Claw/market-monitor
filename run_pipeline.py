#!/usr/bin/env python3
"""CLI for running the data aggregation pipeline."""
import asyncio
import argparse
import json
import sys
from pathlib import Path
from pipeline.config import PipelineConfig, SourceConfig
from pipeline.engine import run_pipeline

def load_config(config_path: str) -> PipelineConfig:
    """Load pipeline configuration from JSON file."""
    with open(config_path) as f:
        data = json.load(f)
    
    sources = [SourceConfig(**src) for src in data["sources"]]
    
    return PipelineConfig(
        sources=sources,
        output_path=data.get("output_path", "output.json"),
        max_concurrent=data.get("max_concurrent", 10),
        retry_attempts=data.get("retry_attempts", 2),
        incremental=data.get("incremental", False)
    )

async def main():
    parser = argparse.ArgumentParser(
        description="Multi-source async data aggregation pipeline"
    )
    parser.add_argument(
        "config",
        help="Path to configuration JSON file"
    )
    parser.add_argument(
        "-o", "--output",
        help="Override output path"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose output"
    )
    
    args = parser.parse_args()
    
    try:
        config = load_config(args.config)
        
        if args.output:
            config.output_path = args.output
        
        if args.verbose:
            print(f"Loading configuration from {args.config}")
            print(f"Sources: {len(config.sources)}")
            print(f"Output: {config.output_path}")
            print("\nRunning pipeline...")
        
        result = await run_pipeline(config)
        
        # Print results
        stats = result["stats"]
        print(f"\n{'='*60}")
        print(f"Pipeline completed in {stats['duration_seconds']}s")
        print(f"{'='*60}")
        print(f"Total records: {stats['total_records']}")
        print(f"Sources succeeded: {stats['sources_succeeded']}")
        print(f"Sources failed: {stats['sources_failed']}")
        
        if stats['errors']:
            print(f"\nErrors:")
            for error in stats['errors']:
                print(f"  - {error['source']}: {error['error']}")
        
        print(f"\nOutput saved to: {config.output_path}")
        
        # Exit with error if any sources failed
        sys.exit(0 if stats['sources_failed'] == 0 else 1)
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
