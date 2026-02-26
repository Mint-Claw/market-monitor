# Bounty #1 Implementation Notes

## What changed in this follow-up

- Added retry support to all source fetchers (`retries`, `retry_backoff_ms`) to improve resiliency on transient failures.
- Added runnable sample artifacts:
  - `sample_config.json`
  - `sample_file.json`
  - `sample_output.json`
- Added performance benchmark script:
  - `benchmark_pipeline.py`

## How to run

```bash
python3 -m unittest -v test_bounty_pipeline.py
python3 benchmark_pipeline.py
python3 bounty_pipeline.py --config sample_config.json --out pipeline_output.json
```

## Acceptance mapping

- ✅ 5 source types supported: REST / CSV / WebSocket / GraphQL / file
- ✅ Config-driven source definitions
- ✅ Concurrent async execution
- ✅ Normalized schema: `{source, timestamp, value, unit}`
- ✅ Merge + sort JSON output + pipeline stats
- ✅ Graceful timeout/failure handling (per-source isolation + retries)
- ✅ Integration test included
- ✅ Benchmark helper for 1000+ records timing check
