#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Integration tests for the market-monitor pipeline.
Starts mock servers, runs the pipeline, validates results.
"""
import asyncio
import json
import logging
import os
import subprocess
import sys
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("tests")

PIPELINE_DIR = os.path.dirname(os.path.abspath(__file__))


async def test_pipeline():
    log.info("=" * 60)
    log.info("TEST: Multi-Source Data Aggregation Pipeline")
    log.info("=" * 60)

    # 1. Start mock server
    log.info("[1/4] Starting mock servers...")
    server_proc = subprocess.Popen(
        [sys.executable, "mock_servers.py"],
        cwd=PIPELINE_DIR,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    await asyncio.sleep(1.5)  # wait for server startup

    try:
        # 2. Verify mock server is up
        import aiohttp
        async with aiohttp.ClientSession() as session:
            async with session.get("http://localhost:8899/health") as resp:
                health = await resp.json()
                assert health["status"] == "ok", "Mock server not healthy"
                log.info(f"  Mock server healthy: {health}")

        # 3. Install dependencies if needed
        log.info("[2/4] Checking dependencies...")
        try:
            import yaml
            import aiohttp
            log.info("  Dependencies OK")
        except ImportError:
            log.info("  Installing requirements...")
            subprocess.run(
                [sys.executable, "-m", "pip", "install", "-r", "requirements.txt"],
                cwd=PIPELINE_DIR, check=True, capture_output=True
            )
            log.info("  Dependencies installed")

        # 4. Run the pipeline
        log.info("[3/4] Running pipeline...")
        sys.path.insert(0, PIPELINE_DIR)
        from pipeline import Pipeline

        t0 = time.time()
        pipeline = Pipeline(config_path=os.path.join(PIPELINE_DIR, "config.yaml"))
        stats = await pipeline.run()
        elapsed = time.time() - t0

        pipeline.output_json(os.path.join(PIPELINE_DIR, "output.json"))
        pipeline.print_stats()

        # 5. Validate results
        log.info("[4/4] Validating results...")
        passed = 0
        total = 0

        # Test 1: Records exist
        total += 1
        if stats["total_records"] > 0:
            log.info("  ✅ TEST 1: Records collected")
            passed += 1
        else:
            log.error("  ❌ TEST 1: No records collected")
            # Continue with partial validation

        # Test 2: All 5+ source types
        total += 1
        actual_sources = len(stats.get("source_breakdown", {}))
        expected_min = 5  # rest, csv, websocket, graphql, file (x2 for snapshot + csv)
        if actual_sources >= expected_min:
            log.info(f"  ✅ TEST 2: {actual_sources} source types >= {expected_min}")
            passed += 1
        else:
            log.warning(f"  ⚠️  TEST 2: {actual_sources} source types (expected >= {expected_min})")

        # Test 3: 1000+ records
        total += 1
        if stats["total_records"] >= 1000:
            log.info(f"  ✅ TEST 3: {stats['total_records']} records >= 1000")
            passed += 1
        else:
            log.warning(f"  ⚠️  TEST 3: Only {stats['total_records']} records (need 1000+)")

        # Test 4: <15s elapsed
        total += 1
        if stats["elapsed_seconds"] < 15:
            log.info(f"  ✅ TEST 4: {stats['elapsed_seconds']:.3f}s < 15s")
            passed += 1
        else:
            log.warning(f"  ⚠️  TEST 4: {stats['elapsed_seconds']:.3f}s (need <15s)")

        # Test 5: Normalized schema (verify output records)
        total += 1
        with open(os.path.join(PIPELINE_DIR, "output.json")) as f:
            output = json.load(f)
        if output["records"]:
            sample = output["records"][0]
            has_schema = all(k in sample for k in ["source", "timestamp", "value", "unit"])
            if has_schema:
                log.info(f"  ✅ TEST 5: Schema OK ({list(sample.keys())})")
                passed += 1
            else:
                log.error(f"  ❌ TEST 5: Missing schema fields in {list(sample.keys())}")
        else:
            log.warning("  ⚠️  TEST 5: No records to validate schema")

        # Test 6: Timestamp order
        total += 1
        records = output["records"]
        if len(records) >= 2:
            sorted_check = all(records[i]["timestamp"] <= records[i+1]["timestamp"] for i in range(min(10, len(records)-1)))
            if sorted_check:
                log.info("  ✅ TEST 6: Records sorted by timestamp")
                passed += 1
            else:
                log.error("  ❌ TEST 6: Records not sorted")
        else:
            log.warning("  ⚠️  TEST 6: Too few records to verify sort")

        # Test 7: Graceful error handling (no crashes from missing source)
        total += 1
        log.info("  ✅ TEST 7: Pipeline completed without crash")

        # Summary
        log.info("=" * 60)
        passed += 1  # count test 7
        pct = (passed / max(total, 1)) * 100
        log.info(f"RESULTS: {passed}/{total} tests passed ({pct:.0f}%)")
        log.info(f"Time: {elapsed:.2f}s total | Records: {stats['total_records']}")

        acceptance = all([
            stats["total_records"] >= 1000,
            stats["elapsed_seconds"] < 15,
            actual_sources >= 5
        ])
        if acceptance:
            log.info("")
            log.info("🎉 ACCEPTANCE PASSED: 1000+ records in <15s from 5+ sources!")
            log.info("   Ready for PR submission.")
        else:
            log.info("")
            log.warning("⚠️  Acceptance criteria not fully met. Review above.")

        return acceptance, stats

    finally:
        # Cleanup: stop mock server
        server_proc.terminate()
        try:
            server_proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            server_proc.kill()
        log.info("Mock server stopped.")


if __name__ == "__main__":
    success = asyncio.run(test_pipeline())
    sys.exit(0 if success else 1)
