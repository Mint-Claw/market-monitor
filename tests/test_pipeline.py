"""Integration tests for the multi-source pipeline."""
import json
import time
from pipeline import Pipeline, NormalizedRecord


def test_all_five_sources(config):
    pipe = Pipeline.from_config(config)
    records, stats = pipe.run_sync()
    assert stats.sources_ok == 5, f"Failed sources: {stats.errors}"
    assert stats.sources_failed == 0
    assert stats.total_records >= 1000


def test_normalization_schema(config):
    pipe = Pipeline.from_config(config)
    records, _ = pipe.run_sync()
    for r in records:
        assert isinstance(r, NormalizedRecord)
        assert r.source and r.timestamp and r.unit
        assert isinstance(r.value, (int, float))


def test_sorted_by_timestamp(config):
    pipe = Pipeline.from_config(config)
    records, _ = pipe.run_sync()
    for i in range(1, len(records)):
        assert records[i].timestamp >= records[i-1].timestamp


def test_performance_under_15s(config):
    t0 = time.perf_counter()
    pipe = Pipeline.from_config(config)
    records, stats = pipe.run_sync()
    assert (time.perf_counter() - t0) < 15.0
    assert stats.total_records >= 1000


def test_json_output(config, tmp_path):
    pipe = Pipeline.from_config(config)
    records, stats = pipe.run_sync()
    out = str(tmp_path / "output.json")
    text = Pipeline.to_json(records, stats, out)
    data = json.loads(text)
    assert "records" in data and "stats" in data
    assert len(data["records"]) == stats.total_records


def test_graceful_failure(config):
    cfg = {"sources": config["sources"] + [
        {"name": "bad", "kind": "rest", "url": "http://127.0.0.1:1/nope", "timeout": 2}
    ]}
    pipe = Pipeline.from_config(cfg)
    records, stats = pipe.run_sync()
    assert stats.sources_ok == 5
    assert stats.sources_failed == 1
    assert stats.total_records >= 1000


def test_incremental(config, tmp_path):
    cfg = {**config, "state_path": str(tmp_path / "state.json")}
    pipe1 = Pipeline.from_config(cfg)
    r1, s1 = pipe1.run_sync()
    assert s1.total_records >= 1000

    pipe2 = Pipeline.from_config(cfg)
    r2, s2 = pipe2.run_sync()
    assert s2.total_records == 0


def test_from_config_file(config, tmp_path):
    p = tmp_path / "config.json"
    p.write_text(json.dumps(config))
    pipe = Pipeline.from_config_file(str(p))
    assert len(pipe.sources) == 5


def test_custom_unit(server):
    """default_unit applies when record has no unit field; mock data has unit=USD so it should use that."""
    cfg = {"sources": [{"name": "custom", "kind": "rest",
           "url": f"{server}/api/rest?limit=10",
           "params": {"default_unit": "BTC"}}]}
    pipe = Pipeline.from_config(cfg)
    records, stats = pipe.run_sync()
    assert stats.sources_ok == 1
    # Mock records have unit=USD, so default_unit doesn't override
    assert all(r.unit == "USD" for r in records)
