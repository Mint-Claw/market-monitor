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
        assert records[i].timestamp >= records[i - 1].timestamp


def test_performance_under_15s(config):
    pipe = Pipeline.from_config(config)
    t0 = time.perf_counter()
    records, stats = pipe.run_sync()
    elapsed = time.perf_counter() - t0
    assert elapsed < 15.0
    assert stats.total_records >= 1000


def test_json_output(config, tmp_path):
    pipe = Pipeline.from_config(config)
    records, stats = pipe.run_sync()
    out_path = str(tmp_path / "output.json")
    text = Pipeline.to_json(records, stats, out_path)
    data = json.loads(text)
    assert "records" in data
    assert "stats" in data
    assert len(data["records"]) == stats.total_records


def test_graceful_failure_handling(config):
    bad_config = dict(config)
    bad_config["sources"] = list(config["sources"]) + [{
        "name": "bad-source", "kind": "rest",
        "url": "http://127.0.0.1:1/nonexistent", "timeout": 2,
    }]
    pipe = Pipeline.from_config(bad_config)
    records, stats = pipe.run_sync()
    assert stats.sources_ok == 5
    assert stats.sources_failed == 1
    assert len(stats.errors) == 1
    assert stats.total_records >= 1000


def test_incremental_updates(config, tmp_path):
    state_file = str(tmp_path / "state.json")
    inc_config = dict(config)
    inc_config["state_path"] = state_file

    pipe1 = Pipeline.from_config(inc_config)
    _, stats1 = pipe1.run_sync()
    assert stats1.total_records >= 1000

    pipe2 = Pipeline.from_config(inc_config)
    _, stats2 = pipe2.run_sync()
    assert stats2.total_records == 0


def test_from_config_file(config, tmp_path):
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))
    pipe = Pipeline.from_config_file(str(config_path))
    assert len(pipe.sources) == 5


def test_custom_field_mapping(mock_server):
    """When record has unit field, it should be used; default_unit is fallback."""
    cfg = {
        "sources": [{
            "name": "custom-rest", "kind": "rest",
            "url": f"{mock_server}/api/rest?limit=10",
            "params": {"default_unit": "BTC"},
        }]
    }
    pipe = Pipeline.from_config(cfg)
    records, stats = pipe.run_sync()
    assert stats.sources_ok == 1
    # Mock data has "unit": "USD", so records use that (not the default)
    assert all(r.unit == "USD" for r in records)
