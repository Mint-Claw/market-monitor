"""Comprehensive tests for the data aggregation pipeline."""

from __future__ import annotations

import asyncio
import json
import textwrap
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from pipeline.engine import PipelineEngine
from pipeline.merger import Merger, prefer_latest
from pipeline.normalizer import Normalizer, default_normalizer
from pipeline.schema import NormalizedRecord, PipelineResult, RecordType
from pipeline.sources.base import DataSource, SourceError
from pipeline.sources.csv_source import CSVSource
from pipeline.sources.file import FileSource
from pipeline.sources.graphql import GraphQLSource
from pipeline.sources.rest import RESTSource
from pipeline.sources.websocket import WebSocketSource


# ---------------------------------------------------------------------------
# Helpers / Fixtures
# ---------------------------------------------------------------------------

class MockSource(DataSource):
    """In-memory source for testing."""

    def __init__(self, name: str, records: list[dict[str, Any]], *, fail: bool = False) -> None:
        super().__init__(name)
        self._records = records
        self._fail = fail

    async def fetch(self) -> list[dict[str, Any]]:
        if self._fail:
            raise SourceError(self.name, "simulated failure")
        return self._records


SAMPLE_RECORDS_A = [
    {"id": "1", "symbol": "BTC", "price": "50000.0", "timestamp": "2025-01-01T00:00:00", "volume": "1.5"},
    {"id": "2", "symbol": "ETH", "price": "3000.0", "timestamp": "2025-01-01T01:00:00", "volume": "10"},
]

SAMPLE_RECORDS_B = [
    {"id": "1", "symbol": "BTC", "price": "50100.0", "timestamp": "2025-01-01T00:05:00", "volume": "2.0"},
    {"id": "3", "symbol": "SOL", "price": "120.0", "timestamp": "2025-01-01T02:00:00"},
]


@pytest.fixture
def tmp_csv(tmp_path: Path) -> Path:
    p = tmp_path / "data.csv"
    p.write_text("symbol,price,volume,timestamp\nBTC,50000,1.5,2025-01-01\nETH,3000,10,2025-01-01\n")
    return p


@pytest.fixture
def tmp_json(tmp_path: Path) -> Path:
    p = tmp_path / "data.json"
    p.write_text(json.dumps({"items": SAMPLE_RECORDS_A}))
    return p


@pytest.fixture
def tmp_xml(tmp_path: Path) -> Path:
    p = tmp_path / "data.xml"
    p.write_text(textwrap.dedent("""\
        <root>
          <record id="1"><symbol>BTC</symbol><price>50000</price></record>
          <record id="2"><symbol>ETH</symbol><price>3000</price></record>
        </root>
    """))
    return p


# ---------------------------------------------------------------------------
# Schema Tests
# ---------------------------------------------------------------------------

class TestSchema:
    def test_normalized_record_creation(self) -> None:
        rec = NormalizedRecord(id="test_1", source="test", value=100.0)
        assert rec.id == "test_1"
        assert rec.source == "test"
        assert rec.record_type == RecordType.OTHER
        assert rec.value == 100.0

    def test_merge_key(self) -> None:
        rec = NormalizedRecord(id="abc", source="src")
        assert rec.merge_key() == "src:abc"

    def test_record_type_enum(self) -> None:
        assert RecordType.PRICE.value == "price"
        assert RecordType.TRADE.value == "trade"

    def test_pipeline_result_defaults(self) -> None:
        pr = PipelineResult()
        assert pr.records == []
        assert pr.errors == []
        assert pr.sources_ok == 0


# ---------------------------------------------------------------------------
# Normalizer Tests
# ---------------------------------------------------------------------------

class TestNormalizer:
    def test_default_normalizer_basic(self) -> None:
        raw = {"id": "1", "symbol": "BTC", "price": "50000", "timestamp": "2025-01-01"}
        rec = default_normalizer(raw, "test")
        assert rec is not None
        assert rec.id == "test_1"
        assert rec.symbol == "BTC"
        assert rec.value == 50000.0
        assert rec.source == "test"

    def test_default_normalizer_detects_price_type(self) -> None:
        raw = {"price": "100", "symbol": "X"}
        rec = default_normalizer(raw, "s")
        assert rec is not None
        assert rec.record_type == RecordType.PRICE

    def test_default_normalizer_detects_trade_type(self) -> None:
        raw = {"trade": "buy", "side": "long"}
        rec = default_normalizer(raw, "s")
        assert rec is not None
        assert rec.record_type == RecordType.TRADE

    def test_default_normalizer_epoch_timestamp(self) -> None:
        raw = {"id": "1", "timestamp": "1704067200"}
        rec = default_normalizer(raw, "s")
        assert rec is not None
        assert rec.timestamp.year == 2024

    def test_default_normalizer_epoch_ms(self) -> None:
        raw = {"id": "1", "timestamp": "1704067200000"}
        rec = default_normalizer(raw, "s")
        assert rec is not None
        assert rec.timestamp.year == 2024

    def test_default_normalizer_metadata_passthrough(self) -> None:
        raw = {"id": "1", "price": "10", "custom_field": "hello"}
        rec = default_normalizer(raw, "s")
        assert rec is not None
        assert rec.metadata == {"custom_field": "hello"}

    def test_normalizer_registry(self) -> None:
        n = Normalizer()
        custom_called = []

        def custom_fn(raw: dict, source: str) -> NormalizedRecord:
            custom_called.append(True)
            return NormalizedRecord(id="custom", source=source, value=999.0)

        n.register("special", custom_fn)
        results = n.normalize([{"x": 1}], "special")
        assert len(results) == 1
        assert results[0].value == 999.0
        assert custom_called

    def test_normalizer_fallback_to_default(self) -> None:
        n = Normalizer()
        results = n.normalize([{"id": "1", "price": "42"}], "unknown_source")
        assert len(results) == 1
        assert results[0].value == 42.0

    def test_normalizer_skips_bad_records(self) -> None:
        n = Normalizer()

        def bad_fn(raw: dict, source: str) -> None:
            return None

        n.register("bad", bad_fn)
        results = n.normalize([{"a": 1}, {"b": 2}], "bad")
        assert results == []


# ---------------------------------------------------------------------------
# Merger Tests
# ---------------------------------------------------------------------------

class TestMerger:
    def test_merge_no_duplicates(self) -> None:
        m = Merger()
        batch = [
            NormalizedRecord(id="1", source="a", timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc)),
            NormalizedRecord(id="2", source="a", timestamp=datetime(2025, 1, 2, tzinfo=timezone.utc)),
        ]
        merged = m.merge(batch)
        assert len(merged) == 2

    def test_merge_deduplicates(self) -> None:
        m = Merger()
        r1 = NormalizedRecord(id="1", source="a", timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc), value=10.0)
        r2 = NormalizedRecord(id="1", source="a", timestamp=datetime(2025, 1, 2, tzinfo=timezone.utc), value=20.0)
        merged = m.merge([r1], [r2])
        assert len(merged) == 1
        assert merged[0].value == 20.0

    def test_merge_multiple_batches(self) -> None:
        m = Merger()
        b1 = [NormalizedRecord(id="1", source="a", timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc))]
        b2 = [NormalizedRecord(id="2", source="b", timestamp=datetime(2025, 1, 2, tzinfo=timezone.utc))]
        b3 = [NormalizedRecord(id="3", source="c", timestamp=datetime(2025, 1, 3, tzinfo=timezone.utc))]
        merged = m.merge(b1, b2, b3)
        assert len(merged) == 3

    def test_merge_sorted_by_timestamp(self) -> None:
        m = Merger()
        late = NormalizedRecord(id="1", source="a", timestamp=datetime(2025, 6, 1, tzinfo=timezone.utc))
        early = NormalizedRecord(id="2", source="a", timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc))
        merged = m.merge([late, early])
        assert merged[0].id == "2"
        assert merged[1].id == "1"

    def test_prefer_latest_resolver(self) -> None:
        old = NormalizedRecord(id="x", source="s", timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc))
        new = NormalizedRecord(id="x", source="s", timestamp=datetime(2025, 6, 1, tzinfo=timezone.utc))
        assert prefer_latest(old, new) is new
        assert prefer_latest(new, old) is new

    def test_custom_conflict_resolver(self) -> None:
        def prefer_first(a: NormalizedRecord, b: NormalizedRecord) -> NormalizedRecord:
            return a

        m = Merger(conflict_resolver=prefer_first)
        r1 = NormalizedRecord(id="1", source="a", timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc), value=10.0)
        r2 = NormalizedRecord(id="1", source="a", timestamp=datetime(2025, 6, 1, tzinfo=timezone.utc), value=99.0)
        merged = m.merge([r1, r2])
        assert merged[0].value == 10.0


# ---------------------------------------------------------------------------
# Source Tests
# ---------------------------------------------------------------------------

class TestCSVSource:
    @pytest.mark.asyncio
    async def test_csv_local_file(self, tmp_csv: Path) -> None:
        src = CSVSource("csv_test", str(tmp_csv))
        records = await src.fetch()
        assert len(records) == 2
        assert records[0]["symbol"] == "BTC"
        assert records[1]["symbol"] == "ETH"

    @pytest.mark.asyncio
    async def test_csv_custom_delimiter(self, tmp_path: Path) -> None:
        p = tmp_path / "semi.csv"
        p.write_text("a;b;c\n1;2;3\n4;5;6\n")
        src = CSVSource("semi", str(p), delimiter=";")
        records = await src.fetch()
        assert len(records) == 2
        assert records[0]["a"] == "1"


class TestFileSource:
    @pytest.mark.asyncio
    async def test_json_with_results_key(self, tmp_json: Path) -> None:
        src = FileSource("json_test", str(tmp_json), results_key="items")
        records = await src.fetch()
        assert len(records) == 2
        assert records[0]["symbol"] == "BTC"

    @pytest.mark.asyncio
    async def test_json_single_object(self, tmp_path: Path) -> None:
        p = tmp_path / "single.json"
        p.write_text(json.dumps({"id": "1", "value": 42}))
        src = FileSource("single", str(p))
        records = await src.fetch()
        assert len(records) == 1
        assert records[0]["id"] == "1"

    @pytest.mark.asyncio
    async def test_xml_parsing(self, tmp_xml: Path) -> None:
        src = FileSource("xml_test", str(tmp_xml), format="xml", xml_record_tag="record")
        records = await src.fetch()
        assert len(records) == 2
        assert records[0]["symbol"] == "BTC"
        assert records[0]["id"] == "1"

    @pytest.mark.asyncio
    async def test_missing_file_raises(self) -> None:
        src = FileSource("missing", "/nonexistent/file.json")
        with pytest.raises(SourceError):
            await src.fetch()

    @pytest.mark.asyncio
    async def test_auto_detect_xml(self, tmp_xml: Path) -> None:
        src = FileSource("auto_xml", str(tmp_xml))
        assert src.format == "xml"

    @pytest.mark.asyncio
    async def test_auto_detect_json(self, tmp_json: Path) -> None:
        src = FileSource("auto_json", str(tmp_json))
        assert src.format == "json"


class TestMockSource:
    @pytest.mark.asyncio
    async def test_mock_source_returns_records(self) -> None:
        src = MockSource("mock", [{"a": 1}, {"b": 2}])
        records = await src.fetch()
        assert len(records) == 2

    @pytest.mark.asyncio
    async def test_mock_source_failure(self) -> None:
        src = MockSource("fail", [], fail=True)
        with pytest.raises(SourceError):
            await src.fetch()

    @pytest.mark.asyncio
    async def test_healthcheck_success(self) -> None:
        src = MockSource("ok", [{"x": 1}])
        assert await src.healthcheck() is True

    @pytest.mark.asyncio
    async def test_healthcheck_failure(self) -> None:
        src = MockSource("bad", [], fail=True)
        assert await src.healthcheck() is False


# ---------------------------------------------------------------------------
# Engine Integration Tests
# ---------------------------------------------------------------------------

class TestPipelineEngine:
    @pytest.mark.asyncio
    async def test_full_pipeline_with_mock_sources(self) -> None:
        engine = PipelineEngine()
        engine.add_source(MockSource("src_a", SAMPLE_RECORDS_A))
        engine.add_source(MockSource("src_b", SAMPLE_RECORDS_B))
        result = await engine.run()

        assert result.sources_ok == 2
        assert result.sources_failed == 0
        assert result.total_raw == 4
        assert result.total_normalized == 4
        assert result.total_merged == 4  # different source prefixes = unique keys
        assert len(result.records) == 4
        assert result.errors == []

    @pytest.mark.asyncio
    async def test_pipeline_handles_source_failure(self) -> None:
        engine = PipelineEngine()
        engine.add_source(MockSource("good", SAMPLE_RECORDS_A))
        engine.add_source(MockSource("bad", [], fail=True))
        result = await engine.run()

        assert result.sources_ok == 1
        assert result.sources_failed == 1
        assert len(result.errors) == 1
        assert result.total_merged == 2

    @pytest.mark.asyncio
    async def test_pipeline_all_sources_fail(self) -> None:
        engine = PipelineEngine()
        engine.add_source(MockSource("f1", [], fail=True))
        engine.add_source(MockSource("f2", [], fail=True))
        result = await engine.run()

        assert result.sources_ok == 0
        assert result.sources_failed == 2
        assert result.total_merged == 0
        assert len(result.errors) == 2

    @pytest.mark.asyncio
    async def test_pipeline_no_sources(self) -> None:
        engine = PipelineEngine()
        result = await engine.run()
        assert result.total_merged == 0
        assert result.sources_ok == 0

    @pytest.mark.asyncio
    async def test_pipeline_with_custom_normalizer(self) -> None:
        normalizer = Normalizer()

        def custom(raw: dict, source: str) -> NormalizedRecord:
            return NormalizedRecord(
                id=f"{source}_{raw.get('id', 'x')}",
                source=source,
                value=float(raw.get("price", 0)) * 2,
                symbol=raw.get("symbol"),
            )

        normalizer.register("doubled", custom)
        engine = PipelineEngine(normalizer=normalizer)
        engine.add_source(MockSource("doubled", [{"id": "1", "price": "100", "symbol": "X"}]))
        result = await engine.run()

        assert result.total_merged == 1
        assert result.records[0].value == 200.0

    @pytest.mark.asyncio
    async def test_pipeline_with_file_sources(self, tmp_csv: Path, tmp_json: Path, tmp_xml: Path) -> None:
        engine = PipelineEngine()
        engine.add_source(CSVSource("csv", str(tmp_csv)))
        engine.add_source(FileSource("json", str(tmp_json), results_key="items"))
        engine.add_source(FileSource("xml", str(tmp_xml), format="xml"))
        result = await engine.run()

        assert result.sources_ok == 3
        assert result.sources_failed == 0
        assert result.total_raw == 6
        assert result.total_merged == 6

    @pytest.mark.asyncio
    async def test_pipeline_concurrency_limit(self) -> None:
        engine = PipelineEngine(max_concurrency=2)
        for i in range(5):
            engine.add_source(MockSource(f"s{i}", [{"id": str(i), "value": str(i)}]))
        result = await engine.run()
        assert result.sources_ok == 5
        assert result.total_merged == 5

    @pytest.mark.asyncio
    async def test_dedup_same_source_same_id(self) -> None:
        """Two records with same source+id should deduplicate."""
        engine = PipelineEngine()
        records = [
            {"id": "1", "price": "100", "timestamp": "2025-01-01"},
            {"id": "1", "price": "200", "timestamp": "2025-06-01"},
        ]
        engine.add_source(MockSource("dup", records))
        result = await engine.run()
        assert result.total_raw == 2
        assert result.total_normalized == 2
        assert result.total_merged == 1
        assert result.records[0].value == 200.0  # prefer latest
