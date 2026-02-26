"""Async pipeline engine orchestrating fetch → normalize → merge."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from .merger import Merger
from .normalizer import Normalizer
from .schema import NormalizedRecord, PipelineResult
from .sources.base import DataSource, SourceError

logger = logging.getLogger(__name__)


class PipelineEngine:
    """Orchestrates concurrent data fetching, normalization, and merging.

    Usage::

        engine = PipelineEngine()
        engine.add_source(RESTSource("prices", url="https://..."))
        engine.add_source(CSVSource("history", path="data.csv"))
        result = await engine.run()

    Args:
        normalizer: Custom :class:`Normalizer` instance (optional).
        merger: Custom :class:`Merger` instance (optional).
        max_concurrency: Limit on simultaneous source fetches.
    """

    def __init__(
        self,
        *,
        normalizer: Normalizer | None = None,
        merger: Merger | None = None,
        max_concurrency: int = 10,
    ) -> None:
        self._sources: list[DataSource] = []
        self._normalizer = normalizer or Normalizer()
        self._merger = merger or Merger()
        self._semaphore = asyncio.Semaphore(max_concurrency)

    # -- configuration -------------------------------------------------------

    def add_source(self, source: DataSource) -> None:
        """Register a data source."""
        self._sources.append(source)
        logger.debug("Registered source %s", source.name)

    @property
    def normalizer(self) -> Normalizer:
        return self._normalizer

    # -- execution -----------------------------------------------------------

    async def _fetch_one(
        self, source: DataSource
    ) -> tuple[str, list[dict[str, Any]] | None, str | None]:
        """Fetch from a single source, respecting the concurrency semaphore."""
        async with self._semaphore:
            try:
                raw = await source.fetch()
                return source.name, raw, None
            except SourceError as exc:
                logger.error("Source %s failed: %s", source.name, exc)
                return source.name, None, str(exc)
            except Exception as exc:
                logger.exception("Unexpected error in source %s", source.name)
                return source.name, None, f"Unexpected: {exc}"

    async def run(self) -> PipelineResult:
        """Execute the full pipeline: fetch → normalize → merge.

        All sources are fetched concurrently (up to *max_concurrency*).
        Failures in individual sources do not abort the pipeline.

        Returns:
            A :class:`PipelineResult` with merged records and diagnostics.
        """
        if not self._sources:
            logger.warning("No sources registered — nothing to do")
            return PipelineResult()

        t0 = time.monotonic()
        logger.info("Pipeline run starting with %d sources", len(self._sources))

        # 1. Concurrent fetch
        tasks = [self._fetch_one(src) for src in self._sources]
        results = await asyncio.gather(*tasks)

        # 2. Normalize each successful batch
        all_batches: list[list[NormalizedRecord]] = []
        errors: list[str] = []
        sources_ok = 0
        sources_failed = 0
        total_raw = 0

        for name, raw, err in results:
            if err is not None:
                errors.append(err)
                sources_failed += 1
                continue
            assert raw is not None
            total_raw += len(raw)
            sources_ok += 1
            normalized = self._normalizer.normalize(raw, name)
            all_batches.append(normalized)

        total_normalized = sum(len(b) for b in all_batches)

        # 3. Merge
        merged = self._merger.merge(*all_batches) if all_batches else []

        elapsed = time.monotonic() - t0
        logger.info(
            "Pipeline finished in %.2fs — %d sources OK, %d failed, "
            "%d raw → %d normalized → %d merged",
            elapsed, sources_ok, sources_failed,
            total_raw, total_normalized, len(merged),
        )

        return PipelineResult(
            records=merged,
            errors=errors,
            sources_ok=sources_ok,
            sources_failed=sources_failed,
            total_raw=total_raw,
            total_normalized=total_normalized,
            total_merged=len(merged),
        )
