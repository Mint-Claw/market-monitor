"""Merge and deduplicate normalized records from multiple sources."""

from __future__ import annotations

import logging
from typing import Callable

from .schema import NormalizedRecord

logger = logging.getLogger(__name__)

# Conflict resolver: given two records with the same merge key, pick one.
ConflictResolver = Callable[[NormalizedRecord, NormalizedRecord], NormalizedRecord]


def prefer_latest(a: NormalizedRecord, b: NormalizedRecord) -> NormalizedRecord:
    """Default resolver: keep the record with the later timestamp."""
    return a if a.timestamp >= b.timestamp else b


class Merger:
    """Merge multiple lists of :class:`NormalizedRecord` into one deduplicated set.

    Args:
        conflict_resolver: Strategy for picking a winner when two records
            share the same :meth:`~NormalizedRecord.merge_key`.
    """

    def __init__(
        self,
        conflict_resolver: ConflictResolver | None = None,
    ) -> None:
        self._resolver = conflict_resolver or prefer_latest

    def merge(self, *batches: list[NormalizedRecord]) -> list[NormalizedRecord]:
        """Merge one or more batches, deduplicating by merge key.

        Returns:
            A single deduplicated list ordered by timestamp ascending.
        """
        seen: dict[str, NormalizedRecord] = {}
        duplicates = 0

        for batch in batches:
            for record in batch:
                key = record.merge_key()
                if key in seen:
                    seen[key] = self._resolver(seen[key], record)
                    duplicates += 1
                else:
                    seen[key] = record

        merged = sorted(seen.values(), key=lambda r: r.timestamp)
        logger.info(
            "Merged %d records (%d duplicates resolved)",
            len(merged),
            duplicates,
        )
        return merged
