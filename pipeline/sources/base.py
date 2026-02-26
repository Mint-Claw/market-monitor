"""Abstract base class for all data sources."""

from __future__ import annotations

import abc
import logging
from typing import Any

logger = logging.getLogger(__name__)


class DataSource(abc.ABC):
    """Abstract interface every data source must implement.

    Subclasses provide a ``name`` and implement :meth:`fetch` which returns
    raw records as a list of dicts.
    """

    def __init__(self, name: str, **kwargs: Any) -> None:
        self.name = name
        self._config = kwargs

    @abc.abstractmethod
    async def fetch(self) -> list[dict[str, Any]]:
        """Fetch raw records from the source.

        Returns:
            A list of raw dictionaries, each representing one record.

        Raises:
            SourceError: If the fetch fails after retries.
        """

    async def healthcheck(self) -> bool:
        """Return ``True`` if the source is reachable."""
        try:
            await self.fetch()
            return True
        except Exception:
            logger.warning("Healthcheck failed for source %s", self.name)
            return False

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} name={self.name!r}>"


class SourceError(Exception):
    """Raised when a data source fails to fetch data."""

    def __init__(self, source_name: str, message: str) -> None:
        self.source_name = source_name
        super().__init__(f"[{source_name}] {message}")
