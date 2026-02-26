"""WebSocket streaming data source."""

from __future__ import annotations

import json
import logging
from typing import Any

try:
    import websockets
except ImportError:  # pragma: no cover
    websockets = None  # type: ignore[assignment]

from .base import DataSource, SourceError

logger = logging.getLogger(__name__)


class WebSocketSource(DataSource):
    """Connect to a WebSocket endpoint and collect messages.

    Args:
        name: Human-readable source identifier.
        url: The ``ws://`` or ``wss://`` endpoint.
        subscribe_msg: Optional JSON-serialisable message sent on connect
            (e.g. a subscription frame).
        max_messages: Stop after receiving this many messages.
        timeout: Seconds to wait for each message before giving up.
    """

    def __init__(
        self,
        name: str,
        url: str,
        *,
        subscribe_msg: dict[str, Any] | None = None,
        max_messages: int = 100,
        timeout: float = 10.0,
    ) -> None:
        super().__init__(name)
        self.url = url
        self.subscribe_msg = subscribe_msg
        self.max_messages = max_messages
        self.timeout = timeout

    async def fetch(self) -> list[dict[str, Any]]:
        """Open the socket, optionally subscribe, and collect messages."""
        records: list[dict[str, Any]] = []
        try:
            async with websockets.connect(self.url) as ws:
                if self.subscribe_msg:
                    await ws.send(json.dumps(self.subscribe_msg))

                for _ in range(self.max_messages):
                    try:
                        raw = await asyncio_wait_for(
                            ws.recv(), timeout=self.timeout
                        )
                    except TimeoutError:
                        break

                    data = json.loads(raw)
                    if isinstance(data, list):
                        records.extend(data)
                    else:
                        records.append(data)

            logger.info(
                "WebSocket source %s collected %d records", self.name, len(records)
            )
            return records

        except Exception as exc:
            raise SourceError(self.name, f"WebSocket error: {exc}") from exc


async def asyncio_wait_for(coro, *, timeout: float):  # type: ignore[no-untyped-def]
    """Thin wrapper around asyncio.wait_for for testability."""
    import asyncio
    return await asyncio.wait_for(coro, timeout=timeout)
