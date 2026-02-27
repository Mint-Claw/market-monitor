"""WebSocket source — connects, collects N messages, disconnects."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any

import websockets

from sources.base import BaseSource, register_source

logger = logging.getLogger(__name__)


@register_source("websocket")
class WebSocketSource(BaseSource):
    """Collect a batch of messages from a WebSocket stream."""

    async def fetch(self) -> list[dict[str, Any]]:
        url = self.config["url"]
        max_records = self.config.get("max_records", 50)
        connect_timeout = self.config.get("connect_timeout_seconds", 10)
        recv_timeout = self.config.get("recv_timeout_seconds", 2)

        logger.info("WS    CONNECT %s (max=%d, connect_timeout=%ds)", url, max_records, connect_timeout)
        records: list[dict[str, Any]] = []

        ws = None
        try:
            # Use explicit connection parameters for better reliability
            ws = await asyncio.wait_for(
                websockets.connect(
                    url,
                    ping_interval=None,  # Disable ping to avoid issues with short connections
                    close_timeout=2,
                ),
                timeout=connect_timeout
            )
            logger.info("WS    %s connected, receiving messages...", self.name)
            
            for i in range(max_records):
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=recv_timeout)
                    data = json.loads(raw)
                    records.append(data)
                    if i == 0:
                        logger.info("WS    %s first message received", self.name)
                except asyncio.TimeoutError:
                    if records:
                        logger.info("WS    %s recv timeout after %d messages (OK)", self.name, len(records))
                    else:
                        logger.warning("WS    %s recv timeout with 0 messages", self.name)
                    break
                except json.JSONDecodeError as e:
                    logger.warning("WS    %s invalid JSON: %s", self.name, e)
                    continue
        except asyncio.TimeoutError:
            logger.warning("WS    %s connect timeout (no messages)", self.name)
        except websockets.exceptions.WebSocketException as exc:
            logger.warning("WS    %s websocket error: %s", self.name, exc)
        except Exception as exc:
            logger.warning("WS    %s error after %d messages: %s", self.name, len(records), exc)
        finally:
            if ws:
                try:
                    await ws.close()
                except Exception:
                    pass  # Ignore close errors

        logger.info("WS    %s -> %d records", self.name, len(records))
        return records

    def normalize(self, raw_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Override: extract Binance-specific fields into a cleaner value."""
        results = []
        for rec in raw_records:
            # Binance uses "T" for trade time in epoch-ms
            ts_ms = rec.get("T") or rec.get("t") or rec.get("E")
            if ts_ms and isinstance(ts_ms, (int, float)):
                ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()
            else:
                ts = datetime.now(timezone.utc).isoformat()

            value = {
                "symbol": rec.get("s", ""),
                "price": rec.get("p", ""),
                "quantity": rec.get("q", ""),
                "trade_id": rec.get("t", ""),
            }
            results.append({
                "source": self.name,
                "timestamp": ts,
                "value": value,
                "unit": self.unit,
            })
        return results
