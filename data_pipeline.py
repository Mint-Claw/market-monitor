#!/usr/bin/env python3
"""
Multi-Source Data Aggregation & Normalization Pipeline.

Fetches data concurrently from 5+ source types (REST, CSV, WebSocket, GraphQL, File),
normalizes all items to a common schema {source, timestamp, value, unit},
and outputs merged, chronologically sorted JSON with execution statistics.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
from datetime import datetime, timezone
from io import StringIO
import json
import logging
from pathlib import Path
import time
from typing import Any, Dict, List, Optional, Union

import aiohttp
import websockets

logger = logging.getLogger("data_pipeline")


def normalize_timestamp(val: Any) -> str:
    """Normalize any timestamp (unix seconds, ms, us, ns, ISO 8601 string, datetime) to UTC ISO 8601 string."""
    if val is None or val == "":
        raise ValueError("Timestamp cannot be None or empty")

    # If it's a numeric string, convert to float
    if isinstance(val, str):
        val_str = val.strip()
        try:
            val = float(val_str)
        except ValueError:
            pass

    # Numeric unix timestamp
    if isinstance(val, (int, float)):
        # Check scale:
        # > 1e16 -> nanoseconds (e.g. 1.7e18)
        # > 1e13 -> microseconds (e.g. 1.7e15)
        # > 1e11 -> milliseconds (e.g. 1.7e12)
        # <= 1e11 -> seconds (e.g. 1.7e9)
        if val > 1e16:
            val = val / 1e9
        elif val > 1e13:
            val = val / 1e6
        elif val > 1e11:
            val = val / 1000.0

        dt = datetime.fromtimestamp(val, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # ISO string or date string
    if isinstance(val, str):
        val_str = val.strip()
        if val_str.endswith("Z"):
            val_clean = val_str[:-1] + "+00:00"
        else:
            val_clean = val_str

        if " " in val_clean and "T" not in val_clean:
            val_clean = val_clean.replace(" ", "T")

        dt = datetime.fromisoformat(val_clean)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    if isinstance(val, datetime):
        dt = val if val.tzinfo is not None else val.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    raise ValueError(f"Unrecognized timestamp format: {val}")


def normalize_value(val: Any) -> Any:
    """Convert value to int, float, or preserved string."""
    if val is None or val == "":
        return 0.0
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return val
    if isinstance(val, str):
        val_str = val.strip().replace(",", "")
        val_clean = val_str.lstrip("$€£¥ ")
        try:
            if "." in val_clean or "e" in val_clean.lower():
                return float(val_clean)
            return int(val_clean)
        except ValueError:
            return val_str
    return val


def _extract_field_value(record: Dict[str, Any], field_path: Optional[str]) -> Any:
    """Retrieve field value supporting dot notation (e.g. 'quote.price' or 'meta.ts')."""
    if not field_path:
        return None
    if field_path in record:
        return record[field_path]
    if "." in field_path:
        current = record
        for part in field_path.split("."):
            if isinstance(current, dict) and part in current:
                current = current[part]
            elif isinstance(current, list) and part.isdigit() and int(part) < len(current):
                current = current[int(part)]
            else:
                return None
        return current
    return None


class DataPipeline:
    def __init__(
        self,
        config: Union[Dict[str, Any], str, Path],
        incremental_store: Optional[str] = None,
        timeout: float = 10.0,
        max_retries: int = 2,
        retry_backoff: float = 0.2,
    ):
        if isinstance(config, (str, Path)):
            with open(config, "r", encoding="utf-8") as f:
                self.config = json.load(f)
        else:
            self.config = config

        self.global_timeout = float(self.config.get("timeout", timeout))
        self.output_path = self.config.get("output") or self.config.get("output_path")
        self.max_retries = int(self.config.get("max_retries", max_retries))
        self.retry_backoff = float(self.config.get("retry_backoff", retry_backoff))

        # Incremental store path
        store_path = incremental_store or self.config.get("incremental_store") or self.config.get("state_file")
        self.incremental_store = Path(store_path) if store_path else None

    def _load_incremental_state(self) -> Dict[str, str]:
        if not self.incremental_store or not self.incremental_store.exists():
            return {}
        try:
            with open(self.incremental_store, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Failed to read incremental store: {e}")
            return {}

    def _save_incremental_state(self, state: Dict[str, str]) -> None:
        if not self.incremental_store:
            return
        try:
            self.incremental_store.parent.mkdir(parents=True, exist_ok=True)
            with open(self.incremental_store, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2, sort_keys=True)
        except OSError as e:
            logger.error(f"Failed to save incremental store: {e}")

    def _extract_path(self, data: Any, path: Optional[str]) -> List[Dict[str, Any]]:
        """Navigate nested dictionary/list using dot-notation (e.g. 'data.prices')."""
        if not path:
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                # Search for first list in dict values if not explicitly specified
                for v in data.values():
                    if isinstance(v, list):
                        return v
                return [data]
            return []

        tokens = [t.strip() for t in path.split(".") if t.strip()]
        current = data
        for token in tokens:
            if isinstance(current, dict) and token in current:
                current = current[token]
            elif isinstance(current, list) and token.isdigit() and int(token) < len(current):
                current = current[int(token)]
            else:
                return []

        if isinstance(current, list):
            return current
        if isinstance(current, dict):
            return [current]
        return []

    def _normalize_record(
        self,
        source_name: str,
        record: Dict[str, Any],
        mapping: Optional[Dict[str, str]],
        default_unit: Optional[str],
    ) -> Dict[str, Any]:
        """Strictly normalize a single record to {source, timestamp, value, unit}."""
        field_map = mapping or {}

        # 1. Resolve timestamp
        ts_field = field_map.get("timestamp")
        raw_ts = None
        if ts_field and ts_field in record:
            raw_ts = record[ts_field]
        else:
            for candidate in ("timestamp", "time", "date", "datetime", "ts", "t", "timestamp_utc"):
                if candidate in record:
                    raw_ts = record[candidate]
                    break
        if raw_ts is None:
            raise ValueError(f"Record missing timestamp field: {record}")
        norm_ts = normalize_timestamp(raw_ts)

        # 2. Resolve value
        val_field = field_map.get("value")
        raw_val = None
        if val_field and val_field in record:
            raw_val = record[val_field]
        else:
            for candidate in ("value", "val", "price", "amount", "count", "close", "v"):
                if candidate in record:
                    raw_val = record[candidate]
                    break
        norm_val = normalize_value(raw_val)

        # 3. Resolve unit
        unit_field = field_map.get("unit")
        raw_unit = None
        if unit_field and unit_field in record:
            raw_unit = record[unit_field]
        else:
            for candidate in ("unit", "currency", "metric", "u"):
                if candidate in record:
                    raw_unit = record[candidate]
                    break
        norm_unit = str(raw_unit or default_unit or "USD")

        return {
            "source": str(source_name),
            "timestamp": norm_ts,
            "value": norm_val,
            "unit": norm_unit,
        }

    def _source_timeout(self, source: Dict[str, Any]) -> float:
        return float(source.get("timeout", self.global_timeout))

    async def _fetch_rest(self, source: Dict[str, Any]) -> List[Dict[str, Any]]:
        timeout_sec = self._source_timeout(source)
        headers = source.get("headers", {})
        params = source.get("params")
        url = source["url"]
        data_path = source.get("data_path")

        last_error: Optional[Exception] = None
        timeout = aiohttp.ClientTimeout(total=timeout_sec)

        for attempt in range(self.max_retries + 1):
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(url, headers=headers, params=params) as resp:
                        resp.raise_for_status()
                        payload = await resp.json()
                        return self._extract_path(payload, data_path)
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
                last_error = exc
                if attempt == self.max_retries:
                    raise
                await asyncio.sleep(self.retry_backoff * (attempt + 1))

        if last_error:
            raise last_error
        return []

    async def _fetch_csv(self, source: Dict[str, Any]) -> List[Dict[str, Any]]:
        delimiter = source.get("delimiter", ",")
        content = ""
        if source.get("url"):
            timeout_sec = self._source_timeout(source)
            timeout = aiohttp.ClientTimeout(total=timeout_sec)
            last_error: Optional[Exception] = None
            for attempt in range(self.max_retries + 1):
                try:
                    async with aiohttp.ClientSession(timeout=timeout) as session:
                        async with session.get(source["url"]) as resp:
                            resp.raise_for_status()
                            content = await resp.text()
                            break
                except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
                    last_error = exc
                    if attempt == self.max_retries:
                        raise
                    await asyncio.sleep(self.retry_backoff * (attempt + 1))
            if not content and last_error:
                raise last_error
        elif source.get("path"):
            path = Path(source["path"])
            content = path.read_text(encoding="utf-8")
        else:
            raise ValueError("CSV source requires 'url' or 'path'")

        reader = csv.DictReader(StringIO(content), delimiter=delimiter)
        return [dict(row) for row in reader]

    async def _fetch_graphql(self, source: Dict[str, Any]) -> List[Dict[str, Any]]:
        timeout_sec = self._source_timeout(source)
        timeout = aiohttp.ClientTimeout(total=timeout_sec)
        query = source.get("query", "")
        variables = source.get("variables", {})
        headers = source.get("headers", {})
        url = source["url"]
        data_path = source.get("data_path")

        last_error: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.post(
                        url,
                        json={"query": query, "variables": variables},
                        headers=headers,
                    ) as resp:
                        resp.raise_for_status()
                        body = await resp.json()
                        data = body.get("data", body)
                        return self._extract_path(data, data_path)
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
                last_error = exc
                if attempt == self.max_retries:
                    raise
                await asyncio.sleep(self.retry_backoff * (attempt + 1))

        if last_error:
            raise last_error
        return []

    async def _fetch_websocket(self, source: Dict[str, Any]) -> List[Dict[str, Any]]:
        timeout_sec = self._source_timeout(source)
        url = source["url"]
        max_messages = int(source.get("max_messages", source.get("msg_count", 10)))
        subscribe_msg = source.get("subscribe_message")
        data_path = source.get("data_path")

        messages: List[Dict[str, Any]] = []
        last_error: Optional[Exception] = None

        for attempt in range(self.max_retries + 1):
            try:
                # websockets 12+ supports open_timeout
                async with websockets.connect(url, open_timeout=timeout_sec) as ws:
                    if subscribe_msg:
                        msg_str = json.dumps(subscribe_msg) if isinstance(subscribe_msg, (dict, list)) else str(subscribe_msg)
                        await ws.send(msg_str)

                    while len(messages) < max_messages:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=timeout_sec)
                        except asyncio.TimeoutError:
                            break
                        item = json.loads(raw)
                        extracted = self._extract_path(item, data_path)
                        messages.extend(extracted)
                return messages
            except (websockets.WebSocketException, OSError, asyncio.TimeoutError) as exc:
                last_error = exc
                if attempt == self.max_retries:
                    raise
                await asyncio.sleep(self.retry_backoff * (attempt + 1))

        if last_error:
            raise last_error
        return messages

    async def _fetch_file(self, source: Dict[str, Any]) -> List[Dict[str, Any]]:
        path = Path(source["path"])
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        suffix = path.suffix.lower()
        if suffix == ".csv":
            content = path.read_text(encoding="utf-8")
            delimiter = source.get("delimiter", ",")
            reader = csv.DictReader(StringIO(content), delimiter=delimiter)
            return [dict(row) for row in reader]

        # JSON file
        content = json.loads(path.read_text(encoding="utf-8"))
        data_path = source.get("data_path")
        return self._extract_path(content, data_path)

    async def _fetch_source(self, source: Dict[str, Any]) -> List[Dict[str, Any]]:
        source_type = source.get("type", "").lower()
        if source_type == "rest":
            return await self._fetch_rest(source)
        elif source_type == "csv":
            return await self._fetch_csv(source)
        elif source_type == "graphql":
            return await self._fetch_graphql(source)
        elif source_type == "websocket":
            return await self._fetch_websocket(source)
        elif source_type == "file":
            return await self._fetch_file(source)
        else:
            raise ValueError(f"Unsupported source type: '{source_type}' for source '{source.get('name')}'")

    def _apply_incremental_filter(
        self,
        records_by_source: Dict[str, List[Dict[str, Any]]],
        state: Dict[str, str],
    ) -> List[Dict[str, Any]]:
        filtered: List[Dict[str, Any]] = []
        for source_name, records in records_by_source.items():
            if not records:
                continue
            last_seen = state.get(source_name)
            new_records = [r for r in records if not last_seen or r["timestamp"] > last_seen]
            filtered.extend(new_records)
            if records:
                highest_ts = max(r["timestamp"] for r in records)
                state[source_name] = max(last_seen or "", highest_ts)

        self._save_incremental_state(state)
        return filtered

    async def run(self, output_path: Optional[str] = None) -> Dict[str, Any]:
        """Execute the multi-source aggregation pipeline."""
        start_time = time.perf_counter()
        sources = self.config.get("sources", [])
        tasks = [self._fetch_source(s) for s in sources]

        # Concurrently fetch from all sources with graceful error handling
        results = await asyncio.gather(*tasks, return_exceptions=True)

        records_by_source: Dict[str, List[Dict[str, Any]]] = {}
        error_details: List[Dict[str, str]] = []
        successful_sources_count = 0

        for source, result in zip(sources, results):
            source_name = source.get("name", "unnamed")
            if isinstance(result, Exception):
                logger.warning(f"Source '{source_name}' failed: {result}")
                records_by_source[source_name] = []
                error_details.append({
                    "source": source_name,
                    "error": str(result),
                    "type": type(result).__name__,
                })
                continue

            try:
                normalized = [
                    self._normalize_record(
                        source_name=source_name,
                        record=item,
                        mapping=source.get("mapping"),
                        default_unit=source.get("unit"),
                    )
                    for item in result
                ]
                records_by_source[source_name] = normalized
                successful_sources_count += 1
            except Exception as e:
                logger.error(f"Normalization failed for source '{source_name}': {e}")
                records_by_source[source_name] = []
                error_details.append({
                    "source": source_name,
                    "error": f"Normalization error: {e}",
                    "type": type(e).__name__,
                })

        # Apply incremental filtering if enabled
        if self.incremental_store:
            state = self._load_incremental_state()
            all_records = self._apply_incremental_filter(records_by_source, state)
        else:
            all_records = []
            for items in records_by_source.values():
                all_records.extend(items)

        # Chronologically sort all records by timestamp (ascending)
        all_records.sort(key=lambda item: item["timestamp"])

        duration = time.perf_counter() - start_time

        stats = {
            "total_records": len(all_records),
            "records": len(all_records),
            "sources_count": len(sources),
            "sources": len(sources),
            "successful_sources": successful_sources_count,
            "failed_sources": len(error_details),
            "errors": len(error_details),
            "error_details": error_details,
            "source_breakdown": {name: len(items) for name, items in records_by_source.items()},
            "duration_seconds": round(duration, 4),
        }

        output_data = {
            "records": all_records,
            "stats": stats,
        }

        dest = output_path or self.output_path
        if dest:
            dest_path = Path(dest)
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            dest_path.write_text(json.dumps(output_data, indent=2), encoding="utf-8")
            logger.info(f"Wrote {len(all_records)} records to {dest}")

        return output_data


def main():
    parser = argparse.ArgumentParser(description="Multi-Source Data Aggregation & Normalization Pipeline")
    parser.add_argument("--config", "-c", required=True, help="Path to config JSON file")
    parser.add_argument("--output", "-o", help="Path to output JSON destination")
    parser.add_argument("--incremental-store", "-i", help="Path to incremental watermark state file")
    parser.add_argument("--timeout", "-t", type=float, default=10.0, help="Global request timeout in seconds")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose debug logging")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    pipeline = DataPipeline(
        config=args.config,
        incremental_store=args.incremental_store,
        timeout=args.timeout,
    )
    result = asyncio.run(pipeline.run(output_path=args.output))
    print(f"Pipeline finished: {result['stats']['total_records']} records processed across {result['stats']['sources_count']} sources in {result['stats']['duration_seconds']}s.")
    if result["stats"]["failed_sources"] > 0:
        print(f"Warning: {result['stats']['failed_sources']} sources encountered errors.")


if __name__ == "__main__":
    main()
