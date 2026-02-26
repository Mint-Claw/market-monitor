#!/usr/bin/env python3
"""Async multi-source aggregation pipeline for bounty #1.

Sources supported:
- REST (JSON list)
- CSV (HTTP/local)
- GraphQL (HTTP POST)
- file (JSON/CSV local)
- websocket (minimal RFC6455 text frame consumer)
"""

from __future__ import annotations

import asyncio
import base64
import csv
import hashlib
import json
import os
import socket
import ssl
import struct
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class SourceConfig:
    name: str
    type: str
    url: str | None = None
    path: str | None = None
    timeout: float = 5.0
    retries: int = 1
    retry_backoff_ms: int = 150
    unit: str | None = None
    query: str | None = None


def normalize_record(source: str, rec: Dict[str, Any], default_unit: str | None = None) -> Dict[str, Any]:
    ts = rec.get("timestamp") or rec.get("time") or rec.get("ts") or int(time.time())
    if isinstance(ts, str) and ts.isdigit():
        ts = int(ts)
    val = rec.get("value")
    if val is None:
        for k in ("price", "close", "amount", "reading"):
            if k in rec:
                val = rec[k]
                break
    return {
        "source": source,
        "timestamp": ts,
        "value": val,
        "unit": rec.get("unit") or default_unit or "unknown",
    }


def _http_json(url: str, timeout: float = 5.0, method: str = "GET", payload: dict | None = None) -> Any:
    data = None
    headers = {"User-Agent": "market-monitor-bounty/1.0"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, method=method, headers=headers, data=data)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def _http_text(url: str, timeout: float = 5.0) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "market-monitor-bounty/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8")


def _read_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


async def _retry(coro_factory, retries: int, backoff_ms: int):
    last_error = None
    for i in range(retries + 1):
        try:
            return await coro_factory()
        except Exception as e:
            last_error = e
            if i >= retries:
                break
            await asyncio.sleep((backoff_ms / 1000.0) * (2 ** i))
    raise last_error


async def fetch_rest(cfg: SourceConfig) -> List[Dict[str, Any]]:
    payload = await _retry(lambda: asyncio.to_thread(_http_json, cfg.url, cfg.timeout), cfg.retries, cfg.retry_backoff_ms)
    rows = payload if isinstance(payload, list) else payload.get("data", payload.get("items", []))
    return [normalize_record(cfg.name, r, cfg.unit) for r in rows]


async def fetch_csv(cfg: SourceConfig) -> List[Dict[str, Any]]:
    text = await _retry(lambda: asyncio.to_thread(_http_text, cfg.url, cfg.timeout), cfg.retries, cfg.retry_backoff_ms)
    rows = list(csv.DictReader(text.splitlines()))
    return [normalize_record(cfg.name, r, cfg.unit) for r in rows]


async def fetch_graphql(cfg: SourceConfig) -> List[Dict[str, Any]]:
    payload = await _retry(
        lambda: asyncio.to_thread(_http_json, cfg.url, cfg.timeout, "POST", {"query": cfg.query or "{ readings { timestamp value unit } }"}),
        cfg.retries,
        cfg.retry_backoff_ms,
    )
    data = payload.get("data", {})
    rows = data.get("readings") or data.get("items") or []
    return [normalize_record(cfg.name, r, cfg.unit) for r in rows]


async def fetch_file(cfg: SourceConfig) -> List[Dict[str, Any]]:
    text = await _retry(lambda: asyncio.to_thread(_read_file, cfg.path), cfg.retries, cfg.retry_backoff_ms)
    if cfg.path.endswith(".json"):
        raw = json.loads(text)
        rows = raw if isinstance(raw, list) else raw.get("items", [])
    else:
        rows = list(csv.DictReader(text.splitlines()))
    return [normalize_record(cfg.name, r, cfg.unit) for r in rows]


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            break
        data += chunk
    return data


def _read_ws_frame(sock: socket.socket) -> str | None:
    hdr = _recv_exact(sock, 2)
    if len(hdr) < 2:
        return None
    b1, b2 = hdr
    opcode = b1 & 0x0F
    masked = (b2 & 0x80) != 0
    ln = b2 & 0x7F
    if ln == 126:
        ln = struct.unpack("!H", _recv_exact(sock, 2))[0]
    elif ln == 127:
        ln = struct.unpack("!Q", _recv_exact(sock, 8))[0]
    mask = _recv_exact(sock, 4) if masked else b""
    payload = _recv_exact(sock, ln)
    if masked:
        payload = bytes(payload[i] ^ mask[i % 4] for i in range(len(payload)))
    if opcode == 8:
        return None
    if opcode == 1:
        return payload.decode("utf-8")
    return None


def _ws_connect_and_read(url: str, timeout: float = 5.0, max_messages: int = 3) -> List[Dict[str, Any]]:
    u = urllib.parse.urlparse(url)
    host = u.hostname
    port = u.port or (443 if u.scheme == "wss" else 80)
    path = u.path or "/"
    if u.query:
        path += "?" + u.query

    raw_sock = socket.create_connection((host, port), timeout=timeout)
    sock: socket.socket = raw_sock
    if u.scheme == "wss":
        ctx = ssl.create_default_context()
        sock = ctx.wrap_socket(raw_sock, server_hostname=host)

    key = base64.b64encode(os.urandom(16)).decode()
    req = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        "Sec-WebSocket-Version: 13\r\n\r\n"
    )
    sock.sendall(req.encode())
    resp = sock.recv(4096).decode("utf-8", errors="ignore")
    if "101" not in resp.splitlines()[0]:
        raise RuntimeError(f"websocket handshake failed: {resp.splitlines()[:1]}")

    items: List[Dict[str, Any]] = []
    sock.settimeout(timeout)
    for _ in range(max_messages):
        msg = _read_ws_frame(sock)
        if not msg:
            break
        try:
            payload = json.loads(msg)
            if isinstance(payload, dict):
                items.append(payload)
            elif isinstance(payload, list):
                items.extend(payload)
        except json.JSONDecodeError:
            continue

    sock.close()
    return items


async def fetch_websocket(cfg: SourceConfig) -> List[Dict[str, Any]]:
    rows = await _retry(lambda: asyncio.to_thread(_ws_connect_and_read, cfg.url, cfg.timeout, 5), cfg.retries, cfg.retry_backoff_ms)
    return [normalize_record(cfg.name, r, cfg.unit) for r in rows]


FETCHERS = {
    "rest": fetch_rest,
    "csv": fetch_csv,
    "graphql": fetch_graphql,
    "file": fetch_file,
    "websocket": fetch_websocket,
}


async def run_pipeline(source_defs: List[Dict[str, Any]]) -> Dict[str, Any]:
    configs = [SourceConfig(**x) for x in source_defs]

    async def _run(cfg: SourceConfig):
        started = time.time()
        try:
            rows = await FETCHERS[cfg.type](cfg)
            return {"source": cfg.name, "ok": True, "rows": rows, "elapsed_ms": int((time.time() - started) * 1000)}
        except Exception as e:
            return {"source": cfg.name, "ok": False, "rows": [], "error": str(e), "elapsed_ms": int((time.time() - started) * 1000)}

    results = await asyncio.gather(*[_run(c) for c in configs])
    merged: List[Dict[str, Any]] = []
    failures = []
    for r in results:
        merged.extend(r["rows"])
        if not r["ok"]:
            failures.append({"source": r["source"], "error": r.get("error")})

    merged.sort(key=lambda x: x.get("timestamp", 0))
    return {
        "records": merged,
        "stats": {
            "total_records": len(merged),
            "source_count": len(configs),
            "success_sources": len(configs) - len(failures),
            "failed_sources": len(failures),
            "failures": failures,
        },
    }


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True, help="Path to JSON config with source definitions")
    p.add_argument("--out", default="pipeline_output.json")
    args = p.parse_args()

    with open(args.config, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    out = asyncio.run(run_pipeline(cfg["sources"]))
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print(json.dumps(out["stats"], indent=2))
