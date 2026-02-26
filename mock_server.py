#!/usr/bin/env python3
from __future__ import annotations

import base64
import hashlib
import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Handler(BaseHTTPRequestHandler):
    def _write(self, code: int, body: str, ctype: str = "application/json"):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body.encode())))
        self.end_headers()
        self.wfile.write(body.encode())

    def do_GET(self):
        if self.path.startswith("/rest"):
            self._write(200, json.dumps({"items": [{"timestamp": 3, "value": 30, "unit": "usd"}]}))
        elif self.path.startswith("/csv"):
            body = "timestamp,value,unit\n2,20,usd\n"
            self._write(200, body, "text/csv")
        else:
            self._write(404, "{}")

    def do_POST(self):
        if self.path.startswith("/graphql"):
            body = json.dumps({"data": {"readings": [{"timestamp": 4, "value": 40, "unit": "usd"}]}})
            self._write(200, body)
        else:
            self._write(404, "{}")

    def log_message(self, format, *args):
        return


def start_http_server(port: int):
    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    return srv


def start_ws_server(port: int):
    def run():
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", port))
        s.listen(5)
        conn, _ = s.accept()
        req = conn.recv(4096).decode(errors="ignore")
        key = ""
        for line in req.split("\r\n"):
            if line.lower().startswith("sec-websocket-key:"):
                key = line.split(":", 1)[1].strip()
        accept = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()
        resp = (
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
        )
        conn.sendall(resp.encode())

        import time
        time.sleep(0.05)
        msg = json.dumps({"timestamp": 1, "value": 10, "unit": "usd"}).encode()
        # unmasked server->client text frame
        conn.sendall(bytes([0x81, len(msg)]) + msg)
        conn.close()
        s.close()

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return t
