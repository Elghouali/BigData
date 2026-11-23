"""Intentionally intermittent local HTTP service."""
import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock


class State:
    def __init__(self):
        self.count = 0
        self.lock = Lock()

    def bump(self):
        with self.lock:
            self.count += 1
            return self.count


class Handler(BaseHTTPRequestHandler):
    state = State()

    def do_GET(self):
        if self.path == "/health":
            self.send_json(200, {"status": "ok"})
        elif self.path == "/down":
            self.send_json(503, {"error": "unavailable"})
        elif self.path == "/flaky":
            count = self.state.bump()
            self.send_json(503 if count % 2 else 200, {"attempt": count})
        else:
            self.send_json(404, {"error": "not_found"})

    def send_json(self, status, body):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=8089)
    args = p.parse_args()
    httpd = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()
