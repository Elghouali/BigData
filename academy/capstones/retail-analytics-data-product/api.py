"""Read-only localhost API for the generated retail data product."""
import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import sqlite3
from urllib.parse import parse_qs, urlparse


def handler_for(database: Path, manifest: Path):
    class Handler(BaseHTTPRequestHandler):
        def respond(self, status, body):
            payload = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self):
            route = urlparse(self.path)
            if route.path == "/health":
                self.respond(200, {"status": "ok", "database_exists": database.exists()})
            elif route.path == "/metrics":
                if not manifest.is_file():
                    return self.respond(503, {"error": "manifest_missing"})
                self.respond(200, json.loads(manifest.read_text(encoding="utf-8")))
            elif route.path == "/kpis":
                if not database.is_file():
                    return self.respond(503, {"error": "database_missing"})
                category = parse_qs(route.query).get("category", [None])[0]
                with sqlite3.connect(database) as db:
                    if category:
                        rows = db.execute(
                            "SELECT order_date,category,paid_orders,revenue_cents FROM daily_kpis WHERE category=? ORDER BY order_date",
                            (category,)
                        ).fetchall()
                    else:
                        rows = db.execute(
                            "SELECT order_date,category,paid_orders,revenue_cents FROM daily_kpis ORDER BY order_date,category"
                        ).fetchall()
                self.respond(200, {"kpis": [
                    {"date": d, "category": c, "paid_orders": n, "revenue_cents": v}
                    for d, c, n, v in rows
                ]})
            else:
                self.respond(404, {"error": "unknown_route"})
    return Handler


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--database", type=Path, default=Path("output/orders.db"))
    p.add_argument("--manifest", type=Path, default=Path("output/manifest.json"))
    p.add_argument("--port", type=int, default=8090)
    args = p.parse_args()
    server = ThreadingHTTPServer(("127.0.0.1", args.port),
                                 handler_for(args.database, args.manifest))
    try:
        server.serve_forever()
    finally:
        server.server_close()
