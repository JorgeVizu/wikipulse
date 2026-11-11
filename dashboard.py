"""Materialize Flink's ranking topic and serve the local dashboard."""
import json
import logging
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Lock, Thread

from kafka import KafkaConsumer

LOCK = Lock()
WINDOWS = {}
PAGE = Path(__file__).with_name("index.html").read_bytes()


def collect():
    try:
        consumer = KafkaConsumer(
            "wiki-ranking", bootstrap_servers="kafka:9092", auto_offset_reset="earliest"
        )
        for record in consumer:
            key = json.loads(record.key)
            end = key["window_end"]
            position = key["rank_no"]
            with LOCK:
                rows = WINDOWS.setdefault(end, {})
                if record.value is None:
                    rows.pop(position, None)
                else:
                    rows[position] = json.loads(record.value)
                for expired in sorted(WINDOWS)[:-20]:
                    del WINDOWS[expired]
    except Exception:
        logging.exception("Ranking consumer stopped")
        os._exit(1)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/":
            body, mime = PAGE, "text/html; charset=utf-8"
        elif self.path == "/api/ranking":
            with LOCK:
                end = max((end for end, rows in WINDOWS.items() if rows), default=None)
                rows = WINDOWS.get(end, {})
                payload = {"window_end": end, "rows": [rows[i] for i in sorted(rows)]}
            body = json.dumps(payload).encode()
            mime = "application/json"
        else:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    Thread(target=collect, daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()

