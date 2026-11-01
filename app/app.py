import os
import random
import threading
import time

from flask import Flask, abort, request
from prometheus_client import Counter, Gauge, Histogram, start_http_server

app = Flask(__name__)

# --- "Low-end service" model -------------------------------------------------
WORKERS = int(os.getenv("WORKERS", "4"))          # only N requests processed at once
QUEUE_TIMEOUT = float(os.getenv("QUEUE_TIMEOUT", "2.0"))  # waiting longer -> 503
ERROR_RATE = float(os.getenv("ERROR_RATE", "0.015"))      # random 500s
SLOW_RATE = float(os.getenv("SLOW_RATE", "0.03"))         # occasional slow outliers
_workers = threading.BoundedSemaphore(WORKERS)

REQUESTS = Counter("app_requests_total", "Total HTTP requests", ["endpoint", "status"])
LATENCY = Histogram(
    "app_request_duration_seconds", "Request duration in seconds (includes queue wait)",
    ["endpoint"],
    buckets=(0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.3, 0.5, 0.75, 1, 1.5, 2, 3, 5),
)
IN_PROGRESS = Gauge("app_requests_in_progress", "Requests currently being handled or queued")
QUERY_LEN = Histogram("app_query_length_chars", "Length of the 'query' parameter",
                      buckets=(0, 5, 10, 20, 50, 100, 500))


def _service_time(query_len: int) -> float:
    """Simulated work: lognormal (median ~50ms), longer queries cost more, rare slow outliers."""
    t = random.lognormvariate(-3.0, 0.5) + query_len * 0.0002
    if random.random() < SLOW_RATE:
        t += random.uniform(0.3, 1.0)
    return t


@app.route("/log")
def log():
    status = "200"
    try:
        with IN_PROGRESS.track_inprogress(), LATENCY.labels("/log").time():
            query = request.args.get("query", "")
            QUERY_LEN.observe(len(query))
            if not _workers.acquire(timeout=QUEUE_TIMEOUT):
                status = "503"  # overloaded: shed load
                abort(503)
            try:
                time.sleep(_service_time(len(query)))
                if random.random() < ERROR_RATE:
                    status = "500"
                    abort(500)
            finally:
                _workers.release()
    finally:
        REQUESTS.labels("/log", status).inc()
    return "ok"


if __name__ == "__main__":
    start_http_server(int(os.getenv("METRICS_PORT", "9091")))  # /metrics
    app.run(host="0.0.0.0", port=int(os.getenv("APP_PORT", "5000")), threaded=True)
