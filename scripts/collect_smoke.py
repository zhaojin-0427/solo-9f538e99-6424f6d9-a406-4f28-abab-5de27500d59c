#!/usr/bin/env python3
"""Smoke test for realtime collection against the Docker Compose demo.

    docker compose up --build -d
    python3 scripts/collect_smoke.py http://127.0.0.1:8000 http://127.0.0.1:8001

Starts a collector for the demo source (symbol DEMO, today UTC), waits until
the lossy UDP stream and the backfill converge at the source's highest
sequence, then publishes minute bars.  Exits non-zero on failure.

The collector itself runs inside the api container, so its backfill URL must
be the compose-internal one; override with COLLECTOR_SOURCE_URL when the API
runs outside Docker (e.g. COLLECTOR_SOURCE_URL=http://127.0.0.1:8001).
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

API = (sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000").rstrip("/")
SOURCE = (sys.argv[2] if len(sys.argv) > 2 else "http://127.0.0.1:8001").rstrip("/")
COLLECTOR_SOURCE_URL = os.environ.get("COLLECTOR_SOURCE_URL", "http://source:8001")
SYMBOL = "DEMO"
TRADE_DATE = datetime.now(timezone.utc).date().isoformat()
UDP_PORT = 9001


def call(method: str, base: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(
        base + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            raw = response.read()
            return response.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def expect(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"FAILED: {message}")


def main() -> None:
    status, body = call("GET", API, "/health")
    expect(status == 200 and body["status"] == "ok", "api health")
    status, body = call("GET", SOURCE, "/health")
    expect(status == 200 and body["status"] == "ok", "source health")

    # (Re)start the collector for the demo day.
    call("DELETE", API, f"/collections/{SYMBOL}/{TRADE_DATE}")
    status, body = call(
        "POST",
        API,
        "/collections",
        {
            "symbol": SYMBOL,
            "trade_date": TRADE_DATE,
            "udp_port": UDP_PORT,
            "source_url": COLLECTOR_SOURCE_URL,
            "poll_interval_seconds": 1.0,
        },
    )
    expect(status in (200, 201), f"start collection: {status} {body}")
    print("collector started:", body["status"])

    # The demo source plays 48 messages; the collector must reach its high
    # even though UDP drops, reorders and duplicates some of them.
    deadline = time.monotonic() + 120
    latest = None
    while time.monotonic() < deadline:
        status, latest = call("GET", API, f"/collections/{SYMBOL}/{TRADE_DATE}")
        expect(status == 200, f"collection status: {status} {latest}")
        source_latest = latest["source_latest_seq"] or 0
        print(
            f"  status={latest['status']} watermark={latest['watermark']}"
            f" pending={latest['pending_events']} source_latest={source_latest}"
        )
        expect(latest["status"] != "paused", f"collector paused: {latest}")
        if source_latest and latest["watermark"] >= source_latest:
            break
        time.sleep(2)
    else:
        raise SystemExit("FAILED: collector did not catch up in time")

    expect(latest["pending_events"] == 0, f"buffer not drained: {latest}")
    watermark = latest["watermark"]

    status, versions = call(
        "GET", API, f"/versions?symbol={SYMBOL}&trade_date={TRADE_DATE}"
    )
    expect(status == 200, f"versions: {status} {versions}")
    expect(
        versions["latest_version"] == watermark
        and versions["event_count"] == watermark,
        f"committed log does not match watermark: {versions}",
    )

    status, published = call(
        "POST", API, f"/publish?symbol={SYMBOL}&trade_date={TRADE_DATE}"
    )
    expect(status == 200 and published["version"] == watermark,
           f"publish: {status} {published}")
    expect(len(published["bars"]) > 0, "expected minute bars")
    print(
        f"published version {published['version']}"
        f" with {len(published['bars'])} minute bars"
    )

    status, snapshot = call(
        "GET", API, f"/snapshots/latest?symbol={SYMBOL}&trade_date={TRADE_DATE}"
    )
    expect(status == 200 and snapshot["bars"] == published["bars"],
           "latest snapshot matches publication")
    print("collection smoke test passed against", API)


if __name__ == "__main__":
    main()
