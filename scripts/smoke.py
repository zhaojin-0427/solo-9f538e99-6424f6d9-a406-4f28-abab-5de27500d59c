#!/usr/bin/env python3
"""Manual smoke test against a running server.

    python3 scripts/smoke.py http://127.0.0.1:8000

Exits non-zero on the first unexpected response.
"""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request

BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://127.0.0.1:8000"
SCOPE = {"symbol": "SMOKE", "trade_date": "2024-01-02"}


def call(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(
        BASE + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def expect(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"FAILED: {message}")


def trade(seq: int, trade_id: int, at: str, price: str, qty: int) -> dict:
    return {
        "seq": seq,
        "type": "trade",
        "trade_id": trade_id,
        "exec_time": at,
        "price": price,
        "quantity": qty,
    }


def main() -> None:
    status, body = call("GET", "/health")
    expect(status == 200 and body["status"] == "ok", "health")

    batch = [
        trade(1, 1, "2024-01-02T09:30:05Z", "100.50", 100),
        trade(2, 2, "2024-01-02T09:31:10Z", "101.00", 200),
    ]
    status, body = call("POST", "/uploads", {**SCOPE, "messages": batch})
    expect(status == 201 and body["version"] == 2, f"first upload: {status} {body}")
    first_upload = body

    status, body = call("POST", "/uploads", {**SCOPE, "messages": batch})
    expect(status == 200 and body["created"] is False, "identical retransmission")
    expect(body["upload_id"] == first_upload["upload_id"], "retransmission returns original id")

    status, body = call("POST", "/uploads", {**SCOPE, "messages": [trade(4, 4, "2024-01-02T09:32:00Z", "1.00", 1)]})
    expect(status == 409 and body["error"]["code"] == "sequence_gap", "gap rejected")

    scope_query = "?" + "&".join(f"{key}={value}" for key, value in SCOPE.items())

    status, body = call("POST", "/publish" + scope_query)
    expect(status == 200 and body["version"] == 2, "publish")
    expect(body["bars"][0]["volume"] == 100, "bar volume")

    status, body = call("POST", "/uploads", {**SCOPE, "messages": [{"seq": 3, "type": "correction", "trade_id": 1, "price": "99.25", "quantity": 50}]})
    expect(status == 201, "correction")

    status, body = call("POST", "/publish" + scope_query)
    expect(status == 200 and body["version"] == 3, "republish")
    expect(body["bars"][0]["open"] == "99.25" and body["bars"][0]["volume"] == 50, "correction applied")

    status, old = call("GET", "/snapshots/2" + scope_query)
    expect(status == 200 and old["bars"][0]["open"] == "100.50", "old version unchanged")

    print("smoke test passed against", BASE)


if __name__ == "__main__":
    main()
