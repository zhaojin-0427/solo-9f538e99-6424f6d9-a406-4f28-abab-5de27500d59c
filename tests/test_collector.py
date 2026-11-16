"""Collection pipeline tests: UDP ingest, HTTP backfill, pause semantics.

The API-level tests run a real retained-day HTTP source in a thread and send
real UDP datagrams to the collector's socket, exercising the same path the
Docker Compose demo uses.
"""

from __future__ import annotations

import json
import socket
import threading
import time
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pytest
from fastapi.testclient import TestClient

from app.schemas import EventInput
from app.store import MAX_EVENTS_PER_DAY, Store
from tests.conftest import cancel, correction, trade

SYMBOL = "AAPL"
DATE = "2024-01-02"
DAY = date(2024, 1, 2)
SCOPE = {"symbol": SYMBOL, "trade_date": DATE}


# -- helpers ----------------------------------------------------------------
def wait_until(predicate, timeout: float = 10.0, interval: float = 0.05) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def free_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def send_udp(port: int, message: dict) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.sendto(json.dumps(message).encode(), ("127.0.0.1", port))


class FakeSource:
    """A retained-day HTTP source with a switchable outage."""

    def __init__(self, messages: list[dict] | None = None, down: bool = False):
        self.messages = list(messages or [])
        self.down = down
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802 (stdlib naming)
                parsed = urlparse(self.path)
                if parsed.path != "/messages":
                    self.send_error(404)
                    return
                if outer.down:
                    self.send_response(503)
                    self.end_headers()
                    return
                params = parse_qs(parsed.query)
                from_seq = int(params.get("from_seq", ["1"])[0])
                limit = int(params.get("limit", ["100"])[0])
                page = [m for m in outer.messages if m["seq"] >= from_seq][:limit]
                body = json.dumps(
                    {
                        "symbol": SYMBOL,
                        "trade_date": DATE,
                        "latest_seq": max((m["seq"] for m in outer.messages), default=0),
                        "messages": page,
                    }
                ).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args) -> None:
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture()
def source():
    instance = FakeSource()
    yield instance
    instance.close()


def start_collection(
    client: TestClient,
    source_url: str,
    udp_port: int = 0,
    poll: float = 0.1,
    expected: int = 201,
) -> dict:
    response = client.post(
        "/collections",
        json={
            "symbol": SYMBOL,
            "trade_date": DATE,
            "udp_port": udp_port,
            "source_url": source_url,
            "poll_interval_seconds": poll,
        },
    )
    assert response.status_code == expected, response.text
    return response.json()


def collection_status(client: TestClient) -> dict:
    response = client.get(f"/collections/{SYMBOL}/{DATE}")
    assert response.status_code == 200, response.text
    return response.json()


def trades(count: int, start_seq: int = 1, price: str = "10.00") -> list[dict]:
    return [
        trade(seq, 1000 + seq, f"2024-01-02T09:{30 + (seq // 60):02d}:{seq % 60:02d}Z",
              price, 10)
        for seq in range(start_seq, start_seq + count)
    ]


# -- store-level pipeline ----------------------------------------------------
@pytest.fixture()
def store(tmp_path):
    instance = Store(tmp_path / "collect.db")
    yield instance
    instance.close()


def make_event(message: dict) -> EventInput:
    from app.collector import parse_message

    return parse_message(message, DAY)


def test_pending_survives_restart_and_still_detects_conflicts(tmp_path) -> None:
    path = tmp_path / "restart.db"
    first = Store(path)
    first.create_collector(SYMBOL, DAY, 0, "http://source", 1.0)
    # seq 2 arrives before seq 1: buffered, not committed.
    outcome = first.ingest_message(SYMBOL, DAY, make_event(trade(
        2, 1002, "2024-01-02T09:30:02Z", "10.00", 5)), "udp")
    assert outcome.kind == "added"
    assert first.commit_pending(SYMBOL, DAY).committed == 0
    first.close()

    # After a restart the buffered message is still there: an identical
    # retransmission is a no-op, a different one is a conflict.
    second = Store(path)
    duplicate = second.ingest_message(SYMBOL, DAY, make_event(trade(
        2, 1002, "2024-01-02T09:30:02Z", "10.00", 5)), "backfill")
    assert duplicate.kind == "duplicate_pending"
    conflict = second.ingest_message(SYMBOL, DAY, make_event(trade(
        2, 1002, "2024-01-02T09:30:02Z", "99.99", 5)), "backfill")
    assert conflict.kind == "conflict"
    assert conflict.seq == 2
    status = second.collector_status(SYMBOL, DAY)
    assert status["status"] == "paused"
    assert status["pause_reason"] == "sequence_conflict"
    assert status["pause_seq"] == 2
    assert status["watermark"] == 0
    second.close()


def test_commit_only_contiguous_and_rule_valid(store: Store) -> None:
    store.create_collector(SYMBOL, DAY, 0, "http://source", 1.0)
    store.ingest_message(SYMBOL, DAY, make_event(trade(
        1, 1001, "2024-01-02T09:30:01Z", "10.00", 5)), "udp")
    # seq 2 references a trade that does not exist: seq 1 commits, the
    # collector pauses exactly at seq 2 and nothing beyond is committed.
    store.ingest_message(SYMBOL, DAY, make_event(correction(2, 9999, "11.00", 1)), "udp")
    store.ingest_message(SYMBOL, DAY, make_event(trade(
        3, 1003, "2024-01-02T09:30:03Z", "10.00", 5)), "udp")
    outcome = store.commit_pending(SYMBOL, DAY)
    assert outcome.paused is True
    assert outcome.pause_reason == "unknown_trade"
    assert outcome.pause_seq == 2
    assert store.latest_version(SYMBOL, DAY) == 1
    status = store.collector_status(SYMBOL, DAY)
    assert status["status"] == "paused"
    assert status["pending_events"] == 2  # the offending and the blocked one


def test_commit_stops_at_daily_cap(store: Store) -> None:
    store.create_collector(SYMBOL, DAY, 0, "http://source", 1.0)
    for message in trades(MAX_EVENTS_PER_DAY + 5):
        store.ingest_message(SYMBOL, DAY, make_event(message), "backfill")
    outcome = store.commit_pending(SYMBOL, DAY)
    assert outcome.paused is True
    assert outcome.pause_reason == "daily_limit_exceeded"
    assert outcome.pause_seq == MAX_EVENTS_PER_DAY + 1
    assert store.latest_version(SYMBOL, DAY) == MAX_EVENTS_PER_DAY


def test_conflict_with_committed_data_pauses_without_overwriting(store: Store) -> None:
    store.create_collector(SYMBOL, DAY, 0, "http://source", 1.0)
    store.ingest_message(SYMBOL, DAY, make_event(trade(
        1, 1001, "2024-01-02T09:30:01Z", "10.00", 5)), "udp")
    assert store.commit_pending(SYMBOL, DAY).committed == 1
    outcome = store.ingest_message(SYMBOL, DAY, make_event(trade(
        1, 1001, "2024-01-02T09:30:01Z", "77.77", 5)), "backfill")
    assert outcome.kind == "conflict"
    status = store.collector_status(SYMBOL, DAY)
    assert status["pause_seq"] == 1
    # The committed event is untouched.
    assert store.versions(SYMBOL, DAY)["event_count"] == 1
    snapshot = store.publish(SYMBOL, DAY)
    assert snapshot["bars"][0]["open"] == "10.00"


# -- API-level flows ----------------------------------------------------------
def test_catches_up_to_source_high_on_start(client: TestClient, source: FakeSource) -> None:
    source.messages = trades(10)
    started = start_collection(client, source.url)
    assert started["status"] == "running"
    assert wait_until(lambda: collection_status(client)["watermark"] == 10)
    final = collection_status(client)
    assert final["source_latest_seq"] == 10
    assert final["pending_events"] == 0
    # The committed events are the normal event log: publish and query work.
    versions = client.get("/versions", params=SCOPE).json()
    assert versions["latest_version"] == 10
    published = client.post("/publish", params=SCOPE).json()
    assert published["version"] == 10
    assert len(published["bars"]) == 1
    assert published["bars"][0]["volume"] == 100


def test_udp_out_of_order_and_duplicates_commit_once(
    client: TestClient, source: FakeSource
) -> None:
    started = start_collection(client, source.url)
    port = started["udp_port"]
    messages = trades(4)
    for message in [messages[1], messages[0], messages[1], messages[3], messages[2], messages[0]]:
        send_udp(port, message)
    assert wait_until(lambda: collection_status(client)["watermark"] == 4)
    versions = client.get("/versions", params=SCOPE).json()
    assert versions["event_count"] == 4
    assert versions["latest_version"] == 4


def test_realtime_continues_during_outage_and_gap_heals_after(
    client: TestClient, source: FakeSource
) -> None:
    source.down = True  # backfill disconnected from the start
    source.messages = trades(4)
    started = start_collection(client, source.url)
    port = started["udp_port"]
    # Realtime delivers 1, 2, 4; seq 3 is lost on the wire.
    for message in [source.messages[0], source.messages[1], source.messages[3]]:
        send_udp(port, message)
    assert wait_until(lambda: collection_status(client)["watermark"] == 2)
    mid = collection_status(client)
    assert mid["pending_events"] == 1  # seq 4 waits for the gap
    assert mid["buffered_max_seq"] == 4
    # The source comes back: the gap is filled automatically.
    source.down = False
    assert wait_until(lambda: collection_status(client)["watermark"] == 4)
    final = collection_status(client)
    assert final["pending_events"] == 0
    assert client.get("/versions", params=SCOPE).json()["event_count"] == 4


def test_conflict_pauses_and_pending_survives_app_restart(
    tmp_path, monkeypatch
) -> None:
    database = tmp_path / "market.db"
    monkeypatch.setenv("MARKET_DB", str(database))
    from app.main import app

    source = FakeSource(trades(2))
    try:
        with TestClient(app) as client:
            started = start_collection(client, source.url)
            port = started["udp_port"]
            assert wait_until(lambda: collection_status(client)["watermark"] == 2)
            # UDP delivers seq 4 while seq 3 is still missing: it is buffered
            # (received but not committed) and cannot advance the watermark.
            send_udp(port, trade(4, 1004, "2024-01-02T09:30:04Z", "10.00", 10))
            assert wait_until(lambda: collection_status(client)["pending_events"] == 1)
            assert collection_status(client)["watermark"] == 2
            source.down = True  # backfill disconnects before the restart

        # Restart the app on the same database.  The buffered realtime
        # message survives and keeps participating in content checks.
        with TestClient(app) as client:
            assert wait_until(
                lambda: collection_status(client)["pending_events"] == 1
            )
            assert collection_status(client)["status"] == "running"
            # The source returns with the missing seq 3 plus a seq 4 whose
            # content differs from the buffered realtime original.
            source.messages = trades(3) + [
                trade(4, 1004, "2024-01-02T09:30:04Z", "99.99", 10)
            ]
            source.down = False
            assert wait_until(
                lambda: collection_status(client)["status"] == "paused"
            )
            paused = collection_status(client)
            assert paused["pause_reason"] == "sequence_conflict"
            assert paused["pause_seq"] == 4
            # The buffered original was valid and committed; the forged
            # message was rejected.  Committed data is never overwritten.
            assert paused["watermark"] == 4
            assert paused["pending_events"] == 0
            published = client.post("/publish", params=SCOPE).json()
            assert published["bars"] == [
                {
                    "minute": "2024-01-02T09:30",
                    "open": "10.00",
                    "high": "10.00",
                    "low": "10.00",
                    "close": "10.00",
                    "volume": 40,
                    "trade_count": 4,
                }
            ]
    finally:
        source.close()


def test_collector_lifecycle_and_validation(client: TestClient, source: FakeSource) -> None:
    assert client.get(f"/collections/{SYMBOL}/{DATE}").status_code == 404
    started = start_collection(client, source.url)
    assert started["udp_port"] > 0  # ephemeral port resolved

    # Same settings again: idempotent 200, still one collector.
    again = start_collection(client, source.url, expected=200)
    assert again["status"] == "running"
    assert len(client.get("/collections").json()["collections"]) == 1

    # Different settings: rejected until the old collector is deleted.
    response = client.post(
        "/collections",
        json={
            "symbol": SYMBOL,
            "trade_date": DATE,
            "udp_port": free_udp_port(),
            "source_url": "http://elsewhere:9",
        },
    )
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "collector_exists"

    # A second collector cannot steal the same UDP port.
    other_port = free_udp_port()
    response = client.post(
        "/collections",
        json={
            "symbol": "MSFT",
            "trade_date": DATE,
            "udp_port": other_port,
            "source_url": source.url,
        },
    )
    assert response.status_code == 201, response.text
    response = client.post(
        "/collections",
        json={
            "symbol": "IBM",
            "trade_date": DATE,
            "udp_port": other_port,
            "source_url": source.url,
        },
    )
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "udp_port_in_use"

    # Delete stops and removes it; committed events stay.
    assert client.delete(f"/collections/{SYMBOL}/{DATE}").status_code == 204
    assert client.get(f"/collections/{SYMBOL}/{DATE}").status_code == 404
    assert client.delete(f"/collections/{SYMBOL}/{DATE}").status_code == 404


def test_invalid_udp_datagrams_are_dropped(client: TestClient, source: FakeSource) -> None:
    started = start_collection(client, source.url)
    port = started["udp_port"]
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.sendto(b"not json at all", ("127.0.0.1", port))
        sock.sendto(json.dumps({"seq": "x"}).encode(), ("127.0.0.1", port))
    send_udp(port, trade(1, 1001, "2024-01-02T09:30:01Z", "10.00", 5))
    assert wait_until(lambda: collection_status(client)["watermark"] == 1)
    assert collection_status(client)["status"] == "running"


def test_collected_day_publishes_identical_bars_to_upload(
    client: TestClient, source: FakeSource
) -> None:
    """A collected day and an uploaded day with the same tape agree."""
    tape = [
        trade(1, 11, "2024-01-02T09:30:05Z", "100.50", 100),
        trade(2, 12, "2024-01-02T09:30:40Z", "100.75", 200),
        correction(3, 11, "100.60", 80),
        cancel(4, 12),
    ]
    source.messages = tape
    start_collection(client, source.url)
    assert wait_until(lambda: collection_status(client)["watermark"] == 4)
    collected = client.post("/publish", params=SCOPE).json()

    upload = client.post(
        "/uploads",
        json={"symbol": "MSFT", "trade_date": DATE, "messages": tape},
    )
    assert upload.status_code == 201, upload.text
    uploaded = client.post(
        "/publish", params={"symbol": "MSFT", "trade_date": DATE}
    ).json()
    assert collected["bars"] == uploaded["bars"]
