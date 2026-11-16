"""End-to-end API tests covering the required ingestion and version semantics."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from fastapi.testclient import TestClient

from tests.conftest import cancel, correction, trade, upload

SYMBOL = "AAPL"
DATE = "2024-01-02"
SCOPE = {"symbol": SYMBOL, "trade_date": DATE}


def bars(client: TestClient, version: int | None = None) -> list[dict]:
    if version is None:
        response = client.get("/snapshots/latest", params=SCOPE)
    else:
        response = client.get(f"/snapshots/{version}", params=SCOPE)
    assert response.status_code == 200, response.text
    return response.json()["bars"]


# -- ingestion ------------------------------------------------------------
def test_appends_consecutive_batches_and_counts_volume(client: TestClient) -> None:
    first = upload(
        client,
        [
            trade(1, 11, "2024-01-02T09:30:05Z", "100.50", 100),
            trade(2, 12, "2024-01-02T09:30:40Z", "100.75", 200),
        ],
    )
    assert first == {
        "upload_id": 1,
        "version": 2,
        "created": True,
        "accepted_events": 2,
        "replayed_events": 0,
    }
    second = upload(
        client, [trade(3, 13, "2024-01-02T09:31:01Z", "101.00", 300)]
    )
    assert second["version"] == 3
    assert client.post("/publish", params=SCOPE).json()["version"] == 3


def test_identical_retransmission_returns_original_result(client: TestClient) -> None:
    batch = [
        trade(1, 11, "2024-01-02T09:30:05Z", "100.50", 100),
        correction(2, 11, "100.60", 80),
        cancel(3, 11),
    ]
    original = upload(client, batch)
    replay = upload(client, batch, expected=200)
    assert replay["created"] is False
    assert replay["upload_id"] == original["upload_id"]
    assert replay["version"] == original["version"]
    assert replay["accepted_events"] == 0
    assert replay["replayed_events"] == 3
    # Retransmissions must not consume the 100-message daily allowance either.
    versions = client.get("/versions", params=SCOPE).json()
    assert versions["event_count"] == 3
    assert versions["latest_version"] == 3
    assert len(versions["uploads"]) == 1


def test_partial_retransmission_with_new_tail_is_accepted(client: TestClient) -> None:
    upload(client, [trade(1, 11, "2024-01-02T09:30:05Z", "100.50", 100)])
    result = upload(
        client,
        [
            trade(1, 11, "2024-01-02T09:30:05Z", "100.50", 100),
            trade(2, 12, "2024-01-02T09:30:06Z", "100.60", 10),
        ],
    )
    assert result["created"] is True
    assert result["accepted_events"] == 1
    assert result["replayed_events"] == 1
    assert result["version"] == 2


def test_sequence_gap_rejects_whole_batch(client: TestClient) -> None:
    upload(client, [trade(1, 11, "2024-01-02T09:30:05Z", "100.50", 100)])
    response = client.post(
        "/uploads",
        json={
            **SCOPE,
            "messages": [
                trade(3, 12, "2024-01-02T09:30:06Z", "100.60", 10),
                trade(4, 13, "2024-01-02T09:30:07Z", "100.70", 10),
            ],
        },
    )
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "sequence_gap"
    assert client.get("/versions", params=SCOPE).json()["latest_version"] == 1
    # Nothing from the rejected batch is visible.
    assert client.get("/versions", params=SCOPE).json()["event_count"] == 1


def test_different_content_for_accepted_sequence_is_rejected(client: TestClient) -> None:
    upload(client, [trade(1, 11, "2024-01-02T09:30:05Z", "100.50", 100)])
    response = client.post(
        "/uploads",
        json={**SCOPE, "messages": [trade(1, 11, "2024-01-02T09:30:05Z", "100.51", 100)]},
    )
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "sequence_conflict"


def test_retransmission_with_different_kind_is_rejected(client: TestClient) -> None:
    upload(client, [trade(1, 11, "2024-01-02T09:30:05Z", "100.50", 100)])
    response = client.post(
        "/uploads", json={**SCOPE, "messages": [correction(1, 11, "100.50", 100)]}
    )
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "sequence_conflict"


def test_duplicate_trade_id_rejected(client: TestClient) -> None:
    upload(client, [trade(1, 11, "2024-01-02T09:30:05Z", "100.50", 100)])
    response = client.post(
        "/uploads",
        json={**SCOPE, "messages": [trade(2, 11, "2024-01-02T09:30:06Z", "100.60", 10)]},
    )
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "duplicate_trade_id"


def test_reference_to_missing_trade_rejected_atomically(client: TestClient) -> None:
    response = client.post(
        "/uploads",
        json={
            **SCOPE,
            "messages": [
                trade(1, 11, "2024-01-02T09:30:05Z", "100.50", 100),
                correction(2, 999, "1.00", 1),
            ],
        },
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "unknown_trade"
    assert client.get("/versions", params=SCOPE).json()["latest_version"] == 0


def test_correction_after_cancel_rejected(client: TestClient) -> None:
    upload(
        client,
        [
            trade(1, 11, "2024-01-02T09:30:05Z", "100.50", 100),
            cancel(2, 11),
        ],
    )
    response = client.post(
        "/uploads", json={**SCOPE, "messages": [correction(3, 11, "100.60", 80)]}
    )
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "trade_cancelled"
    assert client.get("/versions", params=SCOPE).json()["latest_version"] == 2


def test_correction_keeps_original_execution_time(client: TestClient) -> None:
    upload(client, [trade(1, 11, "2024-01-02T09:30:59Z", "100.50", 100)])
    upload(client, [correction(2, 11, "100.60", 80)])
    bars_after = bars(client) if client.post("/publish", params=SCOPE) else []
    assert [bar["minute"] for bar in bars_after] == ["2024-01-02T09:30"]
    assert bars_after[0]["volume"] == 80
    assert bars_after[0]["close"] == "100.60"


def test_daily_cap_is_100_cumulative_messages(client: TestClient) -> None:
    upload(
        client,
        [trade(seq, 1000 + seq, f"2024-01-02T09:30:{seq % 60:02d}Z", "10.00", 1)
         for seq in range(1, 101)],
    )
    response = client.post(
        "/uploads",
        json={**SCOPE, "messages": [trade(101, 5000, "2024-01-02T09:31:00Z", "10.00", 1)]},
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "daily_limit_exceeded"
    assert client.get("/versions", params=SCOPE).json()["latest_version"] == 100


def test_cap_is_per_symbol_and_date(client: TestClient) -> None:
    upload(
        client,
        [trade(seq, 1000 + seq, "2024-01-02T09:30:00Z", "10.00", 1) for seq in range(1, 101)],
    )
    upload(
        client,
        [trade(1, 11, "2024-01-03T09:30:00Z", "10.00", 1)],
        trade_date="2024-01-03",
    )
    upload(
        client,
        [trade(1, 11, "2024-01-02T09:30:00Z", "10.00", 1)],
        symbol="MSFT",
    )


# -- validation -----------------------------------------------------------
def test_rejects_invalid_price_and_quantity(client: TestClient) -> None:
    for price, qty in [("0", 10), ("-1.5", 10), ("NaN", 10), ("100", 0), ("100", -5), ("100", 1.5)]:
        response = client.post(
            "/uploads",
            json={
                **SCOPE,
                "messages": [trade(1, 11, "2024-01-02T09:30:05Z", price, qty)],
            },
        )
        assert response.status_code == 422, (price, qty, response.text)


def test_rejects_absurd_price_literals(client: TestClient) -> None:
    for price in ["1E+100000", "1E-100000", "1E+19"]:
        response = client.post(
            "/uploads",
            json={
                **SCOPE,
                "messages": [trade(1, 11, "2024-01-02T09:30:05Z", price, 10)],
            },
        )
        assert response.status_code == 422, (price, response.text)


def test_exact_large_price_is_preserved(client: TestClient) -> None:
    upload(client, [trade(1, 11, "2024-01-02T09:30:05Z", "123456789.123456789", 1)])
    bar = client.post("/publish", params=SCOPE).json()["bars"][0]
    assert bar["open"] == "123456789.123456789"
    assert bar["volume"] == 1


def test_rejects_naive_exec_time_and_wrong_date(client: TestClient) -> None:
    response = client.post(
        "/uploads",
        json={
            **SCOPE,
            "messages": [trade(1, 11, "2024-01-02T09:30:05", "100.50", 100)],
        },
    )
    assert response.status_code == 422
    response = client.post(
        "/uploads",
        json={
            **SCOPE,
            "messages": [trade(1, 11, "2024-01-03T09:30:05Z", "100.50", 100)],
        },
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_message"


def test_exec_time_in_other_offset_uses_utc_minute(client: TestClient) -> None:
    # 18:00+08:00 is 10:00 UTC, still on 2024-01-02 in UTC.
    upload(client, [trade(1, 11, "2024-01-02T18:00:00+08:00", "100.50", 100)])
    published = client.post("/publish", params=SCOPE).json()
    assert published["bars"][0]["minute"] == "2024-01-02T10:00"


# -- minute aggregation ---------------------------------------------------
def test_ohlcv_sorted_by_time_then_original_sequence(client: TestClient) -> None:
    upload(
        client,
        [
            trade(1, 11, "2024-01-02T09:30:59Z", "100.50", 100),
            trade(2, 12, "2024-01-02T09:30:10Z", "101.00", 200),
            trade(3, 13, "2024-01-02T09:30:10Z", "99.75", 300),
            trade(4, 14, "2024-01-02T09:31:00Z", "100.25", 400),
        ],
    )
    published = client.post("/publish", params=SCOPE).json()
    assert published["bars"] == [
        {
            "minute": "2024-01-02T09:30",
            "open": "101.00",  # seq 2, time 09:30:10
            "high": "101.00",
            "low": "99.75",
            "close": "100.50",  # seq 1, time 09:30:59
            "volume": 600,
            "trade_count": 3,
        },
        {
            "minute": "2024-01-02T09:31",
            "open": "100.25",
            "high": "100.25",
            "low": "100.25",
            "close": "100.25",
            "volume": 400,
            "trade_count": 1,
        },
    ]


def test_open_close_tie_breaks_on_original_sequence(client: TestClient) -> None:
    upload(
        client,
        [
            trade(1, 11, "2024-01-02T09:30:00Z", "10.00", 1),
            trade(2, 12, "2024-01-02T09:30:00Z", "20.00", 1),
        ],
    )
    upload(client, [correction(3, 12, "30.00", 5)])
    bar = client.post("/publish", params=SCOPE).json()["bars"][0]
    assert (bar["open"], bar["high"], bar["low"], bar["close"], bar["volume"]) == (
        "10.00",
        "30.00",
        "10.00",
        "30.00",
        6,
    )


def test_empty_minutes_are_not_emitted(client: TestClient) -> None:
    upload(
        client,
        [
            trade(1, 11, "2024-01-02T09:30:00Z", "10.00", 1),
            trade(2, 12, "2024-01-02T09:45:00Z", "11.00", 2),
        ],
    )
    published = client.post("/publish", params=SCOPE).json()
    assert [bar["minute"] for bar in published["bars"]] == [
        "2024-01-02T09:30",
        "2024-01-02T09:45",
    ]


def test_cancelled_trade_leaves_no_bar_quantity(client: TestClient) -> None:
    upload(
        client,
        [
            trade(1, 11, "2024-01-02T09:30:00Z", "10.00", 100),
            trade(2, 12, "2024-01-02T09:45:00Z", "11.00", 200),
            cancel(3, 11),
        ],
    )
    published = client.post("/publish", params=SCOPE).json()
    assert [bar["minute"] for bar in published["bars"]] == ["2024-01-02T09:45"]
    assert published["bars"][0]["volume"] == 200
    assert published["bars"][0]["trade_count"] == 1


def test_correction_to_cancelled_trade_keeps_original_quantity_out(
    client: TestClient,
) -> None:
    upload(
        client,
        [
            trade(1, 11, "2024-01-02T09:30:00Z", "10.00", 100),
            correction(2, 11, "12.50", 40),
            cancel(3, 11),
        ],
    )
    assert client.post("/publish", params=SCOPE).json()["bars"] == []


# -- versions -------------------------------------------------------------
def test_correction_after_publish_changes_new_version_only(client: TestClient) -> None:
    upload(
        client,
        [
            trade(1, 11, "2024-01-02T09:30:05Z", "100.50", 100),
            trade(2, 12, "2024-01-02T09:31:05Z", "101.00", 200),
        ],
    )
    first = client.post("/publish", params=SCOPE).json()
    assert first["version"] == 2
    assert first["bars"][0]["close"] == "100.50"

    upload(client, [correction(3, 11, "99.00", 50)])
    second = client.post("/publish", params=SCOPE).json()
    assert second["version"] == 3
    assert second["bars"][0] == {
        "minute": "2024-01-02T09:30",
        "open": "99.00",
        "high": "99.00",
        "low": "99.00",
        "close": "99.00",
        "volume": 50,
        "trade_count": 1,
    }

    # Old version is immutable and still queryable at its original watermark.
    assert bars(client, version=2) == first["bars"]
    assert bars(client, version=3) == second["bars"]
    listed = client.get("/snapshots", params=SCOPE).json()["versions"]
    assert [item["version"] for item in listed] == [2, 3]


def test_publish_defaults_to_latest_and_is_idempotent(client: TestClient) -> None:
    upload(
        client,
        [
            trade(1, 11, "2024-01-02T09:30:05Z", "100.50", 100),
            trade(2, 12, "2024-01-02T09:31:05Z", "101.00", 200),
        ],
    )
    first = client.post("/publish", params=SCOPE).json()
    again = client.post("/publish", params=SCOPE).json()
    explicit = client.post("/publish", params={**SCOPE, "version": 1}).json()
    assert first["created"] is True
    assert again["created"] is False
    assert again["bars"] == first["bars"]
    assert explicit["version"] == 1
    assert len(explicit["bars"]) == 1


def test_publish_above_watermark_rejected(client: TestClient) -> None:
    upload(client, [trade(1, 11, "2024-01-02T09:30:05Z", "100.50", 100)])
    response = client.post("/publish", params={**SCOPE, "version": 2})
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "version_not_ready"


def test_latest_snapshot_requires_a_publication(client: TestClient) -> None:
    assert client.get("/snapshots/latest", params=SCOPE).status_code == 404
    assert client.get("/snapshots/1", params=SCOPE).status_code == 404


def test_empty_publication_has_no_bars(client: TestClient) -> None:
    published = client.post("/publish", params=SCOPE).json()
    assert published["version"] == 0
    assert published["bars"] == []


# -- concurrency ----------------------------------------------------------
def test_concurrent_duplicate_uploads_apply_once(client: TestClient) -> None:
    batch = [
        trade(1, 11, "2024-01-02T09:30:05Z", "100.50", 100),
        correction(2, 11, "100.60", 80),
    ]

    def send() -> tuple[int, dict]:
        response = client.post("/uploads", json={**SCOPE, "messages": batch})
        return response.status_code, response.json()

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda _: send(), range(8)))

    statuses = [status for status, _ in results]
    assert all(status in (200, 201) for status in statuses)
    assert statuses.count(201) == 1
    assert all(body["version"] == 2 for _, body in results)
    versions = client.get("/versions", params=SCOPE).json()
    assert versions["event_count"] == 2
    assert len(versions["uploads"]) == 1


def test_concurrent_publishes_are_stable(client: TestClient) -> None:
    upload(
        client,
        [trade(seq, 1000 + seq, f"2024-01-02T09:30:{seq:02d}Z", "10.00", 1) for seq in range(1, 21)],
    )

    def publish(_: int) -> dict:
        response = client.post("/publish", params=SCOPE)
        assert response.status_code == 200, response.text
        return response.json()

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(publish, range(8)))

    assert all(result["version"] == 20 for result in results)
    assert all(result["bars"] == results[0]["bars"] for result in results)
    assert sum(1 for result in results if result["created"]) == 1


def test_publish_racing_an_upload_never_mixes_watermarks(client: TestClient) -> None:
    """A published version contains exactly the events at or below it."""
    upload(
        client,
        [
            trade(1, 11, "2024-01-02T09:30:01Z", "10.00", 1),
            trade(2, 12, "2024-01-02T09:30:02Z", "10.00", 1),
            trade(3, 13, "2024-01-02T09:30:03Z", "10.00", 1),
        ],
    )
    next_trade = [trade(4, 14, "2024-01-02T09:30:04Z", "10.00", 1)]

    with ThreadPoolExecutor(max_workers=2) as pool:
        publish_future = pool.submit(client.post, "/publish", params=SCOPE)
        upload_future = pool.submit(
            client.post, "/uploads", json={**SCOPE, "messages": next_trade}
        )
        published = publish_future.result()
        uploaded = upload_future.result()

    assert published.status_code == 200, published.text
    assert uploaded.status_code in (201, 409), uploaded.text  # 409 if publish ran first
    published = published.json()
    assert published["version"] in (3, 4)
    # Volume equals exactly one unit per event at that watermark: no half state.
    assert sum(bar["volume"] for bar in published["bars"]) == published["version"]
    replay = client.get(f"/snapshots/{published['version']}", params=SCOPE)
    assert replay.json()["bars"] == published["bars"]

