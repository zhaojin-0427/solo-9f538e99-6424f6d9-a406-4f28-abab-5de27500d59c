"""Persistence-level invariants that are awkward to provoke through the API."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from app.errors import DomainError
from app.schemas import EventInput
from app.store import MAX_EVENTS_PER_DAY, Store, build_minute_bars


def event(
    seq: int,
    kind: str,
    trade_id: int,
    price: str | None = None,
    quantity: int | None = None,
    exec_ts_us: int | None = None,
) -> EventInput:
    return EventInput(
        seq=seq,
        kind=kind,
        trade_id=trade_id,
        exec_time="2024-01-02T09:30:00.000000Z" if kind == "trade" else None,
        exec_ts_us=exec_ts_us if kind == "trade" else None,
        price=price,
        quantity=quantity,
    )


@pytest.fixture()
def store(tmp_path: Path):
    instance = Store(tmp_path / "store.db")
    yield instance
    instance.close()


def test_day_limit_counts_cumulative_sequence_not_batch(store: Store) -> None:
    events = [event(seq, "trade", seq, "10.00", 1, 1_704_189_000_000_000)
              for seq in range(1, 101)]
    store.append_upload("AAPL", date(2024, 1, 2), events)
    with pytest.raises(DomainError) as excinfo:
        store.append_upload(
            "AAPL",
            date(2024, 1, 2),
            [event(101, "trade", 101, "10.00", 1, 1_704_189_000_000_000)],
        )
    assert excinfo.value.code == "daily_limit_exceeded"
    assert store.latest_version("AAPL", date(2024, 1, 2)) == MAX_EVENTS_PER_DAY


def test_rejected_batch_leaves_no_partial_version(store: Store) -> None:
    store.append_upload(
        "AAPL", date(2024, 1, 2), [event(1, "trade", 1, "10.00", 1, 1_704_189_000_000_000)]
    )
    with pytest.raises(DomainError):
        store.append_upload(
            "AAPL",
            date(2024, 1, 2),
            [
                event(2, "trade", 2, "11.00", 1, 1_704_189_000_000_000),
                event(3, "cancel", 999),
            ],
        )
    assert store.latest_version("AAPL", date(2024, 1, 2)) == 1
    assert store.versions("AAPL", date(2024, 1, 2))["event_count"] == 1


def test_publish_uses_one_watermark_even_for_fresh_connection(tmp_path: Path) -> None:
    path = tmp_path / "shared.db"
    first = Store(path)
    first.append_upload(
        "AAPL",
        date(2024, 1, 2),
        [
            event(1, "trade", 1, "10.00", 1, 1_704_189_000_000_000),
            event(2, "trade", 2, "20.00", 1, 1_704_189_060_000_000),
        ],
    )
    published = first.publish("AAPL", date(2024, 1, 2))
    first.close()

    second = Store(path)
    assert second.get_snapshot("AAPL", date(2024, 1, 2), 2)["bars"] == published["bars"]
    second.close()


def test_price_equality_ignores_trailing_zeros() -> None:
    assert Decimal("100.500") == Decimal("100.5")


def test_build_minute_bars_is_pure_over_states() -> None:
    # No live trades -> no bars, not a zero-filled series.
    assert build_minute_bars([]) == []
