"""Pin the exact `Decimal` handling before values reach SQLite."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from app.schemas import (
    CorrectionMessage,
    TradeMessage,
    UploadRequest,
    money_key,
    money_str,
    prepare_events,
    utc_timestamp_us,
    utc_text,
)


def test_money_rendering_never_uses_exponent_notation() -> None:
    assert money_str(Decimal("1E+2")) == "100"
    assert money_str(Decimal("100.50")) == "100.50"
    assert money_key(Decimal("100.500")) == money_key(Decimal("100.5"))


def test_microsecond_timestamp_is_exact() -> None:
    moment = datetime(2024, 1, 2, 9, 30, 40, 250123, tzinfo=timezone.utc)
    millis = utc_timestamp_us(moment)
    assert millis == 1_704_187_840_250_123
    assert utc_text(moment) == "2024-01-02T09:30:40.250123Z"


def test_json_number_price_is_not_rounded_through_float() -> None:
    # Pydantic's own JSON parser builds Decimals at 28 significant digits, so
    # exercise the API path: it parses with ``parse_float=Decimal`` first.
    from app.main import _parse_upload_body

    payload = UploadRequest.model_validate(
        _parse_upload_body(
            b'{"symbol":"AAPL","trade_date":"2024-01-02","messages":'
            b'[{"seq":1,"type":"trade","trade_id":1,'
            b'"exec_time":"2024-01-02T09:30:00Z",'
            b'"price":123456789.123456789,"quantity":1}]}'
        ).model_dump()
    )
    price = payload.messages[0].price
    assert price == Decimal("123456789.123456789")
    assert money_str(price) == "123456789.123456789"


def test_decimal_string_and_number_are_equivalent_for_idempotency() -> None:
    as_string = CorrectionMessage(
        seq=1, type="correction", trade_id=1, price="100.50", quantity=10
    )
    as_number = CorrectionMessage.model_validate_json(
        '{"seq":1,"type":"correction","trade_id":1,"price":100.50,"quantity":10}'
    )
    assert money_key(as_string.price) == money_key(as_number.price)


def test_prepare_events_normalises_to_utc() -> None:
    messages = [
        TradeMessage(
            seq=1,
            type="trade",
            trade_id=1,
            exec_time="2024-01-02T18:00:00+08:00",
            price="100.50",
            quantity=10,
        )
    ]
    events = prepare_events(messages, datetime(2024, 1, 2).date())
    assert events[0].exec_time == "2024-01-02T10:00:00.000000Z"
    assert events[0].exec_ts_us == 1_704_189_600_000_000


def test_prepare_events_rejects_exec_time_outside_trade_date() -> None:
    messages = [
        TradeMessage(
            seq=1,
            type="trade",
            trade_id=1,
            exec_time="2024-01-03T00:00:00Z",
            price="100.50",
            quantity=10,
        )
    ]
    try:
        prepare_events(messages, datetime(2024, 1, 2).date())
    except ValueError as exc:
        assert "trade_date" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("expected a ValueError")
