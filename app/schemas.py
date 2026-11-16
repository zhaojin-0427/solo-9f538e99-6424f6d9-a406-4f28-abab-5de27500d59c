"""Request schemas and canonicalisation helpers.

All market-data messages are validated here.  The API accepts prices as JSON
numbers or decimal strings, but never converts them through ``float``: values
stay ``Decimal`` from the request all the way to SQLite (stored as text) and
back to the JSON response (rendered as strings).

Requests are parsed with ``json.loads(..., parse_float=Decimal)`` before
Pydantic sees them (see ``app.main``), so a JSON literal such as
``123456789.123456789`` keeps every digit.  That path yields ``Decimal`` for
fractional numbers and plain ``int`` for integers, which is why quantities and
sequence numbers accept exactly ``int``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Annotated, Literal, Union

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    field_validator,
    model_validator,
)

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
MAX_EXEC_TIME = datetime(2100, 1, 1, tzinfo=timezone.utc)
MAX_SEQ = 2**63 - 1


def money_str(value: Decimal) -> str:
    """Render a Decimal without exponent notation (``1E+2`` -> ``100``)."""
    return format(value, "f")


def money_key(value: Decimal) -> str:
    """Canonical numeric form used for equality and idempotency checks."""
    return str(value.normalize())


def utc_timestamp_us(value: datetime) -> int:
    """Exact microsecond epoch, without going through float."""
    utc_value = value.astimezone(timezone.utc)
    delta = utc_value - EPOCH
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


def utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def normalize_symbol(value: str) -> str:
    value = value.strip()
    if not value:
        raise ValueError("symbol must not be empty")
    if len(value) > 64:
        raise ValueError("symbol must be at most 64 characters")
    if any(ch.isspace() for ch in value):
        raise ValueError("symbol must not contain whitespace")
    return value


def _validated_price(value: Decimal) -> Decimal:
    if not value.is_finite():
        raise ValueError("price must be a finite decimal")
    if value <= 0:
        raise ValueError("price must be greater than zero")
    # Bound the magnitude/precision so a literal like 1E+999999 cannot expand
    # into an unbounded stored string.  18 integer digits and 18 decimals is
    # far beyond any real exchange tick.
    digits = value.as_tuple().digits
    if len(digits) > 30 or not -18 <= value.adjusted() <= 18:
        raise ValueError(
            "price must have at most 30 significant digits and magnitude below 1E+19"
        )
    return value


class _MessageBase(BaseModel):
    model_config = ConfigDict(extra="forbid")

    seq: Annotated[
        StrictInt, Field(gt=0, le=MAX_SEQ, description="Exchange sequence number")
    ]
    trade_id: Annotated[
        StrictInt,
        Field(gt=0, le=MAX_SEQ, description="Exchange trade id referenced by this message"),
    ]


class TradeMessage(_MessageBase):
    """A new execution (逐笔成交)."""

    type: Literal["trade"]
    exec_time: AwareDatetime = Field(
        description="Execution time with an explicit UTC offset; bucketed in UTC"
    )
    price: Decimal = Field(description="Positive exact decimal price")
    quantity: Annotated[StrictInt, Field(gt=0, description="Positive integer quantity")]

    @field_validator("price")
    @classmethod
    def _positive_price(cls, value: Decimal) -> Decimal:
        return _validated_price(value)

    @model_validator(mode="after")
    def _sane_exec_time(self) -> "TradeMessage":
        if not EPOCH <= self.exec_time.astimezone(timezone.utc) < MAX_EXEC_TIME:
            raise ValueError("exec_time must be between 1970-01-01 and 2100-01-01")
        return self


class CorrectionMessage(_MessageBase):
    """A correction (更正): changes only price and quantity of an active trade."""

    type: Literal["correction"]
    price: Decimal = Field(description="Positive exact decimal price")
    quantity: Annotated[StrictInt, Field(gt=0, description="Positive integer quantity")]

    @field_validator("price")
    @classmethod
    def _positive_price(cls, value: Decimal) -> Decimal:
        return _validated_price(value)


class CancelMessage(_MessageBase):
    """A cancellation (撤销) of an existing, not-yet-cancelled trade."""

    type: Literal["cancel"]


MarketMessage = Annotated[
    Union[TradeMessage, CorrectionMessage, CancelMessage],
    Field(discriminator="type"),
]


class UploadRequest(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "symbol": "AAPL",
                    "trade_date": "2024-01-02",
                    "messages": [
                        {
                            "seq": 1,
                            "type": "trade",
                            "trade_id": 1001,
                            "exec_time": "2024-01-02T09:30:05Z",
                            "price": "100.50",
                            "quantity": 100,
                        },
                        {
                            "seq": 2,
                            "type": "trade",
                            "trade_id": 1002,
                            "exec_time": "2024-01-02T09:30:40.250Z",
                            "price": "100.75",
                            "quantity": 200,
                        },
                        {
                            "seq": 3,
                            "type": "correction",
                            "trade_id": 1001,
                            "price": "100.60",
                            "quantity": 80,
                        },
                        {
                            "seq": 4,
                            "type": "cancel",
                            "trade_id": 1002,
                        },
                    ],
                }
            ]
        },
    )

    symbol: str = Field(min_length=1, max_length=64)
    trade_date: date = Field(description="UTC trading date (YYYY-MM-DD)")
    # The hard daily cap is 100 messages, but a batch may replay up to the
    # whole accepted log, so the transport limit is the cap's upper bound.
    messages: Annotated[list[MarketMessage], Field(min_length=1, max_length=100)]

    @field_validator("symbol")
    @classmethod
    def _valid_symbol(cls, value: str) -> str:
        return normalize_symbol(value)


@dataclass(frozen=True)
class EventInput:
    """A validated message in the exact shape written to the event log."""

    seq: int
    kind: str
    trade_id: int
    exec_time: str | None
    exec_ts_us: int | None
    price: str | None
    quantity: int | None


def prepare_events(messages: list[MarketMessage], trade_date: date) -> list[EventInput]:
    """Normalise messages (UTC instants, exact decimal text) before any write.

    Every trade execution must fall inside ``trade_date`` in UTC so that a
    symbol/day partition is self-consistent; corrections and cancellations
    inherit the execution's original instant and therefore need no timestamp.
    """

    prepared: list[EventInput] = []
    for message in messages:
        if isinstance(message, TradeMessage):
            exec_utc = message.exec_time.astimezone(timezone.utc)
            if exec_utc.date() != trade_date:
                raise ValueError(
                    "exec_time in UTC must fall on trade_date "
                    f"({trade_date.isoformat()}); got {utc_text(exec_utc)}"
                )
            prepared.append(
                EventInput(
                    seq=message.seq,
                    kind="trade",
                    trade_id=message.trade_id,
                    exec_time=utc_text(exec_utc),
                    exec_ts_us=utc_timestamp_us(exec_utc),
                    price=money_str(message.price),
                    quantity=message.quantity,
                )
            )
        elif isinstance(message, CorrectionMessage):
            prepared.append(
                EventInput(
                    seq=message.seq,
                    kind="correction",
                    trade_id=message.trade_id,
                    exec_time=None,
                    exec_ts_us=None,
                    price=money_str(message.price),
                    quantity=message.quantity,
                )
            )
        else:
            prepared.append(
                EventInput(
                    seq=message.seq,
                    kind="cancel",
                    trade_id=message.trade_id,
                    exec_time=None,
                    exec_ts_us=None,
                    price=None,
                    quantity=None,
                )
            )

    prepared.sort(key=lambda event: event.seq)
    return prepared
