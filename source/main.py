"""Local example market-data source for the realtime collection demo.

Plays both ends of a lossy realtime link:

* **UDP streamer** — emits the day's exchange messages (same JSON shape as
  ``POST /uploads``) towards the API container, on purpose out of order,
  with duplicates and with configurable sequence numbers dropped entirely.
* **HTTP retained-day source** — ``GET /messages?symbol=&trade_date=
  &from_seq=&limit=`` always serves the complete, correct day from any
  sequence number, which is what the collector backfills from.  Configured
  outage windows (or an on-demand ``POST /chaos/http_down``) make it answer
  503 so the "backfill disconnects, realtime continues, gap heals after
  reconnect" behaviour can be observed.

The message script is a pure function of the trading date, so the UDP stream
and the HTTP source always agree (unless ``POST /chaos/corrupt`` is used to
forge different content for one sequence number, demonstrating the
same-sequence/different-content pause).

Configuration is by environment variable:

* ``SYMBOL`` / ``TRADE_DATE`` — what the UDP streamer plays (default:
  ``DEMO`` / today, UTC).  The HTTP endpoint serves any symbol/date.
* ``SCRIPT_LENGTH`` — number of messages in the day (default 48, max 100).
* ``UDP_TARGET_HOST`` / ``UDP_TARGET_PORT`` — where datagrams go
  (default ``api`` / ``9001``).
* ``UDP_INTERVAL_SECONDS`` / ``UDP_START_DELAY_SECONDS`` — pacing.
* ``UDP_DROP_SEQS`` — comma-separated sequences never sent via UDP.
* ``UDP_DUPLICATE_SEQS`` — comma-separated sequences sent twice.
* ``UDP_REORDER_WINDOW`` — messages are emitted in reversed windows of this
  size (1 = strictly in order).
* ``HTTP_DOWN_WINDOWS`` — comma-separated ``start-end`` second ranges
  (relative to startup) during which ``/messages`` answers 503.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import time
from contextlib import asynccontextmanager
from datetime import date, datetime, time as time_type, timedelta, timezone
from decimal import Decimal
from typing import Annotated, Iterator

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

SYMBOL = os.environ.get("SYMBOL", "DEMO")
TRADE_DATE = os.environ.get(
    "TRADE_DATE", datetime.now(timezone.utc).date().isoformat()
)
SCRIPT_LENGTH = min(100, int(os.environ.get("SCRIPT_LENGTH", "48")))
UDP_TARGET_HOST = os.environ.get("UDP_TARGET_HOST", "api")
UDP_TARGET_PORT = int(os.environ.get("UDP_TARGET_PORT", "9001"))
UDP_INTERVAL = float(os.environ.get("UDP_INTERVAL_SECONDS", "0.2"))
UDP_START_DELAY = float(os.environ.get("UDP_START_DELAY_SECONDS", "3"))


def _parse_seq_set(value: str) -> set[int]:
    return {int(part) for part in value.split(",") if part.strip()}


def _parse_windows(value: str) -> list[tuple[float, float]]:
    windows = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        start, _, end = part.partition("-")
        windows.append((float(start), float(end)))
    return windows


UDP_DROP_SEQS = _parse_seq_set(os.environ.get("UDP_DROP_SEQS", ""))
UDP_DUPLICATE_SEQS = _parse_seq_set(os.environ.get("UDP_DUPLICATE_SEQS", ""))
UDP_REORDER_WINDOW = max(1, int(os.environ.get("UDP_REORDER_WINDOW", "1")))
HTTP_DOWN_WINDOWS = _parse_windows(os.environ.get("HTTP_DOWN_WINDOWS", ""))

_STARTED_AT = time.monotonic()

# Chaos state: on-demand HTTP outage and forged per-sequence content.
_down_until = 0.0
_corrupted: dict[int, dict] = {}


def build_script(trade_date: date, count: int = SCRIPT_LENGTH) -> list[dict]:
    """The complete, deterministic, rule-valid day for ``trade_date``.

    Trades every 20 seconds from 09:30 UTC; every twelfth message pattern
    slots in a correction of the most recent trade or a cancellation of the
    oldest live one, so the day exercises all three message kinds.
    """
    messages: list[dict] = []
    live_trade_ids: list[int] = []
    next_trade_id = 1000
    open_time = datetime.combine(trade_date, time_type(9, 30), tzinfo=timezone.utc)
    for seq in range(1, count + 1):
        slot = seq % 12
        if slot == 6 and live_trade_ids:
            price = Decimal("100") + Decimal(seq % 9) / 4 + Decimal("0.5")
            messages.append(
                {
                    "seq": seq,
                    "type": "correction",
                    "trade_id": live_trade_ids[-1],
                    "price": format(price, "f"),
                    "quantity": 10 + seq % 50,
                }
            )
        elif slot == 9 and len(live_trade_ids) > 1:
            messages.append(
                {"seq": seq, "type": "cancel", "trade_id": live_trade_ids.pop(0)}
            )
        else:
            next_trade_id += 1
            live_trade_ids.append(next_trade_id)
            price = Decimal("100") + Decimal(seq % 7) / 4
            messages.append(
                {
                    "seq": seq,
                    "type": "trade",
                    "trade_id": next_trade_id,
                    "exec_time": (open_time + timedelta(seconds=20 * seq)).strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    ),
                    "price": format(price, "f"),
                    "quantity": 100 + (seq % 5) * 10,
                }
            )
    return messages


def _http_down() -> bool:
    elapsed = time.monotonic() - _STARTED_AT
    if time.monotonic() < _down_until:
        return True
    return any(start <= elapsed <= end for start, end in HTTP_DOWN_WINDOWS)


def _serve_day(symbol: str, trade_date: date) -> list[dict]:
    messages = build_script(trade_date)
    return [
        {**message, **_corrupted.get(message["seq"], {})} for message in messages
    ]


async def udp_streamer() -> None:
    """Blast the day at the API, reordered/duplicated/with gaps, exactly once."""
    await asyncio.sleep(UDP_START_DELAY)
    script = build_script(date.fromisoformat(TRADE_DATE))
    ordered: list[dict] = []
    for index in range(0, len(script), UDP_REORDER_WINDOW):
        ordered.extend(reversed(script[index : index + UDP_REORDER_WINDOW]))
    target = (UDP_TARGET_HOST, UDP_TARGET_PORT)
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        for message in ordered:
            if message["seq"] in UDP_DROP_SEQS:
                continue
            payload = json.dumps(message).encode()
            sock.sendto(payload, target)
            if message["seq"] in UDP_DUPLICATE_SEQS:
                sock.sendto(payload, target)
            await asyncio.sleep(UDP_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI) -> Iterator[None]:
    task = asyncio.create_task(udp_streamer())
    try:
        yield
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


app = FastAPI(
    title="Example Market Data Source",
    summary="Lossy UDP realtime feed plus retained-day HTTP backfill source.",
    lifespan=lifespan,
)


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "symbol": SYMBOL, "trade_date": TRADE_DATE}


@app.get("/messages")
def messages(
    symbol: Annotated[str, Query(min_length=1, max_length=64)],
    trade_date: Annotated[date, Query()],
    from_seq: Annotated[int, Query(ge=1)] = 1,
    limit: Annotated[int, Query(ge=1, le=100)] = 100,
) -> JSONResponse:
    if _http_down():
        return JSONResponse(
            status_code=503,
            content={"error": {"code": "source_down", "message": "simulated outage"}},
        )
    day = _serve_day(symbol, trade_date)
    page = [message for message in day if message["seq"] >= from_seq][:limit]
    return JSONResponse(
        {
            "symbol": symbol,
            "trade_date": trade_date.isoformat(),
            "latest_seq": len(day),
            "messages": page,
        }
    )


@app.get("/chaos")
def chaos_state() -> dict:
    return {
        "http_down": _http_down(),
        "down_for_seconds": max(0.0, _down_until - time.monotonic()),
        "corrupted_seqs": sorted(_corrupted),
        "udp_drop_seqs": sorted(UDP_DROP_SEQS),
        "udp_duplicate_seqs": sorted(UDP_DUPLICATE_SEQS),
        "udp_reorder_window": UDP_REORDER_WINDOW,
        "http_down_windows": HTTP_DOWN_WINDOWS,
    }


@app.post("/chaos/http_down")
def chaos_http_down(seconds: Annotated[float, Query(gt=0, le=600)] = 15.0) -> dict:
    """Simulate a backfill outage: /messages answers 503 for ``seconds``."""
    global _down_until
    _down_until = time.monotonic() + seconds
    return {"http_down": True, "seconds": seconds}


@app.post("/chaos/corrupt")
def chaos_corrupt(
    seq: Annotated[int, Query(ge=1)],
    price: Annotated[str, Query()] = "0.01",
) -> dict:
    """Serve forged content for one sequence (same number, different text)."""
    _corrupted[seq] = {"price": price}
    return {"corrupted_seq": seq, "price": price}


@app.delete("/chaos")
def chaos_reset() -> dict:
    global _down_until
    _down_until = 0.0
    _corrupted.clear()
    return {"reset": True}
