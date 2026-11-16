"""SQLite persistence: idempotent appends and immutable, versioned snapshots.

Versioning model
----------------
* ``events.seq`` is the exchange sequence.  For a symbol/date it must grow as
  consecutive positive integers starting at 1.  The accepted high-water mark
  (``MAX(seq)``) is the *version* of the tape.
* An upload may replay a prefix that is already accepted, but every replayed
  message must match the stored one exactly (kind, trade id, UTC instant,
  numeric price, quantity).  A batch that continues at the high-water mark
  appends the new tail; a batch that is a pure replay is acknowledged with the
  original result and inserts nothing.
* A rejected batch rolls back entirely: the high-water mark and the event log
  are untouched.  Validation, the daily-limit check and insertion share one
  ``BEGIN IMMEDIATE`` transaction, so a failed batch can never leave a half
  version behind.
* A published snapshot is keyed by ``(symbol, trade_date, upto_seq)`` where
  ``upto_seq`` is the high-water mark it was built from.  Snapshot building
  reads the event log inside the same write transaction that stores the bars,
  at exactly one watermark.  Publishing and uploading therefore cannot
  interleave within one version, and older versions remain queryable after a
  newer publication reflects a correction.

Threading: FastAPI runs sync endpoints in a worker thread pool, so one
``sqlite3`` connection is shared.  Every access to it is serialised by the
instance lock; the writes additionally take SQLite's own ``BEGIN IMMEDIATE``
lock so other processes using the same file are excluded too.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Iterator

from .errors import DomainError
from .schemas import EventInput, money_key, money_str

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS uploads (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol        TEXT    NOT NULL,
    trade_date    TEXT    NOT NULL,
    version       INTEGER NOT NULL,
    event_count   INTEGER NOT NULL,
    created_at    TEXT    NOT NULL,
    UNIQUE (symbol, trade_date, version)
);

CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol      TEXT    NOT NULL,
    trade_date  TEXT    NOT NULL,
    seq         INTEGER NOT NULL,
    kind        TEXT    NOT NULL CHECK (kind IN ('trade', 'correction', 'cancel')),
    trade_id    INTEGER NOT NULL,
    exec_time   TEXT,
    exec_ts_us  INTEGER,
    price       TEXT,
    quantity    INTEGER,
    upload_id   INTEGER NOT NULL REFERENCES uploads(id),
    UNIQUE (symbol, trade_date, seq),
    CHECK (
        (kind = 'trade' AND exec_time IS NOT NULL AND exec_ts_us IS NOT NULL
         AND price IS NOT NULL AND quantity IS NOT NULL)
        OR
        (kind = 'correction' AND exec_time IS NULL AND exec_ts_us IS NULL
         AND price IS NOT NULL AND quantity IS NOT NULL)
        OR
        (kind = 'cancel' AND exec_time IS NULL AND exec_ts_us IS NULL
         AND price IS NULL AND quantity IS NULL)
    )
);

-- Only executions claim the trade id; corrections/cancellations refer to it.
CREATE UNIQUE INDEX IF NOT EXISTS events_one_trade_per_id
    ON events (symbol, trade_date, trade_id) WHERE kind = 'trade';

CREATE INDEX IF NOT EXISTS events_by_trade
    ON events (symbol, trade_date, trade_id, seq);

CREATE TABLE IF NOT EXISTS snapshots (
    symbol       TEXT NOT NULL,
    trade_date   TEXT NOT NULL,
    upto_seq     INTEGER NOT NULL,
    bars_json    TEXT NOT NULL,
    published_at TEXT NOT NULL,
    PRIMARY KEY (symbol, trade_date, upto_seq)
);
"""

MAX_EVENTS_PER_DAY = 100


@dataclass(frozen=True)
class TradeState:
    """Latest known state of one exchange trade as of a high-water mark."""

    trade_id: int
    exec_ts_us: int
    exec_time: str
    price: Decimal
    quantity: int
    status: str  # "active" | "cancelled"
    origin_seq: int  # sequence of the original execution, never rewritten
    last_seq: int


@dataclass(frozen=True)
class UploadResult:
    upload_id: int
    version: int
    created: bool
    accepted_events: int
    replayed_events: int


def _now_text() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _upload_payload(row: sqlite3.Row) -> dict:
    return {
        "upload_id": int(row["id"]),
        "version": int(row["version"]),
        "event_count": int(row["event_count"]),
        "created_at": row["created_at"],
    }


def _snapshot_payload(row: sqlite3.Row) -> dict:
    return {
        "symbol": row["symbol"],
        "trade_date": row["trade_date"],
        "version": int(row["upto_seq"]),
        "bars": json.loads(row["bars_json"]),
        "published_at": row["published_at"],
    }


class Store:
    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).expanduser().parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            self.path,
            timeout=30,
            isolation_level=None,
            check_same_thread=False,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.execute("PRAGMA busy_timeout = 30000")
        with self._lock:
            self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # -- transactions -----------------------------------------------------
    @contextmanager
    def _write_transaction(self) -> Iterator[sqlite3.Connection]:
        # IMMEDIATE takes the write lock up front: validation, limit checks and
        # the append are one atomic unit, and other writers serialise.
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            yield self._conn
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise
        else:
            self._conn.execute("COMMIT")

    # -- read helpers -----------------------------------------------------
    @staticmethod
    def _watermark(
        connection: sqlite3.Connection, symbol: str, date_text: str
    ) -> int:
        row = connection.execute(
            "SELECT MAX(seq) AS version FROM events WHERE symbol = ? AND trade_date = ?",
            (symbol, date_text),
        ).fetchone()
        return int(row["version"] or 0)

    def latest_version(self, symbol: str, trade_date: date) -> int:
        with self._lock:
            return self._watermark(self._conn, symbol, trade_date.isoformat())

    @staticmethod
    def _event_at(
        connection: sqlite3.Connection, symbol: str, date_text: str, seq: int
    ) -> sqlite3.Row | None:
        return connection.execute(
            "SELECT * FROM events WHERE symbol = ? AND trade_date = ? AND seq = ?",
            (symbol, date_text, seq),
        ).fetchone()

    @staticmethod
    def _matches(row: sqlite3.Row, event: EventInput) -> bool:
        """Whether a stored event is the same exchange message as ``event``."""
        if row["kind"] != event.kind or int(row["trade_id"]) != event.trade_id:
            return False
        if event.kind == "trade" and int(row["exec_ts_us"]) != event.exec_ts_us:
            return False
        if event.kind in ("trade", "correction"):
            if money_key(Decimal(row["price"])) != money_key(Decimal(event.price)):
                return False
            if int(row["quantity"]) != event.quantity:
                return False
        return True

    @staticmethod
    def _state_as_of(
        connection: sqlite3.Connection, symbol: str, date_text: str, upto_seq: int
    ) -> dict[int, TradeState]:
        states: dict[int, TradeState] = {}
        for row in connection.execute(
            "SELECT * FROM events WHERE symbol = ? AND trade_date = ? AND seq <= ? ORDER BY seq",
            (symbol, date_text, upto_seq),
        ):
            trade_id = int(row["trade_id"])
            kind = row["kind"]
            current = states.get(trade_id)
            if kind == "trade":
                states[trade_id] = TradeState(
                    trade_id=trade_id,
                    exec_ts_us=int(row["exec_ts_us"]),
                    exec_time=row["exec_time"],
                    price=Decimal(row["price"]),
                    quantity=int(row["quantity"]),
                    status="active",
                    origin_seq=int(row["seq"]),
                    last_seq=int(row["seq"]),
                )
            elif current is None:
                raise RuntimeError(f"corrupt log: {kind} for unknown trade {trade_id}")
            elif kind == "correction":
                states[trade_id] = replace(
                    current,
                    price=Decimal(row["price"]),
                    quantity=int(row["quantity"]),
                    last_seq=int(row["seq"]),
                )
            else:  # cancel: price/quantity and the original time stay frozen
                states[trade_id] = replace(
                    current, status="cancelled", last_seq=int(row["seq"])
                )
        return states

    # -- append -----------------------------------------------------------
    def append_upload(
        self,
        symbol: str,
        trade_date: date,
        events: list[EventInput],
        max_events: int = MAX_EVENTS_PER_DAY,
    ) -> UploadResult:
        date_text = trade_date.isoformat()
        if not events:
            raise DomainError(422, "empty_batch", "a batch must contain at least one message")
        if any(event.seq <= 0 for event in events):
            raise DomainError(
                422, "invalid_sequence", "exchange sequence numbers must be positive"
            )

        # Within-batch continuity is a pure request property; check it before
        # opening a write transaction.  Duplicates fail here too.
        expected = events[0].seq
        for event in events:
            if event.seq != expected:
                raise DomainError(
                    409,
                    "sequence_gap",
                    "exchange sequence numbers must be consecutive positive integers",
                    expected_seq=expected,
                    received_seq=event.seq,
                )
            expected += 1

        with self._lock:
            with self._write_transaction() as connection:
                current = self._watermark(connection, symbol, date_text)
                target = events[-1].seq

                replayed = [event for event in events if event.seq <= current]
                fresh = [event for event in events if event.seq > current]

                # Every replayed message must be byte-for-byte the same
                # exchange message; anything else is a conflicting retransmit.
                for event in replayed:
                    row = self._event_at(connection, symbol, date_text, event.seq)
                    if row is None or not self._matches(row, event):
                        raise DomainError(
                            409,
                            "sequence_conflict",
                            "sequence number already accepted with different content",
                            seq=event.seq,
                            version=current,
                        )

                if fresh and fresh[0].seq != current + 1:
                    raise DomainError(
                        409,
                        "sequence_gap",
                        "new messages must continue at the accepted high-water mark",
                        expected_seq=current + 1,
                        received_seq=fresh[0].seq,
                        version=current,
                    )

                if not fresh:
                    # The original result of a pure retransmission is the
                    # upload that first accepted the batch's last sequence.
                    # (A retransmitted range may span two uploads; the last
                    # sequence is the one that identifies the result.)
                    owner = connection.execute(
                        "SELECT * FROM uploads WHERE symbol = ? AND trade_date = ?"
                        " AND version >= ? ORDER BY version LIMIT 1",
                        (symbol, date_text, target),
                    ).fetchone()
                    if owner is None:  # pragma: no cover - log invariant
                        raise RuntimeError("no upload owns the replayed sequences")
                    return UploadResult(
                        upload_id=int(owner["id"]),
                        version=int(owner["version"]),
                        created=False,
                        accepted_events=0,
                        replayed_events=len(replayed),
                    )

                if target > max_events:
                    raise DomainError(
                        422,
                        "daily_limit_exceeded",
                        f"at most {max_events} cumulative messages per symbol per trading day",
                        limit=max_events,
                        requested_through_seq=target,
                        version=current,
                    )

                states = self._state_as_of(connection, symbol, date_text, current)
                for event in fresh:
                    state = states.get(event.trade_id)
                    if event.kind == "trade":
                        if state is not None:
                            raise DomainError(
                                409,
                                "duplicate_trade_id",
                                "trade_id must be unique for this symbol and date",
                                trade_id=event.trade_id,
                                seq=event.seq,
                            )
                        states[event.trade_id] = TradeState(
                            trade_id=event.trade_id,
                            exec_ts_us=event.exec_ts_us,
                            exec_time=event.exec_time,
                            price=Decimal(event.price),
                            quantity=event.quantity,
                            status="active",
                            origin_seq=event.seq,
                            last_seq=event.seq,
                        )
                    elif state is None:
                        raise DomainError(
                            422,
                            "unknown_trade",
                            "message references a trade that does not exist",
                            trade_id=event.trade_id,
                            seq=event.seq,
                        )
                    elif state.status == "cancelled":
                        raise DomainError(
                            409,
                            "trade_cancelled",
                            "a cancelled trade can no longer be corrected or cancelled",
                            trade_id=event.trade_id,
                            seq=event.seq,
                        )
                    elif event.kind == "correction":
                        states[event.trade_id] = replace(
                            state,
                            price=Decimal(event.price),
                            quantity=event.quantity,
                            last_seq=event.seq,
                        )
                    else:  # cancel
                        states[event.trade_id] = replace(
                            state, status="cancelled", last_seq=event.seq
                        )

                cursor = connection.execute(
                    "INSERT INTO uploads (symbol, trade_date, version, event_count,"
                    " created_at) VALUES (?, ?, ?, ?, ?)",
                    (symbol, date_text, target, len(fresh), _now_text()),
                )
                upload_id = int(cursor.lastrowid)
                connection.executemany(
                    "INSERT INTO events (symbol, trade_date, seq, kind, trade_id,"
                    " exec_time, exec_ts_us, price, quantity, upload_id)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        (
                            symbol,
                            date_text,
                            event.seq,
                            event.kind,
                            event.trade_id,
                            event.exec_time,
                            event.exec_ts_us,
                            event.price,
                            event.quantity,
                            upload_id,
                        )
                        for event in fresh
                    ],
                )
                return UploadResult(
                    upload_id=upload_id,
                    version=target,
                    created=True,
                    accepted_events=len(fresh),
                    replayed_events=len(replayed),
                )

    # -- snapshots --------------------------------------------------------
    def publish(
        self, symbol: str, trade_date: date, version: int | None = None
    ) -> dict:
        date_text = trade_date.isoformat()
        with self._lock:
            with self._write_transaction() as connection:
                watermark = self._watermark(connection, symbol, date_text)
                chosen = watermark if version is None else version
                if chosen < 0:
                    raise DomainError(
                        422, "invalid_version", "version must be zero or greater"
                    )
                if chosen > watermark:
                    raise DomainError(
                        409,
                        "version_not_ready",
                        "requested version is above the accepted high-water mark",
                        version=chosen,
                        latest_version=watermark,
                    )
                existing = connection.execute(
                    "SELECT * FROM snapshots WHERE symbol = ? AND trade_date = ?"
                    " AND upto_seq = ?",
                    (symbol, date_text, chosen),
                ).fetchone()
                if existing is not None:
                    return {**_snapshot_payload(existing), "created": False}

                # Exactly one watermark, read inside the transaction that
                # persists it: an interleaved upload is fully in or fully out.
                states = self._state_as_of(connection, symbol, date_text, chosen)
                live = [state for state in states.values() if state.status == "active"]
                bars = build_minute_bars(live)
                published_at = _now_text()
                connection.execute(
                    "INSERT INTO snapshots (symbol, trade_date, upto_seq, bars_json,"
                    " published_at) VALUES (?, ?, ?, ?, ?)",
                    (symbol, date_text, chosen, json.dumps(bars), published_at),
                )
                return {
                    "symbol": symbol,
                    "trade_date": date_text,
                    "version": chosen,
                    "bars": bars,
                    "published_at": published_at,
                    "created": True,
                }

    def get_snapshot(
        self, symbol: str, trade_date: date, version: int | None = None
    ) -> dict:
        date_text = trade_date.isoformat()
        with self._lock:
            if version is None:
                row = self._conn.execute(
                    "SELECT * FROM snapshots WHERE symbol = ? AND trade_date = ?"
                    " ORDER BY upto_seq DESC LIMIT 1",
                    (symbol, date_text),
                ).fetchone()
            else:
                row = self._conn.execute(
                    "SELECT * FROM snapshots WHERE symbol = ? AND trade_date = ?"
                    " AND upto_seq = ?",
                    (symbol, date_text, version),
                ).fetchone()
            if row is None:
                raise DomainError(
                    404,
                    "snapshot_not_found",
                    "no published snapshot for this symbol, date and version",
                    version=version,
                )
            return _snapshot_payload(row)

    def list_snapshot_versions(self, symbol: str, trade_date: date) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT upto_seq, published_at FROM snapshots"
                " WHERE symbol = ? AND trade_date = ? ORDER BY upto_seq",
                (symbol, trade_date.isoformat()),
            ).fetchall()
        return [
            {"version": int(row["upto_seq"]), "published_at": row["published_at"]}
            for row in rows
        ]

    def versions(self, symbol: str, trade_date: date) -> dict:
        date_text = trade_date.isoformat()
        with self._lock:
            latest = self._watermark(self._conn, symbol, date_text)
            uploads = [
                _upload_payload(row)
                for row in self._conn.execute(
                    "SELECT * FROM uploads WHERE symbol = ? AND trade_date = ?"
                    " ORDER BY id",
                    (symbol, date_text),
                ).fetchall()
            ]
            event_count = int(
                self._conn.execute(
                    "SELECT COUNT(*) FROM events WHERE symbol = ? AND trade_date = ?",
                    (symbol, date_text),
                ).fetchone()[0]
            )
            snapshots = [
                {"version": int(row["upto_seq"]), "published_at": row["published_at"]}
                for row in self._conn.execute(
                    "SELECT upto_seq, published_at FROM snapshots"
                    " WHERE symbol = ? AND trade_date = ? ORDER BY upto_seq",
                    (symbol, date_text),
                ).fetchall()
            ]
        return {
            "symbol": symbol,
            "trade_date": date_text,
            "latest_version": latest,
            "event_count": event_count,
            "uploads": uploads,
            "snapshots": snapshots,
        }


def build_minute_bars(trades: Iterable[TradeState]) -> list[dict]:
    """Aggregate live trades into UTC minute OHLCV bars.

    Open and close follow execution time, then the *original* exchange
    sequence number (corrections never rewrite it), so the ordering is stable
    across corrections.  A correction can change a bar's prices and volume but
    never moves the trade to another minute, because its execution time is
    frozen.  Cancelled trades are omitted and minutes with no live trade are
    never emitted.
    """
    buckets: dict[str, list[TradeState]] = {}
    for trade in trades:
        minute = datetime.fromtimestamp(
            trade.exec_ts_us / 1_000_000, tz=timezone.utc
        ).strftime("%Y-%m-%dT%H:%M")
        buckets.setdefault(minute, []).append(trade)

    bars: list[dict] = []
    for minute in sorted(buckets):
        rows = sorted(
            buckets[minute], key=lambda state: (state.exec_ts_us, state.origin_seq)
        )
        prices = [row.price for row in rows]
        bars.append(
            {
                "minute": minute,
                "open": money_str(prices[0]),
                "high": money_str(max(prices)),
                "low": money_str(min(prices)),
                "close": money_str(prices[-1]),
                "volume": sum(row.quantity for row in rows),
                "trade_count": len(rows),
            }
        )
    return bars
