"""Realtime collection: UDP ingest plus HTTP backfill per symbol/day.

Each collection session (one symbol, one UTC trading day) owns:

* a UDP socket receiving realtime datagrams — one exchange message per
  datagram, in the exact ``POST /uploads`` message format;
* a backfill loop polling an HTTP source that retains the full day
  (``GET {source_url}/messages?symbol=&trade_date=&from_seq=&limit=``)
  so gaps left by the lossy realtime link are filled by sequence number.

Both feeds converge on the same persisted pipeline in ``app.store``:

* out-of-order and duplicate messages are buffered (``pending_events``) and
  never stored twice;
* only a contiguous, rule-valid prefix is committed to the event log, so the
  collection progress never exceeds the committed watermark;
* a same-sequence/different-content message — against committed data or
  against the persisted buffer, even after a restart — pauses the collector
  and records the offending sequence number;
* when the backfill source is unreachable the UDP feed keeps buffering, and
  the next successful poll fills the gap automatically.

Collectors are rows in SQLite, so a process restart resumes every running
collector: the UDP socket is rebound and the backfill loop immediately
catches up to the source's highest sequence number.
"""

from __future__ import annotations

import asyncio
import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

from pydantic import TypeAdapter, ValidationError

from .errors import DomainError
from .schemas import EventInput, MarketMessage, prepare_events
from .store import Store

logger = logging.getLogger("market.collector")

BACKFILL_PAGE_LIMIT = 100
SOURCE_TIMEOUT_SECONDS = 5.0

_message_adapter = TypeAdapter(MarketMessage)


class SourceUnavailable(Exception):
    """The backfill source cannot be reached or returned an error."""


def parse_message(document: Any, trade_date: date) -> EventInput:
    """Validate one raw message and normalise it for the event log."""
    message = _message_adapter.validate_python(document)
    return prepare_events([message], trade_date)[0]


class BackfillClient:
    """Blocking HTTP client for the retained-day source; run in a thread."""

    def __init__(self, base_url: str, timeout: float = SOURCE_TIMEOUT_SECONDS) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def fetch_page(
        self, symbol: str, trade_date: date, from_seq: int, limit: int
    ) -> tuple[list[Any], int]:
        query = (
            f"symbol={urllib.parse.quote(symbol)}"
            f"&trade_date={trade_date.isoformat()}"
            f"&from_seq={from_seq}&limit={limit}"
        )
        url = f"{self.base_url}/messages?{query}"
        try:
            with urllib.request.urlopen(url, timeout=self.timeout) as response:
                document = json.loads(response.read(), parse_float=Decimal)
        except (urllib.error.URLError, OSError, json.JSONDecodeError) as exc:
            raise SourceUnavailable(f"backfill source unavailable: {exc}") from exc
        if not isinstance(document, dict) or not isinstance(
            document.get("messages"), list
        ):
            raise SourceUnavailable("backfill source returned a malformed page")
        latest_seq = document.get("latest_seq")
        if not isinstance(latest_seq, int) or latest_seq < 0:
            raise SourceUnavailable("backfill source returned no valid latest_seq")
        return document["messages"], latest_seq


@dataclass(frozen=True)
class CollectorConfig:
    symbol: str
    trade_date: date
    udp_port: int
    source_url: str
    poll_interval_seconds: float


class _UDPProtocol(asyncio.DatagramProtocol):
    def __init__(self, collector: "Collector") -> None:
        self._collector = collector

    def datagram_received(self, data: bytes, addr: Any) -> None:
        self._collector.on_datagram(data)

    def error_received(self, exc: Exception) -> None:  # pragma: no cover - OS dependent
        logger.warning("udp error for %s: %s", self._collector.config.symbol, exc)


class Collector:
    """One running collection session (UDP listener + backfill loop)."""

    def __init__(
        self,
        store: Store,
        config: CollectorConfig,
        on_stopped: Any = None,
    ) -> None:
        self.store = store
        self.config = config
        self.client = BackfillClient(config.source_url)
        self._on_stopped = on_stopped
        self._transport: asyncio.DatagramTransport | None = None
        self._backfill_task: asyncio.Task | None = None
        self._ingest_lock = asyncio.Lock()
        self._running = False

    async def start(self) -> None:
        """Bind the UDP socket and start the backfill loop.

        Raises ``OSError`` if the UDP port cannot be bound.
        """
        loop = asyncio.get_running_loop()
        self._transport, _ = await loop.create_datagram_endpoint(
            lambda: _UDPProtocol(self),
            local_addr=("0.0.0.0", self.config.udp_port),
        )
        self._running = True
        self._backfill_task = asyncio.create_task(self._backfill_loop())

    @property
    def bound_udp_port(self) -> int | None:
        if self._transport is None:
            return None
        sockname = self._transport.get_extra_info("sockname")
        return None if sockname is None else int(sockname[1])

    async def stop(self) -> None:
        self._running = False
        if self._transport is not None:
            self._transport.close()
            self._transport = None
        task = self._backfill_task
        self._backfill_task = None
        if task is not None and task is not asyncio.current_task():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if self._on_stopped is not None:
            self._on_stopped(self)

    # -- realtime feed ----------------------------------------------------
    def on_datagram(self, data: bytes) -> None:
        if not self._running:
            return
        try:
            document = json.loads(data, parse_float=Decimal)
            event = parse_message(document, self.config.trade_date)
        except (json.JSONDecodeError, UnicodeDecodeError, ValidationError, ValueError):
            logger.warning(
                "dropping invalid datagram for %s/%s",
                self.config.symbol, self.config.trade_date,
            )
            return
        asyncio.create_task(self._ingest(event, source="udp"))

    async def _ingest(self, event: EventInput, source: str) -> None:
        async with self._ingest_lock:
            if not self._running:
                return
            outcome = await asyncio.to_thread(
                self.store.ingest_message,
                self.config.symbol,
                self.config.trade_date,
                event,
                source,
            )
            if outcome.kind == "conflict":
                await self._pause()
                return
            if outcome.kind != "added":
                return
            result = await asyncio.to_thread(
                self.store.commit_pending, self.config.symbol, self.config.trade_date
            )
            if result.paused:
                await self._pause()

    # -- backfill ---------------------------------------------------------
    async def _backfill_loop(self) -> None:
        # The first pass runs immediately: on (re)start the collector catches
        # up to the source's highest sequence number before polling.
        while self._running:
            try:
                await self._catch_up()
            except SourceUnavailable as exc:
                # Keep receiving realtime data; the next poll retries and
                # fills the gap once the source is reachable again.
                logger.info(
                    "backfill deferred for %s/%s: %s",
                    self.config.symbol, self.config.trade_date, exc,
                )
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - defensive
                logger.exception(
                    "backfill pass failed for %s/%s",
                    self.config.symbol, self.config.trade_date,
                )
            if not self._running:
                return
            await asyncio.sleep(self.config.poll_interval_seconds)

    async def _catch_up(self) -> None:
        # One pass walks the source from the committed watermark up to the
        # source's highest sequence.  ``from_seq`` only moves forward within
        # a pass, so the loop terminates even if the source skips sequences.
        from_seq: int | None = None
        while self._running:
            if from_seq is None:
                watermark = await asyncio.to_thread(
                    self.store.latest_version,
                    self.config.symbol,
                    self.config.trade_date,
                )
                from_seq = watermark + 1
            messages, latest_seq = await asyncio.to_thread(
                self.client.fetch_page,
                self.config.symbol,
                self.config.trade_date,
                from_seq,
                BACKFILL_PAGE_LIMIT,
            )
            await asyncio.to_thread(
                self.store.note_source_latest,
                self.config.symbol,
                self.config.trade_date,
                latest_seq,
            )
            if not messages:
                return
            async with self._ingest_lock:
                if not self._running:
                    return
                highest: int | None = None
                for raw in messages:
                    try:
                        event = parse_message(raw, self.config.trade_date)
                    except (ValidationError, ValueError) as exc:
                        seq = raw.get("seq") if isinstance(raw, dict) else None
                        await asyncio.to_thread(
                            self.store.pause_collector,
                            self.config.symbol,
                            self.config.trade_date,
                            "invalid_source_message",
                            seq if isinstance(seq, int) else None,
                            f"backfill source sent an invalid message: {exc}",
                        )
                        await self._pause()
                        return
                    highest = event.seq if highest is None else max(highest, event.seq)
                    outcome = await asyncio.to_thread(
                        self.store.ingest_message,
                        self.config.symbol,
                        self.config.trade_date,
                        event,
                        "backfill",
                    )
                    if outcome.kind == "conflict":
                        # Commit the contiguous valid prefix gathered so far
                        # before stopping on the conflicting sequence.
                        await asyncio.to_thread(
                            self.store.commit_pending,
                            self.config.symbol,
                            self.config.trade_date,
                        )
                        await self._pause()
                        return
                result = await asyncio.to_thread(
                    self.store.commit_pending,
                    self.config.symbol,
                    self.config.trade_date,
                )
                if result.paused:
                    await self._pause()
                    return
            if highest is None or highest >= latest_seq:
                return
            watermark = await asyncio.to_thread(
                self.store.latest_version, self.config.symbol, self.config.trade_date
            )
            next_from = max(watermark + 1, highest + 1)
            if next_from <= from_seq:
                # The source is not honouring from_seq; stop rather than spin.
                return
            from_seq = next_from

    async def _pause(self) -> None:
        # The store has already recorded the pause (reason + sequence); shut
        # down the async side.  Called from within _ingest_lock holders.
        await self.stop()


class CollectorManager:
    """Owns all running collectors; survives in ``app.state``."""

    def __init__(self, store: Store) -> None:
        self.store = store
        self._collectors: dict[tuple[str, str], Collector] = {}

    @staticmethod
    def _key(symbol: str, trade_date: date) -> tuple[str, str]:
        return (symbol, trade_date.isoformat())

    def is_running(self, symbol: str, trade_date: date) -> bool:
        return self._key(symbol, trade_date) in self._collectors

    def _forget(self, collector: Collector) -> None:
        # A collector that paused itself (conflict, rule violation, bind
        # failure) must no longer count as running.
        key = self._key(collector.config.symbol, collector.config.trade_date)
        if self._collectors.get(key) is collector:
            del self._collectors[key]

    async def load_all(self) -> None:
        """Resume collectors that were running when the process stopped."""
        for row in await asyncio.to_thread(self.store.list_collectors):
            if row["status"] != "running":
                continue
            try:
                await self._spawn(row)
            except Exception:  # pragma: no cover - defensive
                logger.exception(
                    "cannot resume collector %s/%s",
                    row["symbol"], row["trade_date"],
                )

    async def _spawn(self, row: dict) -> None:
        config = CollectorConfig(
            symbol=row["symbol"],
            trade_date=date.fromisoformat(row["trade_date"]),
            udp_port=int(row["udp_port"]),
            source_url=row["source_url"],
            poll_interval_seconds=float(row["poll_interval_seconds"]),
        )
        collector = Collector(self.store, config, on_stopped=self._forget)
        try:
            await collector.start()
        except OSError as exc:
            await asyncio.to_thread(
                self.store.pause_collector,
                config.symbol,
                config.trade_date,
                "udp_bind_failed",
                None,
                f"cannot bind UDP port {config.udp_port}: {exc}",
            )
            return
        if config.udp_port == 0 and collector.bound_udp_port:
            await asyncio.to_thread(
                self.store.set_collector_udp_port,
                config.symbol,
                config.trade_date,
                collector.bound_udp_port,
            )
        self._collectors[self._key(config.symbol, config.trade_date)] = collector

    async def start_collector(
        self,
        symbol: str,
        trade_date: date,
        udp_port: int,
        source_url: str,
        poll_interval_seconds: float,
    ) -> tuple[dict, bool]:
        """Create or resume a collector; returns ``(status, created)``."""
        existing = await asyncio.to_thread(
            self.store.get_collector, symbol, trade_date
        )
        created = False
        if existing is None:
            await asyncio.to_thread(
                self.store.create_collector,
                symbol,
                trade_date,
                udp_port,
                source_url,
                poll_interval_seconds,
            )
            created = True
        else:
            # udp_port 0 means "any port": it never conflicts with the
            # ephemeral port an existing collector resolved to.
            if (
                udp_port != 0
                and int(existing["udp_port"]) != udp_port
                or existing["source_url"] != source_url
            ):
                raise DomainError(
                    409,
                    "collector_exists",
                    "a collector with different settings already exists;"
                    " delete it before starting a new one",
                    udp_port=existing["udp_port"],
                    source_url=existing["source_url"],
                )
            if existing["status"] == "paused":
                await asyncio.to_thread(
                    self.store.resume_collector, symbol, trade_date
                )
        if not self.is_running(symbol, trade_date):
            row = await asyncio.to_thread(
                self.store.get_collector, symbol, trade_date
            )
            await self._spawn(row)
        status = await asyncio.to_thread(
            self.store.collector_status, symbol, trade_date
        )
        return status, created

    async def stop_collector(self, symbol: str, trade_date: date) -> bool:
        collector = self._collectors.pop(self._key(symbol, trade_date), None)
        if collector is not None:
            await collector.stop()
        return await asyncio.to_thread(
            self.store.delete_collector, symbol, trade_date
        )

    async def shutdown(self) -> None:
        collectors = list(self._collectors.values())
        self._collectors.clear()
        for collector in collectors:
            await collector.stop()
