# Minute Market Data API

Turns exchange tick messages (逐笔成交 / 更正 / 撤销) into versioned minute OHLCV
bars for backtesting. FastAPI + SQLite, delivered with Docker Compose.

A realtime **collection** layer fills gaps in the lossy UDP feed from a
retained-day HTTP source before the minute bars are published.

## Run

```bash
docker compose up --build
# API + Swagger UI:  http://localhost:8000/docs
# OpenAPI JSON:      http://localhost:8000/openapi.json
# Example source:    http://localhost:8001/health
```

Compose starts two services:

* `api` — the market-data API (HTTP 8000, UDP 9001 for the demo collector).
* `source` — a local example source that streams the day over UDP **out of
  order, with duplicates and dropped sequences**, and serves the retained
  full day over HTTP for backfill (with simulated outages).

SQLite lives on the `market-data` named volume (`/data/market.db`), controlled by
`MARKET_DB`. Without Docker:

```bash
python3 -m pip install -r requirements.txt
MARKET_DB=./data/market.db uvicorn app.main:app --port 8000
```

## Endpoints

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/uploads` | Append trade / correction / cancel messages for one symbol + UTC date |
| `POST` | `/collections` | Start (or resume) realtime collection for one symbol + UTC date |
| `GET` | `/collections` | List collectors with progress and pause reasons |
| `GET` | `/collections/{symbol}/{trade_date}` | One collector's progress and pause reason |
| `DELETE` | `/collections/{symbol}/{trade_date}` | Stop a collector and drop its uncommitted buffer |
| `POST` | `/publish?symbol=&trade_date=[&version=]` | Build and store a minute-bar version (defaults to the latest accepted watermark) |
| `GET` | `/snapshots/latest?symbol=&trade_date=` | Newest published version |
| `GET` | `/snapshots/{version}?symbol=&trade_date=` | An exact historical version |
| `GET` | `/snapshots?symbol=&trade_date=` | Published versions |
| `GET` | `/versions?symbol=&trade_date=` | Latest watermark, uploads, publications |
| `GET` | `/health` | Liveness |

### Message shape

```json
{"seq": 1, "type": "trade",      "trade_id": 1001, "exec_time": "2024-01-02T09:30:05Z", "price": "100.50", "quantity": 100}
{"seq": 2, "type": "correction", "trade_id": 1001, "price": "100.60", "quantity": 80}
{"seq": 3, "type": "cancel",     "trade_id": 1001}
```

`price` accepts a JSON number or a decimal string and stays exact end-to-end
(request parsing uses `parse_float=Decimal`; storage and responses are strings).
`quantity` must be a positive integer. `exec_time` requires an explicit offset
and must fall on `trade_date` in UTC; bars are bucketed by UTC execution minute.
UDP datagrams and backfill pages carry the same message shape, one message per
datagram.

## Realtime collection

One collector per symbol/day plugs the holes in the realtime link before
publishing. Start it from `/docs` or curl:

```bash
curl -X POST http://localhost:8000/collections -H 'Content-Type: application/json' -d '{
  "symbol": "DEMO",
  "trade_date": "'$(date -u +%F)'",
  "udp_port": 9001,
  "source_url": "http://source:8001",
  "poll_interval_seconds": 1
}'
curl "http://localhost:8000/collections/DEMO/$(date -u +%F)"
```

* **Feeds.** Each collector binds a UDP port for realtime datagrams (same
  message format) and polls an HTTP source that retains the complete day:
  `GET {source_url}/messages?symbol=&trade_date=&from_seq=&limit=` →
  `{"latest_seq": N, "messages": [...]}`. On start, and again after every
  process restart, the collector immediately catches up to the source's
  highest sequence number, then keeps polling (`poll_interval_seconds`).
* **Ordering & dedup.** Out-of-order and duplicate messages are buffered in a
  persisted pending table and never stored twice. Only the contiguous prefix
  above the committed watermark is committed, through the same
  trade/correction/cancel validation and the same 100-message daily cap as
  `POST /uploads` — so the reported progress (`watermark`) can never exceed
  the committed high-water mark.
* **Backfill outages.** While the HTTP source is unreachable the UDP feed
  keeps buffering; the next successful poll fills the gap automatically.
* **同号异文 (same sequence, different content).** An incoming message that
  differs from the committed event — or from a buffered, not-yet-committed
  one, even after a restart — pauses the collector and records the offending
  sequence (`pause_reason=sequence_conflict`, `pause_seq`). Committed data is
  never overwritten; the valid contiguous prefix gathered so far is still
  committed before the pause. `POST /collections` on a paused collector
  resumes it (a persistent conflict pauses it again); `DELETE` removes the
  collector and its uncommitted buffer.
* **Status** reports `watermark` (committed high-water mark), `pending_events`
  and `buffered_max_seq` (received but uncommitted), `source_latest_seq`, and
  the pause reason when paused.

The collector commits into the same event log as uploads, so `POST /publish`,
`/snapshots/...` and `/versions` work unchanged on collected days.

### Example source chaos controls

The `source` service plays a deterministic 48-message day for any
symbol/date. Its realtime stream is configured (via `docker-compose.yml`) to
drop seqs 7/13/22, duplicate 4/18, deliver in reversed windows of 3, and take
the backfill endpoint down between t=25s and t=35s. At runtime:

```bash
curl -X POST "http://localhost:8001/chaos/http_down?seconds=15"   # simulate a backfill outage
curl -X POST "http://localhost:8001/chaos/corrupt?seq=30&price=0.01"  # forge seq 30 -> collector pauses
curl "http://localhost:8001/chaos"                                 # inspect chaos state
curl -X DELETE "http://localhost:8001/chaos"                       # clear on-demand chaos
```

`scripts/collect_smoke.py` drives the whole flow (start → catch up → publish)
against a running Compose stack.

## Rules implemented

**Ingest**

- A symbol + UTC trading day holds at most **100** cumulative accepted messages,
  enforced by sequence number, so retransmissions do not consume the allowance.
- Sequence numbers are positive integers appended consecutively. A batch may
  replay an already-accepted prefix only when every message is identical
  (kind, trade id, UTC instant, numeric price, quantity); a pure replay returns
  the original `upload_id`/`version` with HTTP 200 and `created=false`.
- A gap, different content at an accepted sequence, a duplicate trade id, or a
  correction/cancel referencing a missing trade rejects the entire batch with
  409/422. Validation and insertion share one `BEGIN IMMEDIATE` transaction, so
  a rejected batch never advances the watermark and leaves no half version.
- Trade ids are unique per symbol/day. Corrections change price and quantity
  only; the original execution time is never rewritten. A cancelled trade can
  no longer be corrected or cancelled.

**Publication**

- `POST /publish` computes bars from the maximum accepted sequence at that
  moment and stores the result under that version. Versions are immutable, so a
  correction that arrives later is reflected only by the next publication while
  older versions remain queryable at their original watermark.
- Snapshot building reads the event log inside the same transaction that stores
  the bars, at exactly one watermark; concurrent uploads are serialised against
  it, so a version can never mix two watermarks.
- OHLCV: open/close order by UTC execution time, then the original sequence.
  `high`/`low` are the extremes; `volume` sums live corrected quantities;
  cancelled trades are excluded. Minutes without a live trade are omitted —
  no synthetic empty bars.

## Layout

```
app/main.py        HTTP layer, exact body parsing, OpenAPI
app/schemas.py     message validation + UTC/Decimal normalisation
app/store.py       SQLite log, transactional append, versioned snapshots,
                   collector state and the persisted pending buffer
app/collector.py   UDP listener + HTTP backfill loop per symbol/day
app/api_models.py  response models
app/errors.py      domain errors -> {"error": {...}}
source/main.py     local example source (lossy UDP + retained-day HTTP)
tests/             pytest suite (API, schema, store, collector)
scripts/smoke.py   smoke test for upload/publish against a running server
scripts/collect_smoke.py  smoke test for the collection flow (Compose demo)
```

## Tests

```bash
python3 -m pytest
```

Docker was not available in the authoring environment, so the suite was run
directly with Python 3.11 and the two-service topology was exercised as local
processes (API + example source, real UDP and HTTP); the image builds on the
same pinned requirements.
