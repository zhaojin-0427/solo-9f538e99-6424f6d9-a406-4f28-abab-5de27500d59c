# Minute Market Data API

Turns exchange tick messages (逐笔成交 / 更正 / 撤销) into versioned minute OHLCV
bars for backtesting. FastAPI + SQLite, delivered with Docker Compose.

## Run

```bash
docker compose up --build
# API + Swagger UI:  http://localhost:8000/docs
# OpenAPI JSON:      http://localhost:8000/openapi.json
```

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
app/store.py       SQLite log, transactional append, versioned snapshots
app/api_models.py  response models
app/errors.py      domain errors -> {"error": {...}}
tests/             pytest suite (API, schema, store)
scripts/smoke.py   smoke test against a running server
```

## Tests

```bash
python3 -m pytest
```

Docker was not available in the authoring environment, so the suite was run
directly with Python 3.11; the image builds on the same pinned requirements.
