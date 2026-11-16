"""FastAPI application: versioned minute market data from exchange messages."""

from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from datetime import date as date_type
from decimal import Decimal
from pathlib import Path
from typing import Annotated, Any, Iterator

from fastapi import FastAPI, Path as PathParam, Query, Request, Response, status
from fastapi.exceptions import RequestValidationError
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from pydantic import AfterValidator, ValidationError
from starlette.concurrency import run_in_threadpool

from .api_models import (
    CollectionListResponse,
    CollectionStatusResponse,
    ErrorResponse,
    HealthResponse,
    PublishResponse,
    SnapshotResponse,
    SnapshotVersionsResponse,
    UploadResponse,
    VersionsResponse,
)
from .collector import CollectorManager
from .errors import DomainError
from .schemas import (
    CollectionStartRequest,
    UploadRequest,
    normalize_symbol,
    prepare_events,
)
from .store import MAX_EVENTS_PER_DAY, Store

MAX_BODY_BYTES = 2 * 1024 * 1024

# Query symbols use the same rules as upload symbols (trimmed, no whitespace).
SymbolQuery = Annotated[str, Query(min_length=1, max_length=64), AfterValidator(normalize_symbol)]

DESCRIPTION = """
Turns exchange trade / correction / cancel messages (逐笔成交及更正、撤销) into
minute OHLCV bars for backtesting.

### Ingest (`POST /uploads`)
* Cumulative messages per symbol per UTC trading day are capped at
  **100** (retransmitted, already-accepted messages do not count twice).
* Sequence numbers are positive integers and must append consecutively.
* Retransmitting an identical already-accepted message returns the original
  result (HTTP 200, `created=false`).
* A gap, a different payload for an accepted sequence number, a duplicate
  trade id, or a reference to a missing trade rejects the **whole** batch
  atomically (HTTP 409/422); nothing is stored.
* Trade ids are unique per symbol/day. A correction changes price and quantity
  only and keeps the original execution time. A cancelled trade can never be
  corrected or cancelled again.

### Publish / query (`POST /publish`, `GET /snapshots/...`)
* Bars are generated from the maximum accepted sequence number at publish
  time. Each publication is stored and addressed by that version.
* Open/close sort by UTC execution time, then original sequence number.
* Prices are exact `Decimal` strings; volume is a positive integer; minutes
  with no live trade are omitted.
* A correction received after an earlier minute was published is reflected by
  the next publication, while the older version remains queryable by version.
* Uploading and publishing are serialised: one version uses exactly one
  watermark, and a failed batch leaves no partial version.

### Realtime collection (`POST /collections`, `GET /collections/...`)
* One collector per symbol/day receives UDP datagrams (same message format)
  and backfills gaps by sequence number from an HTTP source that retains the
  full day; on start and after a restart it catches up to the source's
  highest sequence.
* Out-of-order and duplicate messages are buffered, never stored twice; only
  a contiguous, rule-valid prefix is committed (same trade/correction/cancel
  rules and the same 100-message daily cap), so progress never exceeds the
  committed watermark.
* A same-sequence message with different content — against committed data or
  the persisted buffer, even after a restart — pauses the collector and
  reports the sequence number; committed data is never overwritten.
* While the backfill source is down, realtime messages keep buffering; the
  next successful poll fills the gap automatically.
"""


def _database_path() -> str:
    return os.environ.get("MARKET_DB", str(Path("data/market.db")))


@asynccontextmanager
async def lifespan(app: FastAPI) -> Iterator[None]:
    store = Store(_database_path())
    manager = CollectorManager(store)
    app.state.store = store
    app.state.collector_manager = manager
    await manager.load_all()
    try:
        yield
    finally:
        await manager.shutdown()
        store.close()


app = FastAPI(
    title="Minute Market Data API",
    version="1.0.0",
    summary="Versioned minute OHLCV generated from exchange tick messages.",
    description=DESCRIPTION,
    lifespan=lifespan,
    openapi_tags=[
        {"name": "ingest", "description": "Append exchange messages"},
        {"name": "collect", "description": "Realtime collection with gap backfill"},
        {"name": "bars", "description": "Publish and read versioned minute bars"},
        {"name": "ops", "description": "Health and version introspection"},
    ],
)


def get_store(request: Request) -> Store:
    return request.app.state.store


def get_manager(request: Request) -> CollectorManager:
    return request.app.state.collector_manager


def _parse_upload_body(raw: bytes) -> UploadRequest:
    """Parse the upload body without ever routing prices through ``float``.

    ``json.loads`` would turn a JSON price literal into a binary float and lose
    digits before Pydantic sees it.  ``parse_float=Decimal`` keeps the literal
    exact, so both ``"100.50"`` and ``100.50`` mean the same rational number.
    """

    try:
        document = json.loads(raw, parse_float=Decimal)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise DomainError(
            422, "invalid_json", f"request body is not valid JSON: {exc.msg}"
        ) from exc
    if not isinstance(document, dict):
        raise DomainError(
            422, "invalid_json", "request body must be a JSON object"
        )
    try:
        return UploadRequest.model_validate(document)
    except ValidationError as exc:
        details = [
            {
                "location": [str(part) for part in item.get("loc", ())],
                "message": item.get("msg", "invalid value"),
                "type": item.get("type", "value_error"),
            }
            for item in exc.errors()
        ]
        raise DomainError(
            422,
            "validation_error",
            "Request body or parameters are invalid",
            details=details,
        ) from exc


async def _read_upload_body(request: Request) -> UploadRequest:
    raw = await request.body()
    if len(raw) > MAX_BODY_BYTES:
        raise DomainError(
            413,
            "payload_too_large",
            f"request body must not exceed {MAX_BODY_BYTES} bytes",
        )
    return await run_in_threadpool(_parse_upload_body, raw)


@app.exception_handler(DomainError)
async def domain_error_handler(request: Request, exc: DomainError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content={"error": exc.payload()})


@app.exception_handler(RequestValidationError)
async def validation_error_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    details = [
        {
            "location": [str(part) for part in item.get("loc", ())],
            "message": item.get("msg", "invalid value"),
            "type": item.get("type", "value_error"),
        }
        for item in exc.errors()
    ]
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": {
                "code": "validation_error",
                "message": "Request body or parameters are invalid",
                "details": details,
            }
        },
    )


@app.get("/health", response_model=HealthResponse, tags=["ops"])
def health(request: Request) -> HealthResponse:
    return HealthResponse(status="ok", database=get_store(request).path)


@app.post(
    "/uploads",
    response_model=UploadResponse,
    status_code=status.HTTP_201_CREATED,
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {
                "application/json": {
                    "schema": {"$ref": "#/components/schemas/UploadRequest"}
                }
            },
        }
    },
    responses={
        200: {
            "model": UploadResponse,
            "description": "Retransmission of an accepted batch: original result returned",
        },
        409: {
            "model": ErrorResponse,
            "description": "Sequence gap/conflict, duplicate trade id or cancelled trade",
        },
        422: {"model": ErrorResponse, "description": "Invalid message or daily limit exceeded"},
    },
    tags=["ingest"],
)
async def upload(request: Request, response: Response) -> UploadResponse:
    payload = await _read_upload_body(request)
    try:
        events = prepare_events(payload.messages, payload.trade_date)
    except ValueError as exc:
        raise DomainError(422, "invalid_message", str(exc)) from exc

    result = await run_in_threadpool(
        get_store(request).append_upload,
        payload.symbol,
        payload.trade_date,
        events,
        MAX_EVENTS_PER_DAY,
    )
    if not result.created:
        # Idempotent replay: same result, not a new resource.
        response.status_code = status.HTTP_200_OK
    return UploadResponse(
        upload_id=result.upload_id,
        version=result.version,
        created=result.created,
        accepted_events=result.accepted_events,
        replayed_events=result.replayed_events,
    )


@app.post(
    "/publish",
    response_model=PublishResponse,
    responses={
        409: {"model": ErrorResponse, "description": "Requested version not accepted yet"},
    },
    tags=["bars"],
)
def publish(
    request: Request,
    symbol: SymbolQuery,
    trade_date: Annotated[date_type, Query()],
    version: Annotated[
        int | None,
        Query(ge=0, description="High-water mark to publish; defaults to the latest accepted"),
    ] = None,
) -> PublishResponse:
    return PublishResponse.model_validate(
        get_store(request).publish(symbol, trade_date, version)
    )


@app.get(
    "/snapshots/latest",
    response_model=SnapshotResponse,
    responses={404: {"model": ErrorResponse, "description": "Nothing published yet"}},
    tags=["bars"],
)
def latest_snapshot(
    request: Request,
    symbol: SymbolQuery,
    trade_date: Annotated[date_type, Query()],
) -> SnapshotResponse:
    return SnapshotResponse.model_validate(
        get_store(request).get_snapshot(symbol, trade_date)
    )


@app.get(
    "/snapshots/{version}",
    response_model=SnapshotResponse,
    responses={404: {"model": ErrorResponse, "description": "Version never published"}},
    tags=["bars"],
)
def snapshot_by_version(
    request: Request,
    version: Annotated[int, PathParam(ge=0)],
    symbol: SymbolQuery,
    trade_date: Annotated[date_type, Query()],
) -> SnapshotResponse:
    return SnapshotResponse.model_validate(
        get_store(request).get_snapshot(symbol, trade_date, version)
    )


@app.get("/snapshots", response_model=SnapshotVersionsResponse, tags=["bars"])
def snapshot_versions(
    request: Request,
    symbol: SymbolQuery,
    trade_date: Annotated[date_type, Query()],
) -> SnapshotVersionsResponse:
    return SnapshotVersionsResponse(
        symbol=symbol,
        trade_date=trade_date,
        versions=get_store(request).list_snapshot_versions(symbol, trade_date),
    )


@app.get("/versions", response_model=VersionsResponse, tags=["ops"])
def versions(
    request: Request,
    symbol: SymbolQuery,
    trade_date: Annotated[date_type, Query()],
) -> VersionsResponse:
    return VersionsResponse.model_validate(
        get_store(request).versions(symbol, trade_date)
    )


# -- realtime collection ----------------------------------------------------
SymbolPath = Annotated[
    str, PathParam(min_length=1, max_length=64), AfterValidator(normalize_symbol)
]


@app.post(
    "/collections",
    response_model=CollectionStatusResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        200: {
            "model": CollectionStatusResponse,
            "description": "Collector already existed; running collector returned"
            " unchanged, paused collector resumed",
        },
        409: {
            "model": ErrorResponse,
            "description": "Conflicting collector settings or UDP port in use",
        },
        422: {"model": ErrorResponse, "description": "Invalid request"},
    },
    tags=["collect"],
)
async def start_collection(
    request: Request, response: Response, payload: CollectionStartRequest
) -> CollectionStatusResponse:
    status_dict, created = await get_manager(request).start_collector(
        payload.symbol,
        payload.trade_date,
        payload.udp_port,
        payload.source_url,
        payload.poll_interval_seconds,
    )
    if not created:
        response.status_code = status.HTTP_200_OK
    return CollectionStatusResponse.model_validate(status_dict)


@app.get(
    "/collections",
    response_model=CollectionListResponse,
    tags=["collect"],
)
def list_collections(request: Request) -> CollectionListResponse:
    return CollectionListResponse(
        collections=[
            CollectionStatusResponse.model_validate(item)
            for item in get_store(request).list_collector_statuses()
        ]
    )


@app.get(
    "/collections/{symbol}/{trade_date}",
    response_model=CollectionStatusResponse,
    responses={404: {"model": ErrorResponse, "description": "No collector"}},
    tags=["collect"],
)
def collection_status(
    request: Request,
    symbol: SymbolPath,
    trade_date: Annotated[date_type, PathParam()],
) -> CollectionStatusResponse:
    return CollectionStatusResponse.model_validate(
        get_store(request).collector_status(symbol, trade_date)
    )


@app.delete(
    "/collections/{symbol}/{trade_date}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={404: {"model": ErrorResponse, "description": "No collector"}},
    tags=["collect"],
)
async def stop_collection(
    request: Request,
    symbol: SymbolPath,
    trade_date: Annotated[date_type, PathParam()],
) -> Response:
    stopped = await get_manager(request).stop_collector(symbol, trade_date)
    if not stopped:
        raise DomainError(
            404, "collector_not_found", "no collector for this symbol and date"
        )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


def _openapi() -> dict[str, Any]:
    """Build the spec, adding the manually parsed upload body schema.

    ``/uploads`` parses its own body (to keep prices exact), so FastAPI cannot
    infer the request model.  The model is registered here explicitly so
    Swagger UI and generated clients still see the full contract.
    """

    if app.openapi_schema is not None:
        return app.openapi_schema
    schema = get_openapi(
        title=app.title,
        version=app.version,
        summary=app.summary,
        description=app.description,
        routes=app.routes,
        tags=app.openapi_tags,
    )
    components = schema.setdefault("components", {}).setdefault("schemas", {})
    upload_schema = UploadRequest.model_json_schema(
        ref_template="#/components/schemas/{model}"
    )
    for name, definition in upload_schema.pop("$defs", {}).items():
        components.setdefault(name, definition)
    components["UploadRequest"] = upload_schema
    app.openapi_schema = schema
    return schema


app.openapi = _openapi  # type: ignore[method-assign]
