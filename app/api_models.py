"""Response models used by the OpenAPI schema (``/docs``)."""

from __future__ import annotations

from datetime import date
from typing import Annotated, Literal

from pydantic import BaseModel, Field


class Bar(BaseModel):
    minute: Annotated[
        str, Field(description="UTC minute key, e.g. 2024-01-02T09:30", examples=["2024-01-02T09:30"])
    ]
    open: str = Field(description="Exact decimal string", examples=["100.50"])
    high: str = Field(examples=["101.00"])
    low: str = Field(examples=["100.00"])
    close: str = Field(examples=["100.75"])
    volume: int = Field(description="Sum of live corrected quantities")
    trade_count: int


class UploadResponse(BaseModel):
    upload_id: int
    version: Annotated[int, Field(description="Accepted high-water mark after this batch")]
    created: bool = Field(description="False when the batch was a byte-equivalent replay")
    accepted_events: int
    replayed_events: int


class SnapshotResponse(BaseModel):
    symbol: str
    trade_date: date
    version: Annotated[int, Field(description="High-water mark the bars were computed from")]
    bars: list[Bar]
    published_at: str


class PublishResponse(SnapshotResponse):
    created: bool = Field(description="False when this version had already been published")


class SnapshotVersion(BaseModel):
    version: int
    published_at: str


class SnapshotVersionsResponse(BaseModel):
    symbol: str
    trade_date: date
    versions: list[SnapshotVersion]


class UploadInfo(BaseModel):
    upload_id: int
    version: int
    event_count: int
    created_at: str


class VersionsResponse(BaseModel):
    symbol: str
    trade_date: date
    latest_version: int
    event_count: int
    uploads: list[UploadInfo]
    snapshots: list[SnapshotVersion]


class HealthResponse(BaseModel):
    status: Literal["ok"]
    database: str


class CollectionStatusResponse(BaseModel):
    symbol: str
    trade_date: date
    status: Literal["running", "paused"]
    pause_reason: str | None = Field(
        description="Machine-readable cause when paused, e.g. sequence_conflict"
    )
    pause_seq: int | None = Field(
        description="Sequence number that caused the pause, when applicable"
    )
    pause_detail: str | None
    watermark: int = Field(
        description="Committed high-water mark; collection progress never exceeds it"
    )
    pending_events: int = Field(
        description="Received messages buffered but not yet committed"
    )
    buffered_max_seq: int | None = Field(
        description="Highest sequence number currently buffered"
    )
    source_latest_seq: int | None = Field(
        description="Highest sequence the backfill source reported so far"
    )
    udp_port: int
    source_url: str
    poll_interval_seconds: float
    created_at: str
    updated_at: str


class CollectionListResponse(BaseModel):
    collections: list[CollectionStatusResponse]


class ErrorDetail(BaseModel):
    code: str
    message: str


class ErrorResponse(BaseModel):
    error: ErrorDetail
