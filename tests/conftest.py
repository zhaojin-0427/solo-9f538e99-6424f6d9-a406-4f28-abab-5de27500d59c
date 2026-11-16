"""Shared pytest fixtures: a fresh API backed by a temporary SQLite file."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path, monkeypatch) -> Iterator[TestClient]:
    database = tmp_path / "market.db"
    monkeypatch.setenv("MARKET_DB", str(database))
    # Import after the env var is set so the lifespan opens the temp database.
    from app.main import app

    with TestClient(app) as test_client:
        yield test_client


def trade(seq: int, trade_id: int, at: str, price: str, qty: int) -> dict:
    return {
        "seq": seq,
        "type": "trade",
        "trade_id": trade_id,
        "exec_time": at,
        "price": price,
        "quantity": qty,
    }


def correction(seq: int, trade_id: int, price: str, qty: int) -> dict:
    return {
        "seq": seq,
        "type": "correction",
        "trade_id": trade_id,
        "price": price,
        "quantity": qty,
    }


def cancel(seq: int, trade_id: int) -> dict:
    return {"seq": seq, "type": "cancel", "trade_id": trade_id}


def upload(client: TestClient, messages: list[dict], symbol: str = "AAPL",
           trade_date: str = "2024-01-02", expected: int = 201):
    response = client.post(
        "/uploads",
        json={"symbol": symbol, "trade_date": trade_date, "messages": messages},
    )
    assert response.status_code == expected, response.text
    return response.json()
