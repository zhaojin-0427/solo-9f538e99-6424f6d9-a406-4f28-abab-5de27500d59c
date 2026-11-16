"""Domain errors mapped to stable machine-readable API errors."""

from __future__ import annotations

from typing import Any


class DomainError(Exception):
    """A request was well-formed but violates a market-data rule."""

    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        **context: Any,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.context = {key: value for key, value in context.items() if value is not None}

    def payload(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, **self.context}
