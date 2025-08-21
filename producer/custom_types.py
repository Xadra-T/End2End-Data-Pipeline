from __future__ import annotations

from typing import TypedDict


class Event(TypedDict):
    event_id: str
    user_id: str
    session_id: str
    event_type: str
    event_timestamp: int
    request_latency_ms: int
    status: str
    error_code: int | None
    product_id: int | None
