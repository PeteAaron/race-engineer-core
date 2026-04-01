"""
Raw message model.

Represents an unprocessed message received from the live timing feed.
This layer is for debugging and auditability — it preserves the wire-level
payload before any normalization occurs.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class RawMessage:
    received_at: datetime  # timezone-aware UTC; set by the transport layer
    topic: str             # feed topic / channel name
    payload: Any           # raw payload — untyped; normalization is the boundary
    session_key: int | None = None  # optional session context if available from the feed
