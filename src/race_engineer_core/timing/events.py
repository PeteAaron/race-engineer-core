"""
Normalized event schema v1.

TimingEvent is the internal canonical form of a live timing message after
normalization. All downstream processing — state reduction, event storage,
and replay — operates on this type.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class EventType(str, Enum):
    SESSION_STATUS = "SESSION_STATUS"
    TRACK_STATUS = "TRACK_STATUS"
    RACE_CONTROL_MESSAGE = "RACE_CONTROL_MESSAGE"
    POSITION_UPDATE = "POSITION_UPDATE"
    LAP_UPDATE = "LAP_UPDATE"
    PIT_STOP = "PIT_STOP"
    UNSUPPORTED = "UNSUPPORTED"


@dataclass
class TimingEvent:
    timestamp: datetime          # timezone-aware UTC
    event_type: EventType
    driver: str | None           # driver_ref, or None for session-level events
    payload: dict[str, Any]      # normalized, JSON-serializable
    raw_topic: str | None        # original feed topic — for tracing
    event_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    # event_id is auto-generated at normalization time and preserved through
    # store round-trips. Enables log correlation and future deduplication.
