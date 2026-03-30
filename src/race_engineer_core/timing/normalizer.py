"""
Event normalizer.

Converts raw feed messages into normalized TimingEvents. Each supported topic
has a dedicated handler. Unknown topics produce an UNSUPPORTED event rather
than crashing. Malformed payloads on known topics return None and are logged.

Handlers are deterministic and side-effect free. The event_id on the returned
TimingEvent is unique per call by design — this does not affect determinism
of event content.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from .events import EventType, TimingEvent
from .raw import RawMessage

logger = logging.getLogger(__name__)

_TopicHandler = Callable[[RawMessage], TimingEvent | None]


# ---------------------------------------------------------------------------
# Per-topic handlers
# ---------------------------------------------------------------------------

def _handle_session_info(raw: RawMessage) -> TimingEvent | None:
    try:
        p = raw.payload
        return TimingEvent(
            timestamp=raw.received_at,
            event_type=EventType.SESSION_STATUS,
            driver=None,
            payload={
                "status": str(p.get("Status", "")),
                "name": str(p.get("Name", "")),
                "type": str(p.get("Type", "")),
            },
            raw_topic=raw.topic,
        )
    except (KeyError, TypeError, ValueError, AttributeError):
        logger.warning("normalizer: malformed payload topic=%s received_at=%s", raw.topic, raw.received_at)
        return None


def _handle_track_status(raw: RawMessage) -> TimingEvent | None:
    try:
        p = raw.payload
        return TimingEvent(
            timestamp=raw.received_at,
            event_type=EventType.TRACK_STATUS,
            driver=None,
            payload={
                "status": str(p.get("Status", "")),
                "message": str(p.get("Message", "")),
            },
            raw_topic=raw.topic,
        )
    except (KeyError, TypeError, ValueError, AttributeError):
        logger.warning("normalizer: malformed payload topic=%s received_at=%s", raw.topic, raw.received_at)
        return None


def _handle_race_control_messages(raw: RawMessage) -> TimingEvent | None:
    try:
        p = raw.payload
        # The feed delivers a dict keyed by message index; grab the latest entry.
        messages = p.get("Messages", {})
        if not messages:
            return None
        latest_key = max(messages.keys(), key=lambda k: int(k))
        msg = messages[latest_key]
        return TimingEvent(
            timestamp=raw.received_at,
            event_type=EventType.RACE_CONTROL_MESSAGE,
            driver=None,
            payload={
                "category": str(msg.get("Category", "")),
                "message": str(msg.get("Message", "")),
                "flag": str(msg.get("Flag", "")),
                "lap": msg.get("Lap"),
            },
            raw_topic=raw.topic,
        )
    except (KeyError, TypeError, ValueError, AttributeError):
        logger.warning("normalizer: malformed payload topic=%s received_at=%s", raw.topic, raw.received_at)
        return None


def _handle_position_z(raw: RawMessage) -> TimingEvent | None:
    # Position.z delivers a dict of position entries keyed by driver number.
    # We emit one event per driver number present in the payload.
    # For a minimal v1 we emit a single event representing the full snapshot.
    try:
        p = raw.payload
        entries = p.get("Position", [])
        if not entries:
            return None
        # Take the first entry in the list (most recent position snapshot).
        snapshot = entries[0] if isinstance(entries, list) else entries
        drivers_raw = snapshot.get("Entries", {})
        positions: dict[str, Any] = {}
        for driver_num, data in drivers_raw.items():
            positions[str(driver_num)] = {
                "position": data.get("Line"),
                "status": data.get("Status"),
            }
        return TimingEvent(
            timestamp=raw.received_at,
            event_type=EventType.POSITION_UPDATE,
            driver=None,  # snapshot covers all drivers
            payload={"positions": positions},
            raw_topic=raw.topic,
        )
    except (KeyError, TypeError, ValueError, AttributeError):
        logger.warning("normalizer: malformed payload topic=%s received_at=%s", raw.topic, raw.received_at)
        return None


def _handle_timing_data(raw: RawMessage) -> TimingEvent | None:
    try:
        p = raw.payload
        lines = p.get("Lines", {})
        if not lines:
            return None
        # Emit a single event capturing the lap data dict for all drivers.
        lap_data: dict[str, Any] = {}
        for driver_num, data in lines.items():
            lap_data[str(driver_num)] = {
                "last_lap_time": data.get("LastLapTime", {}).get("Value"),
                "number_of_laps": data.get("NumberOfLaps"),
                "position": data.get("Line"),
            }
        return TimingEvent(
            timestamp=raw.received_at,
            event_type=EventType.LAP_UPDATE,
            driver=None,  # snapshot covers multiple drivers
            payload={"lines": lap_data},
            raw_topic=raw.topic,
        )
    except (KeyError, TypeError, ValueError, AttributeError):
        logger.warning("normalizer: malformed payload topic=%s received_at=%s", raw.topic, raw.received_at)
        return None


def _handle_pit_lane_time_collection(raw: RawMessage) -> TimingEvent | None:
    try:
        p = raw.payload
        pit_times = p.get("PitTimes", {})
        if not pit_times:
            return None
        # Take the most recently keyed entry by numeric key.
        latest_key = max(pit_times.keys(), key=lambda k: int(k))
        entry = pit_times[latest_key]
        return TimingEvent(
            timestamp=raw.received_at,
            event_type=EventType.PIT_STOP,
            driver=str(entry.get("RacingNumber", "")),
            payload={
                "racing_number": str(entry.get("RacingNumber", "")),
                "duration": entry.get("Duration"),
                "lap": entry.get("Lap"),
            },
            raw_topic=raw.topic,
        )
    except (KeyError, TypeError, ValueError, AttributeError):
        logger.warning("normalizer: malformed payload topic=%s received_at=%s", raw.topic, raw.received_at)
        return None


# ---------------------------------------------------------------------------
# Dispatch table — maps topic names to handlers
# ---------------------------------------------------------------------------

_HANDLERS: dict[str, _TopicHandler] = {
    "SessionInfo": _handle_session_info,
    "TrackStatus": _handle_track_status,
    "RaceControlMessages": _handle_race_control_messages,
    "Position.z": _handle_position_z,
    "TimingData": _handle_timing_data,
    "PitLaneTimeCollection": _handle_pit_lane_time_collection,
}


def _make_unsupported(raw: RawMessage) -> TimingEvent:
    return TimingEvent(
        timestamp=raw.received_at,
        event_type=EventType.UNSUPPORTED,
        driver=None,
        payload={},
        raw_topic=raw.topic,
    )


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def normalize(raw: RawMessage) -> TimingEvent | None:
    """
    Convert a raw feed message into a normalized TimingEvent.

    Returns:
        TimingEvent with the appropriate event_type for known topics.
        TimingEvent(event_type=UNSUPPORTED) for unrecognised topics.
        None if the payload on a known topic is malformed.
    """
    handler = _HANDLERS.get(raw.topic)

    if handler is None:
        logger.debug("normalizer: unsupported topic=%s", raw.topic)
        return _make_unsupported(raw)

    event = handler(raw)

    if event is None:
        # Handler already logged; return None to signal the caller to discard.
        return None

    return event


def supported_topics() -> frozenset[str]:
    """Return the set of topic names this normalizer handles."""
    return frozenset(_HANDLERS.keys())
