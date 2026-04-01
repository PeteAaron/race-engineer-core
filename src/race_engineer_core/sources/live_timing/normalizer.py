"""
Event normalizer.

Converts raw feed messages into normalized TimingEvents. Each supported topic
has a dedicated handler. Unknown topics produce an UNSUPPORTED event rather
than crashing. Malformed payloads on known topics return None and are logged.

Handlers are deterministic and side-effect free. The event_id on the returned
TimingEvent is unique per call by design — this does not affect determinism
of event content.

Real F1 feed shape notes (ASP.NET SignalR v2, hub "Streaming"):
  - "SessionInfo"         → SESSION_INFO  (session metadata; no Status field)
  - "SessionStatus"       → SESSION_STATUS (lifecycle: Inactive/Started/Finished/…)
  - "TrackStatus"         → TRACK_STATUS  (Status 1–7; see TRACK_STATUS_MAP below)
  - "RaceControlMessages" → RACE_CONTROL_MESSAGE (Messages dict keyed by int string)
  - "Position.z"          → POSITION_UPDATE (X/Y/Z track coordinates; NOT race order)
  - "TimingData"          → LAP_UPDATE (includes race-order Line and last lap time)
  - "PitLaneTimeCollection" → PIT_STOP (PitTimes dict; confirmed real F1 topic)
  - "DriverList"          → DRIVER_LIST (driver number → name/team mapping)

Compression:
  Topics whose names end in ".z" (e.g. "Position.z") carry base64+raw-deflate
  payloads on the wire. The transport layer (LiveTimingClient._process_ws_message)
  decompresses them before constructing RawMessage, so handlers always receive
  a plain Python dict.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from ...pipeline.events import EventType, TimingEvent
from .raw import RawMessage

logger = logging.getLogger(__name__)

# TrackStatus.Status integer codes (documented in F1 timing data community):
#   1=AllClear, 2=Yellow, 3=? (unused), 4=SafetyCar, 5=Red, 6=VSC, 7=SCEnding
# PROVISIONAL: code 3 has not been observed in production data.
TRACK_STATUS_MAP: dict[str, str] = {
    "1": "AllClear",
    "2": "Yellow",
    "4": "SafetyCar",
    "5": "Red",
    "6": "VirtualSafetyCar",
    "7": "SafetyCarEnding",
}

_TopicHandler = Callable[[RawMessage], TimingEvent | None]


# ---------------------------------------------------------------------------
# Per-topic handlers
# ---------------------------------------------------------------------------

def _handle_session_info(raw: RawMessage) -> TimingEvent | None:
    """
    SessionInfo — session-level metadata.

    Real shape (confirmed against fastf1 / F1 timing data):
        {
          "Key": 9662,
          "Type": "Race",
          "Name": "Race",
          "StartDate": "2024-03-02T15:00:00",
          "EndDate": "2024-03-02T17:00:00",
          "GmtOffset": "03:00:00",
          "Path": "2024/2024-03-02_Bahrain_Grand_Prix/2024-03-02_Race/",
          "Meeting": {
            "Key": 1242, "Name": "Bahrain Grand Prix",
            "OfficialName": "...", "Location": "Bahrain", ...
          },
          "ArchiveStatus": {"Status": "Complete"}
        }

    NOTE: There is NO top-level "Status" field in SessionInfo.
    Session lifecycle status comes from the "SessionStatus" topic.
    """
    try:
        p = raw.payload
        meeting = p.get("Meeting") or {}
        return TimingEvent(
            timestamp=raw.received_at,
            event_type=EventType.SESSION_INFO,
            driver=None,
            payload={
                "key": p.get("Key"),
                "name": str(p.get("Name", "")),
                "type": str(p.get("Type", "")),
                "meeting_name": str(meeting.get("Name", "")),
                "path": str(p.get("Path", "")),
                "gmt_offset": str(p.get("GmtOffset", "")),
            },
            raw_topic=raw.topic,
        )
    except (KeyError, TypeError, ValueError, AttributeError):
        logger.warning("normalizer: malformed payload topic=%s received_at=%s", raw.topic, raw.received_at)
        return None


def _handle_session_status(raw: RawMessage) -> TimingEvent | None:
    """
    SessionStatus — session lifecycle status.

    Real shape:  {"Status": "Started"}
    Known values: "Inactive", "Started", "Aborted", "Finished", "Ends"
    """
    try:
        p = raw.payload
        return TimingEvent(
            timestamp=raw.received_at,
            event_type=EventType.SESSION_STATUS,
            driver=None,
            payload={
                "status": str(p.get("Status", "")),
            },
            raw_topic=raw.topic,
        )
    except (KeyError, TypeError, ValueError, AttributeError):
        logger.warning("normalizer: malformed payload topic=%s received_at=%s", raw.topic, raw.received_at)
        return None


def _handle_track_status(raw: RawMessage) -> TimingEvent | None:
    """
    TrackStatus — track condition flag.

    Real shape:  {"Status": "1", "Message": "AllClear"}
    The "Message" field duplicates the human-readable form of the Status code.
    """
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
    """
    RaceControlMessages — stewards and race director messages.

    Real shape:
        {
          "Messages": {
            "0": {
              "Utc": "2024-03-02T15:02:00",
              "Lap": 1,
              "Category": "Flag",
              "Message": "GREEN LIGHT - PIT EXIT OPEN",
              "Flag": "GREEN",
              "Scope": "Track",
              "Sector": null,
              "RacingNumber": null
            }
          }
        }

    The "Lap" field is absent on some message categories (e.g. SafetyCar
    deployment before lap 1). "Utc" is an ISO-8601 string.

    The feed delivers incremental updates: each message contains only the
    newest entry added since the last update. We take the highest-integer key
    to extract that latest entry.

    PROVISIONAL: the exact set of Category values is not exhaustively documented.
    Known values include "Flag", "SafetyCar", "Other", "Drs", "CarEvent".
    """
    try:
        p = raw.payload
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
                "lap": msg.get("Lap"),          # int or None
                "utc": msg.get("Utc"),          # ISO string or None
                "scope": str(msg.get("Scope", "")),
            },
            raw_topic=raw.topic,
        )
    except (KeyError, TypeError, ValueError, AttributeError):
        logger.warning("normalizer: malformed payload topic=%s received_at=%s", raw.topic, raw.received_at)
        return None


def _handle_position_z(raw: RawMessage) -> TimingEvent | None:
    """
    Position.z — X/Y/Z track coordinates per driver (physical circuit position).

    This is NOT race-order position. Race order (P1/P2/…) comes from
    TimingData via LAP_UPDATE. Position.z gives the car's X/Y/Z coordinates
    on the circuit map, useful for live tracking visualisation.

    Real shape (after base64+deflate decompression by the transport layer):
        {
          "Position": [
            {
              "Timestamp": "00:15:23.456",
              "Entries": {
                "1":  {"Status": "OnTrack", "X": 12345, "Y": 67890, "Z": 100},
                "44": {"Status": "OnTrack", "X": 23456, "Y": 78901, "Z": 110}
              }
            }
          ]
        }

    Status values: "OnTrack", "OffTrack", "OnPitLane", "Stopped".

    PROVISIONAL: Z coordinate semantics (altitude vs. projection artifact) are
    not officially documented.
    """
    try:
        p = raw.payload
        entries = p.get("Position", [])
        if not entries:
            return None
        # Take the first (and typically only) snapshot in the update.
        snapshot = entries[0] if isinstance(entries, list) else entries
        drivers_raw = snapshot.get("Entries", {})
        positions: dict[str, Any] = {}
        for driver_num, data in drivers_raw.items():
            positions[str(driver_num)] = {
                "x": data.get("X"),
                "y": data.get("Y"),
                "z": data.get("Z"),
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
    """
    TimingData — per-driver lap times, race-order position, and status flags.

    Real shape (condensed; many fields omitted):
        {
          "Lines": {
            "1": {
              "Line": 1,
              "RacingNumber": "1",
              "NumberOfLaps": 5,
              "NumberOfPitStops": 0,
              "InPit": false,
              "Retired": false,
              "LastLapTime": {"Value": "1:32.456", "Status": 0,
                              "OverallFastest": false, "PersonalFastest": false},
              "BestLapTime": {"Value": "1:31.421"},
              "GapToLeader": "",
              "IntervalToPositionAhead": {"Value": ""}
            }
          }
        }

    The "Line" field is the live race-order position (1 = leader). This is the
    authoritative source for driver race order — not Position.z.

    LastLapTime.Value is a string formatted as "M:SS.mmm" (e.g. "1:32.456").
    An empty string means the driver has not completed a lap yet this stint.

    PROVISIONAL: incremental updates omit unchanged fields. A message may
    contain only a subset of drivers and only the fields that changed since
    the last push.
    """
    try:
        p = raw.payload
        lines = p.get("Lines", {})
        if not lines:
            return None
        lap_data: dict[str, Any] = {}
        for driver_num, data in lines.items():
            lap_data[str(driver_num)] = {
                "last_lap_time": data.get("LastLapTime", {}).get("Value"),
                "best_lap_time": data.get("BestLapTime", {}).get("Value"),
                "number_of_laps": data.get("NumberOfLaps"),
                "number_of_pit_stops": data.get("NumberOfPitStops"),
                "position": data.get("Line"),   # race order position (int)
                "in_pit": data.get("InPit"),
                "retired": data.get("Retired"),
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
    """
    PitLaneTimeCollection — measured pit lane stop durations.

    Real shape (confirmed real F1 topic):
        {
          "PitTimes": {
            "0": {
              "RacingNumber": "44",
              "Duration": "23.456",
              "Lap": 12,
              "InTime": "14:15:23.456",   # may be absent
              "OutTime": "14:15:46.912"   # may be absent
            }
          }
        }

    The feed delivers one new entry per pit stop. The integer-keyed dict grows
    through the session; we take the highest key as the most recent stop.
    Duration is a decimal-seconds string (e.g. "23.456").

    PROVISIONAL: InTime/OutTime field presence is not consistently observed
    across all sessions.
    """
    try:
        p = raw.payload
        pit_times = p.get("PitTimes", {})
        if not pit_times:
            return None
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
                "in_time": entry.get("InTime"),
                "out_time": entry.get("OutTime"),
            },
            raw_topic=raw.topic,
        )
    except (KeyError, TypeError, ValueError, AttributeError):
        logger.warning("normalizer: malformed payload topic=%s received_at=%s", raw.topic, raw.received_at)
        return None


def _handle_driver_list(raw: RawMessage) -> TimingEvent | None:
    """
    DriverList — driver number → name/team reference data.

    Real shape:
        {
          "1": {
            "RacingNumber": "1",
            "BroadcastName": "M VERSTAPPEN",
            "FullName": "Max VERSTAPPEN",
            "Tla": "VER",
            "Line": 1,
            "TeamName": "Red Bull Racing",
            "TeamColour": "3671C6",
            "FirstName": "Max",
            "LastName": "Verstappen",
            "Reference": "MAXVER01",
            "CountryCode": "NLD"
          },
          ...
        }

    Typically sent once at session start and updated if a driver changes
    (e.g. reserve driver substitution mid-season).

    PROVISIONAL: HeadshotUrl and other media fields are present in live data
    but omitted here to keep payload JSON-serializable and compact.
    """
    try:
        p = raw.payload
        drivers: dict[str, Any] = {}
        for racing_number, data in p.items():
            if not isinstance(data, dict):
                continue
            drivers[str(racing_number)] = {
                "racing_number": str(data.get("RacingNumber", racing_number)),
                "broadcast_name": str(data.get("BroadcastName", "")),
                "full_name": str(data.get("FullName", "")),
                "tla": str(data.get("Tla", "")),
                "team_name": str(data.get("TeamName", "")),
                "team_colour": str(data.get("TeamColour", "")),
            }
        if not drivers:
            return None
        return TimingEvent(
            timestamp=raw.received_at,
            event_type=EventType.DRIVER_LIST,
            driver=None,
            payload={"drivers": drivers},
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
    "SessionStatus": _handle_session_status,
    "TrackStatus": _handle_track_status,
    "RaceControlMessages": _handle_race_control_messages,
    "Position.z": _handle_position_z,
    "TimingData": _handle_timing_data,
    "PitLaneTimeCollection": _handle_pit_lane_time_collection,
    "DriverList": _handle_driver_list,
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
