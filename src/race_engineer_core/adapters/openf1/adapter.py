"""
OpenF1 → internal model adapter.

Translates raw OpenF1 API responses into the canonical internal structures
(TimingEvent, TimingState) used by the live timing path. The goal is that
downstream components — the reducer, replay runner, and eventually the
strategy/query layer — operate on the same internal model regardless of
whether data came from the live SignalR feed or OpenF1.

Convergence map
---------------
OpenF1 endpoint         → Internal event type       → TimingState field(s)
─────────────────────────────────────────────────────────────────────────────
/v1/sessions            → SESSION_INFO              → session_info
                        → SESSION_STATUS (Finished) → session_status
/v1/drivers             → DRIVER_LIST               → driver_list
/v1/race_control        → RACE_CONTROL_MESSAGE       → race_control_messages
/v1/pit                 → PIT_STOP                  → pit_history
/v1/laps                → LAP_UPDATE (per lap/driver)→ latest_laps
/v1/position            → (state projection)        → driver_positions

Position note
─────────────
/v1/position records are sampled ~every 3 seconds throughout a session;
a race produces 30 000+ records. Emitting a POSITION_UPDATE event per
record is impractical. Instead, `final_positions()` builds driver_positions
directly from the last known position per driver. This is documented in
OpenF1SessionLoader.to_state() which applies the projection after the
event-reduce loop.

POSITION_UPDATE events in the live path carry X/Y/Z track coordinates from
Position.z, not race-order positions. OpenF1 /v1/position contains race-order
positions, not coordinates. These are fundamentally different data — merging
them into a single event type would be misleading.

Field name differences between OpenF1 and live timing
───────────────────────────────────────────────────────────────────────────
Live timing field   OpenF1 field        Mapped in adapter as
─────────────────   ────────────────    ────────────────────────────────
tla                 name_acronym        → "tla" in DRIVER_LIST payload
in_time / out_time  (not available)     → None (PROVISIONAL)
track_status        (no endpoint)       → not populated from OpenF1
best_lap_time       (computed)          → running min across driver's laps

PROVISIONAL items
─────────────────
- in_time / out_time in PIT_STOP: OpenF1 /v1/pit does not expose these.
- track_status: No OpenF1 endpoint for flag/safety car state during session.
  TimingState.track_status will be None for OpenF1-loaded sessions.
- best_lap_time: Not directly returned by /v1/laps; computed here as a
  running minimum across a driver's laps sorted by lap_number.
- session path: Not available in OpenF1. SESSION_INFO payload.path = "".
- number_of_pit_stops: Not available per-lap in OpenF1 laps endpoint.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from ...timing.events import EventType, TimingEvent

logger = logging.getLogger(__name__)

# raw_topic prefix for events produced by this adapter.
# Downstream code can use this for provenance filtering if needed.
_TOPIC_PREFIX = "openf1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_date(date_str: str | None) -> datetime | None:
    """Parse an ISO-8601 date string into a timezone-aware datetime, or None."""
    if not date_str:
        return None
    try:
        dt = datetime.fromisoformat(date_str)
        # Ensure timezone-aware; treat naive datetimes as UTC.
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        logger.debug("openf1.adapter: could not parse date %r", date_str)
        return None


def _format_lap_time(seconds: float | None) -> str | None:
    """
    Format a lap duration in seconds to 'M:SS.mmm' string.

    Matches the format used by the live timing path (e.g. '1:32.456').
    Returns None for zero, negative, or missing durations (lap not completed).
    """
    if seconds is None or seconds <= 0:
        return None
    minutes = int(seconds // 60)
    remaining = seconds - minutes * 60
    return f"{minutes}:{remaining:06.3f}"


def _session_start_ts(session: dict[str, Any]) -> datetime:
    """Return the session start datetime, defaulting to epoch UTC if absent."""
    ts = _parse_date(session.get("date_start"))
    return ts if ts is not None else datetime(1970, 1, 1, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Per-endpoint translators
# ---------------------------------------------------------------------------

def session_to_events(session: dict[str, Any]) -> list[TimingEvent]:
    """
    Convert an OpenF1 session record to [SESSION_INFO, SESSION_STATUS] events.

    Historical sessions are always complete, so SESSION_STATUS is always
    "Finished". If a partial/live session were loaded, callers must override
    this (out of scope for v1).
    """
    ts = _session_start_ts(session)

    session_info = TimingEvent(
        timestamp=ts,
        event_type=EventType.SESSION_INFO,
        driver=None,
        payload={
            "key": session.get("session_key"),
            "name": str(session.get("session_name", "")),
            "type": str(session.get("session_type", "")),
            "meeting_name": str(session.get("meeting_name", "")),
            "path": "",  # PROVISIONAL: not available in OpenF1
            "gmt_offset": str(session.get("gmt_offset", "")),
        },
        raw_topic=f"{_TOPIC_PREFIX}:sessions",
    )

    session_status = TimingEvent(
        timestamp=ts,
        event_type=EventType.SESSION_STATUS,
        driver=None,
        payload={"status": "Finished"},
        raw_topic=f"{_TOPIC_PREFIX}:sessions",
    )

    return [session_info, session_status]


def drivers_to_event(
    drivers: list[dict[str, Any]],
    ts: datetime,
) -> TimingEvent | None:
    """
    Convert OpenF1 driver records to a single DRIVER_LIST event.

    OpenF1 uses 'name_acronym' for the 3-letter abbreviation; the live timing
    path calls it 'tla'. Both are mapped to the 'tla' key in the payload.

    Returns None if the list is empty or all records lack a driver_number.
    """
    driver_map: dict[str, Any] = {}
    for d in drivers:
        num = d.get("driver_number")
        if num is None:
            continue
        ref = str(num)
        driver_map[ref] = {
            "racing_number": ref,
            "broadcast_name": str(d.get("broadcast_name", "")),
            "full_name": str(d.get("full_name", "")),
            "tla": str(d.get("name_acronym", "")),          # OpenF1: name_acronym → live: Tla
            "team_name": str(d.get("team_name", "")),
            "team_colour": str(d.get("team_colour") or ""),  # may be null
        }

    if not driver_map:
        return None

    return TimingEvent(
        timestamp=ts,
        event_type=EventType.DRIVER_LIST,
        driver=None,
        payload={"drivers": driver_map},
        raw_topic=f"{_TOPIC_PREFIX}:drivers",
    )


def race_control_to_events(records: list[dict[str, Any]]) -> list[TimingEvent]:
    """
    Convert OpenF1 race control records to RACE_CONTROL_MESSAGE events.

    The payload shape matches the live timing normalizer output for
    RaceControlMessages, making these events transparent to the reducer.
    """
    events: list[TimingEvent] = []
    for record in records:
        ts = _parse_date(record.get("date"))
        if ts is None:
            logger.warning("openf1.adapter: race_control record missing date, skipping")
            continue
        events.append(TimingEvent(
            timestamp=ts,
            event_type=EventType.RACE_CONTROL_MESSAGE,
            driver=None,
            payload={
                "category": str(record.get("category", "")),
                "message": str(record.get("message", "")),
                "flag": str(record.get("flag") or ""),
                "lap": record.get("lap_number"),
                "utc": record.get("date"),
                "scope": str(record.get("scope") or ""),
            },
            raw_topic=f"{_TOPIC_PREFIX}:race_control",
        ))
    return events


def pit_to_events(records: list[dict[str, Any]]) -> list[TimingEvent]:
    """
    Convert OpenF1 pit records to PIT_STOP events.

    PROVISIONAL: OpenF1 /v1/pit does not expose in_time or out_time.
    These fields are set to None. If a future OpenF1 API version adds them,
    update this function.
    """
    events: list[TimingEvent] = []
    for record in records:
        ts = _parse_date(record.get("date"))
        if ts is None:
            logger.warning("openf1.adapter: pit record missing date, skipping")
            continue
        driver = str(record.get("driver_number", ""))
        duration = record.get("pit_duration")
        events.append(TimingEvent(
            timestamp=ts,
            event_type=EventType.PIT_STOP,
            driver=driver,
            payload={
                "racing_number": driver,
                "duration": str(duration) if duration is not None else None,
                "lap": record.get("lap_number"),
                "in_time": None,   # PROVISIONAL: not available in OpenF1
                "out_time": None,  # PROVISIONAL: not available in OpenF1
            },
            raw_topic=f"{_TOPIC_PREFIX}:pit",
        ))
    return events


def laps_to_events(records: list[dict[str, Any]]) -> list[TimingEvent]:
    """
    Convert OpenF1 lap records to LAP_UPDATE events.

    One LAP_UPDATE event is emitted per (driver, lap) pair, in lap-number
    order per driver. The 'best_lap_time' in each event reflects the running
    minimum up to and including that lap — the same running-best semantics
    used in the live timing path.

    'position' (race order) is None in these events because /v1/laps does
    not include race-order position. Race order is sourced separately from
    /v1/position via final_positions() and applied as a state projection.

    'number_of_pit_stops' is None because OpenF1 /v1/laps does not expose
    this per-lap count. It is derivable by cross-referencing /v1/pit, but
    that computation is out of scope for v1.
    """
    # Group by driver and sort by lap_number for correct running-best semantics.
    by_driver: dict[str, list[dict[str, Any]]] = {}
    for r in records:
        num = str(r.get("driver_number", ""))
        if not num:
            continue
        by_driver.setdefault(num, []).append(r)

    events: list[TimingEvent] = []
    for driver_num, laps in by_driver.items():
        laps.sort(key=lambda r: r.get("lap_number") or 0)
        running_best: float | None = None

        for lap in laps:
            duration: float | None = lap.get("lap_duration")
            # Only update running best for valid, non-outlap durations.
            if duration is not None and duration > 0:
                running_best = (
                    duration if running_best is None else min(running_best, duration)
                )

            ts = _parse_date(lap.get("date_start"))
            if ts is None:
                # Fall back to session start; better to have an event than drop it.
                ts = datetime(1970, 1, 1, tzinfo=timezone.utc)

            events.append(TimingEvent(
                timestamp=ts,
                event_type=EventType.LAP_UPDATE,
                driver=driver_num,
                payload={
                    "lines": {
                        driver_num: {
                            "last_lap_time": _format_lap_time(duration),
                            "best_lap_time": _format_lap_time(running_best),
                            "number_of_laps": lap.get("lap_number"),
                            "number_of_pit_stops": None,   # PROVISIONAL
                            "position": None,              # from /v1/position, not /v1/laps
                            "in_pit": bool(lap.get("is_pit_out_lap", False)),
                            "retired": False,
                        }
                    }
                },
                raw_topic=f"{_TOPIC_PREFIX}:laps",
            ))

    return events


def final_positions(position_records: list[dict[str, Any]]) -> dict[str, int]:
    """
    Derive final race-order positions from OpenF1 position records.

    OpenF1 /v1/position contains position snapshots sampled ~every 3 seconds.
    This function returns the most recent position per driver (chronologically
    last record), which is the final race classification at the end of the
    session.

    This is a state projection, not an event sequence. It is applied directly
    to TimingState.driver_positions after the event-reduce loop in
    OpenF1SessionLoader.to_state(). See the module docstring for why this
    approach is used rather than POSITION_UPDATE events.
    """
    latest: dict[str, tuple[datetime, int]] = {}
    for record in position_records:
        driver = str(record.get("driver_number", ""))
        pos = record.get("position")
        ts = _parse_date(record.get("date"))
        if not driver or pos is None or ts is None:
            continue
        try:
            pos_int = int(pos)
        except (TypeError, ValueError):
            continue
        existing = latest.get(driver)
        if existing is None or ts > existing[0]:
            latest[driver] = (ts, pos_int)

    return {driver: pos for driver, (_, pos) in latest.items()}
