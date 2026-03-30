"""
State engine.

TimingState holds the minimal derived state built from the event stream.
reduce() is a pure function: previous state + event → new state.

Contract:
- Never mutate state in place. Always use dataclasses.replace() with
  explicitly copied collections for fields that change.
- No-op events (UNSUPPORTED, or events that genuinely change nothing)
  return the same state object directly — no unnecessary allocation.
- The reducer has no I/O, no logging, no side effects.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from typing import Any

from .events import EventType, TimingEvent

_RACE_CONTROL_CAP = 100  # maximum race control messages retained in state


@dataclass
class TimingState:
    session_status: str | None = None
    track_status: str | None = None
    # Capped ring buffer of recent race control messages.
    race_control_messages: list[dict[str, Any]] = field(default_factory=list)
    # driver_ref → current race position
    driver_positions: dict[str, int] = field(default_factory=dict)
    # driver_ref → latest lap info dict
    latest_laps: dict[str, dict[str, Any]] = field(default_factory=dict)
    # Ordered list of pit stop dicts, oldest first.
    pit_history: list[dict[str, Any]] = field(default_factory=list)


def initial_state() -> TimingState:
    """Return a clean initial state. Call once at session start."""
    return TimingState()


def reduce(state: TimingState, event: TimingEvent) -> TimingState:
    """
    Apply one event to state and return the resulting state.

    Returns the same object for no-op events. Returns a new object
    (via dataclasses.replace) for any event that changes state.
    """
    if event.event_type == EventType.SESSION_STATUS:
        return _apply_session_status(state, event)

    if event.event_type == EventType.TRACK_STATUS:
        return _apply_track_status(state, event)

    if event.event_type == EventType.RACE_CONTROL_MESSAGE:
        return _apply_race_control_message(state, event)

    if event.event_type == EventType.POSITION_UPDATE:
        return _apply_position_update(state, event)

    if event.event_type == EventType.LAP_UPDATE:
        return _apply_lap_update(state, event)

    if event.event_type == EventType.PIT_STOP:
        return _apply_pit_stop(state, event)

    # UNSUPPORTED and any future unknown types are no-ops.
    return state


# ---------------------------------------------------------------------------
# Per-event-type application helpers
# ---------------------------------------------------------------------------

def _apply_session_status(state: TimingState, event: TimingEvent) -> TimingState:
    new_status = event.payload.get("status")
    if new_status == state.session_status:
        return state
    return dataclasses.replace(state, session_status=new_status)


def _apply_track_status(state: TimingState, event: TimingEvent) -> TimingState:
    new_status = event.payload.get("status")
    if new_status == state.track_status:
        return state
    return dataclasses.replace(state, track_status=new_status)


def _apply_race_control_message(state: TimingState, event: TimingEvent) -> TimingState:
    messages = list(state.race_control_messages)
    messages.append(dict(event.payload))
    if len(messages) > _RACE_CONTROL_CAP:
        messages = messages[-_RACE_CONTROL_CAP:]
    return dataclasses.replace(state, race_control_messages=messages)


def _apply_position_update(state: TimingState, event: TimingEvent) -> TimingState:
    # Payload shape: {"positions": {"driver_num": {"position": int, ...}, ...}}
    positions_raw = event.payload.get("positions", {})
    if not positions_raw:
        return state
    new_positions = dict(state.driver_positions)
    changed = False
    for driver_ref, data in positions_raw.items():
        pos = data.get("position")
        if pos is not None:
            try:
                new_pos = int(pos)
            except (TypeError, ValueError):
                continue
            if new_positions.get(driver_ref) != new_pos:
                new_positions[driver_ref] = new_pos
                changed = True
    if not changed:
        return state
    return dataclasses.replace(state, driver_positions=new_positions)


def _apply_lap_update(state: TimingState, event: TimingEvent) -> TimingState:
    lines = event.payload.get("lines", {})
    if not lines:
        return state
    new_laps = dict(state.latest_laps)
    changed = False
    for driver_ref, data in lines.items():
        new_laps[driver_ref] = dict(data)
        changed = True
    if not changed:
        return state
    return dataclasses.replace(state, latest_laps=new_laps)


def _apply_pit_stop(state: TimingState, event: TimingEvent) -> TimingState:
    entry = dict(event.payload)
    history = list(state.pit_history)
    history.append(entry)
    return dataclasses.replace(state, pit_history=history)
