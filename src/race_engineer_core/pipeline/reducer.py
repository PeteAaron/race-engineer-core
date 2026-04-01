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

Race-order position note:
  driver_positions is populated by LAP_UPDATE (TimingData.Line), not by
  POSITION_UPDATE (Position.z). Position.z carries X/Y/Z track coordinates
  which are unrelated to race order. See normalizer.py for details.
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
    session_info: dict[str, Any] = field(default_factory=dict)
    track_status: str | None = None
    # Capped ring buffer of recent race control messages.
    race_control_messages: list[dict[str, Any]] = field(default_factory=list)
    # driver_ref → current race-order position (1 = leader); sourced from TimingData.Line
    driver_positions: dict[str, int] = field(default_factory=dict)
    # driver_ref → latest lap info dict (last_lap_time, position, etc.)
    latest_laps: dict[str, dict[str, Any]] = field(default_factory=dict)
    # Ordered list of pit stop dicts, oldest first.
    pit_history: list[dict[str, Any]] = field(default_factory=list)
    # driver_ref → driver reference dict (broadcast_name, tla, team_name, …)
    driver_list: dict[str, dict[str, Any]] = field(default_factory=dict)


def initial_state() -> TimingState:
    """Return a clean initial state. Call once at session start."""
    return TimingState()


def reduce(state: TimingState, event: TimingEvent) -> TimingState:
    """
    Apply one event to state and return the resulting state.

    Returns the same object for no-op events. Returns a new object
    (via dataclasses.replace) for any event that changes state.
    """
    if event.event_type == EventType.SESSION_INFO:
        return _apply_session_info(state, event)

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

    if event.event_type == EventType.DRIVER_LIST:
        return _apply_driver_list(state, event)

    # UNSUPPORTED and any future unknown types are no-ops.
    return state


# ---------------------------------------------------------------------------
# Per-event-type application helpers
# ---------------------------------------------------------------------------

def _apply_session_info(state: TimingState, event: TimingEvent) -> TimingState:
    new_info = dict(event.payload)
    if new_info == state.session_info:
        return state
    return dataclasses.replace(state, session_info=new_info)


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
    # Payload shape: {"positions": {"driver_num": {"x": int, "y": int, "z": int, ...}, ...}}
    # Real Position.z data carries X/Y/Z track coordinates; there is no "position"
    # (race order) field. If a synthetic event includes "position", it is still
    # handled for backward compatibility with tests and future use.
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
    # Payload shape: {"lines": {"driver_num": {"last_lap_time": ..., "position": int, ...}, ...}}
    # The "position" key (from TimingData.Line) is the authoritative source of
    # race-order position. We update driver_positions here alongside latest_laps.
    lines = event.payload.get("lines", {})
    if not lines:
        return state
    new_laps = dict(state.latest_laps)
    new_positions = dict(state.driver_positions)
    laps_changed = False
    positions_changed = False
    for driver_ref, data in lines.items():
        new_laps[driver_ref] = dict(data)
        laps_changed = True
        pos = data.get("position")
        if pos is not None:
            try:
                new_pos = int(pos)
                if new_positions.get(driver_ref) != new_pos:
                    new_positions[driver_ref] = new_pos
                    positions_changed = True
            except (TypeError, ValueError):
                pass
    if not laps_changed:
        return state
    if positions_changed:
        return dataclasses.replace(state, latest_laps=new_laps, driver_positions=new_positions)
    return dataclasses.replace(state, latest_laps=new_laps)


def _apply_pit_stop(state: TimingState, event: TimingEvent) -> TimingState:
    entry = dict(event.payload)
    history = list(state.pit_history)
    history.append(entry)
    return dataclasses.replace(state, pit_history=history)


def _apply_driver_list(state: TimingState, event: TimingEvent) -> TimingState:
    new_drivers = dict(event.payload.get("drivers", {}))
    if new_drivers == state.driver_list:
        return state
    return dataclasses.replace(state, driver_list=new_drivers)
