"""
Tests for the state reducer.
"""

from __future__ import annotations

import dataclasses
from datetime import datetime, timezone

import pytest

from race_engineer_core.timing.events import EventType, TimingEvent
from race_engineer_core.timing.reducer import (
    TimingState,
    _RACE_CONTROL_CAP,
    initial_state,
    reduce,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TS = datetime(2024, 3, 2, 14, 0, 0, tzinfo=timezone.utc)


def _event(
    event_type: EventType,
    driver: str | None = None,
    payload: dict | None = None,
) -> TimingEvent:
    return TimingEvent(
        timestamp=_TS,
        event_type=event_type,
        driver=driver,
        payload=payload or {},
        raw_topic=None,
    )


# ---------------------------------------------------------------------------
# initial_state
# ---------------------------------------------------------------------------

class TestInitialState:
    def test_all_fields_at_defaults(self):
        s = initial_state()
        assert s.session_status is None
        assert s.track_status is None
        assert s.race_control_messages == []
        assert s.driver_positions == {}
        assert s.latest_laps == {}
        assert s.pit_history == []

    def test_returns_new_instance_each_call(self):
        s1 = initial_state()
        s2 = initial_state()
        assert s1 is not s2


# ---------------------------------------------------------------------------
# SESSION_STATUS
# ---------------------------------------------------------------------------

class TestSessionStatus:
    def test_sets_status(self):
        ev = _event(EventType.SESSION_STATUS, payload={"status": "Started"})
        s = reduce(initial_state(), ev)
        assert s.session_status == "Started"

    def test_no_change_returns_same_object(self):
        base = dataclasses.replace(initial_state(), session_status="Started")
        ev = _event(EventType.SESSION_STATUS, payload={"status": "Started"})
        s = reduce(base, ev)
        assert s is base

    def test_status_update(self):
        base = dataclasses.replace(initial_state(), session_status="Started")
        ev = _event(EventType.SESSION_STATUS, payload={"status": "Finished"})
        s = reduce(base, ev)
        assert s.session_status == "Finished"


# ---------------------------------------------------------------------------
# TRACK_STATUS
# ---------------------------------------------------------------------------

class TestTrackStatus:
    def test_sets_status(self):
        ev = _event(EventType.TRACK_STATUS, payload={"status": "1", "message": "AllClear"})
        s = reduce(initial_state(), ev)
        assert s.track_status == "1"

    def test_no_change_returns_same_object(self):
        base = dataclasses.replace(initial_state(), track_status="1")
        ev = _event(EventType.TRACK_STATUS, payload={"status": "1", "message": ""})
        s = reduce(base, ev)
        assert s is base


# ---------------------------------------------------------------------------
# RACE_CONTROL_MESSAGE
# ---------------------------------------------------------------------------

class TestRaceControlMessage:
    def test_appends_message(self):
        ev = _event(EventType.RACE_CONTROL_MESSAGE, payload={"message": "VSC DEPLOYED"})
        s = reduce(initial_state(), ev)
        assert len(s.race_control_messages) == 1
        assert s.race_control_messages[0]["message"] == "VSC DEPLOYED"

    def test_cap_is_enforced(self):
        state = initial_state()
        for i in range(_RACE_CONTROL_CAP + 5):
            ev = _event(EventType.RACE_CONTROL_MESSAGE, payload={"message": f"msg-{i}"})
            state = reduce(state, ev)
        assert len(state.race_control_messages) == _RACE_CONTROL_CAP
        # Oldest messages should have been dropped; latest should be present.
        assert state.race_control_messages[-1]["message"] == f"msg-{_RACE_CONTROL_CAP + 4}"

    def test_old_state_unchanged(self):
        base = initial_state()
        ev = _event(EventType.RACE_CONTROL_MESSAGE, payload={"message": "X"})
        new = reduce(base, ev)
        assert base.race_control_messages == []
        assert len(new.race_control_messages) == 1


# ---------------------------------------------------------------------------
# POSITION_UPDATE
# ---------------------------------------------------------------------------

class TestPositionUpdate:
    def test_new_driver(self):
        ev = _event(EventType.POSITION_UPDATE, payload={
            "positions": {"44": {"position": 1, "status": "OnTrack"}}
        })
        s = reduce(initial_state(), ev)
        assert s.driver_positions["44"] == 1

    def test_updates_existing_driver(self):
        base = dataclasses.replace(initial_state(), driver_positions={"44": 3})
        ev = _event(EventType.POSITION_UPDATE, payload={
            "positions": {"44": {"position": 1, "status": "OnTrack"}}
        })
        s = reduce(base, ev)
        assert s.driver_positions["44"] == 1

    def test_no_change_returns_same_object(self):
        base = dataclasses.replace(initial_state(), driver_positions={"44": 1})
        ev = _event(EventType.POSITION_UPDATE, payload={
            "positions": {"44": {"position": 1, "status": "OnTrack"}}
        })
        s = reduce(base, ev)
        assert s is base

    def test_old_dict_not_mutated(self):
        base = initial_state()
        ev = _event(EventType.POSITION_UPDATE, payload={
            "positions": {"44": {"position": 1}}
        })
        new = reduce(base, ev)
        assert base.driver_positions == {}
        assert new.driver_positions == {"44": 1}

    def test_empty_positions_is_noop(self):
        base = initial_state()
        ev = _event(EventType.POSITION_UPDATE, payload={"positions": {}})
        s = reduce(base, ev)
        assert s is base


# ---------------------------------------------------------------------------
# LAP_UPDATE
# ---------------------------------------------------------------------------

class TestLapUpdate:
    def test_sets_lap_data(self):
        ev = _event(EventType.LAP_UPDATE, payload={
            "lines": {"44": {"last_lap_time": "1:18.5", "number_of_laps": 10, "position": 1}}
        })
        s = reduce(initial_state(), ev)
        assert "44" in s.latest_laps
        assert s.latest_laps["44"]["last_lap_time"] == "1:18.5"

    def test_updates_existing_driver(self):
        base = dataclasses.replace(initial_state(), latest_laps={"44": {"number_of_laps": 5}})
        ev = _event(EventType.LAP_UPDATE, payload={
            "lines": {"44": {"number_of_laps": 6}}
        })
        s = reduce(base, ev)
        assert s.latest_laps["44"]["number_of_laps"] == 6


# ---------------------------------------------------------------------------
# PIT_STOP
# ---------------------------------------------------------------------------

class TestPitStop:
    def test_appends_to_history(self):
        ev = _event(EventType.PIT_STOP, driver="44", payload={
            "racing_number": "44", "duration": "24.1", "lap": 15
        })
        s = reduce(initial_state(), ev)
        assert len(s.pit_history) == 1
        assert s.pit_history[0]["racing_number"] == "44"

    def test_multiple_pits_append_in_order(self):
        state = initial_state()
        for lap in [15, 32]:
            state = reduce(state, _event(EventType.PIT_STOP, payload={"lap": lap}))
        assert [p["lap"] for p in state.pit_history] == [15, 32]


# ---------------------------------------------------------------------------
# UNSUPPORTED — no-op
# ---------------------------------------------------------------------------

class TestUnsupported:
    def test_returns_same_object(self):
        base = initial_state()
        ev = _event(EventType.UNSUPPORTED)
        s = reduce(base, ev)
        assert s is base

    def test_state_fields_unchanged(self):
        base = dataclasses.replace(initial_state(), session_status="Started")
        ev = _event(EventType.UNSUPPORTED)
        s = reduce(base, ev)
        assert s.session_status == "Started"


# ---------------------------------------------------------------------------
# Immutability
# ---------------------------------------------------------------------------

class TestImmutability:
    def test_reduce_does_not_mutate_input(self):
        base = initial_state()
        ev = _event(EventType.SESSION_STATUS, payload={"status": "Started"})
        _ = reduce(base, ev)
        assert base.session_status is None

    def test_driver_positions_old_ref_unchanged(self):
        base = initial_state()
        original_dict = base.driver_positions
        ev = _event(EventType.POSITION_UPDATE, payload={
            "positions": {"44": {"position": 2}}
        })
        _ = reduce(base, ev)
        assert original_dict == {}
