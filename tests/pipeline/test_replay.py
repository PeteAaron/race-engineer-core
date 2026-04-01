"""
Tests for ReplayRunner.
"""

from __future__ import annotations

import dataclasses
from datetime import datetime, timezone
from pathlib import Path

import pytest

from race_engineer_core.pipeline.events import EventType, TimingEvent
from race_engineer_core.pipeline.reducer import TimingState, initial_state, reduce
from race_engineer_core.pipeline.replay import ReplayRunner
from race_engineer_core.pipeline.store import EventStore

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TS = datetime(2024, 3, 2, 14, 0, 0, tzinfo=timezone.utc)


def _event(event_type: EventType, payload: dict | None = None) -> TimingEvent:
    return TimingEvent(
        timestamp=_TS,
        event_type=event_type,
        driver=None,
        payload=payload or {},
        raw_topic=None,
    )


def _store_with(*events: TimingEvent, tmp_path: Path) -> EventStore:
    store = EventStore(tmp_path / "events.jsonl")
    for ev in events:
        store.append(ev)
    return store


# ---------------------------------------------------------------------------
# Basic replay
# ---------------------------------------------------------------------------

class TestBasicReplay:
    def test_empty_store_returns_initial_state(self, tmp_path: Path):
        store = EventStore(tmp_path / "events.jsonl")
        result = ReplayRunner(store).run()
        expected = initial_state()
        assert result.session_status == expected.session_status
        assert result.track_status == expected.track_status
        assert result.race_control_messages == expected.race_control_messages

    def test_single_event(self, tmp_path: Path):
        ev = _event(EventType.SESSION_STATUS, {"status": "Started"})
        store = _store_with(ev, tmp_path=tmp_path)
        result = ReplayRunner(store).run()
        assert result.session_status == "Started"

    def test_multiple_events_applied_in_order(self, tmp_path: Path):
        e1 = _event(EventType.SESSION_STATUS, {"status": "Started"})
        e2 = _event(EventType.TRACK_STATUS, {"status": "1", "message": "AllClear"})
        e3 = _event(EventType.SESSION_STATUS, {"status": "Finished"})
        store = _store_with(e1, e2, e3, tmp_path=tmp_path)
        result = ReplayRunner(store).run()
        # Last session status wins
        assert result.session_status == "Finished"
        assert result.track_status == "1"


# ---------------------------------------------------------------------------
# Custom reducer injection
# ---------------------------------------------------------------------------

class TestCustomReducer:
    def test_custom_reducer_is_called(self, tmp_path: Path):
        call_count = 0

        def counting_reducer(state: TimingState, event: TimingEvent) -> TimingState:
            nonlocal call_count
            call_count += 1
            return state

        events = [_event(EventType.UNSUPPORTED) for _ in range(5)]
        store = _store_with(*events, tmp_path=tmp_path)
        ReplayRunner(store).run(reducer=counting_reducer)
        assert call_count == 5


# ---------------------------------------------------------------------------
# Custom initial state
# ---------------------------------------------------------------------------

class TestCustomInitialState:
    def test_custom_initial_is_used(self, tmp_path: Path):
        seeded = dataclasses.replace(initial_state(), session_status="Qualifying")
        store = EventStore(tmp_path / "events.jsonl")  # empty
        result = ReplayRunner(store).run(initial=seeded)
        assert result.session_status == "Qualifying"


# ---------------------------------------------------------------------------
# Speed stub
# ---------------------------------------------------------------------------

class TestSpeedStub:
    def test_speed_does_not_affect_result(self, tmp_path: Path):
        ev = _event(EventType.SESSION_STATUS, {"status": "Started"})
        store = _store_with(ev, tmp_path=tmp_path)
        result_1x = ReplayRunner(store).run(speed=1.0)
        result_2x = ReplayRunner(store).run(speed=2.0)
        assert result_1x.session_status == result_2x.session_status


# ---------------------------------------------------------------------------
# Convergence with manual fold
# ---------------------------------------------------------------------------

class TestConvergence:
    def test_replay_equals_manual_fold(self, tmp_path: Path):
        events = [
            _event(EventType.SESSION_STATUS, {"status": "Started"}),
            _event(EventType.TRACK_STATUS, {"status": "2", "message": "Yellow"}),
            _event(EventType.RACE_CONTROL_MESSAGE, {"message": "VSC", "category": "SafetyCar", "flag": "", "lap": 10}),
        ]
        # Manual fold
        manual_state = initial_state()
        for ev in events:
            manual_state = reduce(manual_state, ev)

        # Replay
        store = _store_with(*events, tmp_path=tmp_path)
        replay_state = ReplayRunner(store).run()

        assert replay_state.session_status == manual_state.session_status
        assert replay_state.track_status == manual_state.track_status
        assert replay_state.race_control_messages == manual_state.race_control_messages


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------

class TestDeterminism:
    def test_two_replays_same_result(self, tmp_path: Path):
        events = [
            _event(EventType.SESSION_STATUS, {"status": "Started"}),
            _event(EventType.TRACK_STATUS, {"status": "1", "message": "AllClear"}),
        ]
        store = _store_with(*events, tmp_path=tmp_path)
        runner = ReplayRunner(store)
        result1 = runner.run()
        result2 = runner.run()
        assert result1.session_status == result2.session_status
        assert result1.track_status == result2.track_status
        assert result1.driver_positions == result2.driver_positions
        assert result1.race_control_messages == result2.race_control_messages
