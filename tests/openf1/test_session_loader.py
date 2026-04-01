"""
Tests for OpenF1SessionLoader (session.py).

Verifies that to_state() produces a structurally correct TimingState from
fixture data, that the event store round-trip works, and that final_positions
is applied as a state projection after the reduce loop.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from race_engineer_core.adapters.openf1.session import OpenF1SessionData, OpenF1SessionLoader
from race_engineer_core.timing.reducer import TimingState

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str):
    return json.loads((FIXTURES / name).read_text())


def make_data(
    *,
    session=None,
    drivers=None,
    laps=None,
    position=None,
    pit=None,
    race_control=None,
) -> OpenF1SessionData:
    return OpenF1SessionData(
        session_key=9158,
        session=session or load("session.json")[0],
        drivers=drivers if drivers is not None else load("drivers.json"),
        laps=laps if laps is not None else load("laps.json"),
        position=position if position is not None else load("position.json"),
        pit=pit if pit is not None else load("pit.json"),
        race_control=race_control if race_control is not None else load("race_control.json"),
    )


class TestOpenF1SessionData:
    def test_dataclass_fields(self):
        data = make_data()
        assert data.session_key == 9158
        assert isinstance(data.session, dict)
        assert isinstance(data.drivers, list)
        assert isinstance(data.laps, list)
        assert isinstance(data.position, list)
        assert isinstance(data.pit, list)
        assert isinstance(data.race_control, list)


class TestToStateBasic:
    def setup_method(self):
        client = MagicMock()
        self.loader = OpenF1SessionLoader(client)
        self.data = make_data()
        self.state = self.loader.to_state(self.data)

    def test_returns_timing_state(self):
        assert isinstance(self.state, TimingState)

    def test_session_info_populated(self):
        assert self.state.session_info["name"] == "Race"
        assert self.state.session_info["meeting_name"] == "Abu Dhabi Grand Prix"
        assert self.state.session_info["key"] == 9158

    def test_session_status_finished(self):
        assert self.state.session_status == "Finished"

    def test_driver_list_populated(self):
        assert "1" in self.state.driver_list
        assert "44" in self.state.driver_list
        assert "16" in self.state.driver_list

    def test_driver_tla(self):
        assert self.state.driver_list["1"]["tla"] == "VER"
        assert self.state.driver_list["44"]["tla"] == "HAM"

    def test_latest_laps_populated(self):
        # 2 drivers with valid lap data → both should appear in latest_laps
        assert "1" in self.state.latest_laps
        assert "44" in self.state.latest_laps

    def test_pit_history_populated(self):
        assert len(self.state.pit_history) == 2

    def test_race_control_messages_populated(self):
        assert len(self.state.race_control_messages) == 3

    def test_driver_positions_from_projection(self):
        # Position projection applied after reduce loop
        assert self.state.driver_positions["1"] == 1
        assert self.state.driver_positions["44"] == 3
        assert self.state.driver_positions["16"] == 2

    def test_track_status_none(self):
        # PROVISIONAL: no OpenF1 endpoint for track/flag status
        assert self.state.track_status is None


class TestToStateWithStore:
    def test_events_appended_to_store(self):
        client = MagicMock()
        loader = OpenF1SessionLoader(client)
        data = make_data()

        store = MagicMock()
        loader.to_state(data, store=store)

        assert store.append.call_count > 0

    def test_each_appended_arg_is_timing_event(self):
        from race_engineer_core.timing.events import TimingEvent

        client = MagicMock()
        loader = OpenF1SessionLoader(client)
        data = make_data()

        store = MagicMock()
        loader.to_state(data, store=store)

        for call_args in store.append.call_args_list:
            arg = call_args[0][0]
            assert isinstance(arg, TimingEvent)


class TestToStateEmptyData:
    def test_empty_drivers(self):
        client = MagicMock()
        loader = OpenF1SessionLoader(client)
        data = make_data(drivers=[])
        state = loader.to_state(data)
        assert state.driver_list == {}

    def test_empty_laps(self):
        client = MagicMock()
        loader = OpenF1SessionLoader(client)
        data = make_data(laps=[])
        state = loader.to_state(data)
        assert state.latest_laps == {}

    def test_empty_pit(self):
        client = MagicMock()
        loader = OpenF1SessionLoader(client)
        data = make_data(pit=[])
        state = loader.to_state(data)
        assert state.pit_history == []

    def test_empty_race_control(self):
        client = MagicMock()
        loader = OpenF1SessionLoader(client)
        data = make_data(race_control=[])
        state = loader.to_state(data)
        assert state.race_control_messages == []

    def test_empty_position_leaves_driver_positions_empty(self):
        client = MagicMock()
        loader = OpenF1SessionLoader(client)
        data = make_data(position=[])
        state = loader.to_state(data)
        # No LAP_UPDATE events carry position either (position=None from laps)
        # So driver_positions should be empty
        assert state.driver_positions == {}


class TestFetchDelegatesToClient:
    def test_fetch_calls_client_methods(self):
        client = MagicMock()
        client.get_session.return_value = load("session.json")[0]
        client.get_drivers.return_value = load("drivers.json")
        client.get_laps.return_value = load("laps.json")
        client.get_position.return_value = load("position.json")
        client.get_pit.return_value = load("pit.json")
        client.get_race_control.return_value = load("race_control.json")

        loader = OpenF1SessionLoader(client)
        data = loader.fetch(9158)

        client.get_session.assert_called_once_with(9158)
        client.get_drivers.assert_called_once_with(9158)
        client.get_laps.assert_called_once_with(9158)
        client.get_position.assert_called_once_with(9158)
        client.get_pit.assert_called_once_with(9158)
        client.get_race_control.assert_called_once_with(9158)

        assert data.session_key == 9158
        assert data.session["session_key"] == 9158

    def test_fetch_raises_if_session_not_found(self):
        client = MagicMock()
        client.get_session.return_value = None

        loader = OpenF1SessionLoader(client)
        with pytest.raises(ValueError, match="9999"):
            loader.fetch(9999)


class TestEventOrdering:
    def test_events_are_sorted_by_timestamp(self):
        """Session metadata must appear before time-series events after sort."""
        client = MagicMock()
        loader = OpenF1SessionLoader(client)
        data = make_data()
        events = loader._build_events(data)

        timestamps = [e.timestamp for e in events]
        assert timestamps == sorted(timestamps)

    def test_session_info_is_first_event(self):
        from race_engineer_core.timing.events import EventType
        client = MagicMock()
        loader = OpenF1SessionLoader(client)
        data = make_data()
        events = loader._build_events(data)
        assert events[0].event_type == EventType.SESSION_INFO
