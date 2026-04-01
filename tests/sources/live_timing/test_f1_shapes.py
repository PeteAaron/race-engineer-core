"""
End-to-end integration tests against real F1 live timing message shapes.

These tests use fixtures from tests/sources/live_timing/fixtures/ that represent the
actual payloads delivered by the F1 live timing SignalR feed (after
decompression for .z topics). Each test validates:

  1. normalize()      — correct EventType, key payload fields
  2. ingest()         — state mutation, store persistence
  3. replay()         — final state equals manual fold (consistency)

Running these against the fixture shapes proves the pipeline can handle
real-world feed data end-to-end.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from race_engineer_core.pipeline.events import EventType
from race_engineer_core.pipeline.reducer import initial_state, reduce
from race_engineer_core.pipeline.replay import ReplayRunner
from race_engineer_core.pipeline.store import EventStore
from race_engineer_core.sources.live_timing import ingest
from race_engineer_core.sources.live_timing.normalizer import normalize
from race_engineer_core.sources.live_timing.raw import RawMessage

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FIXTURES = Path(__file__).parent / "fixtures"
_TS = datetime(2024, 3, 2, 15, 2, 0, tzinfo=timezone.utc)


def _load(filename: str) -> dict:
    return json.loads((_FIXTURES / filename).read_text(encoding="utf-8"))


def _raw(topic: str, payload: object) -> RawMessage:
    return RawMessage(received_at=_TS, topic=topic, payload=payload, session_key=9662)


# ---------------------------------------------------------------------------
# SessionInfo — real shape has Key, Name, Type, Meeting; no Status field
# ---------------------------------------------------------------------------

class TestSessionInfoShape:
    def test_produces_session_info_event(self):
        payload = _load("session_info.json")
        event = normalize(_raw("SessionInfo", payload))
        assert event is not None
        assert event.event_type == EventType.SESSION_INFO

    def test_key_fields_extracted(self):
        payload = _load("session_info.json")
        event = normalize(_raw("SessionInfo", payload))
        assert event is not None
        assert event.payload["key"] == 9662
        assert event.payload["name"] == "Race"
        assert event.payload["type"] == "Race"
        assert event.payload["meeting_name"] == "Bahrain Grand Prix"
        assert "2024-03-02_Race" in event.payload["path"]
        assert event.payload["gmt_offset"] == "03:00:00"

    def test_driver_is_none(self):
        event = normalize(_raw("SessionInfo", _load("session_info.json")))
        assert event is not None
        assert event.driver is None

    def test_no_status_key_in_payload(self):
        # Real SessionInfo has no Status field — status comes from SessionStatus topic
        event = normalize(_raw("SessionInfo", _load("session_info.json")))
        assert event is not None
        assert "status" not in event.payload


# ---------------------------------------------------------------------------
# SessionStatus — separate topic for lifecycle state
# ---------------------------------------------------------------------------

class TestSessionStatusShape:
    def test_produces_session_status_event(self):
        payload = _load("session_status.json")
        event = normalize(_raw("SessionStatus", payload))
        assert event is not None
        assert event.event_type == EventType.SESSION_STATUS

    def test_status_extracted(self):
        event = normalize(_raw("SessionStatus", {"Status": "Started"}))
        assert event is not None
        assert event.payload["status"] == "Started"

    def test_all_known_status_values_normalize(self):
        for status in ("Inactive", "Started", "Aborted", "Finished", "Ends"):
            event = normalize(_raw("SessionStatus", {"Status": status}))
            assert event is not None
            assert event.payload["status"] == status


# ---------------------------------------------------------------------------
# TrackStatus
# ---------------------------------------------------------------------------

class TestTrackStatusShape:
    def test_happy_path(self):
        payload = _load("track_status.json")
        event = normalize(_raw("TrackStatus", payload))
        assert event is not None
        assert event.event_type == EventType.TRACK_STATUS
        assert event.payload["status"] == "1"
        assert event.payload["message"] == "AllClear"

    def test_safety_car_status(self):
        event = normalize(_raw("TrackStatus", {"Status": "4", "Message": "SafetyCar"}))
        assert event is not None
        assert event.payload["status"] == "4"


# ---------------------------------------------------------------------------
# RaceControlMessages — real shape has Utc field; Lap may be absent
# ---------------------------------------------------------------------------

class TestRaceControlMessagesShape:
    def test_happy_path(self):
        payload = _load("race_control_messages.json")
        # Fixture has 2 messages; handler takes the latest (key "1")
        event = normalize(_raw("RaceControlMessages", payload))
        assert event is not None
        assert event.event_type == EventType.RACE_CONTROL_MESSAGE

    def test_latest_key_selected(self):
        payload = _load("race_control_messages.json")
        event = normalize(_raw("RaceControlMessages", payload))
        assert event is not None
        assert event.payload["message"] == "VIRTUAL SAFETY CAR DEPLOYED"
        assert event.payload["category"] == "SafetyCar"

    def test_utc_field_present_in_payload(self):
        payload = _load("race_control_messages.json")
        event = normalize(_raw("RaceControlMessages", payload))
        assert event is not None
        assert event.payload["utc"] == "2024-03-02T15:45:00"

    def test_scope_field_present(self):
        payload = _load("race_control_messages.json")
        event = normalize(_raw("RaceControlMessages", payload))
        assert event is not None
        assert event.payload["scope"] == "Track"

    def test_message_without_lap_is_ok(self):
        # Some categories (e.g. SafetyCar) may omit Lap field
        payload = {
            "Messages": {
                "0": {
                    "Utc": "2024-03-02T15:45:00",
                    "Category": "SafetyCar",
                    "Message": "VSC DEPLOYED",
                    "Flag": "YELLOW",
                    "Scope": "Track",
                }
            }
        }
        event = normalize(_raw("RaceControlMessages", payload))
        assert event is not None
        assert event.payload["lap"] is None  # absent key → None


# ---------------------------------------------------------------------------
# Position.z — X/Y/Z coordinates, NOT race order
# ---------------------------------------------------------------------------

class TestPositionZShape:
    def test_produces_position_update_event(self):
        payload = _load("position_z.json")
        event = normalize(_raw("Position.z", payload))
        assert event is not None
        assert event.event_type == EventType.POSITION_UPDATE

    def test_xyz_coordinates_extracted(self):
        payload = _load("position_z.json")
        event = normalize(_raw("Position.z", payload))
        assert event is not None
        pos = event.payload["positions"]
        assert "1" in pos
        assert pos["1"]["x"] == 3987
        assert pos["1"]["y"] == -1234
        assert pos["1"]["z"] == 100
        assert pos["1"]["status"] == "OnTrack"

    def test_pit_lane_status_preserved(self):
        payload = _load("position_z.json")
        event = normalize(_raw("Position.z", payload))
        assert event is not None
        assert event.payload["positions"]["63"]["status"] == "OnPitLane"

    def test_no_race_order_position_key(self):
        # Real Position.z has no race-order (Line/position) field.
        # driver_positions state is populated by TimingData via LAP_UPDATE.
        payload = _load("position_z.json")
        event = normalize(_raw("Position.z", payload))
        assert event is not None
        for driver_data in event.payload["positions"].values():
            assert "position" not in driver_data

    def test_position_update_does_not_set_driver_positions_state(self):
        # Verify the reducer does not update driver_positions from real Position.z
        # events (which lack a race-order "position" key).
        payload = _load("position_z.json")
        event = normalize(_raw("Position.z", payload))
        assert event is not None
        state = reduce(initial_state(), event)
        # driver_positions should remain empty — no race-order data in Position.z
        assert state.driver_positions == {}


# ---------------------------------------------------------------------------
# TimingData — race order position from Line field
# ---------------------------------------------------------------------------

class TestTimingDataShape:
    def test_produces_lap_update_event(self):
        payload = _load("timing_data.json")
        event = normalize(_raw("TimingData", payload))
        assert event is not None
        assert event.event_type == EventType.LAP_UPDATE

    def test_lap_time_extracted(self):
        payload = _load("timing_data.json")
        event = normalize(_raw("TimingData", payload))
        assert event is not None
        lines = event.payload["lines"]
        assert lines["1"]["last_lap_time"] == "1:32.456"
        assert lines["44"]["last_lap_time"] == "1:32.891"

    def test_race_position_extracted_from_line(self):
        payload = _load("timing_data.json")
        event = normalize(_raw("TimingData", payload))
        assert event is not None
        lines = event.payload["lines"]
        assert lines["1"]["position"] == 1
        assert lines["44"]["position"] == 2

    def test_lap_update_sets_driver_positions_in_state(self):
        # TimingData.Line is the source of truth for race-order position.
        payload = _load("timing_data.json")
        event = normalize(_raw("TimingData", payload))
        assert event is not None
        state = reduce(initial_state(), event)
        assert state.driver_positions["1"] == 1
        assert state.driver_positions["44"] == 2

    def test_in_pit_and_retired_flags_present(self):
        payload = _load("timing_data.json")
        event = normalize(_raw("TimingData", payload))
        assert event is not None
        assert event.payload["lines"]["1"]["in_pit"] is False
        assert event.payload["lines"]["1"]["retired"] is False

    def test_best_lap_time_extracted(self):
        payload = _load("timing_data.json")
        event = normalize(_raw("TimingData", payload))
        assert event is not None
        assert event.payload["lines"]["1"]["best_lap_time"] == "1:31.421"


# ---------------------------------------------------------------------------
# PitLaneTimeCollection — real shape includes InTime/OutTime
# ---------------------------------------------------------------------------

class TestPitLaneTimeCollectionShape:
    def test_produces_pit_stop_event(self):
        payload = _load("pit_lane_time_collection.json")
        event = normalize(_raw("PitLaneTimeCollection", payload))
        assert event is not None
        assert event.event_type == EventType.PIT_STOP

    def test_latest_entry_selected(self):
        # Fixture has key "0" (driver 44) and key "1" (driver 1); latest is "1"
        payload = _load("pit_lane_time_collection.json")
        event = normalize(_raw("PitLaneTimeCollection", payload))
        assert event is not None
        assert event.payload["racing_number"] == "1"
        assert event.payload["duration"] == "24.102"
        assert event.payload["lap"] == 13

    def test_in_time_out_time_optional(self):
        # Key "1" in the fixture omits InTime/OutTime
        payload = _load("pit_lane_time_collection.json")
        event = normalize(_raw("PitLaneTimeCollection", payload))
        assert event is not None
        assert event.payload["in_time"] is None
        assert event.payload["out_time"] is None

    def test_driver_set_on_event(self):
        payload = _load("pit_lane_time_collection.json")
        event = normalize(_raw("PitLaneTimeCollection", payload))
        assert event is not None
        assert event.driver == "1"


# ---------------------------------------------------------------------------
# DriverList
# ---------------------------------------------------------------------------

class TestDriverListShape:
    def test_produces_driver_list_event(self):
        payload = _load("driver_list.json")
        event = normalize(_raw("DriverList", payload))
        assert event is not None
        assert event.event_type == EventType.DRIVER_LIST

    def test_driver_fields_extracted(self):
        payload = _load("driver_list.json")
        event = normalize(_raw("DriverList", payload))
        assert event is not None
        drivers = event.payload["drivers"]
        assert "1" in drivers
        assert drivers["1"]["tla"] == "VER"
        assert drivers["1"]["team_name"] == "Red Bull Racing"
        assert drivers["44"]["tla"] == "HAM"

    def test_driver_list_stored_in_state(self):
        payload = _load("driver_list.json")
        event = normalize(_raw("DriverList", payload))
        assert event is not None
        state = reduce(initial_state(), event)
        assert "1" in state.driver_list
        assert state.driver_list["44"]["tla"] == "HAM"
        assert len(state.driver_list) == 3


# ---------------------------------------------------------------------------
# End-to-end pipeline: realistic multi-message session sequence
# ---------------------------------------------------------------------------

class TestEndToEndSessionSequence:
    """
    Pump a realistic sequence of F1 session messages through the full pipeline
    (normalize → ingest → store), then replay and verify the final state
    matches what was built live.
    """

    def _build_sequence(self) -> list[tuple[str, object]]:
        """Return (topic, payload) pairs in chronological order."""
        return [
            ("DriverList",             _load("driver_list.json")),
            ("SessionInfo",            _load("session_info.json")),
            ("SessionStatus",          {"Status": "Started"}),
            ("TrackStatus",            {"Status": "1", "Message": "AllClear"}),
            ("TimingData",             _load("timing_data.json")),
            ("Position.z",             _load("position_z.json")),
            ("RaceControlMessages",    _load("race_control_messages.json")),
            ("PitLaneTimeCollection",  _load("pit_lane_time_collection.json")),
            ("TrackStatus",            {"Status": "1", "Message": "AllClear"}),  # no-op
        ]

    def test_all_messages_ingested(self, tmp_path: Path):
        store = EventStore(tmp_path / "session.jsonl")
        state = initial_state()
        for topic, payload in self._build_sequence():
            state, _ = ingest(_raw(topic, payload), state, store)
        events = list(store.iter_events())
        # All 9 messages should produce events (the DriverList, SessionInfo,
        # SessionStatus, 2x TrackStatus, TimingData, Position.z, RCM, PitStop = 9)
        assert len(events) == 9

    def test_live_state_after_full_sequence(self, tmp_path: Path):
        store = EventStore(tmp_path / "session.jsonl")
        state = initial_state()
        for topic, payload in self._build_sequence():
            state, _ = ingest(_raw(topic, payload), state, store)

        # Session-level state
        assert state.session_status == "Started"
        assert state.session_info["name"] == "Race"
        assert state.session_info["meeting_name"] == "Bahrain Grand Prix"
        assert state.track_status == "1"

        # Driver list
        assert "1" in state.driver_list
        assert state.driver_list["44"]["tla"] == "HAM"

        # Race order from TimingData
        assert state.driver_positions["1"] == 1
        assert state.driver_positions["44"] == 2

        # Lap data
        assert state.latest_laps["1"]["last_lap_time"] == "1:32.456"

        # Race control
        assert len(state.race_control_messages) == 1
        assert state.race_control_messages[0]["message"] == "VIRTUAL SAFETY CAR DEPLOYED"

        # Pit stop history (latest key in fixture is driver "1")
        assert len(state.pit_history) == 1
        assert state.pit_history[0]["racing_number"] == "1"

    def test_replay_converges_with_live_state(self, tmp_path: Path):
        """
        Replay the stored events and verify the final state equals the state
        built during live ingestion. This is the core consistency guarantee.
        """
        store = EventStore(tmp_path / "session.jsonl")
        live_state = initial_state()
        for topic, payload in self._build_sequence():
            live_state, _ = ingest(_raw(topic, payload), live_state, store)

        replay_state = ReplayRunner(store).run()

        assert replay_state.session_status == live_state.session_status
        assert replay_state.session_info == live_state.session_info
        assert replay_state.track_status == live_state.track_status
        assert replay_state.driver_positions == live_state.driver_positions
        assert replay_state.latest_laps == live_state.latest_laps
        assert replay_state.race_control_messages == live_state.race_control_messages
        assert replay_state.pit_history == live_state.pit_history
        assert replay_state.driver_list == live_state.driver_list

    def test_store_round_trip_preserves_event_fields(self, tmp_path: Path):
        """
        After ingestion and reload from disk, event fields are byte-for-byte
        identical to what was stored (no precision loss, no type coercion).
        """
        store = EventStore(tmp_path / "session.jsonl")
        state = initial_state()
        ingested_events = []
        for topic, payload in self._build_sequence():
            _, event = ingest(_raw(topic, payload), state, store)
            if event is not None:
                ingested_events.append(event)
                state = reduce(state, event)

        reloaded = list(store.iter_events())
        assert len(reloaded) == len(ingested_events)
        for original, restored in zip(ingested_events, reloaded):
            assert original.event_id == restored.event_id
            assert original.event_type == restored.event_type
            assert original.payload == restored.payload
            assert original.driver == restored.driver
            assert original.raw_topic == restored.raw_topic
