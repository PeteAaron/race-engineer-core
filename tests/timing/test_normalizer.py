"""
Tests for the event normalizer.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from race_engineer_core.timing.events import EventType
from race_engineer_core.timing.normalizer import normalize, supported_topics
from race_engineer_core.timing.raw import RawMessage

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TS = datetime(2024, 3, 2, 14, 0, 0, tzinfo=timezone.utc)


def _raw(topic: str, payload: object) -> RawMessage:
    return RawMessage(received_at=_TS, topic=topic, payload=payload, session_key=1234)


# ---------------------------------------------------------------------------
# SessionInfo — produces SESSION_INFO, not SESSION_STATUS
# Real shape has no "Status" field; status comes from "SessionStatus" topic.
# ---------------------------------------------------------------------------

class TestSessionInfo:
    def test_happy_path(self):
        raw = _raw("SessionInfo", {
            "Key": 9662,
            "Name": "Race",
            "Type": "Race",
            "GmtOffset": "03:00:00",
            "Path": "2024/2024-03-02_Bahrain_Grand_Prix/2024-03-02_Race/",
            "Meeting": {"Name": "Bahrain Grand Prix"},
        })
        event = normalize(raw)
        assert event is not None
        assert event.event_type == EventType.SESSION_INFO
        assert event.driver is None
        assert event.payload["name"] == "Race"
        assert event.payload["type"] == "Race"
        assert event.payload["key"] == 9662
        assert event.payload["meeting_name"] == "Bahrain Grand Prix"
        assert event.raw_topic == "SessionInfo"

    def test_missing_keys_still_normalizes(self):
        # Missing optional keys fall back to empty string / None; must not crash.
        raw = _raw("SessionInfo", {})
        event = normalize(raw)
        assert event is not None
        assert event.event_type == EventType.SESSION_INFO
        assert event.payload["name"] == ""
        assert event.payload["key"] is None

    def test_malformed_payload_returns_none(self):
        raw = _raw("SessionInfo", None)
        result = normalize(raw)
        assert result is None

    def test_event_id_is_populated(self):
        raw = _raw("SessionInfo", {"Name": "Race", "Type": "Race"})
        event = normalize(raw)
        assert event is not None
        assert len(event.event_id) == 32  # uuid4 hex

    def test_no_status_key_in_payload(self):
        # Real SessionInfo carries no "Status" field — only SessionStatus does.
        raw = _raw("SessionInfo", {"Name": "Qualifying", "Type": "Qualifying"})
        event = normalize(raw)
        assert event is not None
        assert "status" not in event.payload


# ---------------------------------------------------------------------------
# SessionStatus — the real source of session lifecycle status
# ---------------------------------------------------------------------------

class TestSessionStatus:
    def test_happy_path(self):
        raw = _raw("SessionStatus", {"Status": "Started"})
        event = normalize(raw)
        assert event is not None
        assert event.event_type == EventType.SESSION_STATUS
        assert event.payload["status"] == "Started"
        assert event.raw_topic == "SessionStatus"

    def test_finished_status(self):
        raw = _raw("SessionStatus", {"Status": "Finished"})
        event = normalize(raw)
        assert event is not None
        assert event.payload["status"] == "Finished"

    def test_inactive_status(self):
        raw = _raw("SessionStatus", {"Status": "Inactive"})
        event = normalize(raw)
        assert event is not None
        assert event.event_type == EventType.SESSION_STATUS

    def test_empty_payload_returns_event_with_empty_status(self):
        raw = _raw("SessionStatus", {})
        event = normalize(raw)
        assert event is not None
        assert event.payload["status"] == ""

    def test_malformed_payload_returns_none(self):
        raw = _raw("SessionStatus", None)
        assert normalize(raw) is None


# ---------------------------------------------------------------------------
# TrackStatus
# ---------------------------------------------------------------------------

class TestTrackStatus:
    def test_happy_path(self):
        raw = _raw("TrackStatus", {"Status": "1", "Message": "AllClear"})
        event = normalize(raw)
        assert event is not None
        assert event.event_type == EventType.TRACK_STATUS
        assert event.payload["status"] == "1"
        assert event.payload["message"] == "AllClear"

    def test_safety_car(self):
        raw = _raw("TrackStatus", {"Status": "4", "Message": "SafetyCar"})
        event = normalize(raw)
        assert event is not None
        assert event.payload["status"] == "4"

    def test_malformed_payload_returns_none(self):
        raw = _raw("TrackStatus", "not a dict")
        assert normalize(raw) is None


# ---------------------------------------------------------------------------
# RaceControlMessages — real shape has "Utc"; "Lap" is optional
# ---------------------------------------------------------------------------

class TestRaceControlMessages:
    def test_happy_path(self):
        raw = _raw("RaceControlMessages", {
            "Messages": {
                "0": {
                    "Utc": "2024-03-02T15:02:00",
                    "Category": "Flag",
                    "Message": "GREEN LIGHT",
                    "Flag": "GREEN",
                    "Lap": 1,
                    "Scope": "Track",
                },
            }
        })
        event = normalize(raw)
        assert event is not None
        assert event.event_type == EventType.RACE_CONTROL_MESSAGE
        assert event.payload["message"] == "GREEN LIGHT"
        assert event.payload["utc"] == "2024-03-02T15:02:00"
        assert event.payload["scope"] == "Track"

    def test_empty_messages_returns_none(self):
        raw = _raw("RaceControlMessages", {"Messages": {}})
        assert normalize(raw) is None

    def test_latest_key_is_selected(self):
        raw = _raw("RaceControlMessages", {
            "Messages": {
                "0": {"Category": "X", "Message": "first",  "Flag": "", "Scope": ""},
                "5": {"Category": "X", "Message": "latest", "Flag": "", "Scope": ""},
            }
        })
        event = normalize(raw)
        assert event is not None
        assert event.payload["message"] == "latest"

    def test_missing_lap_field_yields_none(self):
        # "Lap" is absent from some real message categories
        raw = _raw("RaceControlMessages", {
            "Messages": {
                "0": {"Category": "SafetyCar", "Message": "VSC", "Flag": "YELLOW", "Scope": "Track"}
            }
        })
        event = normalize(raw)
        assert event is not None
        assert event.payload["lap"] is None


# ---------------------------------------------------------------------------
# Position.z — X/Y/Z track coordinates; NOT race-order position
# ---------------------------------------------------------------------------

class TestPositionZ:
    def test_happy_path(self):
        raw = _raw("Position.z", {
            "Position": [
                {"Entries": {
                    "1":  {"Status": "OnTrack",   "X": 3987, "Y": -1234, "Z": 100},
                    "44": {"Status": "OnTrack",   "X": 4102, "Y": -1189, "Z": 102},
                }}
            ]
        })
        event = normalize(raw)
        assert event is not None
        assert event.event_type == EventType.POSITION_UPDATE
        assert event.driver is None
        pos = event.payload["positions"]
        assert "1" in pos
        assert pos["1"]["x"] == 3987
        assert pos["1"]["y"] == -1234
        assert pos["1"]["z"] == 100
        assert pos["1"]["status"] == "OnTrack"

    def test_xyz_keys_present_no_position_key(self):
        raw = _raw("Position.z", {
            "Position": [{"Entries": {"44": {"Status": "OnTrack", "X": 1, "Y": 2, "Z": 3}}}]
        })
        event = normalize(raw)
        assert event is not None
        driver_data = event.payload["positions"]["44"]
        assert "x" in driver_data
        assert "y" in driver_data
        assert "z" in driver_data
        # No race-order "position" key — that comes from TimingData
        assert "position" not in driver_data

    def test_empty_position_list_returns_none(self):
        raw = _raw("Position.z", {"Position": []})
        assert normalize(raw) is None

    def test_malformed_payload_returns_none(self):
        raw = _raw("Position.z", None)
        assert normalize(raw) is None


# ---------------------------------------------------------------------------
# TimingData — lap times + race-order position from "Line"
# ---------------------------------------------------------------------------

class TestTimingData:
    def test_happy_path(self):
        raw = _raw("TimingData", {
            "Lines": {
                "44": {
                    "LastLapTime": {"Value": "1:18.123"},
                    "BestLapTime": {"Value": "1:17.901"},
                    "NumberOfLaps": 5,
                    "NumberOfPitStops": 1,
                    "Line": 1,
                    "InPit": False,
                    "Retired": False,
                }
            }
        })
        event = normalize(raw)
        assert event is not None
        assert event.event_type == EventType.LAP_UPDATE
        line = event.payload["lines"]["44"]
        assert line["last_lap_time"] == "1:18.123"
        assert line["best_lap_time"] == "1:17.901"
        assert line["position"] == 1
        assert line["number_of_laps"] == 5
        assert line["in_pit"] is False

    def test_empty_lines_returns_none(self):
        raw = _raw("TimingData", {"Lines": {}})
        assert normalize(raw) is None

    def test_missing_lap_time_value_is_none(self):
        # LastLapTime dict may be absent if driver hasn't started
        raw = _raw("TimingData", {
            "Lines": {"1": {"Line": 1, "NumberOfLaps": 0}}
        })
        event = normalize(raw)
        assert event is not None
        assert event.payload["lines"]["1"]["last_lap_time"] is None


# ---------------------------------------------------------------------------
# PitLaneTimeCollection — confirmed real F1 topic; InTime/OutTime optional
# ---------------------------------------------------------------------------

class TestPitLaneTimeCollection:
    def test_happy_path(self):
        raw = _raw("PitLaneTimeCollection", {
            "PitTimes": {
                "0": {
                    "RacingNumber": "44",
                    "Duration": "23.5",
                    "Lap": 12,
                    "InTime": "14:15:23.456",
                    "OutTime": "14:15:46.912",
                }
            }
        })
        event = normalize(raw)
        assert event is not None
        assert event.event_type == EventType.PIT_STOP
        assert event.driver == "44"
        assert event.payload["duration"] == "23.5"
        assert event.payload["in_time"] == "14:15:23.456"
        assert event.payload["out_time"] == "14:15:46.912"

    def test_empty_pit_times_returns_none(self):
        raw = _raw("PitLaneTimeCollection", {"PitTimes": {}})
        assert normalize(raw) is None

    def test_missing_in_out_time_yields_none(self):
        raw = _raw("PitLaneTimeCollection", {
            "PitTimes": {"0": {"RacingNumber": "1", "Duration": "24.1", "Lap": 13}}
        })
        event = normalize(raw)
        assert event is not None
        assert event.payload["in_time"] is None
        assert event.payload["out_time"] is None


# ---------------------------------------------------------------------------
# DriverList
# ---------------------------------------------------------------------------

class TestDriverList:
    def test_happy_path(self):
        raw = _raw("DriverList", {
            "44": {
                "RacingNumber": "44",
                "BroadcastName": "L HAMILTON",
                "FullName": "Lewis HAMILTON",
                "Tla": "HAM",
                "TeamName": "Mercedes",
                "TeamColour": "6CD3BF",
            }
        })
        event = normalize(raw)
        assert event is not None
        assert event.event_type == EventType.DRIVER_LIST
        assert event.driver is None
        drivers = event.payload["drivers"]
        assert "44" in drivers
        assert drivers["44"]["tla"] == "HAM"
        assert drivers["44"]["team_name"] == "Mercedes"

    def test_empty_dict_returns_none(self):
        raw = _raw("DriverList", {})
        assert normalize(raw) is None

    def test_non_dict_values_skipped(self):
        # Feed may include metadata keys at the top level
        raw = _raw("DriverList", {
            "44": {"RacingNumber": "44", "Tla": "HAM", "TeamName": "Mercedes",
                   "BroadcastName": "L HAMILTON", "FullName": "Lewis HAMILTON",
                   "TeamColour": "6CD3BF"},
            "_kf": "some-metadata-string",
        })
        event = normalize(raw)
        assert event is not None
        assert "_kf" not in event.payload["drivers"]
        assert "44" in event.payload["drivers"]


# ---------------------------------------------------------------------------
# Unknown / unsupported topics
# ---------------------------------------------------------------------------

class TestUnsupportedTopic:
    def test_unknown_topic_returns_unsupported_event(self):
        raw = _raw("SomeUnknownFeed.v2", {"data": 123})
        event = normalize(raw)
        assert event is not None
        assert event.event_type == EventType.UNSUPPORTED
        assert event.raw_topic == "SomeUnknownFeed.v2"
        assert event.driver is None

    def test_unsupported_event_has_event_id(self):
        raw = _raw("Nope", {})
        event = normalize(raw)
        assert event is not None
        assert event.event_id  # non-empty


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------

class TestDeterminism:
    def test_same_input_same_content(self):
        raw = _raw("TrackStatus", {"Status": "2", "Message": "Yellow"})
        e1 = normalize(raw)
        e2 = normalize(raw)
        assert e1 is not None
        assert e2 is not None
        assert e1.event_type == e2.event_type
        assert e1.driver == e2.driver
        assert e1.payload == e2.payload
        assert e1.raw_topic == e2.raw_topic
        # event_id is intentionally unique per call
        assert e1.event_id != e2.event_id


# ---------------------------------------------------------------------------
# supported_topics()
# ---------------------------------------------------------------------------

class TestSupportedTopics:
    def test_returns_frozenset(self):
        topics = supported_topics()
        assert isinstance(topics, frozenset)

    def test_contains_known_topics(self):
        topics = supported_topics()
        assert "SessionInfo" in topics
        assert "SessionStatus" in topics
        assert "TrackStatus" in topics
        assert "RaceControlMessages" in topics
        assert "Position.z" in topics
        assert "TimingData" in topics
        assert "PitLaneTimeCollection" in topics
        assert "DriverList" in topics
