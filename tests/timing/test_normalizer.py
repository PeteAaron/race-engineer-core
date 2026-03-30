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
# SessionInfo
# ---------------------------------------------------------------------------

class TestSessionInfo:
    def test_happy_path(self):
        raw = _raw("SessionInfo", {"Status": "Started", "Name": "Race", "Type": "Race"})
        event = normalize(raw)
        assert event is not None
        assert event.event_type == EventType.SESSION_STATUS
        assert event.driver is None
        assert event.payload["status"] == "Started"
        assert event.raw_topic == "SessionInfo"

    def test_missing_keys_still_normalizes(self):
        # Missing keys fall back to empty string; should not crash.
        raw = _raw("SessionInfo", {})
        event = normalize(raw)
        assert event is not None
        assert event.event_type == EventType.SESSION_STATUS

    def test_malformed_payload_returns_none(self):
        raw = _raw("SessionInfo", None)
        result = normalize(raw)
        assert result is None

    def test_event_id_is_populated(self):
        raw = _raw("SessionInfo", {"Status": "Finished"})
        event = normalize(raw)
        assert event is not None
        assert len(event.event_id) == 32  # uuid4 hex


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

    def test_malformed_payload_returns_none(self):
        raw = _raw("TrackStatus", "not a dict")
        assert normalize(raw) is None


# ---------------------------------------------------------------------------
# RaceControlMessages
# ---------------------------------------------------------------------------

class TestRaceControlMessages:
    def test_happy_path(self):
        raw = _raw("RaceControlMessages", {
            "Messages": {
                "0": {"Category": "Flag", "Message": "GREEN LIGHT", "Flag": "GREEN", "Lap": 1},
            }
        })
        event = normalize(raw)
        assert event is not None
        assert event.event_type == EventType.RACE_CONTROL_MESSAGE
        assert event.payload["message"] == "GREEN LIGHT"

    def test_empty_messages_returns_none(self):
        raw = _raw("RaceControlMessages", {"Messages": {}})
        assert normalize(raw) is None

    def test_latest_key_is_selected(self):
        raw = _raw("RaceControlMessages", {
            "Messages": {
                "0": {"Category": "X", "Message": "first", "Flag": "", "Lap": None},
                "5": {"Category": "X", "Message": "latest", "Flag": "", "Lap": None},
            }
        })
        event = normalize(raw)
        assert event is not None
        assert event.payload["message"] == "latest"


# ---------------------------------------------------------------------------
# Position.z
# ---------------------------------------------------------------------------

class TestPositionZ:
    def test_happy_path(self):
        raw = _raw("Position.z", {
            "Position": [
                {"Entries": {"1": {"Line": 1, "Status": "OnTrack"}, "44": {"Line": 2, "Status": "OnTrack"}}}
            ]
        })
        event = normalize(raw)
        assert event is not None
        assert event.event_type == EventType.POSITION_UPDATE
        assert event.driver is None
        assert "1" in event.payload["positions"]
        assert event.payload["positions"]["1"]["position"] == 1

    def test_empty_position_list_returns_none(self):
        raw = _raw("Position.z", {"Position": []})
        assert normalize(raw) is None

    def test_malformed_payload_returns_none(self):
        raw = _raw("Position.z", None)
        assert normalize(raw) is None


# ---------------------------------------------------------------------------
# TimingData
# ---------------------------------------------------------------------------

class TestTimingData:
    def test_happy_path(self):
        raw = _raw("TimingData", {
            "Lines": {
                "44": {"LastLapTime": {"Value": "1:18.123"}, "NumberOfLaps": 5, "Line": 1}
            }
        })
        event = normalize(raw)
        assert event is not None
        assert event.event_type == EventType.LAP_UPDATE
        assert "44" in event.payload["lines"]
        assert event.payload["lines"]["44"]["last_lap_time"] == "1:18.123"

    def test_empty_lines_returns_none(self):
        raw = _raw("TimingData", {"Lines": {}})
        assert normalize(raw) is None


# ---------------------------------------------------------------------------
# PitLaneTimeCollection
# ---------------------------------------------------------------------------

class TestPitLaneTimeCollection:
    def test_happy_path(self):
        raw = _raw("PitLaneTimeCollection", {
            "PitTimes": {
                "0": {"RacingNumber": "44", "Duration": "23.5", "Lap": 12}
            }
        })
        event = normalize(raw)
        assert event is not None
        assert event.event_type == EventType.PIT_STOP
        assert event.driver == "44"
        assert event.payload["duration"] == "23.5"

    def test_empty_pit_times_returns_none(self):
        raw = _raw("PitLaneTimeCollection", {"PitTimes": {}})
        assert normalize(raw) is None


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
        assert "TrackStatus" in topics
        assert "RaceControlMessages" in topics
        assert "Position.z" in topics
        assert "TimingData" in topics
        assert "PitLaneTimeCollection" in topics
