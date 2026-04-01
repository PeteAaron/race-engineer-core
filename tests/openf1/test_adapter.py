"""
Tests for the OpenF1 → internal model adapter (adapter.py).

Covers all six per-endpoint translators and the helper functions.
Uses fixtures from tests/openf1/fixtures/ to keep the expected shapes
close to real API responses.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from race_engineer_core.adapters.openf1.adapter import (
    _format_lap_time,
    _parse_date,
    drivers_to_event,
    final_positions,
    laps_to_events,
    pit_to_events,
    race_control_to_events,
    session_to_events,
)
from race_engineer_core.timing.events import EventType

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str) -> list | dict:
    return json.loads((FIXTURES / name).read_text())


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class TestFormatLapTime:
    def test_typical(self):
        assert _format_lap_time(95.382) == "1:35.382"

    def test_sub_minute(self):
        assert _format_lap_time(58.5) == "0:58.500"

    def test_exactly_two_minutes(self):
        assert _format_lap_time(120.0) == "2:00.000"

    def test_none_returns_none(self):
        assert _format_lap_time(None) is None

    def test_zero_returns_none(self):
        assert _format_lap_time(0) is None

    def test_negative_returns_none(self):
        assert _format_lap_time(-1.5) is None


class TestParseDate:
    def test_tz_aware_iso(self):
        dt = _parse_date("2023-11-26T13:00:05+00:00")
        assert dt is not None
        assert dt.tzinfo is not None
        assert dt.year == 2023

    def test_naive_treated_as_utc(self):
        dt = _parse_date("2023-11-26T13:00:05")
        assert dt is not None
        assert dt.tzinfo == timezone.utc

    def test_none_input(self):
        assert _parse_date(None) is None

    def test_empty_string(self):
        assert _parse_date("") is None

    def test_invalid_string(self):
        assert _parse_date("not-a-date") is None


# ---------------------------------------------------------------------------
# session_to_events
# ---------------------------------------------------------------------------

class TestSessionToEvents:
    def setup_method(self):
        self.session = load("session.json")[0]
        self.events = session_to_events(self.session)

    def test_returns_two_events(self):
        assert len(self.events) == 2

    def test_first_is_session_info(self):
        assert self.events[0].event_type == EventType.SESSION_INFO

    def test_second_is_session_status(self):
        assert self.events[1].event_type == EventType.SESSION_STATUS

    def test_session_info_payload_fields(self):
        payload = self.events[0].payload
        assert payload["key"] == 9158
        assert payload["name"] == "Race"
        assert payload["type"] == "Race"
        assert payload["meeting_name"] == "Abu Dhabi Grand Prix"
        assert payload["gmt_offset"] == "04:00:00"
        assert "path" in payload  # PROVISIONAL: empty string

    def test_session_status_is_finished(self):
        assert self.events[1].payload["status"] == "Finished"

    def test_raw_topic_prefix(self):
        for event in self.events:
            assert event.raw_topic == "openf1:sessions"

    def test_driver_is_none(self):
        for event in self.events:
            assert event.driver is None

    def test_timestamp_from_date_start(self):
        ts = self.events[0].timestamp
        assert ts.year == 2023
        assert ts.month == 11
        assert ts.day == 26


# ---------------------------------------------------------------------------
# drivers_to_event
# ---------------------------------------------------------------------------

class TestDriversToEvent:
    def setup_method(self):
        self.drivers = load("drivers.json")
        self.ts = datetime(2023, 11, 26, 13, 0, 0, tzinfo=timezone.utc)

    def test_returns_driver_list_event(self):
        event = drivers_to_event(self.drivers, self.ts)
        assert event is not None
        assert event.event_type == EventType.DRIVER_LIST

    def test_payload_contains_all_drivers(self):
        event = drivers_to_event(self.drivers, self.ts)
        assert event is not None
        drivers_map = event.payload["drivers"]
        assert "1" in drivers_map
        assert "44" in drivers_map
        assert "16" in drivers_map

    def test_name_acronym_mapped_to_tla(self):
        event = drivers_to_event(self.drivers, self.ts)
        assert event is not None
        assert event.payload["drivers"]["1"]["tla"] == "VER"
        assert event.payload["drivers"]["44"]["tla"] == "HAM"
        assert event.payload["drivers"]["16"]["tla"] == "LEC"

    def test_driver_fields(self):
        event = drivers_to_event(self.drivers, self.ts)
        assert event is not None
        ver = event.payload["drivers"]["1"]
        assert ver["broadcast_name"] == "M VERSTAPPEN"
        assert ver["full_name"] == "Max VERSTAPPEN"
        assert ver["team_name"] == "Red Bull Racing"
        assert ver["racing_number"] == "1"

    def test_raw_topic(self):
        event = drivers_to_event(self.drivers, self.ts)
        assert event is not None
        assert event.raw_topic == "openf1:drivers"

    def test_empty_list_returns_none(self):
        assert drivers_to_event([], self.ts) is None

    def test_records_without_driver_number_skipped(self):
        bad = [{"name_acronym": "TST"}]
        assert drivers_to_event(bad, self.ts) is None

    def test_timestamp_preserved(self):
        event = drivers_to_event(self.drivers, self.ts)
        assert event is not None
        assert event.timestamp == self.ts


# ---------------------------------------------------------------------------
# race_control_to_events
# ---------------------------------------------------------------------------

class TestRaceControlToEvents:
    def setup_method(self):
        self.records = load("race_control.json")
        self.events = race_control_to_events(self.records)

    def test_one_event_per_record(self):
        assert len(self.events) == 3

    def test_event_type(self):
        for event in self.events:
            assert event.event_type == EventType.RACE_CONTROL_MESSAGE

    def test_payload_fields_present(self):
        payload = self.events[0].payload
        assert "category" in payload
        assert "message" in payload
        assert "flag" in payload
        assert "lap" in payload
        assert "utc" in payload
        assert "scope" in payload

    def test_first_message_content(self):
        payload = self.events[0].payload
        assert payload["flag"] == "GREEN"
        assert payload["lap"] == 1
        assert payload["category"] == "Flag"

    def test_raw_topic(self):
        for event in self.events:
            assert event.raw_topic == "openf1:race_control"

    def test_driver_is_none(self):
        for event in self.events:
            assert event.driver is None

    def test_record_missing_date_is_skipped(self):
        bad = [{"category": "Flag", "message": "Test"}]  # no date
        events = race_control_to_events(bad)
        assert events == []

    def test_timestamps_in_order(self):
        ts_list = [e.timestamp for e in self.events]
        assert ts_list == sorted(ts_list)


# ---------------------------------------------------------------------------
# pit_to_events
# ---------------------------------------------------------------------------

class TestPitToEvents:
    def setup_method(self):
        self.records = load("pit.json")
        self.events = pit_to_events(self.records)

    def test_one_event_per_record(self):
        assert len(self.events) == 2

    def test_event_type(self):
        for event in self.events:
            assert event.event_type == EventType.PIT_STOP

    def test_payload_fields(self):
        payload = self.events[0].payload
        assert "racing_number" in payload
        assert "duration" in payload
        assert "lap" in payload
        assert "in_time" in payload
        assert "out_time" in payload

    def test_in_time_out_time_are_none_provisional(self):
        # PROVISIONAL: OpenF1 /v1/pit does not expose these fields
        for event in self.events:
            assert event.payload["in_time"] is None
            assert event.payload["out_time"] is None

    def test_driver_set(self):
        assert self.events[0].driver == "44"
        assert self.events[1].driver == "1"

    def test_duration_is_string(self):
        # Duration is serialised as a string to match live timing format
        assert self.events[0].payload["duration"] == "23.4"

    def test_raw_topic(self):
        for event in self.events:
            assert event.raw_topic == "openf1:pit"

    def test_record_missing_date_is_skipped(self):
        bad = [{"driver_number": 1, "lap_number": 5, "pit_duration": 22.0}]
        events = pit_to_events(bad)
        assert events == []


# ---------------------------------------------------------------------------
# laps_to_events
# ---------------------------------------------------------------------------

class TestLapsToEvents:
    def setup_method(self):
        self.records = load("laps.json")
        self.events = laps_to_events(self.records)

    def test_one_event_per_lap(self):
        # 6 records total (3 laps × 2 drivers)
        assert len(self.events) == 6

    def test_event_type(self):
        for event in self.events:
            assert event.event_type == EventType.LAP_UPDATE

    def test_driver_field_set(self):
        drivers = {e.driver for e in self.events}
        assert "1" in drivers
        assert "44" in drivers

    def test_payload_has_lines_key(self):
        for event in self.events:
            assert "lines" in event.payload

    def test_payload_lines_keyed_by_driver(self):
        for event in self.events:
            assert event.driver in event.payload["lines"]

    def test_pit_out_lap_has_no_lap_time(self):
        # Lap 1 for both drivers has null duration
        lap1_events = [
            e for e in self.events
            if e.payload["lines"][e.driver]["number_of_laps"] == 1
        ]
        assert len(lap1_events) == 2
        for event in lap1_events:
            line = event.payload["lines"][event.driver]
            assert line["last_lap_time"] is None

    def test_running_best_increases_for_driver_1(self):
        # Driver 1: lap2=95.382, lap3=93.451 → best after lap2=95.382, after lap3=93.451
        d1_events = sorted(
            [e for e in self.events if e.driver == "1"],
            key=lambda e: e.payload["lines"]["1"]["number_of_laps"],
        )
        # Lap 1: no duration → best still None
        assert d1_events[0].payload["lines"]["1"]["best_lap_time"] is None
        # Lap 2: best = 95.382
        best_lap2 = d1_events[1].payload["lines"]["1"]["best_lap_time"]
        assert best_lap2 == _format_lap_time(95.382)
        # Lap 3: best = 93.451 (new personal best)
        best_lap3 = d1_events[2].payload["lines"]["1"]["best_lap_time"]
        assert best_lap3 == _format_lap_time(93.451)

    def test_position_is_none(self):
        # PROVISIONAL: /v1/laps has no race-order position
        for event in self.events:
            line = event.payload["lines"][event.driver]
            assert line["position"] is None

    def test_raw_topic(self):
        for event in self.events:
            assert event.raw_topic == "openf1:laps"

    def test_empty_records_returns_empty(self):
        assert laps_to_events([]) == []


# ---------------------------------------------------------------------------
# final_positions
# ---------------------------------------------------------------------------

class TestFinalPositions:
    def setup_method(self):
        self.records = load("position.json")

    def test_returns_dict(self):
        result = final_positions(self.records)
        assert isinstance(result, dict)

    def test_all_drivers_present(self):
        result = final_positions(self.records)
        assert "1" in result
        assert "44" in result
        assert "16" in result

    def test_final_positions_correct(self):
        # Last record in fixture: driver 1→1, 44→3, 16→2
        result = final_positions(self.records)
        assert result["1"] == 1
        assert result["44"] == 3
        assert result["16"] == 2

    def test_later_record_wins(self):
        # Driver 44 starts at position 2 but ends at 3 after the overtake
        # (fixture has earlier record with pos=2 and later record with pos=3)
        result = final_positions(self.records)
        assert result["44"] == 3

    def test_empty_returns_empty(self):
        assert final_positions([]) == {}

    def test_records_missing_fields_skipped(self):
        bad = [{"driver_number": 1}]  # no position, no date
        assert final_positions(bad) == {}
