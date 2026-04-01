"""
Tests for the JSONL event store.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from race_engineer_core.pipeline.events import EventType, TimingEvent
from race_engineer_core.pipeline.store import EventStore

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TS = datetime(2024, 3, 2, 14, 0, 0, tzinfo=timezone.utc)


def _event(
    event_type: EventType = EventType.SESSION_STATUS,
    driver: str | None = None,
    payload: dict | None = None,
    raw_topic: str | None = "SessionInfo",
    event_id: str | None = None,
) -> TimingEvent:
    kwargs = dict(
        timestamp=_TS,
        event_type=event_type,
        driver=driver,
        payload=payload or {"status": "Started"},
        raw_topic=raw_topic,
    )
    if event_id is not None:
        kwargs["event_id"] = event_id
    return TimingEvent(**kwargs)


# ---------------------------------------------------------------------------
# Round-trip
# ---------------------------------------------------------------------------

class TestRoundTrip:
    def test_single_event(self, tmp_path: Path):
        store = EventStore(tmp_path / "events.jsonl")
        ev = _event(event_id="abc123")
        store.append(ev)
        events = list(store.iter_events())
        assert len(events) == 1
        restored = events[0]
        assert restored.event_id == "abc123"
        assert restored.event_type == EventType.SESSION_STATUS
        assert restored.timestamp == _TS
        assert restored.driver is None
        assert restored.payload == {"status": "Started"}
        assert restored.raw_topic == "SessionInfo"

    def test_multiple_events_in_order(self, tmp_path: Path):
        store = EventStore(tmp_path / "events.jsonl")
        types = [EventType.SESSION_STATUS, EventType.TRACK_STATUS, EventType.PIT_STOP]
        for et in types:
            store.append(_event(event_type=et))
        events = list(store.iter_events())
        assert [e.event_type for e in events] == types

    def test_timestamp_round_trip(self, tmp_path: Path):
        ts = datetime(2024, 3, 2, 14, 0, 0, 123456, tzinfo=timezone.utc)
        ev = TimingEvent(
            timestamp=ts,
            event_type=EventType.LAP_UPDATE,
            driver="44",
            payload={"lines": {}},
            raw_topic="TimingData",
        )
        store = EventStore(tmp_path / "events.jsonl")
        store.append(ev)
        restored = list(store.iter_events())[0]
        assert restored.timestamp == ts
        assert restored.timestamp.tzinfo is not None


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_empty_store(self, tmp_path: Path):
        store = EventStore(tmp_path / "events.jsonl")
        assert list(store.iter_events()) == []

    def test_nonexistent_file(self, tmp_path: Path):
        store = EventStore(tmp_path / "does_not_exist.jsonl")
        assert list(store.iter_events()) == []

    def test_corrupt_line_is_skipped(self, tmp_path: Path):
        path = tmp_path / "events.jsonl"
        store = EventStore(path)
        ev = _event(event_id="good1")
        store.append(ev)
        # Manually inject a corrupt line
        with path.open("a", encoding="utf-8") as f:
            f.write("this is not json\n")
        # Append another good event
        store.append(_event(event_id="good2"))

        events = list(store.iter_events())
        assert len(events) == 2
        assert events[0].event_id == "good1"
        assert events[1].event_id == "good2"

    def test_blank_lines_are_skipped(self, tmp_path: Path):
        path = tmp_path / "events.jsonl"
        store = EventStore(path)
        store.append(_event(event_id="x"))
        with path.open("a", encoding="utf-8") as f:
            f.write("\n\n")
        events = list(store.iter_events())
        assert len(events) == 1

    def test_unsupported_event_is_persisted(self, tmp_path: Path):
        store = EventStore(tmp_path / "events.jsonl")
        ev = _event(event_type=EventType.UNSUPPORTED, raw_topic="UnknownTopic", payload={})
        store.append(ev)
        events = list(store.iter_events())
        assert len(events) == 1
        assert events[0].event_type == EventType.UNSUPPORTED

    def test_event_id_preserved(self, tmp_path: Path):
        store = EventStore(tmp_path / "events.jsonl")
        known_id = "deadbeefcafe12345678901234567890"
        store.append(_event(event_id=known_id))
        events = list(store.iter_events())
        assert events[0].event_id == known_id
