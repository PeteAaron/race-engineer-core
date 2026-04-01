"""
Tests for the ingest() pipeline helper.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from race_engineer_core.pipeline.events import EventType, TimingEvent
from race_engineer_core.pipeline.reducer import TimingState, initial_state
from race_engineer_core.pipeline.store import EventStore
from race_engineer_core.sources.live_timing import ingest
from race_engineer_core.sources.live_timing.raw import RawMessage

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TS = datetime(2024, 3, 2, 14, 0, 0, tzinfo=timezone.utc)


def _raw(topic: str, payload: object) -> RawMessage:
    return RawMessage(received_at=_TS, topic=topic, payload=payload, session_key=9999)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

class TestIngestHappyPath:
    def test_session_status_topic_updates_state(self, tmp_path: Path):
        # Session lifecycle status comes from "SessionStatus", not "SessionInfo".
        store = EventStore(tmp_path / "events.jsonl")
        raw = _raw("SessionStatus", {"Status": "Started"})
        new_state, event = ingest(raw, initial_state(), store)
        assert event is not None
        assert event.event_type == EventType.SESSION_STATUS
        assert new_state.session_status == "Started"

    def test_session_info_topic_updates_session_info(self, tmp_path: Path):
        # "SessionInfo" produces SESSION_INFO and populates session_info dict.
        store = EventStore(tmp_path / "events.jsonl")
        raw = _raw("SessionInfo", {
            "Key": 9662, "Name": "Race", "Type": "Race",
            "GmtOffset": "03:00:00", "Path": "2024/...",
            "Meeting": {"Name": "Bahrain Grand Prix"},
        })
        new_state, event = ingest(raw, initial_state(), store)
        assert event is not None
        assert event.event_type == EventType.SESSION_INFO
        assert new_state.session_info["name"] == "Race"
        assert new_state.session_info["meeting_name"] == "Bahrain Grand Prix"

    def test_event_is_persisted(self, tmp_path: Path):
        store = EventStore(tmp_path / "events.jsonl")
        raw = _raw("TrackStatus", {"Status": "1", "Message": "AllClear"})
        ingest(raw, initial_state(), store)
        events = list(store.iter_events())
        assert len(events) == 1
        assert events[0].event_type == EventType.TRACK_STATUS

    def test_state_returned_is_new_object(self, tmp_path: Path):
        store = EventStore(tmp_path / "events.jsonl")
        base = initial_state()
        raw = _raw("SessionStatus", {"Status": "Started"})
        new_state, _ = ingest(raw, base, store)
        assert new_state is not base


# ---------------------------------------------------------------------------
# Malformed payload — normalizer returns None
# ---------------------------------------------------------------------------

class TestIngestMalformedPayload:
    def test_returns_unchanged_state_and_none_event(self, tmp_path: Path):
        store = EventStore(tmp_path / "events.jsonl")
        raw = _raw("TrackStatus", None)  # None payload → normalizer returns None
        base = initial_state()
        new_state, event = ingest(raw, base, store)
        assert event is None
        assert new_state is base

    def test_nothing_written_to_store(self, tmp_path: Path):
        store = EventStore(tmp_path / "events.jsonl")
        raw = _raw("TrackStatus", None)
        ingest(raw, initial_state(), store)
        assert list(store.iter_events()) == []


# ---------------------------------------------------------------------------
# Unsupported topic
# ---------------------------------------------------------------------------

class TestIngestUnsupportedTopic:
    def test_unsupported_event_is_stored(self, tmp_path: Path):
        store = EventStore(tmp_path / "events.jsonl")
        raw = _raw("SomeFutureFeed", {"x": 1})
        new_state, event = ingest(raw, initial_state(), store)
        assert event is not None
        assert event.event_type == EventType.UNSUPPORTED
        stored = list(store.iter_events())
        assert len(stored) == 1
        assert stored[0].event_type == EventType.UNSUPPORTED

    def test_unsupported_event_is_noop_for_state(self, tmp_path: Path):
        store = EventStore(tmp_path / "events.jsonl")
        raw = _raw("SomeFutureFeed", {"x": 1})
        base = initial_state()
        new_state, _ = ingest(raw, base, store)
        # Reducer returns same object for UNSUPPORTED
        assert new_state is base


# ---------------------------------------------------------------------------
# Sequential ingestion
# ---------------------------------------------------------------------------

class TestIngestSequential:
    def test_two_messages_accumulate_state(self, tmp_path: Path):
        store = EventStore(tmp_path / "events.jsonl")
        state = initial_state()

        # SessionStatus sets session_status; TrackStatus sets track_status
        state, _ = ingest(_raw("SessionStatus", {"Status": "Started"}), state, store)
        state, _ = ingest(_raw("TrackStatus", {"Status": "1", "Message": "AllClear"}), state, store)

        assert state.session_status == "Started"
        assert state.track_status == "1"
        assert len(list(store.iter_events())) == 2

    def test_session_info_and_session_status_are_independent(self, tmp_path: Path):
        store = EventStore(tmp_path / "events.jsonl")
        state = initial_state()

        # SessionInfo populates session_info dict, not session_status
        state, _ = ingest(_raw("SessionInfo", {"Name": "Race", "Type": "Race",
                                                "Key": 1, "GmtOffset": "00:00:00",
                                                "Path": "", "Meeting": {}}),
                          state, store)
        assert state.session_info["name"] == "Race"
        assert state.session_status is None  # not set yet

        # SessionStatus then sets the status
        state, _ = ingest(_raw("SessionStatus", {"Status": "Started"}), state, store)
        assert state.session_status == "Started"
        assert state.session_info["name"] == "Race"  # unchanged
