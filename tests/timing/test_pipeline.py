"""
Tests for the ingest() pipeline helper.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from race_engineer_core.timing.events import EventType, TimingEvent
from race_engineer_core.timing.pipeline import ingest
from race_engineer_core.timing.raw import RawMessage
from race_engineer_core.timing.reducer import TimingState, initial_state
from race_engineer_core.timing.store import EventStore

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
    def test_known_topic_updates_state(self, tmp_path: Path):
        store = EventStore(tmp_path / "events.jsonl")
        raw = _raw("SessionInfo", {"Status": "Started", "Name": "Race", "Type": "Race"})
        new_state, event = ingest(raw, initial_state(), store)
        assert event is not None
        assert event.event_type == EventType.SESSION_STATUS
        assert new_state.session_status == "Started"

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
        raw = _raw("SessionInfo", {"Status": "Started"})
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

        state, _ = ingest(_raw("SessionInfo", {"Status": "Started"}), state, store)
        state, _ = ingest(_raw("TrackStatus", {"Status": "1", "Message": "AllClear"}), state, store)

        assert state.session_status == "Started"
        assert state.track_status == "1"
        assert len(list(store.iter_events())) == 2
