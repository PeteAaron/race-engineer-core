"""
Tests for the DataSource enum and load_state() factory (source.py).

Verifies routing logic, argument validation, and injection of custom
clients/stores. No real network calls are made.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from race_engineer_core.sources import DataSource, load_state
from race_engineer_core.pipeline.reducer import TimingState

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str):
    return json.loads((FIXTURES / name).read_text())


def make_mock_client(session_key: int = 9158) -> MagicMock:
    client = MagicMock()
    client.get_session.return_value = load("session.json")[0]
    client.get_drivers.return_value = load("drivers.json")
    client.get_laps.return_value = load("laps.json")
    client.get_position.return_value = load("position.json")
    client.get_pit.return_value = load("pit.json")
    client.get_race_control.return_value = load("race_control.json")
    return client


# ---------------------------------------------------------------------------
# DataSource enum
# ---------------------------------------------------------------------------

class TestDataSource:
    def test_values_are_strings(self):
        assert DataSource.LIVE_TIMING == "live_timing"
        assert DataSource.OPENF1 == "openf1"
        assert DataSource.REPLAY == "replay"

    def test_str_subclass(self):
        assert isinstance(DataSource.OPENF1, str)

    def test_all_three_members(self):
        members = set(DataSource)
        assert DataSource.LIVE_TIMING in members
        assert DataSource.OPENF1 in members
        assert DataSource.REPLAY in members


# ---------------------------------------------------------------------------
# load_state — OPENF1
# ---------------------------------------------------------------------------

class TestLoadStateOpenF1:
    def test_returns_timing_state(self):
        client = make_mock_client()
        state = load_state(DataSource.OPENF1, session_key=9158, openf1_client=client)
        assert isinstance(state, TimingState)

    def test_session_info_populated(self):
        client = make_mock_client()
        state = load_state(DataSource.OPENF1, session_key=9158, openf1_client=client)
        assert state.session_info.get("name") == "Race"

    def test_raises_without_session_key(self):
        client = make_mock_client()
        with pytest.raises(ValueError, match="session_key"):
            load_state(DataSource.OPENF1, openf1_client=client)

    def test_injects_custom_client(self):
        client = make_mock_client()
        load_state(DataSource.OPENF1, session_key=9158, openf1_client=client)
        client.get_session.assert_called_once_with(9158)

    def test_default_client_constructed_when_not_provided(self):
        # source.py imports OpenF1Client locally inside _load_openf1, so patch
        # the class at its definition site.
        with patch(
            "race_engineer_core.sources.openf1.client.OpenF1Client",
        ) as MockClient:
            mock_instance = make_mock_client()
            MockClient.return_value = mock_instance
            state = load_state(DataSource.OPENF1, session_key=9158)
            MockClient.assert_called_once()
            assert isinstance(state, TimingState)

    def test_store_passed_through(self):
        client = make_mock_client()
        store = MagicMock()
        load_state(DataSource.OPENF1, session_key=9158, openf1_client=client, store=store)
        assert store.append.called

    def test_store_path_creates_store(self, tmp_path):
        client = make_mock_client()
        store_file = tmp_path / "test_session.jsonl"
        state = load_state(
            DataSource.OPENF1,
            session_key=9158,
            openf1_client=client,
            store_path=store_file,
        )
        assert isinstance(state, TimingState)
        assert store_file.exists()
        assert store_file.stat().st_size > 0


# ---------------------------------------------------------------------------
# load_state — REPLAY
# ---------------------------------------------------------------------------

class TestLoadStateReplay:
    def _write_store(self, path: Path) -> None:
        """Write a minimal event store for replay tests."""
        client = make_mock_client()
        store_mock = MagicMock()
        events_written = []
        store_mock.append.side_effect = events_written.append

        load_state(DataSource.OPENF1, session_key=9158, openf1_client=client, store=store_mock)

        # Write the events to a real JSONL file using EventStore
        from race_engineer_core.pipeline.store import EventStore
        real_store = EventStore(path)
        for event in events_written:
            real_store.append(event)

    def test_replay_produces_same_session_info(self, tmp_path):
        store_path = tmp_path / "session.jsonl"
        self._write_store(store_path)

        state = load_state(DataSource.REPLAY, store_path=store_path)
        assert isinstance(state, TimingState)
        assert state.session_info.get("name") == "Race"

    def test_raises_without_store(self):
        with pytest.raises(ValueError, match="store_path"):
            load_state(DataSource.REPLAY)

    def test_accepts_pre_constructed_store(self, tmp_path):
        from race_engineer_core.pipeline.store import EventStore

        store_path = tmp_path / "session.jsonl"
        self._write_store(store_path)

        store = EventStore(store_path)
        state = load_state(DataSource.REPLAY, store=store)
        assert isinstance(state, TimingState)


# ---------------------------------------------------------------------------
# load_state — LIVE_TIMING (not supported)
# ---------------------------------------------------------------------------

class TestLoadStateLiveTiming:
    def test_raises_value_error(self):
        with pytest.raises(ValueError, match="live_timing"):
            load_state(DataSource.LIVE_TIMING)

    def test_error_message_mentions_live_timing_client(self):
        with pytest.raises(ValueError, match="LiveTimingClient"):
            load_state(DataSource.LIVE_TIMING)
