"""
Live timing ingestion and replay architecture.

Provides the end-to-end pipeline from raw feed messages to normalized events,
durable event storage, deterministic state reduction, and replay.

Typical live usage::

    from pathlib import Path
    from race_engineer_core.timing import (
        BackoffConfig, LiveTimingClient, LiveTimingClientConfig,
        EventStore, TimingState, initial_state, ingest,
    )

    state = initial_state()
    store = EventStore(Path("session.jsonl"))

    def handle_message(raw):
        nonlocal state
        state, _ = ingest(raw, state, store)

    client = LiveTimingClient(
        LiveTimingClientConfig(url="...", topics=["SessionInfo", "TrackStatus"]),
        on_message=handle_message,
    )
    client.connect()

Replay usage::

    from race_engineer_core.timing import EventStore, ReplayRunner
    store = EventStore(Path("session.jsonl"))
    final_state = ReplayRunner(store).run()
"""

from __future__ import annotations

from .client import BackoffConfig, LiveTimingClient, LiveTimingClientConfig
from .events import EventType, TimingEvent
from .normalizer import normalize, supported_topics
from .pipeline import ingest
from .raw import RawMessage
from .reducer import TimingState, initial_state, reduce
from .replay import ReplayRunner
from .store import EventStore

__all__ = [
    "BackoffConfig",
    "EventStore",
    "EventType",
    "LiveTimingClient",
    "LiveTimingClientConfig",
    "RawMessage",
    "ReplayRunner",
    "TimingEvent",
    "TimingState",
    "initial_state",
    "ingest",
    "normalize",
    "reduce",
    "supported_topics",
]
