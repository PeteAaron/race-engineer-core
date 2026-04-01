"""
Live timing source — primary F1 data input.

Connects to the official F1 live timing feed via ASP.NET SignalR v2,
normalises raw messages into TimingEvents, and feeds them into the
shared pipeline.

    RawMessage (SignalR wire) → normalize() → TimingEvent → pipeline

Typical usage::

    from race_engineer_core.sources.live_timing import (
        LiveTimingClient, LiveTimingClientConfig, ingest,
    )
    from race_engineer_core.pipeline import EventStore, TimingState, initial_state

    state = initial_state()
    store = EventStore(Path("session.jsonl"))

    def handle(raw):
        nonlocal state
        state, _ = ingest(raw, state, store)

    client = LiveTimingClient(
        LiveTimingClientConfig(
            url="https://livetiming.formula1.com/signalr",
            topics=["SessionInfo", "TimingData", "TrackStatus"],
        ),
        on_message=handle,
    )
    client.connect()
"""

from __future__ import annotations

from .client import BackoffConfig, LiveTimingClient, LiveTimingClientConfig
from .ingest import ingest
from .normalizer import normalize, supported_topics
from .raw import RawMessage

__all__ = [
    "BackoffConfig",
    "LiveTimingClient",
    "LiveTimingClientConfig",
    "RawMessage",
    "ingest",
    "normalize",
    "supported_topics",
]
