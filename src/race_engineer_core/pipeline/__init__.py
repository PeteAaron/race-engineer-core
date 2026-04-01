"""
Shared timing pipeline — source-agnostic infrastructure.

Both input sources (live timing and OpenF1) produce TimingEvents that flow
through this pipeline to produce a TimingState.

    TimingEvent → EventStore (persist) → reduce() → TimingState

Typical usage::

    from race_engineer_core.pipeline import (
        EventStore, TimingState, initial_state, reduce, ReplayRunner,
    )

    state = initial_state()
    store = EventStore(Path("session.jsonl"))

    # Advance state with one event
    state = reduce(state, event)

    # Replay a stored session
    final_state = ReplayRunner(store).run()
"""

from __future__ import annotations

from .events import EventType, TimingEvent
from .reducer import TimingState, initial_state, reduce
from .replay import ReplayRunner
from .store import EventStore

__all__ = [
    "EventStore",
    "EventType",
    "ReplayRunner",
    "TimingEvent",
    "TimingState",
    "initial_state",
    "reduce",
]
