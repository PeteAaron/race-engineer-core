"""
Live ingestion pipeline.

ingest() is the single orchestration helper for the live data path:
    normalize → store → reduce

It is the seam between transport (LiveTimingClient) and downstream processing.
The replay path bypasses this function — ReplayRunner reads events that are
already normalized and stored.

UNSUPPORTED events are persisted and passed through reduce. The store is a
complete audit log of everything seen, not only events with known types. This
means logs are re-processable when new EventType members are added later.
"""

from __future__ import annotations

from .events import TimingEvent
from .normalizer import normalize
from .raw import RawMessage
from .reducer import TimingState, reduce
from .store import EventStore


def ingest(
    raw: RawMessage,
    state: TimingState,
    store: EventStore,
) -> tuple[TimingState, TimingEvent | None]:
    """
    Normalize a raw message, persist the resulting event, and advance state.

    Args:
        raw:   The raw message from the transport layer.
        state: The current timing state.
        store: The event store to append to.

    Returns:
        A tuple of (new_state, event). If the message could not be normalized
        (malformed payload on a known topic), state is returned unchanged and
        event is None. UNSUPPORTED events are stored and reduce is still called
        (the reducer treats them as a no-op).
    """
    event = normalize(raw)
    if event is None:
        return state, None
    store.append(event)
    return reduce(state, event), event
