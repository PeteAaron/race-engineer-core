"""
Replay runner.

Reads stored normalized events from an EventStore and replays them through
the same reducer path used during live ingestion. The final TimingState
produced by a full replay is equivalent to the state that was live at the
end of the captured session.

Speed multiplier (v1 stub):
    The `speed` parameter is accepted and logged. In a future implementation,
    the runner would compute delta = event[n].timestamp - event[n-1].timestamp
    and inject time.sleep(delta.total_seconds() / speed) between events.
    No sleep is performed in v1 — replay runs as fast as the store can iterate.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

from .events import TimingEvent
from .reducer import TimingState, initial_state, reduce
from .store import EventStore

logger = logging.getLogger(__name__)

_ReducerFn = Callable[[TimingState, TimingEvent], TimingState]


class ReplayRunner:
    def __init__(self, store: EventStore) -> None:
        self._store = store

    def run(
        self,
        reducer: _ReducerFn = reduce,
        initial: TimingState | None = None,
        speed: float = 1.0,
    ) -> TimingState:
        """
        Replay all events from the store through the reducer.

        Args:
            reducer: The reducer function to apply. Defaults to the standard
                     reduce(). Inject a custom reducer for testing or analysis.
            initial: Starting state. Defaults to initial_state() if not provided.
            speed:   Playback speed multiplier (stub — no effect in v1).

        Returns:
            The final TimingState after all stored events have been applied.
        """
        state = initial if initial is not None else initial_state()
        count = 0

        logger.info(
            "replay start: store=%s speed=%.1f",
            self._store._path,
            speed,
        )

        for event in self._store.iter_events():
            state = reducer(state, event)
            count += 1

        logger.info("replay complete: %d events processed", count)
        return state
