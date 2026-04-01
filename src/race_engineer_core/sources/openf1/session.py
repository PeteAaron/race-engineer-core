"""
OpenF1 session loader.

Orchestrates fetching all data for a session from the OpenF1 API and building
a TimingState that is structurally identical to the state built by the live
timing path.

Architecture position
─────────────────────
This module sits at the boundary between the OpenF1 adapter and the shared
internal model:

    OpenF1 API
        ↓  (HTTP, OpenF1Client)
    OpenF1SessionData   (raw API responses, no transformation)
        ↓  (adapter.py translation functions)
    list[TimingEvent]   (canonical internal events)
        ↓  (reducer.reduce())
    TimingState         (shared with live timing path)

The resulting TimingState is structurally identical to the one produced by
the live path. Downstream components (strategy, chatbot, etc.) should not
need to know which source populated it.

Optional event store
────────────────────
If an EventStore is provided to to_state(), all produced TimingEvents are
appended to it. This enables later replay via ReplayRunner — the same
mechanism used for live sessions. Historical OpenF1 sessions loaded this
way become first-class replay-capable sessions.

Usage
─────
    from race_engineer_core.sources.openf1.client import OpenF1Client
    from race_engineer_core.sources.openf1.session import OpenF1SessionLoader

    client = OpenF1Client()
    loader = OpenF1SessionLoader(client)

    data = loader.fetch(session_key=9158)   # Abu Dhabi 2023 Race
    state = loader.to_state(data)

    print(state.session_info)
    print(state.driver_positions)
"""

from __future__ import annotations

import dataclasses
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from ...pipeline.events import TimingEvent
from ...pipeline.reducer import TimingState, initial_state, reduce
from ...pipeline.store import EventStore
from . import adapter
from .client import OpenF1Client

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Raw data container
# ---------------------------------------------------------------------------

@dataclass
class OpenF1SessionData:
    """
    Raw OpenF1 API responses for a single session.

    This is a plain data holder — no transformation logic lives here.
    Transformation is done by the adapter module.
    """
    session_key: int
    session: dict[str, Any]
    drivers: list[dict[str, Any]]
    laps: list[dict[str, Any]]
    position: list[dict[str, Any]]
    pit: list[dict[str, Any]]
    race_control: list[dict[str, Any]]


# ---------------------------------------------------------------------------
# Session loader
# ---------------------------------------------------------------------------

class OpenF1SessionLoader:
    """
    Fetches and transforms a full session from the OpenF1 API.

    Inject a custom OpenF1Client to override the base URL (useful for testing
    against a local fixture server or for mocking in unit tests).
    """

    def __init__(self, client: OpenF1Client, request_delay_s: float = 0.4) -> None:
        self._client = client
        self._request_delay_s = request_delay_s

    def fetch(self, session_key: int) -> OpenF1SessionData:
        """
        Fetch all data for a session from the OpenF1 API.

        Raises:
            ValueError:   If the session key is not found.
            OpenF1Error:  On network or API errors (from client).

        Note:
            The /v1/position endpoint returns thousands of records for a race.
            This is expected — the full list is held in memory only briefly
            before final_positions() reduces it to a dict.
        """
        logger.info("openf1: fetching session_key=%d", session_key)

        session = self._client.get_session(session_key)
        if session is None:
            raise ValueError(f"Session {session_key} not found in OpenF1")

        logger.info(
            "openf1: session found — %s %s",
            session.get("session_name"),
            session.get("meeting_name"),
        )

        drivers = self._client.get_drivers(session_key)
        logger.info("openf1: fetched %d drivers", len(drivers))
        time.sleep(self._request_delay_s)

        laps = self._client.get_laps(session_key)
        logger.info("openf1: fetched %d lap records", len(laps))
        time.sleep(self._request_delay_s)

        position = self._client.get_position(session_key)
        logger.info("openf1: fetched %d position records", len(position))
        time.sleep(self._request_delay_s)

        pit = self._client.get_pit(session_key)
        logger.info("openf1: fetched %d pit records", len(pit))
        time.sleep(self._request_delay_s)

        race_control = self._client.get_race_control(session_key)
        logger.info("openf1: fetched %d race control messages", len(race_control))

        return OpenF1SessionData(
            session_key=session_key,
            session=session,
            drivers=drivers,
            laps=laps,
            position=position,
            pit=pit,
            race_control=race_control,
        )

    def to_state(
        self,
        data: OpenF1SessionData,
        store: EventStore | None = None,
    ) -> TimingState:
        """
        Build a TimingState from fetched OpenF1 data.

        The returned state is structurally identical to a TimingState built
        by the live timing path — same fields, same types, same semantics.

        Args:
            data:  Session data previously fetched via fetch().
            store: Optional EventStore. If provided, all produced TimingEvents
                   are appended to the store before being reduced, enabling
                   later replay via ReplayRunner.

        Returns:
            A fully populated TimingState for the session.
        """
        events = self._build_events(data)
        logger.info("openf1: processing %d events through reducer", len(events))

        state = initial_state()
        for event in events:
            if store is not None:
                store.append(event)
            state = reduce(state, event)

        # Race-order positions are projected directly from /v1/position records.
        # See adapter.final_positions() for the rationale.
        if data.position:
            positions = adapter.final_positions(data.position)
            if positions:
                state = dataclasses.replace(state, driver_positions=positions)
                logger.info(
                    "openf1: projected %d driver positions from position records",
                    len(positions),
                )

        logger.info(
            "openf1: state built — drivers=%d laps=%d pits=%d rcm=%d",
            len(state.driver_list),
            len(state.latest_laps),
            len(state.pit_history),
            len(state.race_control_messages),
        )
        return state

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _build_events(self, data: OpenF1SessionData) -> list[TimingEvent]:
        """
        Translate raw OpenF1 data into a chronologically sorted list of TimingEvents.

        Session metadata and driver list are placed at the session start time,
        ensuring they appear before all time-series events after sorting.
        """
        session_start = self._session_start(data.session)
        events: list[TimingEvent] = []

        # Session metadata — always first (at session start time)
        events.extend(adapter.session_to_events(data.session))

        # Driver list — at session start time; before any lap/pit data
        driver_event = adapter.drivers_to_event(data.drivers, session_start)
        if driver_event is not None:
            events.append(driver_event)

        # Time-series events — each carries the timestamp of its occurrence
        events.extend(adapter.race_control_to_events(data.race_control))
        events.extend(adapter.pit_to_events(data.pit))
        events.extend(adapter.laps_to_events(data.laps))

        # Sort by timestamp. Python's sort is stable: events with equal
        # timestamps preserve insertion order (metadata before time-series).
        events.sort(key=lambda e: e.timestamp)

        return events

    @staticmethod
    def _session_start(session: dict[str, Any]) -> datetime:
        """Return session start as a timezone-aware datetime."""
        from . import adapter as _a
        ts = _a._session_start_ts(session)
        return ts
