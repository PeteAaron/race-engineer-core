"""
Data source abstraction.

Provides a uniform interface for loading a TimingState from different sources.
This is the integration point for selecting between the OpenF1 historical path
and the replay path. Live timing state is built incrementally by LiveTimingClient
and does not go through this module.

Design
──────
The enum keeps the concept explicit without requiring a heavyweight plugin
system. The factory function load_state() is a thin convenience wrapper — it
delegates immediately to the appropriate adapter or runner.

    DataSource.LIVE_TIMING  — state is built live by LiveTimingClient + ingest()
                              load_state() does not apply; use timing.client directly
    DataSource.OPENF1       — fetch historical session via OpenF1 API → TimingState
    DataSource.REPLAY       — replay stored events from a JSONL store → TimingState

Sources produce structurally identical TimingState objects. Downstream code
should not branch on the source type.
"""

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapters.openf1.client import OpenF1Client
    from .timing.reducer import TimingState
    from .timing.store import EventStore


class DataSource(str, Enum):
    """
    Identifies which data source populates a TimingState.

    Values are intentionally human-readable strings so they can appear
    safely in logs, configs, and CLI arguments.
    """
    LIVE_TIMING = "live_timing"
    OPENF1 = "openf1"
    REPLAY = "replay"


def load_state(
    source: DataSource,
    *,
    session_key: int | None = None,
    store_path: Path | None = None,
    openf1_client: OpenF1Client | None = None,
    store: EventStore | None = None,
) -> TimingState:
    """
    Load or build a TimingState from the specified source.

    Args:
        source:
            Which data source to use.

        session_key:
            Required for DataSource.OPENF1. The OpenF1 session identifier.

        store_path:
            Required for DataSource.REPLAY. Path to the JSONL event store.
            Also accepted for DataSource.OPENF1 — if provided, produced events
            are written to the store (enabling later replay of historical data).

        openf1_client:
            Optional custom OpenF1Client. If omitted, a default client is
            constructed. Inject a test client to avoid real network calls.

        store:
            Pre-constructed EventStore. Takes precedence over store_path when
            both are provided. Useful when the caller already holds an open store.

    Returns:
        A fully populated TimingState.

    Raises:
        ValueError:  If required arguments for the source are missing.
        OpenF1Error: If the OpenF1 API call fails (OPENF1 source only).

    Examples::

        # Historical session via OpenF1 (works without a live session)
        state = load_state(DataSource.OPENF1, session_key=9158)

        # Historical session, also writing events to disk for later replay
        state = load_state(
            DataSource.OPENF1,
            session_key=9158,
            store_path=Path("bahrain_2024.jsonl"),
        )

        # Replay from a previously captured live or OpenF1 session
        state = load_state(DataSource.REPLAY, store_path=Path("session.jsonl"))
    """
    if source == DataSource.OPENF1:
        return _load_openf1(
            session_key=session_key,
            store_path=store_path,
            client=openf1_client,
            store=store,
        )

    if source == DataSource.REPLAY:
        return _load_replay(store_path=store_path, store=store)

    # LIVE_TIMING is not handled here — it is driven by LiveTimingClient.connect()
    raise ValueError(
        f"load_state() does not apply to source={source!r}. "
        "For live timing, use LiveTimingClient.connect() and ingest() instead."
    )


# ---------------------------------------------------------------------------
# Source-specific loaders
# ---------------------------------------------------------------------------

def _load_openf1(
    session_key: int | None,
    store_path: Path | None,
    client: OpenF1Client | None,
    store: EventStore | None,
) -> TimingState:
    from .adapters.openf1.client import OpenF1Client as _Client
    from .adapters.openf1.session import OpenF1SessionLoader
    from .timing.store import EventStore as _Store

    if session_key is None:
        raise ValueError("session_key is required for DataSource.OPENF1")

    resolved_client = client or _Client()
    loader = OpenF1SessionLoader(resolved_client)

    resolved_store: EventStore | None = store
    if resolved_store is None and store_path is not None:
        resolved_store = _Store(store_path)

    data = loader.fetch(session_key)
    return loader.to_state(data, store=resolved_store)


def _load_replay(
    store_path: Path | None,
    store: EventStore | None,
) -> TimingState:
    from .timing.replay import ReplayRunner
    from .timing.store import EventStore as _Store

    resolved_store: EventStore | None = store
    if resolved_store is None:
        if store_path is None:
            raise ValueError("store_path or store is required for DataSource.REPLAY")
        resolved_store = _Store(store_path)

    return ReplayRunner(resolved_store).run()
