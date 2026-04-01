"""
OpenF1 adapter — historical / fallback data source.

Provides access to historical F1 session data via the OpenF1 public API
(https://openf1.org). This adapter is the secondary input path; the primary
path is the live SignalR timing stream (race_engineer_core.sources.live_timing).

Both paths converge on the same internal model:
    TimingEvent → reduce() → TimingState

Typical usage::

    from race_engineer_core.sources.openf1 import OpenF1Client, OpenF1SessionLoader

    client = OpenF1Client()
    loader = OpenF1SessionLoader(client)

    # Find a session
    session = client.find_session(2024, "Race", country_name="Bahrain")
    session_key = session["session_key"]

    # Load and build state
    data = loader.fetch(session_key)
    state = loader.to_state(data)

    print(state.session_info["meeting_name"])  # "Bahrain Grand Prix"
    print(state.driver_positions)              # {"1": 1, "11": 2, ...}

Or use the source abstraction for uniform access::

    from race_engineer_core.sources import DataSource, load_state

    state = load_state(DataSource.OPENF1, session_key=9158)
"""

from .client import OpenF1Client, OpenF1Error
from .session import OpenF1SessionData, OpenF1SessionLoader

__all__ = [
    "OpenF1Client",
    "OpenF1Error",
    "OpenF1SessionData",
    "OpenF1SessionLoader",
]
