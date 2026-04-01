# race-engineer-core

**race-engineer-core** is the data infrastructure layer for a live Formula 1 race intelligence system.

It ingests the official F1 live timing feed (SignalR), normalises messages into a canonical event model, and reduces them to a queryable `TimingState`. Historical sessions can be loaded from the [OpenF1](https://openf1.org) public API using the same pipeline, enabling development and testing outside live sessions.

## Architecture

```
┌─────────────────────┐     ┌──────────────────────┐
│  sources/           │     │  sources/            │
│  live_timing/       │     │  openf1/             │
│                     │     │                      │
│  SignalR transport  │     │  HTTP client         │
│  Message normaliser │     │  Session loader      │
│  (PRIMARY)          │     │  (HISTORICAL)        │
└────────┬────────────┘     └──────────┬───────────┘
         │  TimingEvent                │  TimingEvent
         └──────────────┬──────────────┘
                        ▼
              ┌─────────────────┐
              │  pipeline/      │
              │                 │
              │  EventStore     │
              │  reduce()       │
              │  ReplayRunner   │
              └────────┬────────┘
                       ▼
                  TimingState
```

Both sources produce structurally identical `TimingEvent` objects that flow through the same shared pipeline. Downstream code operates on `TimingState` and is source-agnostic.

## Package structure

```
src/race_engineer_core/
    pipeline/         shared event pipeline: EventStore, reducer, ReplayRunner
    sources/
        live_timing/  PRIMARY: SignalR transport, message normaliser
        openf1/       HISTORICAL: OpenF1 REST API client and session loader
    models/           domain entities (session, driver, lap, stint)
```

## Quick start

Requires Python 3.11+.

```bash
python -m venv .venv
source .venv/Scripts/activate   # Windows
# source .venv/bin/activate     # macOS / Linux
pip install -e ".[dev]"
```

Run the test suite:

```bash
pytest
```

Load a historical session from OpenF1 (works right now, no live session needed):

```bash
python scripts/openf1_demo.py
python scripts/openf1_demo.py --year 2024 --location Bahrain
python scripts/openf1_demo.py --session 9158 --save abu_dhabi.jsonl
python scripts/openf1_demo.py --replay abu_dhabi.jsonl
```

## Source paths

### Live timing (primary)

Connects to `https://livetiming.formula1.com/signalr` using ASP.NET SignalR v2. Available only during active F1 sessions.

```python
from pathlib import Path
from race_engineer_core.sources.live_timing import LiveTimingClient, LiveTimingClientConfig, ingest
from race_engineer_core.pipeline import EventStore, initial_state

state = initial_state()
store = EventStore(Path("session.jsonl"))

def handle(raw):
    nonlocal state
    state, _ = ingest(raw, state, store)

client = LiveTimingClient(
    LiveTimingClientConfig(
        url="https://livetiming.formula1.com/signalr",
        topics=["SessionInfo", "TimingData", "TrackStatus", "DriverList"],
    ),
    on_message=handle,
)
client.connect()
```

### OpenF1 (historical / fallback)

Fetches completed sessions from the [OpenF1 public API](https://openf1.org). No authentication required. Works at any time.

```python
from race_engineer_core.sources import DataSource, load_state

state = load_state(DataSource.OPENF1, session_key=9158)
print(state.session_info)
print(state.driver_positions)
```

### Replay

Both source paths can write events to a JSONL store and replay them later.

```python
from pathlib import Path
from race_engineer_core.sources import DataSource, load_state

state = load_state(DataSource.OPENF1, session_key=9158, store_path=Path("session.jsonl"))
state = load_state(DataSource.REPLAY, store_path=Path("session.jsonl"))
```

## Development scripts

| Script | Purpose |
|---|---|
| `scripts/openf1_demo.py` | Load any historical session and print TimingState |
| `scripts/smoke_test.py` | Three-stage connectivity check for the live SignalR endpoint |

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full internal design.

## Licence

See `LICENSE`.
