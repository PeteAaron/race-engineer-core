# Architecture

## Mental model

race-engineer-core is an event-sourced data pipeline. The core invariant is:

> Both input sources produce structurally identical `TimingEvent` objects. Downstream code is source-agnostic.

## Packages

### `pipeline/`

Source-agnostic infrastructure. Nothing in this package knows about SignalR or OpenF1.

| Module | Responsibility |
|---|---|
| `events.py` | `TimingEvent` dataclass and `EventType` enum — the canonical internal message format |
| `reducer.py` | `TimingState` + `reduce()` — pure function, no I/O |
| `store.py` | `EventStore` — append-only JSONL persistence |
| `replay.py` | `ReplayRunner` — reads a store, replays events through the reducer |

### `sources/live_timing/`

PRIMARY source. Connects to the F1 live timing SignalR hub.

| Module | Responsibility |
|---|---|
| `client.py` | SignalR v2 transport: negotiate → WebSocket → subscribe → receive loop |
| `raw.py` | `RawMessage` — wire-level message before normalisation |
| `normalizer.py` | Per-topic handlers: raw feed message → `TimingEvent` |
| `ingest.py` | `ingest()` — orchestrates normalize → store → reduce for the live path |

### `sources/openf1/`

SECONDARY source. Fetches completed sessions from the OpenF1 public REST API.

| Module | Responsibility |
|---|---|
| `client.py` | HTTP client wrapping OpenF1 REST endpoints (stdlib `urllib` only) |
| `adapter.py` | Per-endpoint translators: OpenF1 records → `TimingEvent` objects |
| `session.py` | `OpenF1SessionLoader` — fetches all endpoints and builds `TimingState` |

### `sources/__init__.py`

`DataSource` enum and `load_state()` factory — the uniform interface for loading state from either non-live source.

## Convergence map

```
OpenF1 endpoint          → Internal event type       → TimingState field
─────────────────────────────────────────────────────────────────────────
/v1/sessions             → SESSION_INFO              → session_info
                         → SESSION_STATUS (Finished) → session_status
/v1/drivers              → DRIVER_LIST               → driver_list
/v1/race_control         → RACE_CONTROL_MESSAGE       → race_control_messages
/v1/pit                  → PIT_STOP                  → pit_history
/v1/laps                 → LAP_UPDATE (per lap)       → latest_laps
/v1/position             → (state projection)        → driver_positions

SignalR topic            → Internal event type       → TimingState field
─────────────────────────────────────────────────────────────────────────
SessionInfo              → SESSION_INFO              → session_info
SessionStatus            → SESSION_STATUS            → session_status
TrackStatus              → TRACK_STATUS              → track_status
RaceControlMessages      → RACE_CONTROL_MESSAGE       → race_control_messages
Position.z               → POSITION_UPDATE           → (x/y/z coordinates)
TimingData               → LAP_UPDATE                → latest_laps, driver_positions
PitLaneTimeCollection    → PIT_STOP                  → pit_history
DriverList               → DRIVER_LIST               → driver_list
```

## Provisional items

Documented gaps in the current implementation:

- **`in_time` / `out_time` in PIT_STOP from OpenF1**: not available in `/v1/pit`.
- **`track_status` from OpenF1**: no OpenF1 endpoint for real-time flag/safety car state.
- **`best_lap_time` in OpenF1 laps**: computed as a running minimum across a driver's laps.
- **`number_of_pit_stops` per lap from OpenF1**: derivable from `/v1/pit` but not yet implemented.
- **Position.z Z coordinate**: altitude vs. projection artefact — not officially documented.
- **EventStore rotation**: files grow unbounded within a session. Compaction not yet implemented.
- **ReplayRunner speed multiplier**: parameter accepted but sleep not yet injected between events.
