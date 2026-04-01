"""
Smoke test for the F1 live timing transport.

Three stages, each progressively deeper:

  Stage 1 — HTTP negotiate
    Confirms the SignalR endpoint is reachable and returns a ConnectionToken.
    Works at all times (not session-dependent).

  Stage 2 — WebSocket connect + subscribe
    Opens the WebSocket and sends the Subscribe invocation.
    Listens for --timeout seconds and prints any messages received.
    Outside a live session the connection opens but no feed messages arrive.
    During a live session you'll see data immediately.

  Stage 3 — Full pipeline
    Routes each received message through normalize() → ingest() → reduce(),
    printing the evolving TimingState after each event.

Usage:
    python scripts/smoke_test.py [--timeout 15] [--stage 1|2|3]

Default: stage 3, 15-second listen window.

F1 live timing calendar (2026 season) — run during a session for live data.
Outside of sessions the negotiate still works and is useful for checking
transport connectivity.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

# Ensure src/ is on the path when run directly (no install needed)
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("smoke_test")

F1_SIGNALR_URL = "https://livetiming.formula1.com/signalr"

F1_TOPICS = [
    "SessionInfo",
    "SessionStatus",
    "TrackStatus",
    "DriverList",
    "RaceControlMessages",
    "TimingData",
    "Position.z",
    "PitLaneTimeCollection",
]


# ---------------------------------------------------------------------------
# Stage 1: HTTP negotiate
# ---------------------------------------------------------------------------

def stage1_negotiate() -> tuple[str, str] | None:
    """Return (ConnectionToken, ProtocolVersion) or None on failure."""
    import json
    import urllib.parse
    import urllib.request

    logger.info("Stage 1 — HTTP negotiate")
    hub = "Streaming"
    conn_data = json.dumps([{"name": hub}])
    params = urllib.parse.urlencode({
        "connectionData": conn_data,
        "clientProtocol": "1.5",
    })
    url = f"{F1_SIGNALR_URL}/negotiate?{params}"
    logger.info("  GET %s", url)
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read())
            token = data["ConnectionToken"]
            protocol = data.get("ProtocolVersion", "1.5")
            logger.info("  OK  ConnectionId=%s  ProtocolVersion=%s", data.get("ConnectionId"), protocol)
            logger.info("  Token (truncated): %s…", token[:24])
            return token, protocol
    except Exception as exc:
        logger.error("  FAILED: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Stage 2: WebSocket connect + subscribe + listen
# ---------------------------------------------------------------------------

def stage2_listen(token: str, protocol: str, timeout_s: int) -> list[tuple[str, object]]:
    """
    Connect, subscribe, and collect raw (topic, payload) pairs for timeout_s seconds.
    Returns list of (topic, payload) tuples received.
    """
    import base64
    import json
    import threading
    import urllib.parse
    import zlib

    import websocket

    logger.info("Stage 2 — WebSocket connect + subscribe (timeout=%ds)", timeout_s)

    hub = "Streaming"
    conn_data = json.dumps([{"name": hub}])
    params = urllib.parse.urlencode({
        "connectionData": conn_data,
        "clientProtocol": protocol,
        "transport": "webSockets",
        "connectionToken": token,
    })
    ws_url = f"wss://livetiming.formula1.com/signalr/connect?{params}"
    logger.info("  Connecting to %s…", ws_url[:80] + "…")

    received: list[tuple[str, object]] = []
    stop = threading.Event()

    def on_message(raw: str) -> None:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return
        for msg in data.get("M", []):
            if msg.get("M") != "feed":
                continue
            args = msg.get("A", [])
            if len(args) < 2:
                continue
            topic: str = args[0]
            payload = args[1]
            if topic.endswith(".z") and isinstance(payload, str):
                try:
                    payload = json.loads(zlib.decompress(base64.b64decode(payload), -zlib.MAX_WBITS))
                except Exception:
                    payload = "<decompression failed>"
            received.append((topic, payload))
            logger.info("  [%3d] %-30s  (payload keys: %s)",
                        len(received), topic,
                        list(payload.keys()) if isinstance(payload, dict) else type(payload).__name__)

    try:
        ws = websocket.WebSocket()
        ws.settimeout(5.0)
        ws.connect(ws_url, origin="https://livetiming.formula1.com")
        logger.info("  Connected. Subscribing to %d topics…", len(F1_TOPICS))
        ws.send(json.dumps({"H": hub, "M": "Subscribe", "A": [F1_TOPICS], "I": 0}))
        logger.info("  Listening for %d seconds…", timeout_s)

        deadline = time.time() + timeout_s
        while time.time() < deadline:
            try:
                raw = ws.recv()
                if raw:
                    on_message(raw)
            except websocket.WebSocketTimeoutException:
                continue
            except Exception:
                break

        ws.close()
    except Exception as exc:
        logger.error("  WebSocket error: %s", exc)

    logger.info("  Received %d messages in %ds", len(received), timeout_s)
    return received


# ---------------------------------------------------------------------------
# Stage 3: Full pipeline
# ---------------------------------------------------------------------------

def stage3_pipeline(messages: list[tuple[str, object]]) -> None:
    """Route messages through the full pipeline and print final state."""
    from datetime import datetime, timezone
    from pathlib import Path
    import dataclasses

    from race_engineer_core.timing.pipeline import ingest
    from race_engineer_core.timing.raw import RawMessage
    from race_engineer_core.timing.reducer import initial_state
    from race_engineer_core.timing.store import EventStore

    logger.info("Stage 3 — Pipeline (normalize → ingest → reduce)")

    store_path = Path("/tmp/smoke_test_events.jsonl")
    store = EventStore(store_path)
    state = initial_state()
    ingested = 0

    for topic, payload in messages:
        raw = RawMessage(
            received_at=datetime.now(tz=timezone.utc),
            topic=topic,
            payload=payload,
            session_key=None,
        )
        state, event = ingest(raw, state, store)
        if event is not None:
            ingested += 1

    logger.info("  Ingested %d/%d messages → %d events stored at %s",
                ingested, len(messages), ingested, store_path)

    logger.info("  Final TimingState:")
    logger.info("    session_status  : %s", state.session_status)
    logger.info("    session_info    : %s", state.session_info.get("name") or "(none)")
    logger.info("    track_status    : %s", state.track_status)
    logger.info("    driver_positions: %s", dict(sorted(state.driver_positions.items(),
                                                         key=lambda x: x[1])) or "(none)")
    logger.info("    drivers known   : %d", len(state.driver_list))
    logger.info("    lap entries     : %d drivers", len(state.latest_laps))
    logger.info("    race control    : %d messages", len(state.race_control_messages))
    logger.info("    pit history     : %d stops", len(state.pit_history))
    if state.race_control_messages:
        last_msg = state.race_control_messages[-1]
        logger.info("    last RC message : %s", last_msg.get("message"))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="F1 live timing smoke test")
    parser.add_argument("--timeout", type=int, default=15,
                        help="Seconds to listen on the WebSocket (default: 15)")
    parser.add_argument("--stage", type=int, default=3, choices=[1, 2, 3],
                        help="Stop after this stage (default: 3 = full pipeline)")
    args = parser.parse_args()

    # Stage 1
    result = stage1_negotiate()
    if result is None:
        logger.error("Stage 1 failed — cannot continue")
        sys.exit(1)
    if args.stage == 1:
        logger.info("Stage 1 complete.")
        return
    token, protocol = result

    # Stage 2
    messages = stage2_listen(token, protocol, args.timeout)
    if args.stage == 2:
        logger.info("Stage 2 complete. %d messages received.", len(messages))
        return

    # Stage 3
    if not messages:
        logger.warning("No messages received — pipeline stage skipped. "
                       "Run during a live F1 session for feed data, or increase --timeout.")
        return
    stage3_pipeline(messages)


if __name__ == "__main__":
    main()
