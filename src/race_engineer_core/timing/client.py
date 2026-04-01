"""
Live timing client.

Implements the F1 live timing transport using the ASP.NET SignalR v2 (legacy)
protocol. The hub is "Streaming" at https://livetiming.formula1.com/signalr.

Protocol sequence:
    1. HTTP GET /negotiate  → receive ConnectionToken and ProtocolVersion
    2. WebSocket connect to /connect?transport=webSockets&connectionToken=…
    3. Send Subscribe hub invocation to register topics
    4. Receive loop: parse SignalR envelope, decompress .z topics, dispatch

Compression:
    Topics whose names end in ".z" (e.g. "Position.z") carry base64-encoded,
    raw-deflate-compressed JSON payloads. _decompress() handles this before
    the payload is wrapped in a RawMessage, so the normalizer always receives
    a plain Python dict.

Threading model:
    LiveTimingClient is synchronous. connect() starts a background
    threading.Thread that manages the reconnect loop. disconnect() sets a
    stop event and joins the thread. The on_message callback is called from
    the background thread — callers must ensure thread safety if they mutate
    shared state in the callback.
"""

from __future__ import annotations

import base64
import json
import logging
import threading
import time
import urllib.parse
import urllib.request
import zlib
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import websocket  # websocket-client>=1.6

from .raw import RawMessage

logger = logging.getLogger(__name__)

_SIGNALR_HUB = "Streaming"    # ASP.NET SignalR v2 hub name — case-sensitive
_DEFAULT_PROTOCOL = "1.5"      # SignalR client protocol version


@dataclass
class BackoffConfig:
    initial_delay_s: float = 1.0
    max_delay_s: float = 60.0
    multiplier: float = 2.0


@dataclass
class LiveTimingClientConfig:
    url: str
    topics: list[str]
    session_key: int | None = None
    backoff: BackoffConfig = field(default_factory=BackoffConfig)


class LiveTimingClient:
    def __init__(
        self,
        config: LiveTimingClientConfig,
        on_message: Callable[[RawMessage], None],
    ) -> None:
        self._config = config
        self._on_message = on_message
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Start the reconnect loop in a background thread."""
        if self._thread is not None and self._thread.is_alive():
            logger.warning("client: connect() called but thread already running")
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._reconnect_loop,
            name="live-timing-client",
            daemon=True,
        )
        self._thread.start()
        logger.info("client: started url=%s topics=%s", self._config.url, self._config.topics)

    def disconnect(self) -> None:
        """Signal the reconnect loop to stop and wait for the thread to exit."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None
        logger.info("client: disconnected")

    # ------------------------------------------------------------------
    # Internal — reconnect loop
    # ------------------------------------------------------------------

    def _reconnect_loop(self) -> None:
        attempt = 0
        while not self._stop.is_set():
            success = self._attempt_connect()
            if success:
                # Connection was established and ran until it dropped.
                # Reset backoff on a clean disconnect triggered by stop event.
                if self._stop.is_set():
                    break
                logger.info("client: connection dropped, reconnecting attempt=%d", attempt + 1)
            else:
                logger.warning(
                    "client: connection failed attempt=%d",
                    attempt,
                )
            delay = self._compute_backoff(attempt)
            logger.info("client: backoff delay=%.1fs", delay)
            # Use event.wait so disconnect() can interrupt a long sleep.
            self._stop.wait(timeout=delay)
            attempt += 1

    def _attempt_connect(self) -> bool:
        """
        Open a SignalR WebSocket connection to the F1 live timing hub.

        Protocol (ASP.NET SignalR v2 / legacy):
            1. HTTP negotiate to obtain a ConnectionToken.
            2. WebSocket connect using the token.
            3. Send a Subscribe invocation for the configured topics.
            4. Receive loop; dispatch each 'feed' hub message.

        Returns True if a connection was established (even if later dropped).
        Returns False if negotiation or the initial WebSocket connect failed.
        """
        logger.info(
            "client: attempting connection url=%s topics=%s",
            self._config.url,
            self._config.topics,
        )
        negotiate_result = self._negotiate()
        if negotiate_result is None:
            return False
        conn_token, protocol_version = negotiate_result

        ws_url = self._build_ws_url(conn_token, protocol_version)
        try:
            ws = websocket.WebSocket()
            ws.settimeout(30.0)
            ws.connect(ws_url, origin="https://livetiming.formula1.com")
        except Exception:
            logger.exception("client: websocket connect failed url=%s", ws_url)
            return False

        logger.info("client: websocket connected")
        try:
            self._subscribe(ws)
            self._receive_loop(ws)
        except Exception:
            logger.exception("client: error during session")
        finally:
            try:
                ws.close()
            except Exception:
                pass

        return True

    # ------------------------------------------------------------------
    # Internal — SignalR protocol
    # ------------------------------------------------------------------

    def _negotiate(self) -> tuple[str, str] | None:
        """
        Perform the SignalR HTTP negotiate handshake.

        Returns (ConnectionToken, ProtocolVersion), or None on failure.
        The ProtocolVersion from the server response is used for the WebSocket
        connect URL rather than assuming "1.5".
        """
        base = self._config.url.rstrip("/")
        conn_data = json.dumps([{"name": _SIGNALR_HUB}])
        params = urllib.parse.urlencode({
            "connectionData": conn_data,
            "clientProtocol": _DEFAULT_PROTOCOL,
        })
        url = f"{base}/negotiate?{params}"
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                data: dict[str, Any] = json.loads(resp.read())
                token: str = data["ConnectionToken"]
                protocol: str = data.get("ProtocolVersion", _DEFAULT_PROTOCOL)
                logger.info(
                    "client: negotiate ok connection_id=%s protocol=%s",
                    data.get("ConnectionId"),
                    protocol,
                )
                return token, protocol
        except Exception:
            logger.exception("client: negotiate failed url=%s", url)
            return None

    def _build_ws_url(self, conn_token: str, protocol_version: str) -> str:
        """Construct the WebSocket connect URL from the negotiated token."""
        base = (
            self._config.url
            .rstrip("/")
            .replace("https://", "wss://")
            .replace("http://", "ws://")
        )
        conn_data = json.dumps([{"name": _SIGNALR_HUB}])
        params = urllib.parse.urlencode({
            "connectionData": conn_data,
            "clientProtocol": protocol_version,
            "transport": "webSockets",
            "connectionToken": conn_token,
        })
        return f"{base}/connect?{params}"

    def _subscribe(self, ws: websocket.WebSocket) -> None:
        """Send a Subscribe hub invocation for the configured topics."""
        msg = json.dumps({
            "H": _SIGNALR_HUB,
            "M": "Subscribe",
            "A": [self._config.topics],
            "I": 0,
        })
        ws.send(msg)
        logger.info("client: subscribed to %d topics", len(self._config.topics))

    def _receive_loop(self, ws: websocket.WebSocket) -> None:
        """
        Block-receive SignalR messages until the stop event is set or the
        socket is closed. Each 'feed' method invocation is dispatched.
        """
        while not self._stop.is_set():
            try:
                raw = ws.recv()
            except websocket.WebSocketTimeoutException:
                continue
            except Exception:
                logger.info("client: websocket closed or errored in recv")
                break
            if not raw:
                break
            self._process_ws_message(raw)

    def _process_ws_message(self, raw: str) -> None:
        """
        Parse a raw SignalR envelope and dispatch feed messages.

        Envelope format:
            {"M": [{"H": "Streaming", "M": "feed", "A": [topic, payload, ts]}, ...]}

        Keepalive frames are empty objects ({}) or have no "M" key and are
        silently ignored.

        .z topics arrive as base64+deflate compressed strings in A[1]. They
        are decompressed here before dispatch so the normalizer always receives
        a plain dict.
        """
        try:
            data: dict[str, Any] = json.loads(raw)
        except json.JSONDecodeError:
            logger.debug("client: unparseable ws frame len=%d", len(raw))
            return

        for msg in data.get("M", []):
            if msg.get("M") != "feed":
                continue
            args = msg.get("A", [])
            if len(args) < 2:
                continue
            topic: str = args[0]
            payload: Any = args[1]

            # .z topics arrive as base64+raw-deflate compressed JSON strings.
            if topic.endswith(".z") and isinstance(payload, str):
                try:
                    payload = self._decompress(payload)
                except Exception:
                    logger.warning("client: decompression failed topic=%s", topic)
                    continue

            self._dispatch(payload, topic)

    # ------------------------------------------------------------------
    # Internal — dispatch and message construction
    # ------------------------------------------------------------------

    def _dispatch(self, raw_payload: Any, topic: str) -> None:
        """
        Construct a RawMessage and deliver it to the on_message callback.

        Called from _process_ws_message once the real SignalR connection is
        in place. Payload is always a Python dict at this point — .z topics
        have already been decompressed.
        """
        msg = RawMessage(
            received_at=datetime.now(tz=timezone.utc),
            topic=topic,
            payload=raw_payload,
            session_key=self._config.session_key,
        )
        try:
            self._on_message(msg)
        except Exception:
            logger.exception("client: on_message callback raised for topic=%s", topic)

    # ------------------------------------------------------------------
    # Internal — helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _decompress(compressed: str) -> Any:
        """
        Decompress a base64-encoded, raw-deflate JSON payload.

        F1 live timing uses raw deflate (no zlib wrapper), signalled by
        wbits=-MAX_WBITS in the Python zlib API. Confirmed against the
        fastf1 reference implementation (api.py parse() function).
        """
        raw_bytes = base64.b64decode(compressed)
        decompressed = zlib.decompress(raw_bytes, -zlib.MAX_WBITS)
        return json.loads(decompressed)

    def _compute_backoff(self, attempt: int) -> float:
        """Exponential backoff capped at max_delay_s."""
        cfg = self._config.backoff
        delay = cfg.initial_delay_s * (cfg.multiplier ** attempt)
        return min(delay, cfg.max_delay_s)
