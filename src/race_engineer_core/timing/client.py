"""
Live timing client scaffold.

Provides the connection-oriented structure for receiving a SignalR-style live
timing feed. The transport is the only stub — all other concerns (backoff,
topic subscription, handler dispatch, RawMessage construction) are fully
implemented.

Transport integration point:
    _attempt_connect() is where the SignalR wire connection goes. In v1 it
    logs the connection attempt and returns False, causing the reconnect loop
    to back off and retry. Wire a real SignalR client (e.g. signalrcore) here
    in a follow-on task.

Threading model:
    LiveTimingClient is synchronous. connect() starts a background
    threading.Thread that manages the reconnect loop. disconnect() sets a
    stop event and joins the thread. The on_message callback is called from
    the background thread — callers must ensure thread safety if they mutate
    shared state in the callback.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .raw import RawMessage

logger = logging.getLogger(__name__)


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
        Attempt to open the transport connection.

        Returns True if a connection was established (even if it later dropped).
        Returns False if the connection could not be opened.

        TODO: Wire a real SignalR client here. Steps:
            1. Perform SignalR HTTP negotiation against self._config.url
            2. Open the WebSocket transport
            3. Send hub invocations to subscribe to self._config.topics
            4. Enter a receive loop; for each message call self._dispatch(payload, topic)
            5. On clean close return True; on error return False
        """
        logger.info(
            "client: attempting connection url=%s topics=%s",
            self._config.url,
            self._config.topics,
        )
        # Scaffold: transport not yet wired. Return False to trigger backoff.
        return False

    # ------------------------------------------------------------------
    # Internal — dispatch and message construction
    # ------------------------------------------------------------------

    def _dispatch(self, raw_payload: Any, topic: str) -> None:
        """
        Construct a RawMessage and deliver it to the on_message callback.

        Called from within the transport receive loop (_attempt_connect) once
        the real SignalR connection is in place.
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

    def _compute_backoff(self, attempt: int) -> float:
        """Exponential backoff capped at max_delay_s."""
        cfg = self._config.backoff
        delay = cfg.initial_delay_s * (cfg.multiplier ** attempt)
        return min(delay, cfg.max_delay_s)
