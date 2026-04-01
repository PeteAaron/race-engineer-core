"""
Tests for the LiveTimingClient transport layer.

These tests exercise the SignalR protocol handling without requiring a live
network connection. The actual WebSocket/HTTP calls are not made; instead we
test the protocol-parsing and decompression logic directly.
"""

from __future__ import annotations

import base64
import json
import zlib
from collections.abc import Callable
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from race_engineer_core.timing.client import (
    BackoffConfig,
    LiveTimingClient,
    LiveTimingClientConfig,
    _SIGNALR_HUB,
)
from race_engineer_core.timing.raw import RawMessage


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_client(
    on_message: Callable[[RawMessage], None] | None = None,
    topics: list[str] | None = None,
) -> LiveTimingClient:
    cfg = LiveTimingClientConfig(
        url="https://livetiming.formula1.com/signalr",
        topics=topics or ["SessionInfo", "TrackStatus"],
    )
    return LiveTimingClient(cfg, on_message or (lambda _: None))


def _compress(data: dict) -> str:
    """
    Compress a dict to the base64+raw-deflate format used by F1 .z topics.
    Mirrors _decompress() in the client.
    """
    raw = json.dumps(data).encode("utf-8")
    compressor = zlib.compressobj(wbits=-zlib.MAX_WBITS)
    compressed = compressor.compress(raw) + compressor.flush()
    return base64.b64encode(compressed).decode("ascii")


# ---------------------------------------------------------------------------
# _decompress()
# ---------------------------------------------------------------------------

class TestDecompress:
    def test_roundtrip_simple_dict(self):
        original = {"Status": "1", "Message": "AllClear"}
        result = LiveTimingClient._decompress(_compress(original))
        assert result == original

    def test_roundtrip_nested_dict(self):
        original = {
            "Position": [
                {"Timestamp": "00:01:23.456",
                 "Entries": {"1": {"Status": "OnTrack", "X": 100, "Y": 200, "Z": 0}}}
            ]
        }
        result = LiveTimingClient._decompress(_compress(original))
        assert result == original

    def test_roundtrip_unicode(self):
        original = {"Message": "Vérification de la sécurité"}
        result = LiveTimingClient._decompress(_compress(original))
        assert result == original

    def test_bad_base64_raises(self):
        with pytest.raises(Exception):
            LiveTimingClient._decompress("not-valid-base64!!!")

    def test_bad_deflate_raises(self):
        # Valid base64 but not valid deflate data
        with pytest.raises(Exception):
            LiveTimingClient._decompress(base64.b64encode(b"not-deflate").decode())


# ---------------------------------------------------------------------------
# _process_ws_message() — SignalR envelope parsing
# ---------------------------------------------------------------------------

class TestProcessWsMessage:
    def test_dispatches_feed_message(self):
        received: list[RawMessage] = []
        client = _make_client(on_message=received.append)

        envelope = json.dumps({
            "M": [
                {"H": "streaming", "M": "feed",
                 "A": ["TrackStatus", {"Status": "1", "Message": "AllClear"}, "00:01:00"]}
            ]
        })
        client._process_ws_message(envelope)

        assert len(received) == 1
        assert received[0].topic == "TrackStatus"
        assert received[0].payload == {"Status": "1", "Message": "AllClear"}

    def test_ignores_non_feed_methods(self):
        received: list[RawMessage] = []
        client = _make_client(on_message=received.append)

        envelope = json.dumps({
            "M": [{"H": "streaming", "M": "init", "A": []}]
        })
        client._process_ws_message(envelope)
        assert received == []

    def test_ignores_keepalive_empty_frame(self):
        received: list[RawMessage] = []
        client = _make_client(on_message=received.append)
        client._process_ws_message("{}")
        assert received == []

    def test_ignores_malformed_json(self):
        received: list[RawMessage] = []
        client = _make_client(on_message=received.append)
        client._process_ws_message("not json {{{{")
        assert received == []

    def test_dispatches_multiple_messages_in_one_frame(self):
        received: list[RawMessage] = []
        client = _make_client(on_message=received.append)

        envelope = json.dumps({
            "M": [
                {"H": "streaming", "M": "feed",
                 "A": ["TrackStatus", {"Status": "1"}, "ts"]},
                {"H": "streaming", "M": "feed",
                 "A": ["SessionStatus", {"Status": "Started"}, "ts"]},
            ]
        })
        client._process_ws_message(envelope)
        assert len(received) == 2
        assert received[0].topic == "TrackStatus"
        assert received[1].topic == "SessionStatus"

    def test_decompresses_z_topic(self):
        received: list[RawMessage] = []
        client = _make_client(on_message=received.append)

        payload = {
            "Position": [{"Timestamp": "00:01:00", "Entries": {
                "44": {"Status": "OnTrack", "X": 100, "Y": 200, "Z": 0}
            }}]
        }
        compressed = _compress(payload)

        envelope = json.dumps({
            "M": [{"H": "streaming", "M": "feed",
                   "A": ["Position.z", compressed, "ts"]}]
        })
        client._process_ws_message(envelope)

        assert len(received) == 1
        assert received[0].topic == "Position.z"
        assert received[0].payload == payload

    def test_z_topic_decompression_failure_skips_message(self):
        received: list[RawMessage] = []
        client = _make_client(on_message=received.append)

        envelope = json.dumps({
            "M": [{"H": "streaming", "M": "feed",
                   "A": ["Position.z", "INVALID_COMPRESSED_DATA", "ts"]}]
        })
        # Should not raise; just skip the undecompressable message
        client._process_ws_message(envelope)
        assert received == []

    def test_non_z_topic_with_string_payload_not_decompressed(self):
        received: list[RawMessage] = []
        client = _make_client(on_message=received.append)

        # A topic that doesn't end in .z with a string payload should pass through
        envelope = json.dumps({
            "M": [{"H": "streaming", "M": "feed",
                   "A": ["Heartbeat", "ping", "ts"]}]
        })
        client._process_ws_message(envelope)
        assert len(received) == 1
        assert received[0].payload == "ping"

    def test_message_has_utc_received_at(self):
        received: list[RawMessage] = []
        client = _make_client(on_message=received.append)

        before = datetime.now(tz=timezone.utc)
        envelope = json.dumps({"M": [
            {"H": "streaming", "M": "feed",
             "A": ["TrackStatus", {"Status": "1"}, "ts"]}
        ]})
        client._process_ws_message(envelope)
        after = datetime.now(tz=timezone.utc)

        assert received[0].received_at.tzinfo is not None
        assert before <= received[0].received_at <= after


# ---------------------------------------------------------------------------
# _build_ws_url()
# ---------------------------------------------------------------------------

class TestBuildWsUrl:
    def test_https_converted_to_wss(self):
        client = _make_client()
        url = client._build_ws_url("token123", "1.5")
        assert url.startswith("wss://")

    def test_http_converted_to_ws(self):
        cfg = LiveTimingClientConfig(
            url="http://example.com/signalr",
            topics=["TrackStatus"],
        )
        client = LiveTimingClient(cfg, lambda _: None)
        url = client._build_ws_url("tok", "1.5")
        assert url.startswith("ws://")

    def test_url_contains_connect_path(self):
        client = _make_client()
        url = client._build_ws_url("token123", "1.5")
        assert "/connect?" in url

    def test_url_contains_connection_token(self):
        client = _make_client()
        url = client._build_ws_url("mytoken", "1.5")
        assert "mytoken" in url

    def test_url_contains_websockets_transport(self):
        client = _make_client()
        url = client._build_ws_url("tok", "1.5")
        assert "webSockets" in url

    def test_url_uses_protocol_from_negotiate(self):
        client = _make_client()
        url = client._build_ws_url("tok", "1.5")
        assert "1.5" in url

    def test_hub_name_in_connection_data(self):
        client = _make_client()
        url = client._build_ws_url("tok", "1.5")
        # connectionData should contain the Streaming hub name (URL-encoded)
        assert "Streaming" in url


# ---------------------------------------------------------------------------
# _compute_backoff()
# ---------------------------------------------------------------------------

class TestComputeBackoff:
    def test_attempt_0_returns_initial_delay(self):
        client = _make_client()
        assert client._compute_backoff(0) == pytest.approx(1.0)

    def test_exponential_growth(self):
        client = _make_client()
        assert client._compute_backoff(1) == pytest.approx(2.0)
        assert client._compute_backoff(2) == pytest.approx(4.0)
        assert client._compute_backoff(3) == pytest.approx(8.0)

    def test_capped_at_max(self):
        client = _make_client()
        # After enough attempts the cap kicks in
        assert client._compute_backoff(100) == pytest.approx(60.0)

    def test_custom_backoff_config(self):
        cfg = LiveTimingClientConfig(
            url="https://livetiming.formula1.com/signalr",
            topics=[],
            backoff=BackoffConfig(initial_delay_s=5.0, max_delay_s=120.0, multiplier=3.0),
        )
        client = LiveTimingClient(cfg, lambda _: None)
        assert client._compute_backoff(0) == pytest.approx(5.0)
        assert client._compute_backoff(1) == pytest.approx(15.0)
        assert client._compute_backoff(100) == pytest.approx(120.0)


# ---------------------------------------------------------------------------
# _dispatch() — session_key propagation
# ---------------------------------------------------------------------------

class TestDispatch:
    def test_session_key_propagated(self):
        received: list[RawMessage] = []
        cfg = LiveTimingClientConfig(
            url="https://livetiming.formula1.com/signalr",
            topics=["TrackStatus"],
            session_key=9662,
        )
        client = LiveTimingClient(cfg, received.append)
        client._dispatch({"Status": "1"}, "TrackStatus")
        assert received[0].session_key == 9662

    def test_topic_preserved(self):
        received: list[RawMessage] = []
        client = _make_client(on_message=received.append)
        client._dispatch({"x": 1}, "CustomTopic")
        assert received[0].topic == "CustomTopic"

    def test_callback_exception_does_not_propagate(self):
        def bad_callback(_: RawMessage) -> None:
            raise RuntimeError("intentional error")

        client = _make_client(on_message=bad_callback)
        # Should not raise
        client._dispatch({"Status": "1"}, "TrackStatus")


# ---------------------------------------------------------------------------
# Hub name constant
# ---------------------------------------------------------------------------

class TestHubName:
    def test_hub_name_is_capitalised_streaming(self):
        # The F1 SignalR hub is "Streaming" (capital S). Confirmed against
        # fastf1 and python-signalr-client reference implementations.
        assert _SIGNALR_HUB == "Streaming"
