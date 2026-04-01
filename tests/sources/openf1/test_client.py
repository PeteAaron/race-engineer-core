"""
Tests for OpenF1Client (client.py).

All tests use a stub HTTP server (via urllib mock / monkeypatching) — no real
network calls are made. Tests cover URL construction, param filtering,
error handling, and each high-level method.
"""

from __future__ import annotations

import io
import json
import urllib.error
from unittest.mock import MagicMock, patch

import pytest

from race_engineer_core.sources.openf1.client import OpenF1Client, OpenF1Error


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def mock_response(data: list | dict, status: int = 200) -> MagicMock:
    """Return a mock that behaves like urllib.request.urlopen context manager."""
    body = json.dumps(data).encode()
    resp = MagicMock()
    resp.read.return_value = body
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    return resp


def patch_urlopen(data: list | dict, status: int = 200):
    return patch(
        "race_engineer_core.sources.openf1.client.urllib.request.urlopen",
        return_value=mock_response(data, status),
    )


# ---------------------------------------------------------------------------
# _get — low level
# ---------------------------------------------------------------------------

class TestGetMethod:
    def test_returns_parsed_list(self):
        client = OpenF1Client()
        with patch_urlopen([{"a": 1}]):
            result = client._get("sessions")
        assert result == [{"a": 1}]

    def test_params_appended_to_url(self):
        client = OpenF1Client()
        with patch(
            "race_engineer_core.sources.openf1.client.urllib.request.urlopen",
            return_value=mock_response([]),
        ) as mock_open:
            client._get("sessions", {"session_key": 9158})
            called_url = mock_open.call_args[0][0]
            assert "session_key=9158" in called_url

    def test_none_params_filtered_out(self):
        client = OpenF1Client()
        with patch(
            "race_engineer_core.sources.openf1.client.urllib.request.urlopen",
            return_value=mock_response([]),
        ) as mock_open:
            client._get("sessions", {"year": 2024, "location": None})
            called_url = mock_open.call_args[0][0]
            assert "year=2024" in called_url
            assert "location" not in called_url

    def test_no_params_no_question_mark(self):
        client = OpenF1Client()
        with patch(
            "race_engineer_core.sources.openf1.client.urllib.request.urlopen",
            return_value=mock_response([]),
        ) as mock_open:
            client._get("sessions")
            called_url = mock_open.call_args[0][0]
            assert "?" not in called_url

    def test_http_error_raises_openf1_error(self):
        client = OpenF1Client()
        with patch(
            "race_engineer_core.sources.openf1.client.urllib.request.urlopen",
            side_effect=urllib.error.HTTPError(
                url="", code=404, msg="Not Found", hdrs=None, fp=None
            ),
        ):
            with pytest.raises(OpenF1Error, match="404"):
                client._get("sessions")

    def test_url_error_raises_openf1_error(self):
        client = OpenF1Client()
        with patch(
            "race_engineer_core.sources.openf1.client.urllib.request.urlopen",
            side_effect=urllib.error.URLError(reason="Connection refused"),
        ):
            with pytest.raises(OpenF1Error, match="Network error"):
                client._get("sessions")

    def test_invalid_json_raises_openf1_error(self):
        client = OpenF1Client()
        resp = MagicMock()
        resp.read.return_value = b"not json"
        resp.__enter__ = lambda s: s
        resp.__exit__ = MagicMock(return_value=False)
        with patch(
            "race_engineer_core.sources.openf1.client.urllib.request.urlopen",
            return_value=resp,
        ):
            with pytest.raises(OpenF1Error, match="Invalid JSON"):
                client._get("sessions")

    def test_non_list_json_raises_openf1_error(self):
        client = OpenF1Client()
        with patch_urlopen({"key": "value"}):  # dict, not list
            with pytest.raises(OpenF1Error, match="Expected list"):
                client._get("sessions")

    def test_custom_base_url(self):
        client = OpenF1Client(base_url="http://localhost:8080")
        with patch(
            "race_engineer_core.sources.openf1.client.urllib.request.urlopen",
            return_value=mock_response([]),
        ) as mock_open:
            client._get("sessions")
            called_url = mock_open.call_args[0][0]
            assert called_url.startswith("http://localhost:8080")

    def test_trailing_slash_stripped_from_base_url(self):
        client = OpenF1Client(base_url="http://localhost:8080/")
        with patch(
            "race_engineer_core.sources.openf1.client.urllib.request.urlopen",
            return_value=mock_response([]),
        ) as mock_open:
            client._get("sessions")
            called_url = mock_open.call_args[0][0]
            assert "//sessions" not in called_url


# ---------------------------------------------------------------------------
# High-level methods
# ---------------------------------------------------------------------------

class TestListSessions:
    def test_passes_year_filter(self):
        client = OpenF1Client()
        with patch.object(client, "_get", return_value=[]) as mock_get:
            client.list_sessions(year=2024)
            mock_get.assert_called_once_with("sessions", {"year": 2024, "session_type": None, "country_name": None, "location": None})

    def test_returns_list(self):
        client = OpenF1Client()
        with patch.object(client, "_get", return_value=[{"session_key": 9158}]):
            result = client.list_sessions()
        assert result == [{"session_key": 9158}]


class TestFindSession:
    def test_returns_first_match(self):
        client = OpenF1Client()
        sessions = [{"session_key": 9158}, {"session_key": 9200}]
        with patch.object(client, "list_sessions", return_value=sessions):
            result = client.find_session(2023, "Race")
        assert result == {"session_key": 9158}

    def test_returns_none_when_empty(self):
        client = OpenF1Client()
        with patch.object(client, "list_sessions", return_value=[]):
            result = client.find_session(2023, "Race")
        assert result is None

    def test_passes_filters_to_list_sessions(self):
        client = OpenF1Client()
        with patch.object(client, "list_sessions", return_value=[]) as mock_list:
            client.find_session(2024, "Race", country_name="Bahrain", location="Sakhir")
            mock_list.assert_called_once_with(
                year=2024,
                session_type="Race",
                country_name="Bahrain",
                location="Sakhir",
            )


class TestGetSession:
    def test_returns_first_result(self):
        client = OpenF1Client()
        with patch.object(client, "_get", return_value=[{"session_key": 9158}]):
            result = client.get_session(9158)
        assert result == {"session_key": 9158}

    def test_returns_none_when_not_found(self):
        client = OpenF1Client()
        with patch.object(client, "_get", return_value=[]):
            result = client.get_session(9999)
        assert result is None

    def test_queries_sessions_endpoint(self):
        client = OpenF1Client()
        with patch.object(client, "_get", return_value=[]) as mock_get:
            client.get_session(9158)
            mock_get.assert_called_once_with("sessions", {"session_key": 9158})


class TestPerSessionEndpoints:
    """Each per-session method calls the correct endpoint with session_key."""

    @pytest.mark.parametrize("method,endpoint", [
        ("get_drivers", "drivers"),
        ("get_laps", "laps"),
        ("get_position", "position"),
        ("get_pit", "pit"),
        ("get_race_control", "race_control"),
    ])
    def test_endpoint_and_param(self, method, endpoint):
        client = OpenF1Client()
        with patch.object(client, "_get", return_value=[]) as mock_get:
            getattr(client, method)(9158)
            mock_get.assert_called_once_with(endpoint, {"session_key": 9158})
