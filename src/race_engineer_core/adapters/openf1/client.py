"""
OpenF1 HTTP client.

Wraps the OpenF1 REST API (https://openf1.org) using stdlib urllib only —
no new runtime dependencies.

All methods return parsed JSON. Errors are raised as OpenF1Error so callers
have a single exception type to handle.

Rate limiting:
    OpenF1 enforces: 3 requests/second and 30 requests/minute.
    The client does not enforce delays automatically — callers are responsible.
    OpenF1SessionLoader.fetch() applies a 0.4 s inter-request delay (2.5 req/s),
    which stays within both limits for a single session fetch (6 requests).

Authentication:
    None required. The API is fully open.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.openf1.org/v1"


class OpenF1Error(Exception):
    """Raised when an OpenF1 API request fails."""


class OpenF1Client:
    """
    HTTP client for the OpenF1 REST API.

    Args:
        base_url:   Override the API base URL (useful for testing).
        timeout_s:  Per-request timeout in seconds.
    """

    def __init__(
        self,
        base_url: str = _BASE_URL,
        timeout_s: float = 30.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout_s = timeout_s

    # ------------------------------------------------------------------
    # Low-level
    # ------------------------------------------------------------------

    def _get(self, endpoint: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """
        GET /{endpoint}?{params} and return the parsed JSON array.

        Raises:
            OpenF1Error: on any HTTP error, network failure, or invalid JSON.
        """
        url = f"{self._base_url}/{endpoint}"
        if params:
            # Filter out None values so callers can pass optional params freely.
            clean = {k: v for k, v in params.items() if v is not None}
            if clean:
                url += "?" + urllib.parse.urlencode(clean)

        logger.debug("openf1: GET %s", url)
        try:
            with urllib.request.urlopen(url, timeout=self._timeout_s) as resp:
                body = resp.read()
                data = json.loads(body)
        except urllib.error.HTTPError as exc:
            raise OpenF1Error(f"HTTP {exc.code} from /{endpoint}") from exc
        except urllib.error.URLError as exc:
            raise OpenF1Error(f"Network error from /{endpoint}: {exc.reason}") from exc
        except json.JSONDecodeError as exc:
            raise OpenF1Error(f"Invalid JSON from /{endpoint}") from exc

        if not isinstance(data, list):
            raise OpenF1Error(
                f"Expected list from /{endpoint}, got {type(data).__name__}"
            )
        logger.debug("openf1: /%s returned %d records", endpoint, len(data))
        return data  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Session discovery
    # ------------------------------------------------------------------

    def list_sessions(
        self,
        *,
        year: int | None = None,
        session_type: str | None = None,
        country_name: str | None = None,
        location: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Return sessions matching the given filters.

        All parameters are optional; omitting all returns the full session list
        (potentially large — use at least one filter in production).

        Session types: "Practice 1", "Practice 2", "Practice 3",
                       "Sprint Qualifying", "Sprint", "Qualifying", "Race".
        """
        return self._get("sessions", {
            "year": year,
            "session_type": session_type,
            "country_name": country_name,
            "location": location,
        })

    def find_session(
        self,
        year: int,
        session_type: str,
        *,
        country_name: str | None = None,
        location: str | None = None,
    ) -> dict[str, Any] | None:
        """
        Find a single session by year + session type, with optional location filter.

        Returns the first matching session, or None if not found.

        Example::

            client.find_session(2024, "Race", country_name="Bahrain")
        """
        results = self.list_sessions(
            year=year,
            session_type=session_type,
            country_name=country_name,
            location=location,
        )
        return results[0] if results else None

    def get_session(self, session_key: int) -> dict[str, Any] | None:
        """Return the session record for the given key, or None if not found."""
        results = self._get("sessions", {"session_key": session_key})
        return results[0] if results else None

    # ------------------------------------------------------------------
    # Per-session data
    # ------------------------------------------------------------------

    def get_drivers(self, session_key: int) -> list[dict[str, Any]]:
        """Return all driver records for the session."""
        return self._get("drivers", {"session_key": session_key})

    def get_laps(self, session_key: int) -> list[dict[str, Any]]:
        """
        Return all lap records for the session.

        Each record covers one driver's one lap. A full race produces
        roughly (num_drivers × num_laps) records (~1 000–1 400 for a race).
        """
        return self._get("laps", {"session_key": session_key})

    def get_position(self, session_key: int) -> list[dict[str, Any]]:
        """
        Return race-order position records for the session.

        Positions are sampled at roughly 3-second intervals. A 90-minute race
        produces tens of thousands of records. The adapter uses only the final
        position per driver; the full list is not stored in state.
        """
        return self._get("position", {"session_key": session_key})

    def get_pit(self, session_key: int) -> list[dict[str, Any]]:
        """Return pit stop records for the session."""
        return self._get("pit", {"session_key": session_key})

    def get_race_control(self, session_key: int) -> list[dict[str, Any]]:
        """Return race control message records for the session."""
        return self._get("race_control", {"session_key": session_key})
