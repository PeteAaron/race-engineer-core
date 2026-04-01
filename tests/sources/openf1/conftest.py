"""
Suppress inter-request delays for all OpenF1 unit tests.

OpenF1SessionLoader.fetch() sleeps between API calls to avoid rate-limiting
the live API. In tests the client is mocked, so the delay serves no purpose
and would make the suite ~15× slower.
"""

from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(
        "race_engineer_core.sources.openf1.session.time.sleep",
        lambda _: None,
    )
