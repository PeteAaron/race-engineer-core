"""
Append-only event store.

Persists normalized TimingEvents to a JSONL file (one event per line).
Events are written and read in order, making the file a reliable audit log
suitable for replay.

v1 limitations (documented for follow-on work):
- File handle opened and closed per call — adequate for ~5-10 msg/s live feeds.
  A context-manager buffered mode should be added before production load.
- No compaction or rotation — files grow unbounded within a session.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from .events import EventType, TimingEvent

logger = logging.getLogger(__name__)


class EventStore:
    def __init__(self, path: Path) -> None:
        self._path = path

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def append(self, event: TimingEvent) -> None:
        """Serialize and append one event to the store."""
        line = self._serialize(event)
        with self._path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
        logger.debug(
            "store write: event_id=%s topic=%s event_type=%s",
            event.event_id,
            event.raw_topic,
            event.event_type.value,
        )

    # ------------------------------------------------------------------
    # Read / replay
    # ------------------------------------------------------------------

    def iter_events(self) -> Iterator[TimingEvent]:
        """
        Yield stored events in insertion order.

        Corrupt or unparseable lines are skipped with a warning log.
        The iterator never raises — callers can rely on receiving all
        well-formed events regardless of file corruption.
        """
        if not self._path.exists():
            return

        logger.info("store replay: path=%s", self._path)
        count = 0
        skipped = 0

        with self._path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    event = self._deserialize(line)
                    count += 1
                    yield event
                except (json.JSONDecodeError, KeyError, ValueError) as exc:
                    skipped += 1
                    logger.warning("store replay: skipping corrupt line err=%s", exc)

        logger.info("store replay: complete events=%d skipped=%d", count, skipped)

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    @staticmethod
    def _serialize(event: TimingEvent) -> str:
        d: dict[str, Any] = {
            "event_id": event.event_id,
            "timestamp": event.timestamp.isoformat(),  # includes UTC offset
            "event_type": event.event_type.value,
            "driver": event.driver,
            "payload": event.payload,
            "raw_topic": event.raw_topic,
        }
        return json.dumps(d, ensure_ascii=False)

    @staticmethod
    def _deserialize(line: str) -> TimingEvent:
        from datetime import datetime

        d = json.loads(line)
        return TimingEvent(
            event_id=d["event_id"],
            timestamp=datetime.fromisoformat(d["timestamp"]),
            event_type=EventType(d["event_type"]),
            driver=d["driver"],
            payload=d["payload"],
            raw_topic=d["raw_topic"],
        )
