"""
Base types shared across domain models.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class DocumentType(str, Enum):
    SPORTING_REGULATIONS = "SPORTING_REGULATIONS"
    TECHNICAL_REGULATIONS = "TECHNICAL_REGULATIONS"
    FINANCIAL_REGULATIONS = "FINANCIAL_REGULATIONS"
    RACE_DIRECTOR_NOTE = "RACE_DIRECTOR_NOTE"
    STEWARDS_DECISION = "STEWARDS_DECISION"
    OTHER = "OTHER"


class SessionStatus(str, Enum):
    SCHEDULED = "SCHEDULED"
    STARTED = "STARTED"
    FINISHED = "FINISHED"
    ABORTED = "ABORTED"


class TyreCompound(str, Enum):
    SOFT = "SOFT"
    MEDIUM = "MEDIUM"
    HARD = "HARD"
    INTERMEDIATE = "INTERMEDIATE"
    WET = "WET"
    UNKNOWN = "UNKNOWN"


@dataclass
class Freshness:
    data_timestamp: datetime
    lag_seconds: float


@dataclass
class EvidenceItem:
    type: str
    source: str
    data: dict[str, Any] = field(default_factory=dict)
