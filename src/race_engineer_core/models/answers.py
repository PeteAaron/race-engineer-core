"""
Answer types: the structured response contract for race-state queries.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .base import EvidenceItem, Freshness


class AnswerClassification(str, Enum):
    FACT = "Fact"
    INFERENCE = "Inference"
    RULE_BACKED = "Rule-backed"
    OFFICIAL_DOCUMENT_SUMMARY = "Official document summary"
    UNKNOWN = "Unknown/unavailable"


@dataclass
class ResolverRequest:
    intent: str
    driver_ref: str | None = None
    session_key: str | int | None = None
    comparison_target: str | None = None
    context: dict[str, Any] = field(default_factory=dict)


@dataclass
class ResolverResponse:
    classification: AnswerClassification
    answer_text: str
    confidence: float
    evidence: list[EvidenceItem] = field(default_factory=list)
    result: dict[str, Any] | None = None
    freshness: Freshness | None = None
    source_trace: list[str] = field(default_factory=list)
