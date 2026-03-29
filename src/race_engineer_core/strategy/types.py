"""
Strategy inference types.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from race_engineer_core.models.answers import AnswerClassification, EvidenceItem


class StrategyInferenceType(str, Enum):
    UNDERCUT_THREAT = "undercut_threat"
    OVERCUT_OPPORTUNITY = "overcut_opportunity"
    DRS_TRAIN = "drs_train"
    BENEFIT_FROM_SC = "benefit_from_sc"
    STRATEGY_STATE = "strategy_state"


@dataclass
class StrategyInference:
    inference_type: StrategyInferenceType
    driver_ref: str
    classification: AnswerClassification
    summary: str
    confidence: float
    evidence: list[EvidenceItem] = field(default_factory=list)
    rival_ref: str | None = None
    details: dict = field(default_factory=dict)
