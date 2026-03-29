"""
Core domain entities for F1 session data and race state.

These are skeletal placeholders. Full field definitions will be added
as the adapter and resolver layers are implemented.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from .base import SessionStatus, TyreCompound


@dataclass
class Driver:
    driver_ref: str
    driver_number: int
    full_name: str
    team_ref: str


@dataclass
class Team:
    team_ref: str
    name: str


@dataclass
class Circuit:
    circuit_key: str
    name: str
    country: str


@dataclass
class Meeting:
    meeting_key: int
    circuit: Circuit
    name: str
    date: datetime
    round_number: int
    season: int


@dataclass
class Session:
    session_key: int
    meeting_key: int
    session_type: str
    status: SessionStatus
    start_time: datetime | None = None
    end_time: datetime | None = None


@dataclass
class Lap:
    driver_ref: str
    session_key: int
    lap_number: int
    lap_time_ms: int | None = None
    sector_1_ms: int | None = None
    sector_2_ms: int | None = None
    sector_3_ms: int | None = None


@dataclass
class Stint:
    driver_ref: str
    session_key: int
    stint_number: int
    compound: TyreCompound
    lap_start: int
    lap_end: int | None = None
    tyre_age_at_start: int = 0


@dataclass
class PitStop:
    driver_ref: str
    session_key: int
    lap_number: int
    pit_duration_ms: int | None = None
    new_compound: TyreCompound | None = None


@dataclass
class PositionSnapshot:
    driver_ref: str
    session_key: int
    lap_number: int
    position: int


@dataclass
class IntervalSnapshot:
    driver_ref: str
    session_key: int
    lap_number: int
    gap_to_leader_ms: int | None = None
    interval_ms: int | None = None
