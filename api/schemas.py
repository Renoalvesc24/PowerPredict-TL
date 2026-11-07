"""
schemas.py - Pydantic models of the ingest API.

Value fields are Optional on purpose: the bot sends ~0.5 % broken readings
(missing / impossible values) and validation of the VALUES is the job of Spark
Streaming (veracity), not of the gateway. The gateway checks the STRUCTURE.
"""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class Reading(BaseModel):
    model_config = ConfigDict(extra="ignore")

    reading_id: str = Field(..., max_length=40)
    meter_id: str = Field(..., max_length=20, examples=["TL-DIL-000412"])
    timestamp: str = Field(..., examples=["2026-11-07 20:00:00"])
    municipality: str = Field(..., max_length=40)
    customer_type: str = Field(..., max_length=20)
    contracted_kva: float
    phase: str = "single"
    voltage_v: Optional[float] = None
    current_a: Optional[float] = None
    power_kw: Optional[float] = None
    energy_kwh: Optional[float] = None
    power_factor: Optional[float] = None
    frequency_hz: Optional[float] = None
    temperature_c: Optional[float] = None
    true_type: Optional[str] = Field(None, description="generator ground truth - evaluation only, never a model input")
    producer_ts: Optional[int] = Field(None, description="epoch ms when the bot created the reading")


class ReadingBatch(BaseModel):
    bot_name: str = "bot"
    scenario: Optional[str] = None
    readings: list[Reading] = Field(..., min_length=1, max_length=20000)


class DetectRequest(BaseModel):
    """One reading to classify on demand (manual check). Missing meter fields are
    taken from tbl_meters when meter_id is given; current is derived from P, V, PF."""
    meter_id: Optional[str] = None
    customer_type: Optional[str] = None
    municipality: Optional[str] = None
    contracted_kva: Optional[float] = None
    voltage_v: float = Field(..., examples=[188.0])
    power_kw: float = Field(..., examples=[0.35])
    power_factor: float = Field(0.94, examples=[0.94])
    frequency_hz: float = Field(50.0, examples=[50.0])
    temperature_c: float = Field(28.0, examples=[28.0])
    current_a: Optional[float] = None
    timestamp: Optional[str] = None
