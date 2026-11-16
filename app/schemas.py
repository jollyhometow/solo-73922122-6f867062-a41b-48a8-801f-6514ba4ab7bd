"""Request/response models. All datetimes must carry a timezone (UTC)."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from pydantic import AwareDatetime, BaseModel, Field, field_validator


def _utc(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc)


class RegisterRequest(BaseModel):
    meter_no: str = Field(min_length=1, max_length=64)
    valid_from: AwareDatetime
    valid_to: AwareDatetime
    line: str = Field(min_length=1, max_length=64)

    @field_validator("valid_from", "valid_to")
    @classmethod
    def _to_utc(cls, v: datetime) -> datetime:
        return _utc(v)


class ReadingItem(BaseModel):
    reading_uid: str = Field(min_length=1, max_length=128)
    meter_no: str = Field(min_length=1, max_length=64)
    ts: AwareDatetime
    wh: int  # integer watt-hours, negative values allowed (reversal readings)

    @field_validator("ts")
    @classmethod
    def _to_utc(cls, v: datetime) -> datetime:
        return _utc(v)


class IngestRequest(BaseModel):
    readings: list[ReadingItem] = Field(min_length=1, max_length=1000)


class CorrectRequest(BaseModel):
    meter_no: str = Field(min_length=1, max_length=64)
    valid_from: AwareDatetime
    valid_to: AwareDatetime
    new_line: str = Field(min_length=1, max_length=64)
    # Attribution-timeline version the client observed; optimistic lock.
    base_version: int = Field(ge=1)

    @field_validator("valid_from", "valid_to")
    @classmethod
    def _to_utc(cls, v: datetime) -> datetime:
        return _utc(v)


class OwnershipOut(BaseModel):
    id: int
    meter_no: str
    valid_from: datetime
    valid_to: datetime
    line: str
    version: int


class MeterOut(BaseModel):
    meter_no: str
    version: int
    updated_at: datetime


class ReadingOut(BaseModel):
    reading_uid: str
    meter_no: str
    ts: datetime
    wh: int
    ownership_id: int
    line: str
    valid_from: datetime
    valid_to: datetime


class VersionedResponse(BaseModel):
    meter_no: str
    version: int


class IngestResponse(BaseModel):
    ingested: int
    readings: list[ReadingOut]


class AuditMeterSummary(BaseModel):
    meter_no: str
    expected: int = Field(description="scheduled instants inside this meter's "
                                      "segments on the audited line")
    present: int = Field(description="readings currently attributed to the "
                                     "line inside the window")
    missing: int = Field(description="scheduled instants with no reading")


class AuditMissingInterval(BaseModel):
    meter_no: str
    interval_from: datetime = Field(
        description="first missing scheduled instant of the run")
    interval_to: datetime = Field(
        description="right-open end: last missing instant + interval_seconds")
    missing_count: int


class AuditDuplicate(BaseModel):
    meter_no: str
    ts: datetime
    count: int = Field(description="readings at this instant (>1)")


class CoverageAuditResponse(BaseModel):
    line: str
    window_start: datetime
    window_end: datetime
    interval_seconds: int
    expected_total: int
    missing_total: int
    meters: list[AuditMeterSummary]
    missing_intervals: list[AuditMissingInterval]
    duplicates: list[AuditDuplicate]
