from __future__ import annotations

import enum
from datetime import datetime

from dateutil import parser as date_parser
from pydantic import BaseModel, ConfigDict, Field, field_validator


class ShipmentStatus(str, enum.Enum):
    TRANSIT = "TRANSIT"
    DELIVERED = "DELIVERED"
    EXCEPTION = "EXCEPTION"


_STATUS_ALIASES: dict[str, ShipmentStatus] = {
    "transit": ShipmentStatus.TRANSIT,
    "in_transit": ShipmentStatus.TRANSIT,
    "in-transit": ShipmentStatus.TRANSIT,
    "shipped": ShipmentStatus.TRANSIT,
    "picked_up": ShipmentStatus.TRANSIT,
    "out_for_delivery": ShipmentStatus.TRANSIT,
    "delivered": ShipmentStatus.DELIVERED,
    "complete": ShipmentStatus.DELIVERED,
    "completed": ShipmentStatus.DELIVERED,
    "exception": ShipmentStatus.EXCEPTION,
    "failed": ShipmentStatus.EXCEPTION,
    "returned": ShipmentStatus.EXCEPTION,
    "lost": ShipmentStatus.EXCEPTION,
    "damaged": ShipmentStatus.EXCEPTION,
}


class Shipment(BaseModel):
    """Normalized shipment update. This is the extraction target schema."""

    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    vendor_id: str = Field(..., min_length=1, max_length=100)
    tracking_number: str = Field(..., min_length=1, max_length=200)
    status: ShipmentStatus
    timestamp: datetime = Field(
        ..., description="Event time, parsed to UTC ISO-8601 datetime."
    )

    @field_validator("status", mode="before")
    @classmethod
    def _coerce_status(cls, v: object) -> object:
        if isinstance(v, ShipmentStatus):
            return v
        if isinstance(v, str):
            key = v.strip().lower().replace(" ", "_")
            if key in _STATUS_ALIASES:
                return _STATUS_ALIASES[key]
            upper = v.strip().upper()
            if upper in ShipmentStatus.__members__:
                return ShipmentStatus[upper]
        return v

    @field_validator("timestamp", mode="before")
    @classmethod
    def _parse_timestamp(cls, v: object) -> object:
        if isinstance(v, datetime):
            return v
        if isinstance(v, str):
            return date_parser.parse(v)
        return v
