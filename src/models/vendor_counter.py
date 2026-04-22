from __future__ import annotations

from datetime import date

from sqlalchemy import Date, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from src.db import Base


class VendorCounter(Base):
    """Per-vendor per-day event counter for daily quota enforcement."""

    __tablename__ = "vendor_counters"

    vendor_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    day: Mapped[date] = mapped_column(Date, primary_key=True)
    event_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
