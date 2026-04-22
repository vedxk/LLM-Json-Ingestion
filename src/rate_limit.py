"""
Per-vendor rate limiting. Two tiers (see README §3.10):

  1. In-memory token bucket — short-burst protection, keyed by vendor_id.
     Resets on process restart. Redis-backed sliding window is a prod gap.

  2. DB-backed daily cap — counts events per vendor per UTC day in
     `vendor_counters`. Protects LLM cost budget against a single vendor 100×'ing
     their volume.

Both return 429 Too Many Requests when exceeded (distinct from 503 which means
we are globally overloaded — see §3.7).
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.vendor_counter import VendorCounter


@dataclass
class RateLimit:
    """Token bucket parameters. `per_sec` refills; `burst` is max capacity."""

    per_sec: float
    burst: int


# --------------------------------------------------------------------------- #
# Token bucket (tier 1)
# --------------------------------------------------------------------------- #


@dataclass
class _BucketState:
    tokens: float
    last_refill: float  # monotonic seconds
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class TokenBucketLimiter:
    """Async-safe per-key token bucket. One instance per app process."""

    def __init__(self) -> None:
        self._buckets: dict[str, _BucketState] = {}
        self._global_lock = asyncio.Lock()

    async def _get_bucket(self, key: str, cfg: RateLimit) -> _BucketState:
        bucket = self._buckets.get(key)
        if bucket is not None:
            return bucket
        async with self._global_lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = _BucketState(tokens=float(cfg.burst), last_refill=time.monotonic())
                self._buckets[key] = bucket
        return bucket

    async def try_acquire(self, key: str, cfg: RateLimit) -> tuple[bool, float]:
        """
        Try to consume one token.

        Returns (allowed, retry_after_seconds). If allowed is False, the caller
        should respond 429 with Retry-After: retry_after_seconds.
        """
        bucket = await self._get_bucket(key, cfg)
        async with bucket.lock:
            now = time.monotonic()
            elapsed = now - bucket.last_refill
            bucket.tokens = min(float(cfg.burst), bucket.tokens + elapsed * cfg.per_sec)
            bucket.last_refill = now
            if bucket.tokens >= 1.0:
                bucket.tokens -= 1.0
                return True, 0.0
            missing = 1.0 - bucket.tokens
            retry_after = max(0.001, missing / cfg.per_sec)
            return False, retry_after


# Singleton used by the ingest endpoint. Easy to swap in tests.
token_bucket = TokenBucketLimiter()


# --------------------------------------------------------------------------- #
# Daily cap (tier 2) — DB-backed
# --------------------------------------------------------------------------- #


async def increment_and_check_daily_cap(
    session: AsyncSession,
    vendor_id: str,
    daily_cap: int | None,
) -> tuple[bool, int]:
    """
    Atomically increment the vendor's counter for today and check against the cap.

    Returns (allowed, current_count). If allowed is False, the caller should
    reject with 429.

    If daily_cap is None, no limit is enforced but the counter is still updated
    (useful for telemetry).
    """
    today = datetime.now(timezone.utc).date()

    # UPSERT: insert with event_count=1 if row doesn't exist, else increment.
    stmt = (
        pg_insert(VendorCounter)
        .values(vendor_id=vendor_id, day=today, event_count=1)
        .on_conflict_do_update(
            index_elements=[VendorCounter.vendor_id, VendorCounter.day],
            set_={"event_count": VendorCounter.event_count + 1},
        )
        .returning(VendorCounter.event_count)
    )
    result = await session.execute(stmt)
    current = result.scalar_one()

    if daily_cap is not None and current > daily_cap:
        return False, current
    return True, current


async def get_daily_count(session: AsyncSession, vendor_id: str) -> int:
    today = datetime.now(timezone.utc).date()
    result = await session.execute(
        select(VendorCounter.event_count).where(
            VendorCounter.vendor_id == vendor_id,
            VendorCounter.day == today,
        )
    )
    row = result.scalar_one_or_none()
    return int(row or 0)
