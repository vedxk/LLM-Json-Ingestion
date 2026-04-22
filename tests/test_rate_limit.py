"""Token bucket (tier 1 of §3.10). Pure in-memory; no DB needed."""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from src.rate_limit import RateLimit, TokenBucketLimiter


@pytest.mark.asyncio
async def test_burst_allowed_then_throttled() -> None:
    bucket = TokenBucketLimiter()
    cfg = RateLimit(per_sec=1.0, burst=3)

    # Freeze time so we can exhaust the burst deterministically.
    with patch("src.rate_limit.time.monotonic", return_value=0.0):
        for _ in range(3):
            allowed, _ = await bucket.try_acquire("v1", cfg)
            assert allowed is True
        allowed, retry_after = await bucket.try_acquire("v1", cfg)
        assert allowed is False
        assert retry_after > 0


@pytest.mark.asyncio
async def test_refill_over_time() -> None:
    bucket = TokenBucketLimiter()
    cfg = RateLimit(per_sec=10.0, burst=2)

    with patch("src.rate_limit.time.monotonic") as mono:
        mono.return_value = 0.0
        await bucket.try_acquire("v", cfg)  # 1 left
        await bucket.try_acquire("v", cfg)  # 0 left
        mono.return_value = 0.5  # 10 tokens/sec * 0.5s = 5 tokens → capped at burst=2
        allowed, _ = await bucket.try_acquire("v", cfg)
        assert allowed is True


@pytest.mark.asyncio
async def test_buckets_are_per_key() -> None:
    bucket = TokenBucketLimiter()
    cfg = RateLimit(per_sec=1.0, burst=1)

    with patch("src.rate_limit.time.monotonic", return_value=0.0):
        allowed_a, _ = await bucket.try_acquire("a", cfg)
        allowed_b, _ = await bucket.try_acquire("b", cfg)
        assert allowed_a and allowed_b  # separate buckets


@pytest.mark.asyncio
async def test_concurrent_access_is_safe() -> None:
    """20 coroutines racing for a bucket with burst=5 should admit exactly 5."""
    bucket = TokenBucketLimiter()
    cfg = RateLimit(per_sec=0.01, burst=5)

    with patch("src.rate_limit.time.monotonic", return_value=0.0):
        results = await asyncio.gather(
            *(bucket.try_acquire("x", cfg) for _ in range(20))
        )
    admitted = sum(1 for allowed, _ in results if allowed)
    assert admitted == 5
