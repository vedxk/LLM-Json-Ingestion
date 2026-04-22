"""
Integration tests for the ingest endpoint's idempotency + rate-limiting
behavior. Hits the real FastAPI app + real Postgres.

Run:
    docker compose up -d db
    pytest --integration -q tests/test_integration_ingest.py

Stop any running uvicorn first — its worker will pick up the test events and
fire LLM calls on junk payloads.
"""

from __future__ import annotations

import asyncio
import json
from collections import Counter

import httpx
import pytest
from sqlalchemy import func, select

from src.db import session_scope
from src.models.raw_event import RawEvent
from src.rate_limit import RateLimit, token_bucket
from src.vendors import NoAuth, VendorConfig, VendorRegistry

pytestmark = pytest.mark.integration


SHIPMENT_PAYLOAD = {
    "TRACKING_NUMBER": "794613512345",
    "EVENT_TYPE": "DELIVERED",
    "EVENT_TIMESTAMP": "2026-04-21T14:32:11-07:00",
}


def _post(
    client: httpx.AsyncClient, vendor: str, key: str | None = None, body: dict | None = None
):
    headers: dict[str, str] = {"Content-Type": "application/json"}
    if key is not None:
        headers["Idempotency-Key"] = key
    return client.post(
        f"/webhooks/{vendor}",
        headers=headers,
        content=json.dumps(body or SHIPMENT_PAYLOAD),
    )


def _reset_vendor(vendor_id: str, per_sec: float, burst: int, daily_cap: int | None = 1_000_000) -> None:
    """Register (or replace) a vendor with explicit limits for a test."""
    VendorRegistry._vendors.pop(vendor_id, None)
    token_bucket._buckets.pop(vendor_id, None)
    VendorRegistry.register(
        VendorConfig(
            vendor_id=vendor_id,
            auth=NoAuth(),
            rate_limit=RateLimit(per_sec=per_sec, burst=burst),
            daily_cap=daily_cap,
        )
    )


async def _raw_event_count(vendor_id: str) -> int:
    async with session_scope() as session:
        result = await session.execute(
            select(func.count()).select_from(RawEvent).where(RawEvent.vendor_id == vendor_id)
        )
        return int(result.scalar_one())


# --------------------------------------------------------------------------- #
# Idempotency — same payload fired N times
# --------------------------------------------------------------------------- #


async def test_same_idempotency_key_deduped_many_times(integration_client: httpx.AsyncClient) -> None:
    """N identical POSTs with the same Idempotency-Key: 1 accepted, N-1 duplicate.
    Only one raw_events row is persisted."""
    _reset_vendor("fedex", per_sec=1000.0, burst=1000)
    N = 10
    key = "evt-abc-123"

    responses = [await _post(integration_client, "fedex", key=key) for _ in range(N)]

    assert all(r.status_code == 200 for r in responses), [r.status_code for r in responses]
    statuses = Counter(r.json()["status"] for r in responses)
    assert statuses["accepted"] == 1
    assert statuses["duplicate"] == N - 1
    assert await _raw_event_count("fedex") == 1


async def test_same_body_no_key_deduped_via_body_hash(integration_client: httpx.AsyncClient) -> None:
    """No Idempotency-Key header → dedup falls back to SHA-256 of the body."""
    _reset_vendor("fedex", per_sec=1000.0, burst=1000)
    N = 5

    responses = [await _post(integration_client, "fedex") for _ in range(N)]

    assert all(r.status_code == 200 for r in responses)
    statuses = Counter(r.json()["status"] for r in responses)
    assert statuses["accepted"] == 1
    assert statuses["duplicate"] == N - 1


async def test_distinct_keys_same_body_all_accepted(integration_client: httpx.AsyncClient) -> None:
    """Different Idempotency-Keys with identical bodies: header wins over body
    hash, so every request is treated as a fresh event."""
    _reset_vendor("fedex", per_sec=1000.0, burst=1000)
    N = 8

    for i in range(N):
        r = await _post(integration_client, "fedex", key=f"key-{i}")
        assert r.status_code == 200, r.text
        assert r.json()["status"] == "accepted"

    assert await _raw_event_count("fedex") == N


async def test_dedup_is_vendor_scoped(integration_client: httpx.AsyncClient) -> None:
    """The same Idempotency-Key to different vendors is two distinct events."""
    _reset_vendor("fedex", per_sec=1000.0, burst=1000)
    _reset_vendor("maersk", per_sec=1000.0, burst=1000)
    key = "shared-key"

    r1 = await _post(integration_client, "fedex", key=key)
    r2 = await _post(integration_client, "maersk", key=key)

    assert r1.json()["status"] == "accepted"
    assert r2.json()["status"] == "accepted"
    assert r1.json()["raw_event_id"] != r2.json()["raw_event_id"]


# --------------------------------------------------------------------------- #
# Per-vendor rate limit — spikes
# --------------------------------------------------------------------------- #


async def test_sequential_spike_fills_burst_then_429s(integration_client: httpx.AsyncClient) -> None:
    """Fire `burst + overflow` requests fast-sequentially. First `burst` must be
    accepted; at least one subsequent request must be 429."""
    _reset_vendor("seq_spike", per_sec=5.0, burst=10)

    results = [(await _post(integration_client, "seq_spike", key=f"k-{i}")).status_code for i in range(15)]

    # First 10 always succeed (no refills happen in a few-ms window).
    assert results[:10] == [200] * 10
    # The tail has at least one 429. (A slow CI box might let a token refill at
    # 5/s = every 200ms, so we don't assert the exact count.)
    assert 429 in results[10:]


async def test_concurrent_spike_bounded_by_burst(integration_client: httpx.AsyncClient) -> None:
    """Fire a 50-request async spike. With a near-zero refill rate, exactly
    `burst` return 200 and the rest 429 — even though they all hit the endpoint
    in the same event loop tick."""
    _reset_vendor("concurrent_spike", per_sec=0.01, burst=5)  # refill: 1 every 100s

    results = await asyncio.gather(
        *[_post(integration_client, "concurrent_spike", key=f"c-{i}") for i in range(50)]
    )
    codes = [r.status_code for r in results]

    assert codes.count(200) == 5
    assert codes.count(429) == 45
    # Side effect: only the 5 accepted requests wrote a raw_event.
    assert await _raw_event_count("concurrent_spike") == 5


async def test_bucket_refills_and_accepts_again(integration_client: httpx.AsyncClient) -> None:
    """After exhausting the burst, waiting long enough for a token refill lets
    the next request through."""
    _reset_vendor("refill", per_sec=10.0, burst=3)  # refill: 1 every 100ms

    for i in range(3):
        r = await _post(integration_client, "refill", key=f"b-{i}")
        assert r.status_code == 200

    # Immediate follow-up is throttled.
    r = await _post(integration_client, "refill", key="immediate")
    assert r.status_code == 429

    # Wait for 2+ tokens to refill.
    await asyncio.sleep(0.25)
    r = await _post(integration_client, "refill", key="after-refill")
    assert r.status_code == 200


async def test_rate_limits_are_isolated_per_vendor(integration_client: httpx.AsyncClient) -> None:
    """Exhausting vendor A's bucket must not affect vendor B."""
    _reset_vendor("vendor_a", per_sec=0.01, burst=2)
    _reset_vendor("vendor_b", per_sec=0.01, burst=2)

    # Burn vendor_a's bucket.
    for i in range(3):
        await _post(integration_client, "vendor_a", key=f"a-{i}")

    r_a = await _post(integration_client, "vendor_a", key="a-extra")
    r_b = await _post(integration_client, "vendor_b", key="b-1")

    assert r_a.status_code == 429
    assert r_b.status_code == 200


async def test_duplicate_request_still_consumes_a_token(integration_client: httpx.AsyncClient) -> None:
    """Rate-limit check (step 3) runs BEFORE dedup (step 6). So firing the same
    Idempotency-Key N times drains the bucket just like distinct events would.

    This is the edge case behind 'a chatty vendor retrying a 200 response'."""
    _reset_vendor("dup_drain", per_sec=0.01, burst=3)
    key = "same-key"

    codes = [(await _post(integration_client, "dup_drain", key=key)).status_code for _ in range(6)]

    assert codes == [200, 200, 200, 429, 429, 429]
    # Only the very first produced a raw_event; the next two were duplicates
    # (200 + status="duplicate"); the last three were rejected at the bucket.
    assert await _raw_event_count("dup_drain") == 1


# --------------------------------------------------------------------------- #
# Daily cap — DB-backed tier
# --------------------------------------------------------------------------- #


async def test_daily_cap_rejects_once_exceeded(integration_client: httpx.AsyncClient) -> None:
    """With daily_cap=3 and an unthrottled bucket: first 3 accepted, rest 429.
    Rejected requests do NOT persist a raw_event or bump the counter."""
    _reset_vendor("capped", per_sec=1000.0, burst=1000, daily_cap=3)

    codes = [(await _post(integration_client, "capped", key=f"cap-{i}")).status_code for i in range(5)]

    assert codes == [200, 200, 200, 429, 429]
    assert await _raw_event_count("capped") == 3


async def test_daily_cap_counter_stays_frozen_after_cap_hit(integration_client: httpx.AsyncClient) -> None:
    """Once the cap is hit, repeated rejected requests don't creep the counter
    forward (transaction rollback undoes the increment)."""
    from src.models.vendor_counter import VendorCounter

    _reset_vendor("frozen", per_sec=1000.0, burst=1000, daily_cap=2)

    # Fill the cap.
    for i in range(2):
        r = await _post(integration_client, "frozen", key=f"f-{i}")
        assert r.status_code == 200

    # Reject a handful more — should stay rejected, counter pinned at 2.
    for i in range(5):
        r = await _post(integration_client, "frozen", key=f"f-over-{i}")
        assert r.status_code == 429

    async with session_scope() as session:
        count = (
            await session.execute(
                select(VendorCounter.event_count).where(VendorCounter.vendor_id == "frozen")
            )
        ).scalar_one()
    assert count == 2
