"""
FastAPI app + per-vendor ingest endpoint.

The endpoint's job is to ACK fast and not lose data:
  1. Look up vendor in VendorRegistry (404 if unknown).
  2. (Optional) verify HMAC signature.
  3. Per-vendor token-bucket rate limit (429 if exceeded).
  4. Check global backlog (503 if we're overloaded).
  5. Parse body as JSON.
  6. Dedup via seen_keys (idempotency key or body hash).
  7. Insert raw_events with a tight statement timeout (§3.9).
  8. Increment daily counter; enforce daily cap (429 if exceeded).
  9. NOTIFY the worker.
  10. Return 200.

Everything intelligent happens in the worker.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request, Response
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError

from src.config import get_settings
from src.db import init_db, session_scope
from src.dedup import body_hash, check_and_record, compute_dedup_key
from src.event_types import register_default_event_types
from src.llm.base import LLMClient
from src.llm.mock import MockLLM
from src.llm.openai_client import OpenAILLM
from src.models.raw_event import EventStatus, RawEvent
from src.queue import notify_new_event
from src.rate_limit import increment_and_check_daily_cap, token_bucket
from src.vendors import VendorRegistry, register_default_vendors
from src.worker import run_worker

log = logging.getLogger(__name__)


def _build_llm() -> LLMClient:
    settings = get_settings()
    if settings.openai_api_key and not os.environ.get("GLACIS_USE_MOCK_LLM"):
        return OpenAILLM(
            api_key=settings.openai_api_key,
            classifier_model=settings.openai_classifier_model,
            extractor_model=settings.openai_extractor_model,
        )
    log.warning("OPENAI_API_KEY not set or GLACIS_USE_MOCK_LLM=1; using MockLLM")
    return MockLLM()


@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[no-untyped-def]
    settings = get_settings()
    register_default_event_types()
    register_default_vendors(hmac_enabled=settings.hmac_verify_enabled)

    await init_db()

    llm = _build_llm()
    app.state.llm = llm

    worker_task = asyncio.create_task(run_worker(llm))
    app.state.worker_task = worker_task
    log.info("app startup complete")

    try:
        yield
    finally:
        worker_task.cancel()
        try:
            await worker_task
        except (asyncio.CancelledError, Exception):
            pass


app = FastAPI(title="Glacis Webhook Ingestion", lifespan=lifespan)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/vendors")
async def list_vendors() -> dict[str, Any]:
    return {
        "vendors": [
            {
                "vendor_id": v.vendor_id,
                "rate_limit": {"per_sec": v.rate_limit.per_sec, "burst": v.rate_limit.burst},
                "daily_cap": v.daily_cap,
                "hints": v.hints,
            }
            for v in VendorRegistry.all()
        ]
    }


def _headers_to_dict(request: Request) -> dict[str, str]:
    return {k: v for k, v in request.headers.items()}


@app.post("/webhooks/{vendor_id}")
async def ingest_webhook(vendor_id: str, request: Request, response: Response) -> dict[str, Any]:
    settings = get_settings()

    # 1. Vendor lookup
    vendor = VendorRegistry.get(vendor_id)
    if vendor is None:
        raise HTTPException(status_code=404, detail=f"unknown vendor {vendor_id!r}")

    body = await request.body()
    headers = _headers_to_dict(request)

    # 2. Auth (no-op by default; HMAC when configured)
    try:
        vendor.auth.verify(body=body, headers=headers)
    except ValueError as e:
        raise HTTPException(status_code=401, detail=f"auth failed: {e}") from e

    # 3. Per-vendor token bucket (§3.10)
    allowed, retry_after = await token_bucket.try_acquire(vendor_id, vendor.rate_limit)
    if not allowed:
        response.headers["Retry-After"] = f"{retry_after:.3f}"
        raise HTTPException(status_code=429, detail="per-vendor rate limit exceeded")

    # 4. Global backlog shed (§3.7 layer 3)
    async with session_scope() as session:
        backlog = await session.execute(
            select(func.count()).select_from(RawEvent).where(RawEvent.status == EventStatus.PENDING)
        )
        backlog_count = int(backlog.scalar_one())
    if backlog_count >= settings.ingest_backlog_threshold:
        response.headers["Retry-After"] = "5"
        raise HTTPException(
            status_code=503,
            detail=f"ingest backlog {backlog_count} exceeds threshold; retry later",
        )

    # 5. Parse
    try:
        body_json = json.loads(body) if body else {}
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"invalid JSON body: {e}") from e
    if not isinstance(body_json, dict):
        raise HTTPException(status_code=400, detail="top-level JSON must be an object")

    # 6–9: inside a short-timeout transaction
    try:
        async with session_scope() as session:
            # Enforce statement timeout for this connection (§3.9)
            await session.execute(
                text(f"SET LOCAL statement_timeout = {settings.db_ingest_statement_timeout_ms}")
            )

            # 6. Dedup
            dedup_key = compute_dedup_key(
                vendor_id=vendor_id,
                body=body,
                headers=headers,
                header_names=vendor.idempotency_headers,
            )
            result = await check_and_record(session, dedup_key)
            if result.is_duplicate:
                return {"status": "duplicate", "dedup_key": dedup_key}

            # 7. Persist raw_event (always record the body hash for audit even
            # when dedup used a vendor-supplied idempotency key).
            raw_event = RawEvent(
                vendor_id=vendor_id,
                headers={k: v for k, v in headers.items() if len(v) < 1024},
                body_json=body_json,
                body_hash=body_hash(body),
                idempotency_key=dedup_key,
                status=EventStatus.PENDING,
            )
            session.add(raw_event)
            await session.flush()  # populate raw_event.id

            # 8. Daily cap
            cap_ok, current = await increment_and_check_daily_cap(
                session, vendor_id, vendor.daily_cap
            )
            if not cap_ok:
                response.headers["Retry-After"] = "3600"
                raise HTTPException(
                    status_code=429,
                    detail=(
                        f"daily cap {vendor.daily_cap} exceeded for {vendor_id}; "
                        f"count={current}"
                    ),
                )

            # 9. NOTIFY worker
            await notify_new_event(session, raw_event.id)

            return {
                "status": "accepted",
                "raw_event_id": raw_event.id,
                "dedup_key": dedup_key,
                "daily_count": current,
            }

    except HTTPException:
        raise
    except DBAPIError as e:
        # Statement timeout / connection error / etc. — shed to 503.
        log.error("DB error on ingest vendor=%s: %s", vendor_id, e)
        response.headers["Retry-After"] = "1"
        raise HTTPException(status_code=503, detail="ingest DB unavailable") from e
