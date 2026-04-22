"""
Worker loop (README §3.1, §3.5, §3.7).

For each pending raw_event:
  1. Mark PROCESSING.
  2. Classify via LLM → label.
  3. If label is known: extract via LLM with retry-on-validation-error.
  4. Persist normalized row.
  5. Mark DONE.
  Any persistent failure → DLQ row + mark DEAD.

Concurrency is bounded by an asyncio.Semaphore (default 8).
Global LLM rate is bounded by a simple leaky-bucket limiter.
Wake-up is NOTIFY-driven; a periodic sweep catches missed notifications.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from pydantic import ValidationError
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import get_settings
from src.db import session_scope
from src.event_types import register_default_event_types
from src.llm.base import LLMClient, LLMError
from src.models.dead_letter import DeadLetter
from src.models.raw_event import EventStatus, RawEvent
from src.queue import pg_listener
from src.registry import UNCLASSIFIED_LABEL, EventTypeRegistry
from src.vendors import VendorRegistry, register_default_vendors

log = logging.getLogger(__name__)

MAX_EXTRACT_RETRIES = 2
SWEEP_INTERVAL_S = 5.0


# --------------------------------------------------------------------------- #
# Simple leaky-bucket global rate limiter (tier 2 of §3.7)
# --------------------------------------------------------------------------- #


class GlobalRateLimiter:
    def __init__(self, per_sec: float) -> None:
        self._per_sec = max(per_sec, 0.001)
        self._min_interval = 1.0 / self._per_sec
        self._next_slot = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            wait = self._next_slot - now
            if wait > 0:
                await asyncio.sleep(wait)
                now = time.monotonic()
            self._next_slot = max(self._next_slot, now) + self._min_interval


# --------------------------------------------------------------------------- #
# Per-event processing
# --------------------------------------------------------------------------- #


async def _mark_status(
    session: AsyncSession,
    raw_event_id: int,
    status: EventStatus,
    *,
    last_error: str | None = None,
    classified_label: str | None = None,
    attempts_increment: bool = False,
) -> None:
    values: dict[str, Any] = {"status": status}
    if last_error is not None:
        values["last_error"] = last_error
    if classified_label is not None:
        values["classified_label"] = classified_label
    if attempts_increment:
        values["attempts"] = RawEvent.attempts + 1
    await session.execute(update(RawEvent).where(RawEvent.id == raw_event_id).values(**values))


async def _claim_pending(session: AsyncSession, raw_event_id: int) -> RawEvent | None:
    """Atomically move PENDING -> PROCESSING. Returns the row if we claimed it,
    else None (already claimed, done, or dead).

    Done in two steps: a conditional UPDATE (row-level atomic) followed by a
    SELECT to fetch the row. The WHERE clause on status ensures at most one
    worker wins the transition.
    """
    result = await session.execute(
        update(RawEvent)
        .where(RawEvent.id == raw_event_id, RawEvent.status == EventStatus.PENDING)
        .values(status=EventStatus.PROCESSING, attempts=RawEvent.attempts + 1)
    )
    if result.rowcount == 0:
        return None
    fetched = await session.execute(select(RawEvent).where(RawEvent.id == raw_event_id))
    return fetched.scalar_one_or_none()


async def _dead_letter(
    session: AsyncSession,
    raw_event: RawEvent,
    stage: str,
    error: str,
) -> None:
    dl = DeadLetter(
        raw_event_id=raw_event.id,
        stage=stage,
        error=error,
        payload_snapshot=raw_event.body_json,
    )
    session.add(dl)
    await _mark_status(session, raw_event.id, EventStatus.DEAD, last_error=error)
    log.warning("dead-lettered raw_event=%s stage=%s error=%s", raw_event.id, stage, error)


async def process_event(
    raw_event_id: int,
    llm: LLMClient,
    rate_limiter: GlobalRateLimiter,
) -> None:
    async with session_scope() as session:
        raw_event = await _claim_pending(session, raw_event_id)
        if raw_event is None:
            return  # already claimed by another worker, or terminal

        vendor = VendorRegistry.get(raw_event.vendor_id)
        hint = vendor.hints if vendor else None
        allowed = EventTypeRegistry.labels()

        # --- classification ---
        try:
            await rate_limiter.acquire()
            cls_result = await llm.classify(
                payload=raw_event.body_json,
                allowed_labels=allowed,
                vendor_hint=hint,
            )
        except LLMError as e:
            await _dead_letter(session, raw_event, stage="classify", error=str(e))
            return

        label = cls_result.classification.label
        log.info(
            "classified raw_event=%s label=%s confidence=%.2f",
            raw_event.id,
            label,
            cls_result.classification.confidence,
        )

        if label == UNCLASSIFIED_LABEL:
            await _mark_status(
                session, raw_event.id, EventStatus.DONE, classified_label=label
            )
            return

        entry = EventTypeRegistry.try_get(label)
        if entry is None:
            await _dead_letter(
                session,
                raw_event,
                stage="classify",
                error=f"classifier returned unknown label {label!r}",
            )
            return

        # --- extraction with retry-on-validation-error (§3.5 layer 3) ---
        validator_error_hint: str | None = None
        json_schema = entry.json_schema()
        last_err: str | None = None
        model_obj = None

        for attempt in range(1, MAX_EXTRACT_RETRIES + 2):
            try:
                await rate_limiter.acquire()
                ext_result = await llm.extract(
                    payload=raw_event.body_json,
                    label=label,
                    prompt=entry.prompt,
                    json_schema=json_schema,
                    vendor_hint=hint,
                    validator_error_hint=validator_error_hint,
                )
            except LLMError as e:
                last_err = f"LLM call failed on attempt {attempt}: {e}"
                log.warning(last_err)
                validator_error_hint = None  # not a validation problem
                continue

            try:
                model_obj = entry.schema.model_validate(ext_result.data)
                break
            except ValidationError as e:
                last_err = f"validation failed on attempt {attempt}: {e}"
                log.warning(last_err)
                # Feed the validation error back to the LLM for the next attempt.
                validator_error_hint = str(e)
                continue

        if model_obj is None:
            await _dead_letter(
                session,
                raw_event,
                stage="extract",
                error=last_err or "extraction failed without diagnostic",
            )
            return

        # --- persist ---
        try:
            await entry.persister(session, raw_event.id, raw_event.vendor_id, model_obj)
            await _mark_status(
                session, raw_event.id, EventStatus.DONE, classified_label=label
            )
        except Exception as e:  # noqa: BLE001 — we genuinely want to catch anything on persist
            await _dead_letter(session, raw_event, stage="persist", error=str(e))


# --------------------------------------------------------------------------- #
# Sweep + listen loop
# --------------------------------------------------------------------------- #


async def _sweep_pending(limit: int = 100) -> list[int]:
    async with session_scope() as session:
        result = await session.execute(
            select(RawEvent.id)
            .where(RawEvent.status == EventStatus.PENDING)
            .order_by(RawEvent.id)
            .limit(limit)
        )
        return [row for row in result.scalars()]


async def run_worker(llm: LLMClient) -> None:
    settings = get_settings()
    register_default_event_types()
    register_default_vendors(hmac_enabled=settings.hmac_verify_enabled)

    semaphore = asyncio.Semaphore(settings.worker_concurrency)
    rate_limiter = GlobalRateLimiter(settings.llm_global_rate_per_sec)
    active: set[asyncio.Task[None]] = set()

    async def _process(event_id: int) -> None:
        async with semaphore:
            try:
                await process_event(event_id, llm, rate_limiter)
            except Exception:
                log.exception("unhandled error processing event=%s", event_id)

    def _schedule(event_id: int) -> None:
        task = asyncio.create_task(_process(event_id))
        active.add(task)
        task.add_done_callback(active.discard)

    log.info("worker starting")
    async with pg_listener() as notify_queue:
        # initial catch-up for anything already pending
        for eid in await _sweep_pending(limit=1000):
            _schedule(eid)

        while True:
            try:
                event_id = await asyncio.wait_for(notify_queue.get(), timeout=SWEEP_INTERVAL_S)
                _schedule(event_id)
            except asyncio.TimeoutError:
                # Periodic sweep for notifications we might have missed.
                for eid in await _sweep_pending(limit=200):
                    _schedule(eid)
