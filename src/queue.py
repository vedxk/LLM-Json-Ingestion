"""
Outbox notifier (README §3.2).

Postgres `LISTEN/NOTIFY`:
- Ingest endpoint calls `NOTIFY raw_events_new, '<id>'` after committing.
- Worker `LISTEN`s on that channel and wakes on demand.
- On startup or if notifications are missed (connection drops), the worker
  falls back to a short periodic poll.

This module exposes:
- `notify_new_event(session, raw_event_id)` — fire-and-forget from ingest.
- `listen_for_events(connection, handler)` — long-running loop for the worker.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.db import get_engine

log = logging.getLogger(__name__)

CHANNEL = "raw_events_new"


async def notify_new_event(session: AsyncSession, raw_event_id: int) -> None:
    """Emit a NOTIFY inside the current transaction. The payload is the id so
    the worker can jump straight to it without polling."""
    await session.execute(
        text("SELECT pg_notify(:channel, :payload)"),
        {"channel": CHANNEL, "payload": str(raw_event_id)},
    )


@asynccontextmanager
async def pg_listener() -> AsyncIterator[asyncio.Queue[int]]:
    """
    Async context manager that yields an asyncio.Queue receiving raw_event_ids
    from Postgres NOTIFY. Uses the underlying asyncpg connection directly.

    The worker should still run a periodic catch-up sweep on pending rows in
    case a NOTIFY was missed (connection drop, worker startup, etc.).
    """
    engine = get_engine()
    queue: asyncio.Queue[int] = asyncio.Queue(maxsize=10_000)

    async with engine.connect() as sqla_conn:
        await sqla_conn.execution_options(isolation_level="AUTOCOMMIT")
        driver_conn = await sqla_conn.get_raw_connection()
        asyncpg_conn = driver_conn.driver_connection

        def _on_notify(_conn: object, _pid: int, _channel: str, payload: str) -> None:
            try:
                queue.put_nowait(int(payload))
            except asyncio.QueueFull:
                log.warning("notify queue full; dropping %s", payload)
            except ValueError:
                log.warning("invalid notify payload %r", payload)

        await asyncpg_conn.add_listener(CHANNEL, _on_notify)
        log.info("LISTENing on %s", CHANNEL)
        try:
            yield queue
        finally:
            await asyncpg_conn.remove_listener(CHANNEL, _on_notify)
            log.info("removed listener on %s", CHANNEL)
