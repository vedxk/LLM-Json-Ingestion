"""
Re-drive dead-lettered events.

Usage:
    python -m scripts.replay_dlq                    # replay all DEAD events
    python -m scripts.replay_dlq --stage extract    # filter by failure stage
    python -m scripts.replay_dlq --limit 50

How it works:
    1. For each DEAD raw_event matching the filter, flip its status back to
       PENDING (attempts counter preserved) and NOTIFY the worker.
    2. The worker picks it up via LISTEN and retries it normally.

This is intentionally minimal — a production ops UI would let you inspect and
edit the payload before replaying, tag replays for audit, etc. (README §6.)
"""

from __future__ import annotations

import argparse
import asyncio
import logging

from sqlalchemy import and_, select, update

from src.db import dispose_db, session_scope
from src.models.dead_letter import DeadLetter
from src.models.raw_event import EventStatus, RawEvent
from src.queue import notify_new_event

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


async def replay(stage: str | None, limit: int) -> int:
    replayed = 0
    async with session_scope() as session:
        conds = [RawEvent.status == EventStatus.DEAD]
        if stage:
            q = (
                select(RawEvent)
                .join(DeadLetter, DeadLetter.raw_event_id == RawEvent.id)
                .where(and_(*conds, DeadLetter.stage == stage))
                .limit(limit)
            )
        else:
            q = select(RawEvent).where(and_(*conds)).limit(limit)

        rows = (await session.execute(q)).scalars().all()
        for row in rows:
            await session.execute(
                update(RawEvent)
                .where(RawEvent.id == row.id, RawEvent.status == EventStatus.DEAD)
                .values(status=EventStatus.PENDING, last_error=None)
            )
            await notify_new_event(session, row.id)
            replayed += 1
            log.info("replayed raw_event=%s (vendor=%s)", row.id, row.vendor_id)
    return replayed


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay dead-lettered events.")
    parser.add_argument("--stage", default=None, help="filter by DLQ stage (classify|extract|persist)")
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()

    try:
        replayed = asyncio.run(replay(args.stage, args.limit))
        print(f"replayed {replayed} events")
    finally:
        asyncio.run(dispose_db())


if __name__ == "__main__":
    main()
