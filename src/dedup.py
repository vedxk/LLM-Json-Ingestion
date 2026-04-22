"""
Idempotency / dedup (README §3.3).

Strategy, in order of preference:
  1. Vendor-supplied idempotency key (e.g. Idempotency-Key, X-Event-Id).
  2. SHA-256 of the raw body as fallback.

Keys are namespaced by vendor_id to avoid cross-vendor collisions.

Storage: `seen_keys` table. A primary-key INSERT either succeeds (first time)
or raises IntegrityError (duplicate). No SELECT-then-INSERT race.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.seen_key import SeenKey


@dataclass(frozen=True)
class DedupResult:
    key: str
    is_duplicate: bool


def body_hash(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def extract_idempotency_key(
    headers: dict[str, str], header_names: tuple[str, ...]
) -> str | None:
    lower = {k.lower(): v for k, v in headers.items()}
    for name in header_names:
        v = lower.get(name.lower())
        if v:
            return v.strip()
    return None


def compute_dedup_key(
    *,
    vendor_id: str,
    body: bytes,
    headers: dict[str, str],
    header_names: tuple[str, ...],
) -> str:
    idem = extract_idempotency_key(headers, header_names)
    if idem:
        return f"{vendor_id}:idem:{idem}"
    return f"{vendor_id}:hash:{body_hash(body)}"


async def check_and_record(session: AsyncSession, key: str) -> DedupResult:
    """
    Atomic insert into `seen_keys`. If the row already exists, we know we've
    seen this key before and the caller should short-circuit.

    Postgres-specific: uses INSERT ... ON CONFLICT DO NOTHING RETURNING key,
    which returns the inserted key on first insert and nothing on duplicate.
    """
    stmt = (
        pg_insert(SeenKey)
        .values(key=key)
        .on_conflict_do_nothing(index_elements=[SeenKey.key])
        .returning(SeenKey.key)
    )
    result = await session.execute(stmt)
    inserted = result.scalar_one_or_none()
    return DedupResult(key=key, is_duplicate=inserted is None)
