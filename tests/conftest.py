"""
Shared test fixtures.

Two modes:
- Unit tests: import src modules directly, no DB needed. Run anywhere.
- Integration tests: need Postgres from docker-compose. Set TEST_DATABASE_URL
  or it defaults to the local docker-compose db. Integration tests are marked
  with @pytest.mark.integration and skipped by default unless --integration is
  passed or INTEGRATION=1.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import text


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--integration",
        action="store_true",
        default=False,
        help="run integration tests that require a live Postgres",
    )


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers", "integration: test requires a live Postgres (skipped by default)"
    )


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    run_integration = config.getoption("--integration") or os.environ.get("INTEGRATION") == "1"
    if run_integration:
        return
    skip_integration = pytest.mark.skip(
        reason="integration test (pass --integration or set INTEGRATION=1 to run)"
    )
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)


# --------------------------------------------------------------------------- #
# Integration fixture: clean DB + ASGI client against the real FastAPI app.
#
# We deliberately skip the app's lifespan (no worker, no LLM) because these
# tests exercise the ingest path only. The ingest endpoint depends on:
#   - DB tables (init_db)
#   - VendorRegistry (register_default_vendors)
#   - TokenBucketLimiter singleton (reset between tests)
# All of which we set up manually.
# --------------------------------------------------------------------------- #


@pytest_asyncio.fixture
async def integration_client() -> AsyncIterator[httpx.AsyncClient]:
    """httpx client wired to the FastAPI app with fresh DB + registry state.

    Requires `docker compose up -d db`. Stop any running uvicorn first, or its
    worker will pick up the test events and fire real LLM calls.
    """
    from src.app import app
    from src.db import dispose_db, init_db, session_scope
    from src.rate_limit import token_bucket
    from src.vendors import VendorRegistry, register_default_vendors

    # The async engine is a module-level singleton; any asyncpg connection it
    # cached on a previous test's event loop is invalid now. Start from scratch.
    await dispose_db()
    await init_db()

    async with session_scope() as session:
        await session.execute(
            text(
                "TRUNCATE raw_events, seen_keys, vendor_counters, "
                "dead_letters, shipments, invoices RESTART IDENTITY CASCADE"
            )
        )

    VendorRegistry.clear()
    register_default_vendors(hmac_enabled=False)
    token_bucket._buckets.clear()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client

    await dispose_db()
