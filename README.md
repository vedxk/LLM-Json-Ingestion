# LLM Webhook Ingestion

AI-powered webhook ingestion service. Accepts arbitrary vendor JSON, durably ACKs in well under a second, then asynchronously classifies via LLM, normalizes into strict schemas, and persists — with idempotency, per-vendor rate limiting, and layered LLM reliability.

**Status:** 57 tests passing (46 unit + 11 integration).

---

## 1. Run it

**Prereqs:** Docker (for Postgres), Python 3.11.

```bash
# 1. Start Postgres
docker compose up -d db

# 2. Python env + deps
python3.11 -m venv .venv
.venv/bin/pip install -e ".[dev]"

# 3. Configure
cp .env.example .env
# Edit .env: set OPENAI_API_KEY. If omitted, the app falls back to MockLLM
# with a warning — fine for a smoke test.

# 4. Run the app (ingest + background worker in one process)
.venv/bin/uvicorn src.app:app --reload --port 8000

# 5. In another terminal, fire the sample payloads
./scripts/seed_webhooks.sh

# 6. Inspect
docker exec -it glacis-webhook-db psql -U glacis -d glacis \
  -c "select id, vendor_id, status, classified_label from raw_events order by id desc limit 10;" \
  -c "select * from shipments order by id desc limit 5;" \
  -c "select * from invoices order by id desc limit 5;"
```

**Tests:**
```bash
.venv/bin/pytest -q                    # unit tests (no DB)
.venv/bin/pytest --integration -q      # + integration tests (needs DB up)
```

**DLQ replay (stub):**
```bash
.venv/bin/python -m scripts.replay_dlq --stage extract --limit 50
```

Single-request cURL:
```bash
curl -sS -X POST http://localhost:8000/webhooks/fedex \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: evt-42" \
  --data-binary @scripts/sample_payloads/fedex_delivered.json
```

---

## 2. Architecture

Two pieces of runtime code running in one uvicorn process, communicating only via Postgres.

```
                 ┌────────────────────────────┐
  Vendor ──POST──▶  /webhooks/{vendor_id}      │   FastAPI, stateless
                 │   1. vendor lookup (404)    │
                 │   2. verify HMAC (optional) │
                 │   3. per-vendor rate limit  │
                 │      (429 if exceeded)      │
                 │   4. global backlog shed    │
                 │      (503 if > threshold)   │
                 │   5. parse JSON             │
                 │   6. dedup (seen_keys)      │
                 │   7. insert raw_event       │──┐  200 in ~50ms
                 │   8. bump daily counter     │  │
                 │      (429 if cap)           │  │
                 │   9. pg_notify worker       │  │
                 └────────────────────────────┘   │
                                                   ▼
                                  ┌─────────────────────────┐
                                  │  raw_events (Postgres)   │   durable queue
                                  │  status: pending → ...   │
                                  │  LISTEN/NOTIFY wake-up   │
                                  └──────────┬──────────────┘
                                             │ notify
                                             ▼
                              ┌──────────────────────────────┐
                              │  Worker (asyncio, in-process) │
                              │   a. atomic claim            │
                              │   b. classify (LLM, cheap)   │
                              │   c. extract (LLM, strong,   │
                              │      up to 3 attempts with   │
                              │      error-feedback retry)   │
                              │   d. persist normalized row  │
                              │   any failure → DLQ          │
                              └──────┬───────────────────────┘
                                     ▼
              ┌───────────┐    ┌──────────┐    ┌──────────────┐
              │ shipments │    │ invoices │    │ dead_letters │
              └───────────┘    └──────────┘    └──────────────┘
```

Principle: the handler does the minimum work to not lose data (one DB write) and ACKs. Everything intelligent is async. Handler code: [src/app.py:112](src/app.py#L112). Worker: [src/worker.py:126](src/worker.py#L126).

### 3.1 Request flow, step by step

Numbered to match the handler at [src/app.py:112-219](src/app.py#L112-L219).

| Step | What | Failure → |
|---|---|---|
| 1 | `VendorRegistry.get(vendor_id)` | 404 |
| 2 | `vendor.auth.verify(body, headers)` (NoAuth by default; HMAC when enabled) | 401 |
| 3 | Per-vendor token bucket `try_acquire` | 429 |
| 4 | Count `raw_events` where status=pending; compare to `INGEST_BACKLOG_THRESHOLD` | 503 |
| 5 | `json.loads(body)`; must be an object | 400 |
| 6 | Compute dedup key, `INSERT ... ON CONFLICT DO NOTHING RETURNING key` on `seen_keys` | 200 duplicate (short-circuit) |
| 7 | Insert `raw_events` row (status=pending) | — |
| 8 | UPSERT `vendor_counters`; compare to `daily_cap` | 429 |
| 9 | `SELECT pg_notify('raw_events_new', raw_event_id)` | — |
| 10 | Return 200 `{status: accepted, raw_event_id, ...}` | — |

Steps 6–9 run inside one transaction with `SET LOCAL statement_timeout = 300`. Any `DBAPIError` inside that block is mapped to 503 at [src/app.py:215](src/app.py#L215). On rejection from step 8, the whole transaction rolls back — the counter bump and the raw_event insert are both undone.

### 3.2 Worker flow

`run_worker` ([src/worker.py:244](src/worker.py#L244)) opens a `pg_listener` on channel `raw_events_new` and loops:

1. Wait up to 5s for a NOTIFY payload (a raw_event_id).
2. On timeout, sweep up to 200 rows still in `pending` (safety net for missed NOTIFYs).
3. Schedule each id into `process_event(id)` under a semaphore (default 8).

`process_event` is one transaction per event:
- **Claim**: `UPDATE raw_events SET status='processing' WHERE id=? AND status='pending'`. `rowcount=0` means someone else got there first — return silently. This is how the NOTIFY/sweep race is made harmless.
- **Classify**: one LLM call. Cheap model (`gpt-4.1-mini`), `temperature=0`, structured-output schema `{label, confidence, reason}`. Allowed labels = `EventTypeRegistry.labels()` + `"unclassified"`. Any exception → DLQ with stage=classify.
- **If `unclassified`**: mark DONE, return. No extraction cost.
- **Extract** (up to 3 attempts): strong model (`gpt-4.1`), JSON schema generated from the Pydantic class. Retry on `LLMError` (blind retry) or `ValidationError` (retry with the Pydantic error text fed back into the next prompt). If all attempts fail → DLQ with stage=extract.
- **Persist**: registry's persister function adds a `ShipmentRecord` or `InvoiceRecord`. Exception → DLQ with stage=persist.
- **Mark DONE** with `classified_label`.

### 3.3 Data model

| Table | Key columns | Purpose |
|---|---|---|
| `raw_events` | id, vendor_id, status, body_json, body_hash | Durable queue. `pending → processing → done/dead`. |
| `seen_keys` | key (PK) | Idempotency. INSERT ON CONFLICT = the dedup check. |
| `vendor_counters` | (vendor_id, day) (PK) | Daily quota. Hot row — see §6.2. |
| `shipments` | id, raw_event_id (FK CASCADE) | Normalized output. |
| `invoices` | id, raw_event_id (FK CASCADE) | Normalized output. |
| `dead_letters` | id, raw_event_id (FK CASCADE), stage, error | Triage queue, retains `payload_snapshot`. |

Every normalized row keeps `raw_event_id` so you can always audit back to the original payload. Schemas in [src/models/](src/models/). No Alembic — `Base.metadata.create_all()` at startup.

### 3.4 Two registries

Both are module-level singletons populated at startup; they are the seams that make vendor/event onboarding data-driven.

**`VendorRegistry`** ([src/vendors.py](src/vendors.py)) — per-vendor config. 4 hardcoded defaults:

| vendor_id | per_sec / burst | daily_cap | HMAC header | LLM hint |
|---|---|---|---|---|
| maersk | 50 / 100 | 10,000 | X-Maersk-Signature | container-oriented JSON |
| fedex | 100 / 200 | 50,000 | X-Fedex-Signature | UPPER_SNAKE_CASE field names |
| acme_invoicing | 20 / 50 | 5,000 | X-Acme-Signature | amounts in cents |
| generic | 10 / 20 | 1,000 | none | — |

The LLM hint is injected into both the classifier and extractor prompts — a cheap way to bias outputs per vendor.

**`EventTypeRegistry`** ([src/registry.py](src/registry.py)) — 2 hardcoded event types:

```
"shipment" → (schema=Shipment, table=shipments, prompt=SHIPMENT_PROMPT,
              persister=_persist_shipment)
"invoice"  → (schema=Invoice,  table=invoices,  prompt=INVOICE_PROMPT,
              persister=_persist_invoice)
```

Plus the reserved label `"unclassified"` (sentinel, never has an entry).

Adding a new event type is one registry entry — no changes to the handler, worker, or classifier. The classifier's allowed labels are *derived* from `EventTypeRegistry.labels()`.

### 3.5 Concurrency bounds

Four separate limits, each guarding a different resource:

| Bound | Default | Scope | What happens when hit |
|---|---|---|---|
| Per-vendor token bucket | per-vendor in [src/vendors.py](src/vendors.py) | Single app process, in-memory | 429 to that vendor |
| Global ingest backlog | 10,000 pending rows | Whole system | 503 to all vendors |
| Worker concurrency semaphore | 8 | Per worker (= per process) | New events queue in Python |
| Global LLM rate limiter | 50/s | Per process | Calls queue in Python |

Bounds #3 and #4 are independent because a single event can make up to 4 LLM calls (1 classify + up to 3 extract).

### 3.6 LLM reliability — five layers

Each layer catches a different failure class:

1. **Provider structured outputs.** `response_format={"type": "json_schema", "strict": true}` — OpenAI enforces shape server-side.
2. **Pydantic validation on receipt.** Guards against provider regressions.
3. **Retry with validation error feedback (up to 2 extra attempts).** We feed the Pydantic error back into the next prompt: *"Your previous response failed validation: `amount` must be a number, got `one hundred dollars`."*
4. **Semantic post-checks.** `currency` against an ISO-4217 allow-list; `status` coerced via a 14-entry alias map; `timestamp` via `dateutil` to UTC.
5. **Dead letter on persistent failure.** Row → DEAD + `dead_letters` with full failure trail. Worker continues; human triages.

---

## 4. Tradeoffs decided

### 4.1 Durable write on ingest, process async

Single DB insert + 200 OK. LLM never on the request path.
**Benefit:** vendor SLA decoupled from LLM latency (0.5–5s) and LLM flakiness. **Cost:** ~2–5ms DB write on the hot path.

### 4.2 DB-backed queue, not Redis/SQS

`raw_events` + `LISTEN/NOTIFY` is the queue.
**Benefit:** zero extra infra, restart-survivable, outbox semantics for free. **Cost:** doesn't scale past ~1 node before contention; fixed in §7.2.

### 4.3 Two-stage LLM (classify → extract), not one-shot

**Benefit:** `unclassified` short-circuits with zero extraction cost; independent retries; cheap classifier + strong extractor. **Cost:** two provider calls when extraction happens.

### 4.4 Dedup via vendor-header-first, body-hash fallback

**Benefit:** vendors that send event ids (most real ones) get exact-match dedup. **Cost:** payloads with per-request timestamps/uuids miss dedup when vendors don't send a key.

### 4.5 In-memory token bucket, not Redis-backed

**Benefit:** ~30 lines, no dependency. **Cost:** resets on restart; doesn't share across app nodes. §7.1 closes this.

### 4.6 Tight statement timeout on the ingest transaction

**Benefit:** caps the worst-case DB phase so vendor SLA is preserved when the DB slows down. **Cost:** can't distinguish "DB is sick" from "row-lock queue is long" — see §6.2.

### 4.7 Deliberate cuts for assignment scope

- No Dockerfile for the app; only Postgres dockerized.
- `create_all()` instead of Alembic migrations.
- HMAC implemented but off by default.
- Structured logs only (no Prometheus/OTel).
- No Redis / external queue.
- No load tuning.

Each is cheap to retrofit; see §7.

---

## 5. Tests

### 5.1 Layout

| File | Scope | Tests |
|---|---|---|
| [tests/test_schemas.py](tests/test_schemas.py) | Pydantic: status aliases, timestamp parsing, ISO-4217, `extra="forbid"` | 11 |
| [tests/test_vendor.py](tests/test_vendor.py) | VendorRegistry; NoAuth + HMACAuth paths | 8 |
| [tests/test_mock_llm.py](tests/test_mock_llm.py) | MockLLM classify/extract; hallucinate-then-correct; error rate | 7 |
| [tests/test_dedup.py](tests/test_dedup.py) | Body hash, header precedence, case insensitivity, vendor scoping | 7 |
| [tests/test_registry.py](tests/test_registry.py) | EventTypeRegistry; JSON schema derivation | 5 |
| [tests/test_rate_limit.py](tests/test_rate_limit.py) | Token bucket: burst / refill / isolation / concurrency | 4 |
| [tests/test_worker_pipeline.py](tests/test_worker_pipeline.py) | Classify → validate → retry → DLQ pipeline | 4 |
| [tests/test_integration_ingest.py](tests/test_integration_ingest.py) | HTTP endpoint + real Postgres | 11 |

### 5.2 What the integration suite covers

Marked `@pytest.mark.integration`, skipped by default. Run with `--integration` or `INTEGRATION=1`. Requires `docker compose up -d db`. **Stop any running uvicorn first** — its worker would otherwise pick up the test events and fire LLM calls on them.

**Idempotency under duplicate fire** — verifies the dedup contract under the shapes vendors actually produce:
- 10 identical POSTs with the same `Idempotency-Key` → 1 accepted + 9 duplicate, exactly 1 `raw_events` row.
- 5 identical POSTs with no key → dedup via SHA-256 body hash.
- 8 distinct keys with identical bodies → all accepted (header wins over hash).
- Same key to two vendors → both accepted (keys are vendor-scoped).

**Spikes and rate limits:**
- Sequential 15-request spike with burst=10 → first 10 × 200, tail throttled.
- 50 concurrent requests via `asyncio.gather`, burst=5, near-zero refill → exactly 5 × 200 and 45 × 429, 5 rows persisted.
- Exhaust bucket, sleep past refill interval, next request succeeds.
- Vendor A exhausted; vendor B unaffected.
- Duplicate requests still consume tokens (rate-limit check runs before dedup).

**Daily cap:**
- `daily_cap=3`, 5 requests → `[200, 200, 200, 429, 429]`, 3 rows persisted.
- Rejected requests do not creep the counter (transaction rollback undoes the bump).

### 5.3 Gaps in coverage

- Worker end-to-end against real Postgres (LISTEN/NOTIFY, sweep, atomic claim race).
- Real `OpenAILLM` adapter (covered indirectly via MockLLM + pipeline tests).
- Global ingest backlog shed (needs a seeded backlog).
- HMAC-enabled mode against the live endpoint.

---

## 6. Known issues

### 6.1 `Retry-After` header is dropped on 429/503

Sites: [src/app.py:133](src/app.py#L133), [src/app.py:194](src/app.py#L194), [src/app.py:218](src/app.py#L218).

The handler sets the header on the injected `Response` *then* raises `HTTPException`. FastAPI's exception handler builds its own response and discards those headers.

**Fix:** pass the header into the exception itself:
```python
raise HTTPException(
    status_code=429,
    detail="per-vendor rate limit exceeded",
    headers={"Retry-After": f"{retry_after:.3f}"},
)
```

Integration tests don't assert on this header so they pass despite the bug.

### 6.2 300ms statement timeout collides with row-lock contention

The `vendor_counters` UPSERT takes a row-level lock on `(vendor_id, day)`. Every request for the same vendor on the same day serializes on that one row.

Under concurrent bursts (observed at ~100+ concurrent per vendor), queued waiters exceed 300ms and are canceled by `statement_timeout` — which the handler catches as `DBAPIError` and returns 503 "ingest DB unavailable." These 503s aren't real; they're lock-queue overflow.

Fundamentally, `statement_timeout` can't tell "DB is sick" apart from "you're waiting in line." Options are compared in §7.1.

### 6.3 No reclaim of stuck PROCESSING rows

If the worker crashes or is cancelled mid-event, the row stays `PROCESSING` forever. The sweep query only picks up `PENDING`.

**Fix:** startup task to `UPDATE raw_events SET status='pending' WHERE status='processing' AND updated_at < now() - interval '5 min' AND attempts < 5`, with DLQ fallback when attempts are exhausted.

### 6.4 In-memory token bucket is per-process

`TokenBucketLimiter._buckets` lives in a module-level dict. Multiple uvicorn workers or multi-node deploys have *independent* buckets — a vendor gets N× their configured rate.

### 6.5 Daily counter hot row = vendor SLA risk

The mechanism behind §6.2. Because there is exactly one row per vendor per day, high per-vendor concurrency is serialized on that row regardless of DB scale. This is a design-level issue, not a config one.

---

## 7. Productionization roadmap

In rough ROI order. The first two close §6.2, §6.4, and §6.5 in one move.

### 7.1 Redis-backed rate limit + daily counter

Replaces both the in-memory token bucket and the `vendor_counters` table with Redis. `INCR`/`EXPIRE` for the counter, a small Lua script for the sliding-window bucket.

Why this is the #1 ROI move:
- **Fixes §6.4** — Redis is shared state; multi-process and multi-node deploys get correct vendor limits.
- **Fixes §6.5 and §6.2** — Redis `INCR` is ~100µs, single-threaded but without row-level locking. No queue forms on the "hot row" because there is no row. The 300ms timeout stops catching contention because there's nothing to contend with.
- **Decouples latency from vendor concurrency** — the handler's DB phase becomes a pure `raw_events` + `seen_keys` insert, both on distinct keys, no contention.

Counter durability on Redis crash is weaker than Postgres, but the counter is a budget tool — overshoot by a few is acceptable. The authoritative event record is still `raw_events`.

### 7.2 Durable log in front of the DB

Kafka / SQS / Redpanda as the first hop. Handler writes to the log and ACKs; a replayer drains into `raw_events`.

Benefits:
- DB outages become replayable lag, not 503s.
- Prompt/schema iteration via replay becomes nearly free.
- Ingest and processing scale independently.

This is the cleanest answer to README §3.9 ("what happens when the DB can't accept writes"). It's the architecturally correct version of §7.1 + §7.5 combined.

### 7.3 Per-vendor HMAC and proper secret handling

`HMACAuth` is already implemented and wired through the VendorRegistry — just defaulted off (`HMAC_VERIFY_ENABLED=false`). Enable per vendor, load secrets from a secret store (not `.env`), add vendor-specific quirks where needed (Stripe-style multi-version signatures, timestamp windows).

### 7.4 PROCESSING-row reclaim on worker startup

Closes §6.3 in a dozen lines. Sweep `status='processing' AND last_updated_at < now() - interval` back to `pending`, with a hard DLQ fallback when `attempts` exceeds a bound.

### 7.5 Retry-After propagation

Closes §6.1. One-line fix per call site; audit every non-2xx path while you're at it.

### 7.6 Observability

- `request_id` propagation from ingest → worker → LLM call.
- OTel spans around every LLM call (capture model, input_tokens, output_tokens, duration).
- Per-stage p50/p95/p99 latency (ingest, classify, extract, persist).
- Per-vendor, per-day token and cost counters.

### 7.7 Alerting

- DLQ size trend.
- Oldest-PENDING age (worker lag).
- LLM error-rate spikes.
- Cost spikes per vendor.
- Counter-contention rate (when §7.1 hasn't shipped yet — tells you §6.2 is firing).

### 7.8 Prompt evals in CI + canary

- Golden set of real payloads → expected normalized rows; run on every prompt change.
- Shadow traffic: route new prompts to 1% of events; diff extraction output against production.

### 7.9 Schema versioning and backfill

- `schema_version` column on normalized rows.
- A replayer that regenerates from `raw_events` under a new prompt/schema version.

### 7.10 Operational polish

- Dockerfile for the app itself (only Postgres is dockerized now).
- Alembic migrations in place of `create_all()`.
- Admin UI for DLQ triage (currently `scripts/replay_dlq.py` only).
- Multi-tenant isolation for LLM API keys.

---

## 8. Tech choices

- **Python 3.11 + FastAPI + Pydantic v2** — best ergonomics for structured LLM outputs. Pydantic → JSON schema → provider → validated model, end to end.
- **PostgreSQL 15 via docker-compose** — real concurrent writes, `LISTEN/NOTIFY`, `JSONB` indexing on raw payloads, matches production posture.
- **SQLAlchemy 2.0 async + asyncpg** — typed ORM, native async driver matching FastAPI's event loop.
- **OpenAI Python SDK** — structured outputs via `response_format=json_schema`. Classifier = cheap model, extractor = stronger model.
- **pytest + pytest-asyncio + httpx** — async test suite. Integration tests hit the real app through `httpx.ASGITransport` without going through uvicorn.
- **No Celery/RQ** — asyncio worker in-process. Durability comes from the DB, not a queue library.
