"""Dedup key computation (pure logic, no DB)."""

from __future__ import annotations

from src.dedup import body_hash, compute_dedup_key, extract_idempotency_key


def test_body_hash_is_deterministic() -> None:
    assert body_hash(b"hello") == body_hash(b"hello")
    assert body_hash(b"hello") != body_hash(b"world")


def test_extract_idempotency_key_prefers_first_match() -> None:
    headers = {"X-Event-Id": "evt-2", "Idempotency-Key": "evt-1"}
    # Idempotency-Key listed first, should win.
    assert extract_idempotency_key(
        headers, ("Idempotency-Key", "X-Event-Id")
    ) == "evt-1"


def test_extract_idempotency_key_case_insensitive() -> None:
    headers = {"idempotency-key": "evt-1"}
    assert extract_idempotency_key(headers, ("Idempotency-Key",)) == "evt-1"


def test_extract_returns_none_when_absent() -> None:
    assert extract_idempotency_key({}, ("Idempotency-Key",)) is None


def test_compute_dedup_key_uses_header_when_present() -> None:
    key = compute_dedup_key(
        vendor_id="maersk",
        body=b"{}",
        headers={"Idempotency-Key": "evt-1"},
        header_names=("Idempotency-Key",),
    )
    assert key == "maersk:idem:evt-1"


def test_compute_dedup_key_falls_back_to_body_hash() -> None:
    key = compute_dedup_key(
        vendor_id="maersk",
        body=b'{"ok":true}',
        headers={},
        header_names=("Idempotency-Key",),
    )
    assert key.startswith("maersk:hash:")
    assert len(key.split(":")[-1]) == 64  # sha256 hex


def test_dedup_key_is_vendor_scoped() -> None:
    """Two vendors sending the same body should get different keys."""
    k1 = compute_dedup_key(
        vendor_id="v1", body=b"same", headers={}, header_names=("Idempotency-Key",)
    )
    k2 = compute_dedup_key(
        vendor_id="v2", body=b"same", headers={}, header_names=("Idempotency-Key",)
    )
    assert k1 != k2
