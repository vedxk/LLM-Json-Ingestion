"""VendorRegistry behavior + HMAC auth (the real, not-stubbed implementation)."""

from __future__ import annotations

import hashlib
import hmac
import os

import pytest

from src.rate_limit import RateLimit
from src.vendors import HMACAuth, NoAuth, VendorConfig, VendorRegistry


@pytest.fixture(autouse=True)
def _clean() -> object:
    VendorRegistry.clear()
    yield
    VendorRegistry.clear()


def test_register_and_require() -> None:
    cfg = VendorConfig(
        vendor_id="v1", auth=NoAuth(), rate_limit=RateLimit(per_sec=1, burst=1)
    )
    VendorRegistry.register(cfg)
    assert VendorRegistry.require("v1") is cfg


def test_unknown_vendor_is_none() -> None:
    assert VendorRegistry.get("nope") is None


def test_duplicate_registration_rejected() -> None:
    cfg = VendorConfig(
        vendor_id="v1", auth=NoAuth(), rate_limit=RateLimit(per_sec=1, burst=1)
    )
    VendorRegistry.register(cfg)
    with pytest.raises(ValueError):
        VendorRegistry.register(cfg)


def test_no_auth_accepts_anything() -> None:
    NoAuth().verify(body=b"anything", headers={})


def test_hmac_auth_verifies_correct_signature(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MY_SECRET", "s3cret")
    body = b'{"ok":true}'
    sig = hmac.new(b"s3cret", body, hashlib.sha256).hexdigest()
    auth = HMACAuth(header="X-Sig", secret_env="MY_SECRET")
    auth.verify(body=body, headers={"X-Sig": sig})
    # Also tolerate the sha256= prefix some vendors use.
    auth.verify(body=body, headers={"x-sig": f"sha256={sig}"})


def test_hmac_auth_rejects_bad_signature(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MY_SECRET", "s3cret")
    auth = HMACAuth(header="X-Sig", secret_env="MY_SECRET")
    with pytest.raises(ValueError, match="signature mismatch"):
        auth.verify(body=b'{"ok":true}', headers={"X-Sig": "deadbeef"})


def test_hmac_auth_missing_header() -> None:
    os.environ["S"] = "x"
    auth = HMACAuth(header="X-Sig", secret_env="S")
    with pytest.raises(ValueError, match="missing signature header"):
        auth.verify(body=b"x", headers={})


def test_hmac_auth_missing_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("NOPE", raising=False)
    auth = HMACAuth(header="X-Sig", secret_env="NOPE")
    with pytest.raises(ValueError, match="secret env var"):
        auth.verify(body=b"x", headers={"X-Sig": "abc"})
