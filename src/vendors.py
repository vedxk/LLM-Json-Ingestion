"""
VendorRegistry — per-vendor configuration: auth scheme, rate limits, daily cap,
optional LLM hints. Parallel to EventTypeRegistry.

Onboarding a new vendor is one registry entry + an optional secret env var.
See README §3.10.
"""

from __future__ import annotations

import hashlib
import hmac
import os
from dataclasses import dataclass, field
from typing import Protocol

from src.rate_limit import RateLimit


class AuthScheme(Protocol):
    """Vendor auth verifier. `verify` raises ValueError on failure."""

    def verify(self, *, body: bytes, headers: dict[str, str]) -> None: ...


@dataclass(frozen=True)
class NoAuth:
    """No-op auth — used when HMAC verification is disabled (assignment default)."""

    def verify(self, *, body: bytes, headers: dict[str, str]) -> None:  # noqa: D401
        return None


@dataclass(frozen=True)
class HMACAuth:
    """
    Vendor-supplied HMAC-SHA256 signature in a given header.

    The header value is expected to be the hex digest of HMAC-SHA256(secret, body).
    Some vendors prefix with 'sha256='; we tolerate that. Vendor-specific quirks
    (timestamp windows, multi-version signatures like Stripe) belong in a subclass.
    """

    header: str
    secret_env: str

    def verify(self, *, body: bytes, headers: dict[str, str]) -> None:
        provided = headers.get(self.header) or headers.get(self.header.lower())
        if not provided:
            raise ValueError(f"missing signature header {self.header!r}")
        secret = os.environ.get(self.secret_env)
        if not secret:
            raise ValueError(f"secret env var {self.secret_env!r} not set")
        if provided.startswith("sha256="):
            provided = provided.split("=", 1)[1]
        expected = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, provided):
            raise ValueError("signature mismatch")


@dataclass(frozen=True)
class VendorConfig:
    vendor_id: str
    auth: AuthScheme
    rate_limit: RateLimit
    daily_cap: int | None = None
    hints: str | None = None
    # Header names the vendor uses for idempotency. Checked in order.
    idempotency_headers: tuple[str, ...] = field(
        default=("Idempotency-Key", "X-Event-Id", "X-Idempotency-Key")
    )


class _VendorRegistry:
    def __init__(self) -> None:
        self._vendors: dict[str, VendorConfig] = {}

    def register(self, config: VendorConfig) -> None:
        if config.vendor_id in self._vendors:
            raise ValueError(f"vendor {config.vendor_id!r} already registered")
        self._vendors[config.vendor_id] = config

    def get(self, vendor_id: str) -> VendorConfig | None:
        return self._vendors.get(vendor_id)

    def require(self, vendor_id: str) -> VendorConfig:
        cfg = self._vendors.get(vendor_id)
        if cfg is None:
            raise KeyError(vendor_id)
        return cfg

    def all(self) -> list[VendorConfig]:
        return list(self._vendors.values())

    def clear(self) -> None:
        """For tests only."""
        self._vendors.clear()


VendorRegistry = _VendorRegistry()


# --------------------------------------------------------------------------- #
# Default vendor fixtures.
#
# In production these would come from a config file or admin UI. For the
# assignment, hardcoding a few plausible vendors keeps the demo self-contained.
#
# To add a vendor: append a VendorConfig entry. Optionally set the referenced
# secret env var if HMAC is enabled.
# --------------------------------------------------------------------------- #


def register_default_vendors(hmac_enabled: bool = False) -> None:
    """Idempotent. Called once at app startup."""

    def _auth(header: str, secret_env: str) -> AuthScheme:
        return HMACAuth(header=header, secret_env=secret_env) if hmac_enabled else NoAuth()

    defaults = [
        VendorConfig(
            vendor_id="maersk",
            auth=_auth("X-Maersk-Signature", "MAERSK_SECRET"),
            rate_limit=RateLimit(per_sec=50, burst=100),
            daily_cap=10_000,
            hints="Maersk sends container-oriented JSON; shipment updates dominate.",
        ),
        VendorConfig(
            vendor_id="fedex",
            auth=_auth("X-Fedex-Signature", "FEDEX_SECRET"),
            rate_limit=RateLimit(per_sec=100, burst=200),
            daily_cap=50_000,
            hints="FedEx sends tracking events; field names are UPPER_SNAKE_CASE.",
        ),
        VendorConfig(
            vendor_id="acme_invoicing",
            auth=_auth("X-Acme-Signature", "ACME_SECRET"),
            rate_limit=RateLimit(per_sec=20, burst=50),
            daily_cap=5_000,
            hints="ACME invoicing platform; amounts in cents; currency always present.",
        ),
        VendorConfig(
            vendor_id="generic",
            auth=NoAuth(),  # no HMAC even in prod — this is a catch-all sandbox vendor
            rate_limit=RateLimit(per_sec=10, burst=20),
            daily_cap=1_000,
            hints=None,
        ),
    ]

    for cfg in defaults:
        if VendorRegistry.get(cfg.vendor_id) is None:
            VendorRegistry.register(cfg)
