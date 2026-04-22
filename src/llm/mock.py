"""
MockLLM — used exclusively in the test suite. Simulates the behaviors that
matter for reliability testing:

- Configurable latency (to exercise concurrency).
- Configurable error rate (to exercise retry + DLQ).
- Configurable hallucination rate (to exercise Pydantic validation + retry-with-
  error-feedback).
- Deterministic classification: heuristic on payload keys. Good enough for tests.
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass, field
from typing import Any

from src.llm.base import (
    Classification,
    ClassificationResult,
    ExtractionResult,
    LLMError,
)


def _looks_like_shipment(payload: dict[str, Any]) -> bool:
    keys = {k.lower() for k in _flatten_keys(payload)}
    hits = {"tracking_number", "tracking", "awb", "container", "shipment", "status"}
    status_words = {"transit", "delivered", "exception", "in_transit", "out_for_delivery"}
    text = _flatten_values_str(payload).lower()
    return bool(keys & hits) or any(w in text for w in status_words)


def _looks_like_invoice(payload: dict[str, Any]) -> bool:
    keys = {k.lower() for k in _flatten_keys(payload)}
    hits = {"invoice", "invoice_id", "amount", "currency", "amount_cents", "total"}
    return bool(keys & hits)


def _flatten_keys(obj: Any) -> list[str]:
    out: list[str] = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            out.append(str(k))
            out.extend(_flatten_keys(v))
    elif isinstance(obj, list):
        for item in obj:
            out.extend(_flatten_keys(item))
    return out


def _flatten_values_str(obj: Any) -> str:
    parts: list[str] = []
    if isinstance(obj, dict):
        for v in obj.values():
            parts.append(_flatten_values_str(v))
    elif isinstance(obj, list):
        for v in obj:
            parts.append(_flatten_values_str(v))
    elif isinstance(obj, str):
        parts.append(obj)
    return " ".join(parts)


@dataclass
class MockLLM:
    latency_s: float = 0.0
    error_rate: float = 0.0
    hallucination_rate: float = 0.0
    rng: random.Random = field(default_factory=lambda: random.Random(0))

    async def _maybe_fail(self) -> None:
        if self.latency_s > 0:
            await asyncio.sleep(self.latency_s)
        if self.error_rate > 0 and self.rng.random() < self.error_rate:
            raise LLMError("simulated provider error")

    async def classify(
        self,
        *,
        payload: dict[str, Any],
        allowed_labels: list[str],
        vendor_hint: str | None = None,
    ) -> ClassificationResult:
        await self._maybe_fail()
        label = "unclassified"
        reason = "no match"
        if "shipment" in allowed_labels and _looks_like_shipment(payload):
            label = "shipment"
            reason = "tracking/shipment-like fields present"
        elif "invoice" in allowed_labels and _looks_like_invoice(payload):
            label = "invoice"
            reason = "invoice/amount/currency-like fields present"
        return ClassificationResult(
            classification=Classification(label=label, confidence=0.95, reason=reason),
            model="mock-classifier",
        )

    async def extract(
        self,
        *,
        payload: dict[str, Any],
        label: str,
        prompt: str,
        json_schema: dict[str, Any],
        vendor_hint: str | None = None,
        validator_error_hint: str | None = None,
    ) -> ExtractionResult:
        await self._maybe_fail()

        hallucinate = (
            self.hallucination_rate > 0
            and validator_error_hint is None  # always correct on retry
            and self.rng.random() < self.hallucination_rate
        )

        if label == "shipment":
            data = _extract_shipment(payload, vendor_hint, hallucinate)
        elif label == "invoice":
            data = _extract_invoice(payload, vendor_hint, hallucinate)
        else:
            raise LLMError(f"mock does not support label {label!r}")

        return ExtractionResult(data=data, model="mock-extractor", attempts=1)


def _get_first(payload: dict[str, Any], keys: list[str], default: Any = None) -> Any:
    """Look for keys (case-insensitive) at any depth; return first hit."""
    for k in keys:
        v = _find_nested(payload, k.lower())
        if v is not None:
            return v
    return default


def _find_nested(obj: Any, target_lower: str) -> Any:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if str(k).lower() == target_lower:
                return v
            found = _find_nested(v, target_lower)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = _find_nested(item, target_lower)
            if found is not None:
                return found
    return None


def _extract_shipment(
    payload: dict[str, Any], vendor_hint: str | None, hallucinate: bool
) -> dict[str, Any]:
    vendor_id = _get_first(
        payload, ["vendor_id", "carrier", "vendor", "source"], default="unknown"
    )
    tracking = _get_first(
        payload,
        ["tracking_number", "tracking", "awb", "container_number", "trackingNumber"],
        default="UNKNOWN",
    )
    status_raw = _get_first(
        payload, ["status", "event_type", "state"], default="TRANSIT"
    )
    timestamp = _get_first(
        payload, ["timestamp", "event_time", "updated_at", "time", "ts"],
        default="2026-04-21T00:00:00Z",
    )
    if hallucinate:
        # Simulate a common failure mode: status returned as a free-text phrase.
        status_raw = "looks like it's moving"
    return {
        "vendor_id": str(vendor_id),
        "tracking_number": str(tracking),
        "status": str(status_raw),
        "timestamp": str(timestamp),
    }


def _extract_invoice(
    payload: dict[str, Any], vendor_hint: str | None, hallucinate: bool
) -> dict[str, Any]:
    vendor_id = _get_first(
        payload, ["vendor_id", "vendor", "biller", "source"], default="unknown"
    )
    invoice_id = _get_first(
        payload, ["invoice_id", "invoice_number", "id", "reference"], default="UNKNOWN"
    )
    amount = _get_first(payload, ["amount", "total", "amount_due"])
    amount_cents = _get_first(payload, ["amount_cents", "total_cents"])
    if amount is None and amount_cents is not None:
        try:
            amount = float(amount_cents) / 100.0
        except (TypeError, ValueError):
            amount = 0.0
    try:
        amount_f = float(amount) if amount is not None else 0.0
    except (TypeError, ValueError):
        amount_f = 0.0

    currency = _get_first(payload, ["currency", "ccy"], default="USD")

    if hallucinate:
        # Simulate hallucinated currency code that will fail ISO-4217 allow-list.
        currency = "XYZ"
    return {
        "vendor_id": str(vendor_id),
        "invoice_id": str(invoice_id),
        "amount": amount_f,
        "currency": str(currency).upper(),
    }
