"""
Worker extraction pipeline with retry-on-validation (§3.5 layer 3).

These tests exercise the layered validation/retry logic without touching a DB,
by calling the LLM extract + Pydantic validate loop directly.
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from src.llm.base import (
    ExtractionResult,
    LLMClient,
    Classification,
    ClassificationResult,
)
from src.llm.mock import MockLLM
from src.schemas import Invoice


@pytest.mark.asyncio
async def test_extract_then_validate_happy() -> None:
    llm = MockLLM()
    result = await llm.extract(
        payload={
            "vendor_id": "acme",
            "invoice_id": "INV-1",
            "amount_cents": 12345,
            "currency": "USD",
        },
        label="invoice",
        prompt="",
        json_schema=Invoice.model_json_schema(),
    )
    model = Invoice.model_validate(result.data)
    assert model.currency == "USD"
    assert model.amount == pytest.approx(123.45)


@pytest.mark.asyncio
async def test_extract_hallucinates_but_validator_catches() -> None:
    llm = MockLLM(hallucination_rate=1.0)
    bad = await llm.extract(
        payload={
            "vendor_id": "acme",
            "invoice_id": "INV-1",
            "amount": 1.0,
            "currency": "USD",
        },
        label="invoice",
        prompt="",
        json_schema=Invoice.model_json_schema(),
    )
    with pytest.raises(ValidationError):
        Invoice.model_validate(bad.data)


@pytest.mark.asyncio
async def test_retry_with_error_feedback_succeeds() -> None:
    """Simulates the full §3.5 layer 3 loop: first extract hallucinates →
    validate fails → retry with the validation error → succeeds."""
    llm = MockLLM(hallucination_rate=1.0)

    payload: dict[str, Any] = {
        "vendor_id": "acme",
        "invoice_id": "INV-1",
        "amount": 1.0,
        "currency": "USD",
    }

    # Attempt 1
    r1 = await llm.extract(
        payload=payload,
        label="invoice",
        prompt="",
        json_schema=Invoice.model_json_schema(),
    )
    try:
        Invoice.model_validate(r1.data)
        pytest.fail("first attempt should have failed validation")
    except ValidationError as e:
        err_hint = str(e)

    # Attempt 2 (with validator_error_hint)
    r2 = await llm.extract(
        payload=payload,
        label="invoice",
        prompt="",
        json_schema=Invoice.model_json_schema(),
        validator_error_hint=err_hint,
    )
    good = Invoice.model_validate(r2.data)
    assert good.currency == "USD"


class _AlwaysFailsLLM:
    """Simulates an LLM that always returns a structurally-valid but semantically
    garbage currency, even when given an error hint. Worker must eventually DLQ."""

    async def classify(
        self, *, payload, allowed_labels, vendor_hint=None
    ) -> ClassificationResult:
        return ClassificationResult(
            classification=Classification(label="invoice", confidence=1.0, reason=""),
            model="test",
        )

    async def extract(
        self,
        *,
        payload,
        label,
        prompt,
        json_schema,
        vendor_hint=None,
        validator_error_hint=None,
    ) -> ExtractionResult:
        return ExtractionResult(
            data={
                "vendor_id": "v",
                "invoice_id": "i",
                "amount": 1.0,
                "currency": "XYZ",  # never in ISO-4217 allow-list
            },
            model="test",
            attempts=1,
        )


@pytest.mark.asyncio
async def test_persistent_failure_never_validates() -> None:
    """Demonstrates the DLQ-exit condition: no amount of retrying with feedback
    will save a response that's always semantically wrong."""
    llm: LLMClient = _AlwaysFailsLLM()
    for hint in (None, "first fail", "second fail"):
        r = await llm.extract(
            payload={},
            label="invoice",
            prompt="",
            json_schema=Invoice.model_json_schema(),
            validator_error_hint=hint,
        )
        with pytest.raises(ValidationError):
            Invoice.model_validate(r.data)
