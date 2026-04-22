"""
MockLLM semantics. These are the behaviors the worker relies on; if they
drift, the reliability stack tests drift with them.
"""

from __future__ import annotations

import pytest

from src.llm.base import LLMError
from src.llm.mock import MockLLM


@pytest.mark.asyncio
async def test_classifies_shipment_like_payload() -> None:
    llm = MockLLM()
    result = await llm.classify(
        payload={"tracking_number": "ABC123", "status": "IN_TRANSIT"},
        allowed_labels=["shipment", "invoice", "unclassified"],
    )
    assert result.classification.label == "shipment"


@pytest.mark.asyncio
async def test_classifies_invoice_like_payload() -> None:
    llm = MockLLM()
    result = await llm.classify(
        payload={"invoice_id": "INV-1", "amount": 100.0, "currency": "USD"},
        allowed_labels=["shipment", "invoice", "unclassified"],
    )
    assert result.classification.label == "invoice"


@pytest.mark.asyncio
async def test_classifies_unknown_payload_as_unclassified() -> None:
    llm = MockLLM()
    result = await llm.classify(
        payload={"something": "else", "entirely": "different"},
        allowed_labels=["shipment", "invoice", "unclassified"],
    )
    assert result.classification.label == "unclassified"


@pytest.mark.asyncio
async def test_extracts_shipment_fields() -> None:
    llm = MockLLM()
    result = await llm.extract(
        payload={
            "vendor_id": "maersk",
            "tracking_number": "MSKU1",
            "status": "in_transit",
            "timestamp": "2026-04-21T00:00:00Z",
        },
        label="shipment",
        prompt="",
        json_schema={},
    )
    assert result.data["tracking_number"] == "MSKU1"
    assert result.data["vendor_id"] == "maersk"


@pytest.mark.asyncio
async def test_extracts_invoice_with_cents_conversion() -> None:
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
        json_schema={},
    )
    assert result.data["amount"] == pytest.approx(123.45)


@pytest.mark.asyncio
async def test_hallucinates_then_corrects_on_retry() -> None:
    """With hallucination_rate=1.0, first attempt is bad; retry (with
    validator_error_hint) always succeeds."""
    llm = MockLLM(hallucination_rate=1.0)
    bad = await llm.extract(
        payload={"invoice_id": "INV", "amount": 1, "currency": "USD", "vendor_id": "v"},
        label="invoice",
        prompt="",
        json_schema={},
    )
    assert bad.data["currency"] == "XYZ"  # hallucinated

    good = await llm.extract(
        payload={"invoice_id": "INV", "amount": 1, "currency": "USD", "vendor_id": "v"},
        label="invoice",
        prompt="",
        json_schema={},
        validator_error_hint="currency XYZ is not valid",
    )
    assert good.data["currency"] == "USD"  # corrected


@pytest.mark.asyncio
async def test_error_rate_raises() -> None:
    llm = MockLLM(error_rate=1.0)
    with pytest.raises(LLMError):
        await llm.classify(
            payload={"tracking_number": "x"},
            allowed_labels=["shipment", "unclassified"],
        )
