"""Pydantic schemas and their semantic post-checks (README §3.5 layer 4)."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from src.schemas import Invoice, Shipment, ShipmentStatus


class TestShipment:
    def test_happy_path(self) -> None:
        s = Shipment(
            vendor_id="maersk",
            tracking_number="MSKU1234567",
            status=ShipmentStatus.TRANSIT,
            timestamp=datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc),
        )
        assert s.status is ShipmentStatus.TRANSIT

    def test_status_alias_in_transit(self) -> None:
        s = Shipment.model_validate(
            {
                "vendor_id": "v",
                "tracking_number": "t",
                "status": "in_transit",
                "timestamp": "2026-04-21T12:00:00Z",
            }
        )
        assert s.status is ShipmentStatus.TRANSIT

    def test_status_alias_delivered_verbose(self) -> None:
        s = Shipment.model_validate(
            {
                "vendor_id": "v",
                "tracking_number": "t",
                "status": "completed",
                "timestamp": "2026-04-21T12:00:00Z",
            }
        )
        assert s.status is ShipmentStatus.DELIVERED

    def test_status_alias_exception(self) -> None:
        for s_in in ("failed", "lost", "damaged", "returned"):
            s = Shipment.model_validate(
                {
                    "vendor_id": "v",
                    "tracking_number": "t",
                    "status": s_in,
                    "timestamp": "2026-04-21T12:00:00Z",
                }
            )
            assert s.status is ShipmentStatus.EXCEPTION, f"{s_in} should map to EXCEPTION"

    def test_unknown_status_fails(self) -> None:
        with pytest.raises(ValidationError):
            Shipment.model_validate(
                {
                    "vendor_id": "v",
                    "tracking_number": "t",
                    "status": "looks like it's moving",  # the hallucination in MockLLM
                    "timestamp": "2026-04-21T12:00:00Z",
                }
            )

    def test_timestamp_parsed_from_various_formats(self) -> None:
        for ts in (
            "2026-04-21T12:00:00Z",
            "2026-04-21 12:00:00+00:00",
            "Tue, 21 Apr 2026 12:00:00 GMT",
        ):
            s = Shipment.model_validate(
                {
                    "vendor_id": "v",
                    "tracking_number": "t",
                    "status": "TRANSIT",
                    "timestamp": ts,
                }
            )
            assert s.timestamp is not None


class TestInvoice:
    def test_happy_path(self) -> None:
        i = Invoice(vendor_id="acme", invoice_id="INV-1", amount=100.5, currency="USD")
        assert i.currency == "USD"

    def test_currency_allowed_lowercase(self) -> None:
        i = Invoice.model_validate(
            {"vendor_id": "v", "invoice_id": "i", "amount": 1.0, "currency": "eur"}
        )
        assert i.currency == "EUR"

    def test_unknown_currency_fails(self) -> None:
        # The hallucination MockLLM simulates.
        with pytest.raises(ValidationError):
            Invoice.model_validate(
                {"vendor_id": "v", "invoice_id": "i", "amount": 1.0, "currency": "XYZ"}
            )

    def test_negative_amount_fails(self) -> None:
        with pytest.raises(ValidationError):
            Invoice.model_validate(
                {"vendor_id": "v", "invoice_id": "i", "amount": -1.0, "currency": "USD"}
            )

    def test_extra_field_forbidden(self) -> None:
        """extra='forbid' keeps hallucinated extra fields from leaking through."""
        with pytest.raises(ValidationError):
            Invoice.model_validate(
                {
                    "vendor_id": "v",
                    "invoice_id": "i",
                    "amount": 1.0,
                    "currency": "USD",
                    "bogus_field": "hallucinated",
                }
            )
