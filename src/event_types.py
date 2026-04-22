"""
Built-in event-type registrations: shipments and invoices.

This module is imported once at app startup to populate EventTypeRegistry.
To add a new type tomorrow, create a new module alongside this one, define a
Pydantic schema + prompt + persister, and call EventTypeRegistry.register(...).
No other file needs to change.
"""

from __future__ import annotations

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.invoice import InvoiceRecord
from src.models.shipment import ShipmentRecord, ShipmentStatus
from src.registry import EventTypeRegistry
from src.schemas import Invoice, Shipment

# --------------------------------------------------------------------------- #
# Prompts
# --------------------------------------------------------------------------- #

SHIPMENT_PROMPT = """\
You are extracting a shipment update from an arbitrary vendor webhook payload.

Return JSON matching the provided schema exactly. Rules:

- `vendor_id`: the identifier the vendor uses for itself, or the vendor_id
  hint if the payload only identifies an account/customer.
- `tracking_number`: the carrier tracking number, awb, container number, or
  equivalent identifier. If multiple identifiers exist, prefer the one the
  carrier would recognize externally.
- `status`: one of TRANSIT, DELIVERED, EXCEPTION.
    * TRANSIT: picked up, in transit, out for delivery, shipped, label created.
    * DELIVERED: delivered, completed, signed for.
    * EXCEPTION: failed delivery, returned, damaged, lost, held by customs.
  If ambiguous, prefer TRANSIT over EXCEPTION.
- `timestamp`: the event time as an ISO-8601 datetime (UTC). If the payload
  gives a local time with a timezone, convert to UTC. If no timezone is given
  assume UTC. If no timestamp is present, use the payload's most recent
  event/update time.

Do not invent fields. If a required field genuinely cannot be found, you may
leave it empty and the response will fail validation — which is acceptable;
never fabricate.
"""

INVOICE_PROMPT = """\
You are extracting an invoice record from an arbitrary vendor webhook payload.

Return JSON matching the provided schema exactly. Rules:

- `vendor_id`: the identifier the vendor uses for itself.
- `invoice_id`: the vendor's invoice number / id / reference.
- `amount`: the total amount due as a number. If the payload shows amounts in
  minor units (cents), convert to the major unit (dollars). Do not include
  currency symbols or thousands separators.
- `currency`: an ISO-4217 three-letter code (e.g. USD, EUR, GBP). Uppercase.

Do not invent fields. If a required field genuinely cannot be found, you may
leave it empty and the response will fail validation — which is acceptable;
never fabricate.
"""


# --------------------------------------------------------------------------- #
# Persisters
# --------------------------------------------------------------------------- #


async def _persist_shipment(
    session: AsyncSession, raw_event_id: int, vendor_id: str, model: BaseModel
) -> None:
    assert isinstance(model, Shipment)
    row = ShipmentRecord(
        raw_event_id=raw_event_id,
        vendor_id=model.vendor_id or vendor_id,
        tracking_number=model.tracking_number,
        status=ShipmentStatus(model.status.value),
        timestamp=model.timestamp,
    )
    session.add(row)


async def _persist_invoice(
    session: AsyncSession, raw_event_id: int, vendor_id: str, model: BaseModel
) -> None:
    assert isinstance(model, Invoice)
    row = InvoiceRecord(
        raw_event_id=raw_event_id,
        vendor_id=model.vendor_id or vendor_id,
        invoice_id=model.invoice_id,
        amount=model.amount,
        currency=model.currency,
    )
    session.add(row)


# --------------------------------------------------------------------------- #
# Registrations (called once at startup from src.app)
# --------------------------------------------------------------------------- #


def register_default_event_types() -> None:
    """Idempotent: safe to call multiple times in tests."""
    if EventTypeRegistry.try_get("shipment") is None:
        EventTypeRegistry.register(
            label="shipment",
            schema=Shipment,
            table="shipments",
            prompt=SHIPMENT_PROMPT,
            persister=_persist_shipment,
        )
    if EventTypeRegistry.try_get("invoice") is None:
        EventTypeRegistry.register(
            label="invoice",
            schema=Invoice,
            table="invoices",
            prompt=INVOICE_PROMPT,
            persister=_persist_invoice,
        )
