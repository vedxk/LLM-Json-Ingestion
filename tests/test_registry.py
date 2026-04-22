"""
EventTypeRegistry: adding a new event type is data, not code.

This test mimics the README §3.6 scenario: add a `CustomsDeclaration` type
and verify the worker's dispatch path would find it without any other change.
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from src.registry import UNCLASSIFIED_LABEL, EventTypeRegistry


@pytest.fixture(autouse=True)
def _clean_registry() -> object:
    EventTypeRegistry.clear()
    yield
    EventTypeRegistry.clear()


class CustomsDeclaration(BaseModel):
    vendor_id: str
    declaration_id: str
    country_of_origin: str
    hs_code: str


async def _noop_persister(session, raw_event_id, vendor_id, model) -> None:  # type: ignore[no-untyped-def]
    return None


def test_register_new_type_is_listed() -> None:
    EventTypeRegistry.register(
        label="customs_declaration",
        schema=CustomsDeclaration,
        table="customs_declarations",
        prompt="Extract a customs declaration...",
        persister=_noop_persister,
    )
    labels = EventTypeRegistry.labels()
    assert "customs_declaration" in labels
    assert UNCLASSIFIED_LABEL in labels


def test_get_returns_entry() -> None:
    EventTypeRegistry.register(
        label="customs_declaration",
        schema=CustomsDeclaration,
        table="customs_declarations",
        prompt="Extract...",
        persister=_noop_persister,
    )
    entry = EventTypeRegistry.get("customs_declaration")
    assert entry.table == "customs_declarations"
    assert entry.schema is CustomsDeclaration


def test_json_schema_is_derived_from_pydantic() -> None:
    EventTypeRegistry.register(
        label="customs_declaration",
        schema=CustomsDeclaration,
        table="customs_declarations",
        prompt="Extract...",
        persister=_noop_persister,
    )
    schema = EventTypeRegistry.get("customs_declaration").json_schema()
    assert "properties" in schema
    for field in ("vendor_id", "declaration_id", "country_of_origin", "hs_code"):
        assert field in schema["properties"]


def test_duplicate_registration_rejected() -> None:
    EventTypeRegistry.register(
        label="customs_declaration",
        schema=CustomsDeclaration,
        table="customs_declarations",
        prompt="Extract...",
        persister=_noop_persister,
    )
    with pytest.raises(ValueError):
        EventTypeRegistry.register(
            label="customs_declaration",
            schema=CustomsDeclaration,
            table="customs_declarations",
            prompt="Extract...",
            persister=_noop_persister,
        )


def test_reserved_label_rejected() -> None:
    with pytest.raises(ValueError):
        EventTypeRegistry.register(
            label=UNCLASSIFIED_LABEL,
            schema=CustomsDeclaration,
            table="x",
            prompt="x",
            persister=_noop_persister,
        )
