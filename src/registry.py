"""
EventTypeRegistry — single source of truth for each event type the system
knows how to classify, extract, and persist.

Adding a new event type tomorrow is a ~30-line change:
  1. Define a Pydantic schema (src/schemas/<name>.py).
  2. Define an extraction prompt.
  3. Call EventTypeRegistry.register(...) once at startup.

The worker, classifier, and extractor are all type-agnostic. They look up the
entry from this registry at runtime. See README §3.6.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

# Label reserved for payloads that don't match any known type.
UNCLASSIFIED_LABEL = "unclassified"

# Type of a function that persists an extracted + validated model to its table.
# Signature: (session, raw_event_id, vendor_id, model) -> None
Persister = Callable[[AsyncSession, int, str, BaseModel], Awaitable[None]]


@dataclass(frozen=True)
class EventTypeEntry:
    label: str
    schema: type[BaseModel]
    table: str
    prompt: str
    persister: Persister

    def json_schema(self) -> dict[str, Any]:
        """JSON schema derived from the Pydantic model. Fed to the LLM provider
        as the structured-output target."""
        return self.schema.model_json_schema()


class _EventTypeRegistry:
    def __init__(self) -> None:
        self._entries: dict[str, EventTypeEntry] = {}

    def register(
        self,
        *,
        label: str,
        schema: type[BaseModel],
        table: str,
        prompt: str,
        persister: Persister,
    ) -> None:
        if label == UNCLASSIFIED_LABEL:
            raise ValueError(f"{UNCLASSIFIED_LABEL!r} is reserved")
        if label in self._entries:
            raise ValueError(f"event type {label!r} already registered")
        self._entries[label] = EventTypeEntry(
            label=label,
            schema=schema,
            table=table,
            prompt=prompt,
            persister=persister,
        )

    def get(self, label: str) -> EventTypeEntry:
        return self._entries[label]

    def try_get(self, label: str) -> EventTypeEntry | None:
        return self._entries.get(label)

    def labels(self) -> list[str]:
        """All known labels plus the reserved 'unclassified'. This is what the
        classifier is allowed to emit."""
        return [*self._entries.keys(), UNCLASSIFIED_LABEL]

    def known_labels(self) -> list[str]:
        return list(self._entries.keys())

    def clear(self) -> None:
        """For tests only."""
        self._entries.clear()


EventTypeRegistry = _EventTypeRegistry()
