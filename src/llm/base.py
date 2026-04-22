"""
LLMClient protocol — the seam between our business logic and the LLM provider.

The worker depends only on this interface; swapping providers or using the mock
in tests does not touch the worker. See README §3.8.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


class LLMError(Exception):
    """Raised when the LLM call fails for any reason (network, provider 5xx,
    repeated schema-violating responses)."""


@dataclass(frozen=True)
class Classification:
    label: str
    confidence: float
    reason: str


@dataclass(frozen=True)
class ClassificationResult:
    classification: Classification
    model: str
    input_tokens: int | None = None
    output_tokens: int | None = None


@dataclass(frozen=True)
class ExtractionResult:
    data: dict[str, Any]
    model: str
    attempts: int
    input_tokens: int | None = None
    output_tokens: int | None = None


class LLMClient(Protocol):
    async def classify(
        self,
        *,
        payload: dict[str, Any],
        allowed_labels: list[str],
        vendor_hint: str | None = None,
    ) -> ClassificationResult: ...

    async def extract(
        self,
        *,
        payload: dict[str, Any],
        label: str,
        prompt: str,
        json_schema: dict[str, Any],
        vendor_hint: str | None = None,
        validator_error_hint: str | None = None,
    ) -> ExtractionResult: ...
