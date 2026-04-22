"""Prompt templates shared by classifier and extractor. See README §3.4–§3.5."""

from __future__ import annotations

import json
from typing import Any

CLASSIFIER_SYSTEM = """\
You are a webhook payload classifier for a supply-chain platform.

Given a vendor's raw JSON payload, decide which known event type it represents.
Return JSON of the exact shape:

  { "label": "<one of the allowed labels>",
    "confidence": <float between 0 and 1>,
    "reason": "<1-2 sentence justification>" }

Rules:
- If the payload is clearly one of the known types, return that label with high
  confidence (> 0.8).
- If the payload does not match any known type, return the literal label
  "unclassified".
- If the payload is ambiguous, return "unclassified" — do not guess.
- Never invent labels. Only the provided allowed_labels are valid.
"""


def classifier_user_prompt(
    *,
    payload: dict[str, Any],
    allowed_labels: list[str],
    vendor_hint: str | None,
) -> str:
    hint_block = f"Vendor hint: {vendor_hint}\n\n" if vendor_hint else ""
    return (
        f"{hint_block}"
        f"Allowed labels: {allowed_labels}\n\n"
        f"Payload:\n```json\n{json.dumps(payload, indent=2, default=str)}\n```"
    )


def extractor_user_prompt(
    *,
    payload: dict[str, Any],
    type_prompt: str,
    vendor_hint: str | None,
    validator_error_hint: str | None,
) -> str:
    parts: list[str] = []
    if vendor_hint:
        parts.append(f"Vendor hint: {vendor_hint}")
    parts.append(type_prompt.strip())
    if validator_error_hint:
        parts.append(
            "Your previous response failed validation. Fix the error and "
            "return a valid response:\n" + validator_error_hint
        )
    parts.append(
        "Payload:\n```json\n" + json.dumps(payload, indent=2, default=str) + "\n```"
    )
    return "\n\n".join(parts)


CLASSIFIER_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["label", "confidence", "reason"],
    "properties": {
        "label": {"type": "string"},
        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
        "reason": {"type": "string"},
    },
}
