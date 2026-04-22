"""
OpenAI adapter. Uses the Responses API's structured-output mode via
`response_format={"type": "json_schema", ...}` so the provider enforces our
JSON shape server-side. See README §3.5 layer 1.

Schema constraints for OpenAI structured outputs:
- `additionalProperties` must be False on object schemas.
- All properties must be listed in `required`.
- Schema name must be a-z/A-Z/0-9/_.
We post-process the Pydantic-emitted JSON schema to meet these constraints.
"""

from __future__ import annotations

import json
from copy import deepcopy
from typing import Any

from openai import AsyncOpenAI, OpenAIError

from src.llm.base import (
    Classification,
    ClassificationResult,
    ExtractionResult,
    LLMError,
)
from src.llm.prompts import (
    CLASSIFIER_JSON_SCHEMA,
    CLASSIFIER_SYSTEM,
    classifier_user_prompt,
    extractor_user_prompt,
)


def _tighten_schema_for_openai(schema: dict[str, Any]) -> dict[str, Any]:
    """Make a JSON schema satisfy OpenAI structured-output constraints."""
    s = deepcopy(schema)

    def _walk(node: Any) -> Any:
        if isinstance(node, dict):
            if node.get("type") == "object":
                node["additionalProperties"] = False
                props = node.get("properties", {})
                if props:
                    node["required"] = list(props.keys())
            for v in node.values():
                _walk(v)
        elif isinstance(node, list):
            for item in node:
                _walk(item)
        return node

    _walk(s)
    # OpenAI requires a top-level `additionalProperties: false` for object schemas.
    if s.get("type") == "object":
        s["additionalProperties"] = False
    return s


class OpenAILLM:
    def __init__(
        self,
        *,
        api_key: str,
        classifier_model: str = "gpt-4o-mini",
        extractor_model: str = "gpt-4o",
        request_timeout_s: float = 30.0,
    ) -> None:
        self._client = AsyncOpenAI(api_key=api_key, timeout=request_timeout_s)
        self._classifier_model = classifier_model
        self._extractor_model = extractor_model

    async def classify(
        self,
        *,
        payload: dict[str, Any],
        allowed_labels: list[str],
        vendor_hint: str | None = None,
    ) -> ClassificationResult:
        try:
            response = await self._client.chat.completions.create(
                model=self._classifier_model,
                messages=[
                    {"role": "system", "content": CLASSIFIER_SYSTEM},
                    {
                        "role": "user",
                        "content": classifier_user_prompt(
                            payload=payload,
                            allowed_labels=allowed_labels,
                            vendor_hint=vendor_hint,
                        ),
                    },
                ],
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": "classification",
                        "strict": True,
                        "schema": _tighten_schema_for_openai(CLASSIFIER_JSON_SCHEMA),
                    },
                },
                temperature=0.0,
            )
        except OpenAIError as e:
            raise LLMError(f"classification OpenAI call failed: {e}") from e

        content = response.choices[0].message.content
        if not content:
            raise LLMError("classification response was empty")
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError as e:
            raise LLMError(f"classification response was not valid JSON: {e}") from e

        label = parsed.get("label")
        if label not in allowed_labels:
            raise LLMError(f"classification returned unknown label {label!r}")

        usage = response.usage
        return ClassificationResult(
            classification=Classification(
                label=str(label),
                confidence=float(parsed.get("confidence", 0.0)),
                reason=str(parsed.get("reason", "")),
            ),
            model=self._classifier_model,
            input_tokens=usage.prompt_tokens if usage else None,
            output_tokens=usage.completion_tokens if usage else None,
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
        tightened = _tighten_schema_for_openai(json_schema)
        schema_name = f"extract_{label}"[:64]
        try:
            response = await self._client.chat.completions.create(
                model=self._extractor_model,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are an extraction engine. Return JSON matching "
                            "the provided schema exactly. Do not fabricate fields."
                        ),
                    },
                    {
                        "role": "user",
                        "content": extractor_user_prompt(
                            payload=payload,
                            type_prompt=prompt,
                            vendor_hint=vendor_hint,
                            validator_error_hint=validator_error_hint,
                        ),
                    },
                ],
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": schema_name,
                        "strict": True,
                        "schema": tightened,
                    },
                },
                temperature=0.0,
            )
        except OpenAIError as e:
            raise LLMError(f"extraction OpenAI call failed: {e}") from e

        content = response.choices[0].message.content
        if not content:
            raise LLMError("extraction response was empty")
        try:
            data = json.loads(content)
        except json.JSONDecodeError as e:
            raise LLMError(f"extraction response was not valid JSON: {e}") from e

        usage = response.usage
        return ExtractionResult(
            data=data,
            model=self._extractor_model,
            attempts=1,
            input_tokens=usage.prompt_tokens if usage else None,
            output_tokens=usage.completion_tokens if usage else None,
        )
