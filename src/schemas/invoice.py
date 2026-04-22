from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, field_validator

# ISO-4217 currency allow-list. Not exhaustive — covers the common ones we expect
# from logistics/financial vendors. Extend as new vendors onboard.
_ISO_4217: frozenset[str] = frozenset(
    {
        "USD", "EUR", "GBP", "JPY", "CNY", "AUD", "CAD", "CHF", "HKD", "SGD",
        "INR", "KRW", "SEK", "NOK", "DKK", "NZD", "MXN", "BRL", "ZAR", "AED",
        "SAR", "TRY", "THB", "MYR", "IDR", "PHP", "VND", "TWD", "ILS", "PLN",
        "CZK", "HUF", "RON", "RUB",
    }
)


class Invoice(BaseModel):
    """Normalized invoice record. This is the extraction target schema."""

    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    vendor_id: str = Field(..., min_length=1, max_length=100)
    invoice_id: str = Field(..., min_length=1, max_length=200)
    amount: float = Field(..., ge=0.0)
    currency: str = Field(..., min_length=3, max_length=3)

    @field_validator("currency", mode="before")
    @classmethod
    def _validate_currency(cls, v: object) -> object:
        if isinstance(v, str):
            code = v.strip().upper()
            if code not in _ISO_4217:
                raise ValueError(f"unknown currency {code!r} (not in ISO-4217 allow-list)")
            return code
        return v
