"""Canonical Transaction model (from Plaid)"""

import math
import datetime as _dt
from datetime import date, datetime, timezone
from typing import List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from src.utils.classification import sensitive_field


class Transaction(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    # Required — SchemaValidationError if missing
    transaction_id: str = Field(..., description="Plaid stable unique ID — primary Tier 1 key")
    account_id: str = sensitive_field(default=..., description="Links transaction to the account")
    amount: float = Field(..., description="Positive = debit, negative = credit")
    date: _dt.date = Field(..., description="Transaction date (not settlement date)")

    # Optional — set to None if missing/invalid, log warning, continue
    merchant_name: Optional[str] = Field(None, description="Plaid-normalized merchant name — Tier 2 key")
    name: Optional[str] = Field(None, description="Raw institution description — Tier 2 fallback")
    payment_channel: Optional[str] = Field(None, description="e.g. 'in store', 'online', 'other'")
    category: Optional[List[str]] = Field(None, description="Plaid category hierarchy")

    # ETL-added — always set by transformer, never from source
    source: Literal["plaid"] = Field(default="plaid")
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    raw_file_ref: Optional[str] = Field(
        None,
        description=(
            "Cloud object path set by Databricks Auto Loader in prod. "
            "Always None in-memory/stub mode — never a local file path."
        ),
    )

    @field_validator("amount")
    @classmethod
    def amount_must_be_finite(cls, v: float) -> float:
        if not math.isfinite(v):
            raise ValueError("amount must be a finite float (NaN and inf are not allowed)")
        return v

    @field_validator("date", mode="before")
    @classmethod
    def coerce_date_string(cls, v):
        if isinstance(v, date):
            return v
        if isinstance(v, str):
            return date.fromisoformat(v)
        raise ValueError(f"Cannot coerce {type(v)} to date")
