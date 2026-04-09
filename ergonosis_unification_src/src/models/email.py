"""Canonical Email model (from Microsoft Graph)"""

from datetime import datetime, timezone
from typing import List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from src.utils.classification import pii_field, sensitive_field


class Email(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    # Required
    message_id: str = Field(..., description="Graph API unique message ID — primary Tier 1 key")
    received_at: datetime = Field(..., description="When the email was received")
    sender: str = pii_field(default=..., description="Sender email address")
    recipients: List[str] = pii_field(
        default=..., description="List of recipient email addresses", min_length=1
    )

    # Optional
    subject: Optional[str] = sensitive_field(default=None, description="Email subject — used in fuzzy matching", max_length=998)
    body_preview: Optional[str] = sensitive_field(
        default=None, description="First ~255 chars of body — V2 semantic matching", max_length=255
    )
    thread_id: Optional[str] = Field(None, description="Conversation thread grouping")

    # ETL-added
    source: Literal["microsoft_graph"] = Field(default="microsoft_graph")
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    raw_file_ref: Optional[str] = Field(None)

    @field_validator("sender", mode="before")
    @classmethod
    def sender_must_contain_at(cls, v):
        if v is None:
            return None
        if isinstance(v, str) and "@" not in v:
            import re as _re
            if _re.fullmatch(r"[0-9a-f]{64}", v):
                return v
            # Soft fail: invalid email format — set to None
            return None
        return v

    @field_validator("body_preview", mode="before")
    @classmethod
    def truncate_body_preview(cls, v):
        if v is None:
            return v
        if isinstance(v, str) and len(v) > 255:
            return v[:255]
        return v
