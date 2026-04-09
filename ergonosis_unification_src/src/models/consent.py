from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4
from pydantic import BaseModel, Field


class UserConsent(BaseModel):
    consent_id: str = Field(default_factory=lambda: str(uuid4()))
    user_id_hash: str = Field(...)
    consent_type: str = Field(...)   # "data_processing" | "data_retention"
    granted: bool = Field(...)
    granted_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    expires_at: Optional[datetime] = Field(None)
    source: str = Field(...)         # "admin" | "api" | "stub"
    run_id: Optional[str] = Field(None)
