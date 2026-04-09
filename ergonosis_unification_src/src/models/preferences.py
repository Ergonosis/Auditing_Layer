from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4
from pydantic import BaseModel, Field


class UserPreference(BaseModel):
    pref_id: str = Field(default_factory=lambda: str(uuid4()))
    user_id_hash: str = Field(...)
    opted_out: bool = Field(default=False)
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source: str = Field(default="api")
