from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4
from pydantic import BaseModel, Field


class AccessAuditRecord(BaseModel):
    audit_id: str = Field(default_factory=lambda: str(uuid4()))
    event_time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    operation: str = Field(...)
    caller: str = Field(default="unknown")
    entity_id_hash: Optional[str] = Field(None)
    entity_type: Optional[str] = Field(None)
    run_id: Optional[str] = Field(None)
    result_count: Optional[int] = Field(None)
    success: bool = Field(default=True)
