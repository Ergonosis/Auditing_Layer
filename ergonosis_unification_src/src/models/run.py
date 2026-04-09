"""Watermark model for incremental run tracking"""

from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field

from src.constants import EntityType


class Watermark(BaseModel):
    """Tracks last successfully processed timestamp per entity type for incremental runs."""

    model_config = ConfigDict(str_strip_whitespace=True)

    entity_type: EntityType = Field(...)
    last_processed_at: datetime = Field(...)
    run_id: str = Field(..., description="Which run set this watermark")
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
