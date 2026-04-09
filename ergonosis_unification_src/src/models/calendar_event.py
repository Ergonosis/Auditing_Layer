"""Canonical CalendarEvent model (from Microsoft Graph) — V1 schema only, matching deferred to V2"""

from datetime import datetime, timezone
from typing import List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from src.utils.classification import pii_field, sensitive_field


class CalendarEvent(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    # Required
    event_id: str = Field(..., description="Graph API unique event ID — primary Tier 1 key")
    start_time: datetime = Field(..., description="Event start — critical for temporal matching")
    end_time: datetime = Field(..., description="Event end")
    organizer: str = pii_field(default=..., description="Organizer email address")

    # Optional
    subject: Optional[str] = sensitive_field(default=None, description="Event title — used in fuzzy matching", max_length=998)
    attendees: Optional[List[str]] = pii_field(default=None, description="List of attendee email addresses")
    location: Optional[str] = Field(None, description="Physical or virtual location", max_length=500)

    # ETL-added
    source: Literal["microsoft_graph"] = Field(default="microsoft_graph")
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    raw_file_ref: Optional[str] = Field(None)

    @model_validator(mode="after")
    def end_time_must_not_precede_start_time(self) -> "CalendarEvent":
        if self.end_time < self.start_time:
            raise ValueError(
                f"end_time ({self.end_time}) must be >= start_time ({self.start_time})"
            )
        return self
