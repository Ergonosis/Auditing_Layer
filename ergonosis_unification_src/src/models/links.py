"""Output table models for the five pipeline output tables"""

from datetime import datetime, timezone
from typing import List, Optional
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator  # noqa: F401 — Field used directly on most fields

from src.utils.classification import sensitive_field

from src.constants import (
    AmbiguityStatus,
    EntityType,
    FeedbackSignal,
    FeedbackSource,
    MatchTier,
    MatchType,
    ResolvedBy,
    RunStatus,
    RunType,
    UnmatchedReasonCode,
)


class EntityLink(BaseModel):
    """Primary output: confirmed link between two entities."""

    model_config = ConfigDict(str_strip_whitespace=True)

    link_id: str = Field(default_factory=lambda: str(uuid4()))
    source_id: str = Field(...)
    target_id: str = Field(...)
    source_type: EntityType = Field(...)
    target_type: EntityType = Field(...)
    match_type: MatchType = Field(...)
    match_tier: MatchTier = Field(..., description="Which tier produced the match")
    confidence: float = Field(..., ge=0.0, le=1.0)
    linkage_key: str = sensitive_field(
        default=..., description="Field(s) that triggered the match, e.g. 'transaction_id:msg_abc'", max_length=500
    )
    rationale: Optional[str] = sensitive_field(
        default=None, description="Human-readable explanation (V2 populates this)", max_length=2000
    )
    rule_version: str = Field(default="1.0")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    effective_from: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    effective_to: Optional[datetime] = Field(None, description="Null = currently active link")
    is_current: bool = Field(default=True)
    superseded_by_link_id: Optional[str] = Field(None)
    superseded_in_run_id: Optional[str] = Field(None)


class UnmatchedEntity(BaseModel):
    """Records that fell through all tiers. Natural V2 input queue."""

    model_config = ConfigDict(str_strip_whitespace=True)

    entity_id: str = Field(...)
    entity_type: EntityType = Field(...)
    target_type: Optional[EntityType] = Field(
        None, description="Which target pool this entity was unmatched against"
    )
    reason_code: UnmatchedReasonCode = Field(...)
    run_id: str = Field(...)
    logged_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    v2_processed: bool = Field(default=False)


class AmbiguousMatch(BaseModel):
    """Records with multiple candidates above threshold. Auditing layer resolves these."""

    model_config = ConfigDict(str_strip_whitespace=True)

    ambiguity_id: str = Field(default_factory=lambda: str(uuid4()))
    source_entity_id: str = Field(...)
    source_type: Optional[EntityType] = Field(None)
    target_type: Optional[EntityType] = Field(None)
    candidate_ids: List[str] = Field(
        ..., min_length=2, description="Must have at least 2 candidates to be ambiguous"
    )
    candidate_scores: List[float] = Field(...)
    status: AmbiguityStatus = Field(default=AmbiguityStatus.PENDING)
    resolved_link_id: Optional[str] = Field(None)
    resolved_by: Optional[ResolvedBy] = Field(None)
    logged_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("candidate_scores", mode="after")
    @classmethod
    def scores_must_be_in_range(cls, v):
        for score in v:
            if not (0.0 <= score <= 1.0):
                raise ValueError(f"candidate_score {score} out of bounds [0.0, 1.0]")
        return v

    @model_validator(mode="after")
    def candidate_lists_must_align(self) -> "AmbiguousMatch":
        if len(self.candidate_ids) != len(self.candidate_scores):
            raise ValueError(
                f"candidate_ids length ({len(self.candidate_ids)}) must equal "
                f"candidate_scores length ({len(self.candidate_scores)})"
            )
        return self


class LinkFeedback(BaseModel):
    """Feedback written by auditing layer or human reviewers to improve link quality."""

    model_config = ConfigDict(str_strip_whitespace=True)

    feedback_id: str = Field(default_factory=lambda: str(uuid4()))
    link_id: str = Field(...)
    signal: FeedbackSignal = Field(...)
    source: FeedbackSource = Field(...)
    reason: Optional[str] = Field(None, max_length=1000)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    processed: bool = Field(default=False, description="Whether next unification run has acted on this")


class RunLog(BaseModel):
    """Status record written for each pipeline run."""

    model_config = ConfigDict(str_strip_whitespace=True)

    run_id: str = Field(default_factory=lambda: str(uuid4()))
    run_type: RunType = Field(...)
    status: RunStatus = Field(default=RunStatus.SUCCESS)
    start_time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    end_time: Optional[datetime] = Field(None)
    records_processed: int = Field(default=0)
    links_created: int = Field(default=0)
    unmatched_count: int = Field(default=0)
    ambiguous_count: int = Field(default=0)
    failure_reason: Optional[str] = Field(None, max_length=500)
