"""Tier 3: Fuzzy deterministic matching."""

from datetime import date, datetime
from typing import List, Optional, Union

from rapidfuzz import fuzz

from src.constants import (
    DEFAULT_CONFIDENCE_TIER3_MIN,
    EntityType,
    MatchTier,
    MatchType,
    UnmatchedReasonCode,
)
from src.models.calendar_event import CalendarEvent
from src.models.email import Email
from src.models.links import AmbiguousMatch, EntityLink, UnmatchedEntity
from src.models.transaction import Transaction
from src.matching.normalizer import (
    normalize_merchant_name,
    normalize_subject,
    date_within_window,
)


class Tier3Matcher:
    """
    Tier 3: Fuzzy deterministic matching.
    Uses rapidfuzz for string similarity + date window for temporal proximity.

    Outcomes:
    - Single candidate above min_similarity_score within date window → EntityLink
    - Multiple candidates above threshold → AmbiguousMatch (NOT a link)
    - No candidates → UnmatchedEntity

    Confidence = similarity score, clamped to [0.50, 0.84].

    Uses WRatio scorer from rapidfuzz for better handling of:
    - Partial string matches (e.g. "WHOLEFDS" matching "Whole Foods")
    - Transpositions
    - Different token orders
    """

    def __init__(self, config: dict, rule_version: str = "1.0"):
        self.config = config
        self.rule_version = rule_version

    def match(
        self,
        source: Union[Transaction, Email, CalendarEvent],
        candidates: List[Union[Transaction, Email, CalendarEvent]],
        source_type: EntityType,
        target_type: EntityType,
        run_id: str,
    ) -> Union[EntityLink, AmbiguousMatch, UnmatchedEntity]:
        """
        Run fuzzy matching cascade:
        1. For each candidate: compute similarity on configured fields, check date window
        2. Filter to candidates above min_similarity_score
        3. If 0 candidates → return UnmatchedEntity(reason_code=NO_CANDIDATE_FOUND)
        4. If 1 candidate → return EntityLink
        5. If >1 candidate → return AmbiguousMatch (all candidates + their scores)

        Linkage_key: "fuzzy:{field_used}:{score:.2f}"
        """
        source_id = getattr(source, self._id_field(source_type))

        if not candidates:
            return UnmatchedEntity(
                entity_id=source_id,
                entity_type=source_type,
                target_type=target_type,
                reason_code=UnmatchedReasonCode.NO_CANDIDATE_FOUND,
                run_id=run_id,
            )

        rule_key = f"{source_type.value}_to_{target_type.value}"
        rule = self.config.get("match_rules", {}).get(rule_key, {}).get("tier3_fuzzy", {})
        min_score = rule.get("min_similarity_score", 0.80)
        date_window = rule.get("date_window_days", 3)
        fields = rule.get("fields", [])
        field_map = rule.get("field_map", {})

        # Date-bucket pre-filter: discard candidates outside the date window before
        # any string scoring — avoids O(n×m) fuzzy comparisons on unrelated records.
        src_date = self._get_date_field(source, source_type)
        if src_date is not None:
            candidates = [
                c for c in candidates
                if self._get_date_field(c, target_type) is not None
                and date_within_window(src_date, self._get_date_field(c, target_type), date_window)
            ]

        if not candidates:
            return UnmatchedEntity(
                entity_id=source_id,
                entity_type=source_type,
                target_type=target_type,
                reason_code=UnmatchedReasonCode.NO_CANDIDATE_FOUND,
                run_id=run_id,
            )

        scored: List[tuple] = []  # (candidate, score, field_used)
        for candidate in candidates:
            result = self._compute_score(source, candidate, source_type, target_type, fields, date_window, field_map)
            if result is not None:
                score, field_used = result
                if score >= min_score:
                    scored.append((candidate, score, field_used))

        if len(scored) == 0:
            return UnmatchedEntity(
                entity_id=source_id,
                entity_type=source_type,
                target_type=target_type,
                reason_code=UnmatchedReasonCode.NO_CANDIDATE_FOUND,
                run_id=run_id,
            )

        if len(scored) == 1:
            candidate, score, field_used = scored[0]
            target_id = getattr(candidate, self._id_field(target_type))
            confidence = self._clamp_confidence(score)
            src_field, tgt_field = field_used
            linkage_key = (
                f"fuzzy:{src_field}->{tgt_field}:{score:.2f}"
                if src_field != tgt_field
                else f"fuzzy:{src_field}:{score:.2f}"
            )
            return EntityLink(
                source_id=source_id,
                target_id=target_id,
                source_type=source_type,
                target_type=target_type,
                match_type=MatchType.DETERMINISTIC,
                match_tier=MatchTier.TIER3_FUZZY,
                confidence=confidence,
                linkage_key=linkage_key,
                rule_version=self.rule_version,
            )

        # Multiple candidates above threshold → AmbiguousMatch
        candidate_ids = [getattr(c, self._id_field(target_type)) for c, _, _ in scored]
        candidate_scores = [s for _, s, _ in scored]
        return AmbiguousMatch(
            source_entity_id=source_id,
            source_type=source_type,
            target_type=target_type,
            candidate_ids=candidate_ids,
            candidate_scores=candidate_scores,
        )

    def _compute_score(
        self,
        source: Union[Transaction, Email, CalendarEvent],
        candidate: Union[Transaction, Email, CalendarEvent],
        source_type: EntityType,
        target_type: EntityType,
        fields: List[str],
        date_window: int,
        field_map: dict = None,
    ) -> Optional[tuple]:
        """
        Returns (similarity_score, (src_field, tgt_field)) or None if date window check fails.
        Steps:
        1. Check date window first (cheap reject)
        2. Try each configured source field in order (first non-None pair wins)
        3. Look up the target field via field_map (falls back to same field name)
        4. Normalize both strings
        5. Use rapidfuzz.fuzz.WRatio, divide by 100 to get 0.0–1.0
        """
        if field_map is None:
            field_map = {}

        src_date = self._get_date_field(source, source_type)
        tgt_date = self._get_date_field(candidate, target_type)
        if src_date is None or tgt_date is None:
            return None
        if not date_within_window(src_date, tgt_date, date_window):
            return None

        for src_field in fields:
            tgt_field = field_map.get(src_field, src_field)
            src_val = getattr(source, src_field, None)
            tgt_val = getattr(candidate, tgt_field, None)
            if src_val is None or tgt_val is None:
                continue

            src_norm = self._normalize_field(src_field, src_val)
            tgt_norm = self._normalize_field(tgt_field, tgt_val)
            if not src_norm or not tgt_norm:
                continue

            score = fuzz.WRatio(src_norm, tgt_norm) / 100.0
            return score, (src_field, tgt_field)

        return None

    def _normalize_field(self, field: str, value: str) -> str:
        if field in ("merchant_name", "name"):
            return normalize_merchant_name(value)
        if field == "subject":
            return normalize_subject(value)
        return value.lower().strip()

    def _get_date_field(self, entity, entity_type: EntityType):
        """Extract the relevant date from an entity for window comparison."""
        # Transaction → .date
        # Email → .received_at
        # CalendarEvent → .start_time
        if entity_type == EntityType.TRANSACTION:
            return getattr(entity, "date", None)
        elif entity_type == EntityType.EMAIL:
            return getattr(entity, "received_at", None)
        elif entity_type == EntityType.CALENDAR_EVENT:
            return getattr(entity, "start_time", None)
        return None

    def _clamp_confidence(self, score: float) -> float:
        """Apply floor of DEFAULT_CONFIDENCE_TIER3_MIN. No ceiling — score reflects actual similarity."""
        return max(DEFAULT_CONFIDENCE_TIER3_MIN, score)

    def _id_field(self, entity_type: EntityType) -> str:
        return {
            EntityType.TRANSACTION: "transaction_id",
            EntityType.EMAIL: "message_id",
            EntityType.CALENDAR_EVENT: "event_id",
        }[entity_type]
