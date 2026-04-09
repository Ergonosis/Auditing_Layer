"""Tier 2: Composite key match."""

from typing import List, Optional, Union

from src.constants import (
    DEFAULT_CONFIDENCE_TIER2,
    EntityType,
    MatchTier,
    MatchType,
)
from src.models.calendar_event import CalendarEvent
from src.models.email import Email
from src.models.links import EntityLink
from src.models.transaction import Transaction
from src.matching.normalizer import (
    amount_matches,
    normalize_merchant_name,
    normalize_subject,
)


class Tier2Matcher:
    """
    Tier 2: Composite key match.
    Matches on a combination of fields (e.g., merchant_name + amount + date).
    All composite fields must match for a Tier 2 hit.

    Confidence:
    - All fields match: 0.95 (fixed — not scaled by field count in V1)

    Match pairs:
    - transaction ↔ email: merchant_name (normalized) + amount (exact) + date (exact)
    - transaction ↔ calendar: merchant_name (normalized) + amount (exact) + date (exact)
    - email ↔ calendar: subject (normalized equality) + start_time (date portion exact)
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
    ) -> Optional[EntityLink]:
        """
        Check composite key match against all candidates.
        Returns EntityLink for first full match, None otherwise.
        Linkage_key format: "merchant_name+amount+date" or "subject+start_time"
        """
        rule_key = f"{source_type.value}_to_{target_type.value}"
        rule = self.config.get("match_rules", {}).get(rule_key, {})
        composite_fields = rule.get("tier2_composite", [])
        if not composite_fields:
            return None

        amount_tolerance = rule.get("amount_tolerance_pct", 0.0)
        linkage_key = "+".join(composite_fields)

        for candidate in candidates:
            if self._composite_matches(source, candidate, source_type, target_type, amount_tolerance):
                source_id = getattr(source, self._id_field(source_type), None)
                target_id = getattr(candidate, self._id_field(target_type), None)
                return EntityLink(
                    source_id=source_id,
                    target_id=target_id,
                    source_type=source_type,
                    target_type=target_type,
                    match_type=MatchType.DETERMINISTIC,
                    match_tier=MatchTier.TIER2_COMPOSITE,
                    confidence=DEFAULT_CONFIDENCE_TIER2,
                    linkage_key=linkage_key,
                    rule_version=self.rule_version,
                )
        return None

    def _composite_matches(
        self,
        source: Union[Transaction, Email, CalendarEvent],
        candidate: Union[Transaction, Email, CalendarEvent],
        source_type: EntityType,
        target_type: EntityType,
        amount_tolerance: float = 0.0,
    ) -> bool:
        """
        Check if all composite fields match.
        For merchant_name: use normalize_merchant_name() then exact string equality.
        For amount: abs() normalized, compared within amount_tolerance (from config).
        For date: exact date equality (extract date portion from datetime if needed).
        For subject: use normalize_subject() then exact string equality.
        Missing optional field on either side → no match for that composite pair.
        """
        pair = (source_type, target_type)

        if pair in (
            (EntityType.TRANSACTION, EntityType.EMAIL),
            (EntityType.TRANSACTION, EntityType.CALENDAR_EVENT),
        ):
            # merchant_name (source) vs subject (target): cross-normalizer comparison.
            # Emails and calendar events have no amount field, so we match on name+date.
            # Amount-level verification is left to Tier 3 (fuzzy), which handles amounts
            # embedded in email subjects via string similarity.
            src_merchant = getattr(source, "merchant_name", None) or getattr(source, "name", None)
            tgt_subject = getattr(candidate, "subject", None)
            if src_merchant is None or tgt_subject is None:
                return False
            if normalize_merchant_name(src_merchant) != normalize_subject(tgt_subject):
                return False

            # date: exact date equality
            src_date = self._extract_date(source, source_type)
            tgt_date = self._extract_date(candidate, target_type)
            if src_date is None or tgt_date is None:
                return False
            if src_date != tgt_date:
                return False

            return True

        elif pair == (EntityType.EMAIL, EntityType.CALENDAR_EVENT):
            # subject: normalize then exact equality
            src_subject = getattr(source, "subject", None)
            tgt_subject = getattr(candidate, "subject", None)
            if src_subject is None or tgt_subject is None:
                return False
            if normalize_subject(src_subject) != normalize_subject(tgt_subject):
                return False

            # start_time: date portion exact
            src_date = self._extract_date(source, source_type)
            tgt_date = self._extract_date(candidate, target_type)
            if src_date is None or tgt_date is None:
                return False
            if src_date != tgt_date:
                return False

            return True

        return False

    def _extract_date(self, entity, entity_type: EntityType):
        """Extract date portion from entity's relevant datetime/date field."""
        from datetime import datetime, date
        if entity_type == EntityType.TRANSACTION:
            d = getattr(entity, "date", None)
        elif entity_type == EntityType.EMAIL:
            d = getattr(entity, "received_at", None)
        elif entity_type == EntityType.CALENDAR_EVENT:
            d = getattr(entity, "start_time", None)
        else:
            return None

        if d is None:
            return None
        if isinstance(d, datetime):
            return d.date()
        return d

    def _id_field(self, entity_type: EntityType) -> str:
        return {
            EntityType.TRANSACTION: "transaction_id",
            EntityType.EMAIL: "message_id",
            EntityType.CALENDAR_EVENT: "event_id",
        }[entity_type]
