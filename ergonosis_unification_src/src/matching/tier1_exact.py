"""Tier 1: Exact key match."""

from typing import List, Optional, Tuple, Union

from src.constants import (
    DEFAULT_CONFIDENCE_TIER1,
    EntityType,
    MatchTier,
    MatchType,
)
from src.models.calendar_event import CalendarEvent
from src.models.email import Email
from src.models.links import EntityLink
from src.models.transaction import Transaction


class Tier1Matcher:
    """
    Tier 1: Exact key match.
    Checks for shared unique identifiers between entity pairs.
    A record exits the cascade immediately on a Tier 1 match.

    Match pairs and their Tier 1 keys (from unification_config.yaml):
    - transaction ↔ email: transaction_id == message_id
    - transaction ↔ calendar: transaction_id == event_id
    - email ↔ calendar: message_id == event_id

    In practice, Tier 1 matches are rare (would require the aggregation layer
    to embed cross-entity IDs). This tier exists for future-proofing when
    Plaid or Graph APIs add cross-references.
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
        Check if any candidate shares an exact key with source.
        Returns EntityLink on first match, None if no match found.
        Sets match_tier=TIER1_EXACT, match_type=DETERMINISTIC, confidence=1.0.
        """
        key_pairs = self._get_tier1_keys(source_type, target_type)
        if not key_pairs:
            return None

        for candidate in candidates:
            for source_field, target_field in key_pairs:
                source_val = self._get_field_value(source, source_field)
                target_val = self._get_field_value(candidate, target_field)
                if source_val is not None and target_val is not None and source_val == target_val:
                    source_id = self._get_field_value(source, self._id_field(source_type))
                    target_id = self._get_field_value(candidate, self._id_field(target_type))
                    return EntityLink(
                        source_id=source_id,
                        target_id=target_id,
                        source_type=source_type,
                        target_type=target_type,
                        match_type=MatchType.DETERMINISTIC,
                        match_tier=MatchTier.TIER1_EXACT,
                        confidence=DEFAULT_CONFIDENCE_TIER1,
                        linkage_key=f"{source_field}:{source_val}",
                        rule_version=self.rule_version,
                    )
        return None

    def _get_tier1_keys(
        self, source_type: EntityType, target_type: EntityType
    ) -> List[Tuple[str, str]]:
        """
        Read tier1_keys from config for the given entity pair.
        Returns list of (source_field, target_field) tuples to compare.
        """
        rule_key = self._rule_key(source_type, target_type)
        rule = self.config.get("match_rules", {}).get(rule_key)
        if not rule:
            return []
        # tier1_keys is a [source_field, target_field] pair in config, not a list of pairs.
        keys = rule.get("tier1_keys", [])
        if len(keys) >= 2:
            return [(keys[0], keys[1])]
        return []

    def _get_field_value(
        self, entity: Union[Transaction, Email, CalendarEvent], field: str
    ) -> Optional[str]:
        """Safe field access — returns None if field doesn't exist."""
        return getattr(entity, field, None)

    def _id_field(self, entity_type: EntityType) -> str:
        return {
            EntityType.TRANSACTION: "transaction_id",
            EntityType.EMAIL: "message_id",
            EntityType.CALENDAR_EVENT: "event_id",
        }[entity_type]

    def _rule_key(self, source_type: EntityType, target_type: EntityType) -> str:
        return f"{source_type.value}_to_{target_type.value}"
