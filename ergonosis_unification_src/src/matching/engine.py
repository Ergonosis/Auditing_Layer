"""Three-tier cascade orchestrator for entity matching."""

from dataclasses import dataclass, field
from typing import List, Union

from src.constants import EntityType, UnmatchedReasonCode
from src.models.calendar_event import CalendarEvent
from src.models.email import Email
from src.models.links import AmbiguousMatch, EntityLink, UnmatchedEntity
from src.models.transaction import Transaction
from src.matching.tier1_exact import Tier1Matcher
from src.matching.tier2_composite import Tier2Matcher
from src.matching.tier3_fuzzy import Tier3Matcher
from src.utils.logging import get_logger
from src.utils.metrics import ambiguity_rate, tier_hit_rate, unmatched_rate


@dataclass
class MatchingResult:
    links: List[EntityLink]
    unmatched: List[UnmatchedEntity]
    ambiguous: List[AmbiguousMatch]
    run_id: str

    @property
    def total_entities(self) -> int:
        return len(self.links) + len(self.unmatched) + len(self.ambiguous)

    @property
    def match_rate(self) -> float:
        if self.total_entities == 0:
            return 0.0
        return len(self.links) / self.total_entities


class MatchingEngine:
    """
    Orchestrates the three-tier matching cascade.

    For each (source_entity, target_entity_pool, source_type, target_type):
    1. Run Tier 1 → if match, write EntityLink, exit cascade
    2. Run Tier 2 → if match, write EntityLink, exit cascade
    3. Run Tier 3 → returns EntityLink, AmbiguousMatch, or UnmatchedEntity
    4. Write result to appropriate output list

    Reads match rules from unification_config.yaml.
    Updates tier_hit_rate, unmatched_rate, ambiguity_rate metrics after each batch.
    """

    def __init__(self, config: dict, rule_version: str = "1.0"):
        self.config = config
        self.rule_version = rule_version
        self.tier1 = Tier1Matcher(config, rule_version=rule_version)
        self.tier2 = Tier2Matcher(config, rule_version=rule_version)
        self.tier3 = Tier3Matcher(config, rule_version=rule_version)
        self.logger = get_logger(__name__)

    def run_matching(
        self,
        transactions: List[Transaction],
        emails: List[Email],
        calendar_events: List[CalendarEvent],
        run_id: str,
    ) -> MatchingResult:
        """
        Run the full matching cascade for a batch of entities.
        Returns MatchingResult containing:
          - links: List[EntityLink]
          - unmatched: List[UnmatchedEntity]
          - ambiguous: List[AmbiguousMatch]

        In V1, runs these entity pairs:
          - transaction → email (each transaction matched against all emails)
          - transaction → calendar_event: the full T1→T2→T3 cascade runs when
            calendar_events is non-empty. In V1, fetch_calendar_events() always
            returns stub fixture data, so calendar matching executes against those
            fixtures. Production-quality calendar extraction (Microsoft Graph calendar
            extraction module) is deferred to V2. When calendar_events is empty
            (e.g. fetch explicitly returns []), all transactions are logged as
            unmatched for calendar with reason_code=NO_CANDIDATE_FOUND.

        Entities that were not the SOURCE of a match check are also checked as targets.
        An email with no matching transaction → UnmatchedEntity(entity_type=EMAIL).
        """
        # Deduplicate transactions by transaction_id
        seen_txn_ids: set = set()
        deduped_transactions: List[Transaction] = []
        for txn in transactions:
            if txn.transaction_id in seen_txn_ids:
                self.logger.warning(
                    "Duplicate transaction_id detected — skipping",
                    transaction_id=txn.transaction_id,
                )
                continue
            seen_txn_ids.add(txn.transaction_id)
            deduped_transactions.append(txn)

        links: List[EntityLink] = []
        unmatched: List[UnmatchedEntity] = []
        ambiguous: List[AmbiguousMatch] = []

        # Track which emails were matched (as a target)
        matched_email_ids: set = set()

        # transaction → email
        for txn in deduped_transactions:
            result = self._run_cascade_for_pair(
                source=txn,
                candidates=emails,
                source_type=EntityType.TRANSACTION,
                target_type=EntityType.EMAIL,
                run_id=run_id,
            )
            if isinstance(result, EntityLink):
                links.append(result)
                matched_email_ids.add(result.target_id)
            elif isinstance(result, AmbiguousMatch):
                ambiguous.append(result)
                matched_email_ids.update(result.candidate_ids)
            else:
                unmatched.append(result)

        # transaction → calendar_event (V1: deferred — log all as unmatched if no events)
        for txn in deduped_transactions:
            if not calendar_events:
                unmatched.append(
                    UnmatchedEntity(
                        entity_id=txn.transaction_id,
                        entity_type=EntityType.TRANSACTION,
                        target_type=EntityType.CALENDAR_EVENT,
                        reason_code=UnmatchedReasonCode.NO_CANDIDATE_FOUND,
                        run_id=run_id,
                    )
                )
            else:
                result = self._run_cascade_for_pair(
                    source=txn,
                    candidates=calendar_events,
                    source_type=EntityType.TRANSACTION,
                    target_type=EntityType.CALENDAR_EVENT,
                    run_id=run_id,
                )
                if isinstance(result, EntityLink):
                    links.append(result)
                elif isinstance(result, AmbiguousMatch):
                    ambiguous.append(result)
                else:
                    unmatched.append(result)

        # email → calendar_event (V1: unmatched-txn emails are matched against calendar events)
        # Emails already linked to a transaction are skipped.
        for email in emails:
            if email.message_id in matched_email_ids:
                continue
            if not calendar_events:
                continue
            result = self._run_cascade_for_pair(
                source=email,
                candidates=calendar_events,
                source_type=EntityType.EMAIL,
                target_type=EntityType.CALENDAR_EVENT,
                run_id=run_id,
            )
            if isinstance(result, EntityLink):
                links.append(result)
                matched_email_ids.add(email.message_id)
            elif isinstance(result, AmbiguousMatch):
                ambiguous.append(result)
                matched_email_ids.add(email.message_id)
            # UnmatchedEntity from email→calendar: leave email out of matched_email_ids
            # so it falls through to the unmatched loop below (recorded as email→transaction)

        # Emails with no matching transaction → UnmatchedEntity
        for email in emails:
            if email.message_id not in matched_email_ids:
                unmatched.append(
                    UnmatchedEntity(
                        entity_id=email.message_id,
                        entity_type=EntityType.EMAIL,
                        target_type=EntityType.TRANSACTION,
                        reason_code=UnmatchedReasonCode.NO_CANDIDATE_FOUND,
                        run_id=run_id,
                    )
                )

        result_obj = MatchingResult(
            links=links,
            unmatched=unmatched,
            ambiguous=ambiguous,
            run_id=run_id,
        )
        self._compute_metrics(result_obj)
        return result_obj

    def _run_cascade_for_pair(
        self,
        source: Union[Transaction, Email, CalendarEvent],
        candidates: List,
        source_type: EntityType,
        target_type: EntityType,
        run_id: str,
    ) -> Union[EntityLink, AmbiguousMatch, UnmatchedEntity]:
        """Run T1 → T2 → T3 for a single source entity against a pool of candidates."""
        source_id = getattr(source, self._id_field(source_type))

        if not candidates:
            return UnmatchedEntity(
                entity_id=source_id,
                entity_type=source_type,
                target_type=target_type,
                reason_code=UnmatchedReasonCode.NO_CANDIDATE_FOUND,
                run_id=run_id,
            )

        # Tier 1
        t1_result = self.tier1.match(source, candidates, source_type, target_type)
        if t1_result is not None:
            return t1_result

        # Tier 2
        t2_result = self.tier2.match(source, candidates, source_type, target_type)
        if t2_result is not None:
            return t2_result

        # Tier 3
        return self.tier3.match(source, candidates, source_type, target_type, run_id)

    def _compute_metrics(self, result: MatchingResult) -> None:
        """
        Update Prometheus gauges after a full matching run:
        - tier_hit_rate per tier per entity pair
        - unmatched_rate per entity type
        - ambiguity_rate per entity pair
        """
        total = result.total_entities
        if total == 0:
            return

        from src.constants import MatchTier

        # Tier hit rates
        for tier in (MatchTier.TIER1_EXACT, MatchTier.TIER2_COMPOSITE, MatchTier.TIER3_FUZZY):
            for pair_label in ("transaction_to_email", "transaction_to_calendar_event"):
                tier_links = [
                    lk for lk in result.links
                    if lk.match_tier == tier
                    and f"{lk.source_type.value}_to_{lk.target_type.value}" == pair_label
                ]
                pair_total = len([
                    lk for lk in result.links
                    if f"{lk.source_type.value}_to_{lk.target_type.value}" == pair_label
                ]) + len([
                    u for u in result.unmatched
                    if u.target_type is not None
                    and f"{u.entity_type.value}_to_{u.target_type.value}" == pair_label
                ])
                rate = len(tier_links) / pair_total if pair_total > 0 else 0.0
                tier_hit_rate.labels(tier=tier.value, entity_pair=pair_label).set(rate)

        # Unmatched rate per entity type
        for entity_type in EntityType:
            type_unmatched = [u for u in result.unmatched if u.entity_type == entity_type]
            rate = len(type_unmatched) / total
            unmatched_rate.labels(entity_type=entity_type.value).set(rate)

        # Ambiguity rate per entity pair
        for pair_label in ("transaction_to_email", "transaction_to_calendar_event"):
            pair_ambiguous = len([
                a for a in result.ambiguous
                if a.target_type is not None
                and f"{a.source_type.value}_to_{a.target_type.value}" == pair_label
            ]) if any(a.target_type is not None for a in result.ambiguous) else len(result.ambiguous)
            rate = pair_ambiguous / total
            ambiguity_rate.labels(entity_pair=pair_label).set(rate)

    def _id_field(self, entity_type: EntityType) -> str:
        return {
            EntityType.TRANSACTION: "transaction_id",
            EntityType.EMAIL: "message_id",
            EntityType.CALENDAR_EVENT: "event_id",
        }[entity_type]
