"""
Governed query interface for the auditing layer.

This is the ONLY entry point the downstream CrewAI auditing layer should use
to read from or write feedback to the unification store. Direct SQL queries
against the underlying tables (as in the legacy context_tools.py) bypass
the governing logic here and should be replaced with calls to this module.
"""

import hashlib
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple

from src.constants import FeedbackSignal, FeedbackSource
from src.models.audit import AccessAuditRecord
from src.models.links import AmbiguousMatch, EntityLink, LinkFeedback, RunLog, UnmatchedEntity
from src.utils.errors import ConsentRequiredError, StorageError
from src.utils.logging import get_logger


@dataclass
class LinkedBundle:
    """
    Wraps an EntityLink with convenience fields for the auditing layer.

    linked_entity_id and linked_entity_type always refer to the OTHER entity
    (not the one that was queried). This makes iteration over results simple:
    for bundle in get_linked_entities("txn_001", "transaction"):
        print(bundle.linked_entity_id, bundle.linked_entity_type, bundle.confidence)
    """
    link: EntityLink
    linked_entity_id: str
    linked_entity_type: str
    confidence: float
    match_tier: str


class UnifiedQueryInterface:
    """
    Public-facing query interface for the unification layer.

    Provides four operations for the auditing layer:
    - get_linked_entities: fetch all confirmed links for a given entity
    - get_unlinked_entities: fetch entities that fell through all matching tiers
    - get_ambiguous_matches: fetch matches awaiting human/agent resolution
    - write_feedback: submit a feedback signal (confirmed/rejected/flagged) on a link

    All reads are filtered to is_current=True links only.
    All writes go through validation before touching storage.
    """

    def __init__(self, storage):
        """
        Args:
            storage: LocalStore or DeltaClient instance from get_storage_backend().
        """
        self._storage = storage
        self._logger = get_logger(__name__)

    def _check_consent(self, user_id_hash: str) -> None:
        """Raise ConsentRequiredError if user has no active data_processing consent or has opted out."""
        if not self._storage.has_active_consent(user_id_hash, "data_processing"):
            raise ConsentRequiredError(
                f"No active data_processing consent for user {user_id_hash[:8]}…"
            )
        pref = self._storage.get_user_preference(user_id_hash)
        if pref and pref.get("opted_out"):
            raise ConsentRequiredError(
                f"User {user_id_hash[:8]}… has opted out of data processing."
            )

    def _log_access(self, operation: str, entity_id: str = None, entity_type: str = None, result_count: int = None) -> None:
        entity_id_hash = hashlib.sha256(entity_id.encode()).hexdigest()[:12] if entity_id else None
        try:
            self._storage.log_access(AccessAuditRecord(
                operation=operation,
                entity_id_hash=entity_id_hash,
                entity_type=entity_type,
                result_count=result_count,
            ))
        except Exception:
            pass

    def get_linked_entities(
        self, entity_id: str, entity_type: str, *, user_id_hash: str
    ) -> List[LinkedBundle]:
        """
        Returns all current links for the given entity (as source OR target).

        The linked_entity_id/type in each LinkedBundle is the OTHER end of the link
        (i.e., the entity this one is linked to), not the query entity itself.

        Args:
            entity_id: The ID of the entity (e.g. transaction_id, message_id, event_id).
            entity_type: One of "transaction", "email", "calendar_event".
            user_id_hash: SHA-256 hash of the user's email. Must have active data_processing consent.

        Returns:
            List of LinkedBundle. Empty list if no current links exist.

        Raises:
            ConsentRequiredError: If user has no active consent or has opted out.
            StorageError: On storage read failure.
        """
        self._check_consent(user_id_hash)
        try:
            links: List[EntityLink] = self._storage.get_linked_entities(entity_id, entity_type)
        except Exception as exc:
            raise StorageError(f"get_linked_entities failed: {exc}") from exc

        bundles = []
        for link in links:
            # Determine which side is the OTHER entity
            if link.source_id == entity_id and link.source_type.value == entity_type:
                other_id = link.target_id
                other_type = link.target_type.value
            else:
                other_id = link.source_id
                other_type = link.source_type.value

            bundles.append(LinkedBundle(
                link=link,
                linked_entity_id=other_id,
                linked_entity_type=other_type,
                confidence=link.confidence,
                match_tier=link.match_tier.value,
            ))

        entity_id_hash = hashlib.sha256(entity_id.encode()).hexdigest()[:12]
        self._logger.info(
            "get_linked_entities",
            entity_id=entity_id_hash,
            entity_type=entity_type,
            count=len(bundles),
        )
        self._log_access("get_linked_entities", entity_id=entity_id, entity_type=entity_type, result_count=len(bundles))
        return bundles

    def get_unlinked_entities(
        self,
        entity_type: str,
        date_range: Optional[Tuple[datetime, datetime]] = None,
        v2_processed: bool = False,
        *,
        user_id_hash: str,
    ) -> List[UnmatchedEntity]:
        """
        Returns entities that fell through all matching tiers (unmatched).

        These are the V2 input queue — entities embedding-based matching should
        attempt next. Use v2_processed=True to fetch records V2 has already handled.

        Args:
            entity_type: One of "transaction", "email", "calendar_event".
            date_range: Optional (start, end) datetime tuple to filter by logged_at.
                        Both bounds are inclusive.
            v2_processed: If True, return records V2 has processed. Default False.
            user_id_hash: SHA-256 hash of the user's email. Must have active data_processing consent.

        Returns:
            List of UnmatchedEntity. Empty list if none found.

        Raises:
            ConsentRequiredError: If user has no active consent or has opted out.
            StorageError: On storage read failure.
        """
        self._check_consent(user_id_hash)
        try:
            records: List[UnmatchedEntity] = self._storage.get_unmatched(
                entity_type=entity_type, v2_processed=v2_processed
            )
        except Exception as exc:
            raise StorageError(f"get_unlinked_entities failed: {exc}") from exc

        if date_range is not None:
            start, end = date_range
            # Normalize to offset-naive UTC for comparison if needed
            def _strip_tz(dt: datetime) -> datetime:
                return dt.replace(tzinfo=None) if dt.tzinfo else dt

            start_naive = _strip_tz(start)
            end_naive = _strip_tz(end)
            records = [
                r for r in records
                if start_naive <= _strip_tz(r.logged_at) <= end_naive
            ]

        self._logger.info(
            "get_unlinked_entities",
            entity_type=entity_type,
            count=len(records),
            date_range=str(date_range) if date_range else None,
        )
        self._log_access("get_unlinked_entities", entity_type=entity_type, result_count=len(records))
        return records

    def get_ambiguous_matches(self, status: str = "pending", *, user_id_hash: str) -> List[AmbiguousMatch]:
        """
        Returns ambiguous match records for auditing/resolution.

        Ambiguous matches are cases where multiple candidates scored above the
        fuzzy matching threshold — they need human or agent review to determine
        the correct link. This is the primary queue the auditing agents should
        consume for ambiguity resolution.

        Args:
            status: One of "pending", "resolved", "dismissed". Default "pending".
            user_id_hash: SHA-256 hash of the user's email. Must have active data_processing consent.

        Returns:
            List of AmbiguousMatch. Empty list if none found.

        Raises:
            ConsentRequiredError: If user has no active consent or has opted out.
            StorageError: On storage read failure.
        """
        self._check_consent(user_id_hash)
        try:
            records = self._storage.get_ambiguous(status=status)
        except Exception as exc:
            raise StorageError(f"get_ambiguous_matches failed: {exc}") from exc

        self._logger.info(
            "get_ambiguous_matches",
            status=status,
            count=len(records),
        )
        self._log_access("get_ambiguous_matches", result_count=len(records))
        return records

    def write_feedback(
        self,
        link_id: str,
        signal: str,
        source: str,
        reason: Optional[str] = None,
        *,
        user_id_hash: str,
    ) -> LinkFeedback:
        """
        Submit a feedback signal from the auditing layer to the unification store.

        This is the ONLY write path the auditing layer should use. Feedback is
        processed at the start of the next pipeline run by FeedbackProcessor.

        Args:
            link_id: The link_id of the EntityLink being evaluated.
            signal: "confirmed", "rejected", or "flagged".
            source: "autonomous" (agent) or "human".
            reason: Optional human-readable explanation.
            user_id_hash: SHA-256 hash of the user's email. Must have active data_processing consent.

        Returns:
            The created LinkFeedback record.

        Raises:
            ConsentRequiredError: If user has no active consent or has opted out.
            ValueError: If signal or source are not valid enum values.
            StorageError: On storage write failure.
        """
        self._check_consent(user_id_hash)
        # Validate against enums — raises ValueError for invalid values
        try:
            signal_enum = FeedbackSignal(signal)
        except ValueError:
            valid = [s.value for s in FeedbackSignal]
            raise ValueError(f"Invalid signal '{signal}'. Must be one of: {valid}")

        try:
            source_enum = FeedbackSource(source)
        except ValueError:
            valid = [s.value for s in FeedbackSource]
            raise ValueError(f"Invalid source '{source}'. Must be one of: {valid}")

        feedback = LinkFeedback(
            link_id=link_id,
            signal=signal_enum,
            source=source_enum,
            reason=reason,
        )

        try:
            self._storage.insert_feedback(feedback)
        except Exception as exc:
            raise StorageError(f"write_feedback failed: {exc}") from exc

        self._logger.info(
            "Feedback written",
            link_id=link_id,
            signal=signal,
            source=source,
            feedback_id=feedback.feedback_id,
        )
        self._log_access("write_feedback", entity_id=link_id)
        return feedback

    def get_last_run_status(self) -> Optional[RunLog]:
        """
        Returns the most recent run log entry regardless of status.
        Useful for the auditing layer to check if unification has completed
        before processing its results.

        Returns:
            RunLog if any runs exist, None otherwise.
        """
        try:
            result = self._storage.get_last_run()
        except Exception as exc:
            raise StorageError(f"get_last_run_status failed: {exc}") from exc
        self._log_access("get_last_run_status")
        return result

    def get_entity(self, entity_id: str, entity_type: str, *, user_id_hash: str):
        """
        Returns the raw canonical Silver record for an entity by ID.

        Enables auditing agents and downstream workflows to retrieve full entity
        content (subject, body_preview, merchant_name, etc.) without live API calls.

        Args:
            entity_id: The entity's primary key (transaction_id / message_id / event_id).
            entity_type: "transaction" | "email" | "calendar_event"
            user_id_hash: SHA-256 hash of user email. Must have active data_processing consent.

        Returns:
            Transaction, Email, or CalendarEvent Pydantic model, or None if not found.

        Raises:
            ConsentRequiredError: No active consent or user opted out.
            ValueError: Unrecognized entity_type.
            StorageError: Storage read failure.
        """
        self._check_consent(user_id_hash)
        if entity_type not in {"transaction", "email", "calendar_event"}:
            raise ValueError(
                f"Invalid entity_type '{entity_type}'. "
                "Must be one of: 'transaction', 'email', 'calendar_event'"
            )
        try:
            if entity_type == "transaction":
                result = self._storage.get_transaction(entity_id)
            elif entity_type == "email":
                result = self._storage.get_email(entity_id)
            else:
                result = self._storage.get_calendar_event(entity_id)
        except Exception as exc:
            raise StorageError(f"get_entity failed: {exc}") from exc
        self._log_access(
            "get_entity",
            entity_id=entity_id,
            entity_type=entity_type,
            result_count=1 if result is not None else 0,
        )
        return result

    def get_entities_by_type(self, entity_type: str, *, user_id_hash: str) -> list:
        """
        Returns all Silver records of the given entity_type.
        Only "email" and "calendar_event" are valid — use get_unlinked_entities()
        for transactions.
        """
        self._check_consent(user_id_hash)
        if entity_type not in {"email", "calendar_event"}:
            raise ValueError(
                f"get_entities_by_type supports 'email' and 'calendar_event' only, got '{entity_type}'"
            )
        try:
            if entity_type == "email":
                records = self._storage.get_all_emails()
            else:
                records = self._storage.get_all_calendar_events()
        except Exception as exc:
            raise StorageError(f"get_entities_by_type failed: {exc}") from exc
        self._log_access("get_entities_by_type", entity_type=entity_type, result_count=len(records))
        return records
