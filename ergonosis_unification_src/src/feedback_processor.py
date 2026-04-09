"""Feedback processor — applies auditing layer signals before each pipeline run."""

from dataclasses import dataclass

from src.constants import FeedbackSignal, UnmatchedReasonCode, EntityType
from src.models.links import LinkFeedback, UnmatchedEntity
from src.utils.errors import FeedbackProcessingError
from src.utils.logging import get_logger
from src.utils.metrics import feedback_latency_seconds, feedback_rejection_rate


@dataclass
class FeedbackResult:
    confirmed: int = 0
    rejected: int = 0
    flagged: int = 0
    errors: int = 0


class FeedbackProcessor:
    """
    Processes all unprocessed link_feedback records at the start of each pipeline run.

    Signal semantics:
    - CONFIRMED: link is correct — mark processed, no structural change.
    - REJECTED:  link is wrong — soft-delete the link, re-queue the source entity as
                 UnmatchedEntity(reason=NO_CANDIDATE_FOUND) so next run can re-attempt,
                 mark processed.
    - FLAGGED:   uncertain — log for investigation, mark processed. No structural change
                 in V1 (no automated resolution logic yet).

    Updates the feedback_rejection_rate Prometheus gauge after processing all signals.
    Errors on individual records are logged and counted but do not halt processing of
    remaining records. A FeedbackProcessingError is raised only on storage-level failure
    when fetching the feedback list itself.
    """

    def __init__(self, storage):
        self._storage = storage
        self._logger = get_logger(__name__)

    def process_all(self, run_id: str) -> FeedbackResult:
        """
        Process every unprocessed feedback signal in storage.

        Args:
            run_id: The current pipeline run ID (used when inserting re-queued unmatched records).

        Returns:
            FeedbackResult with counts of each outcome.

        Raises:
            FeedbackProcessingError: If the initial fetch of unprocessed feedback fails.
        """
        try:
            pending: list[LinkFeedback] = self._storage.get_unprocessed_feedback()
        except Exception as exc:
            raise FeedbackProcessingError(
                f"Failed to fetch unprocessed feedback: {exc}"
            ) from exc

        result = FeedbackResult()

        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)

        for feedback in pending:
            try:
                latency = (now - feedback.created_at.replace(tzinfo=timezone.utc) if feedback.created_at.tzinfo is None else now - feedback.created_at).total_seconds()
                feedback_latency_seconds.observe(latency)
            except Exception:
                pass
            try:
                if feedback.signal == FeedbackSignal.CONFIRMED:
                    self._handle_confirmed(feedback)
                    result.confirmed += 1
                elif feedback.signal == FeedbackSignal.REJECTED:
                    self._handle_rejected(feedback, run_id)
                    result.rejected += 1
                elif feedback.signal == FeedbackSignal.FLAGGED:
                    self._handle_flagged(feedback)
                    result.flagged += 1
            except Exception as exc:
                self._logger.error(
                    "Failed to process feedback record",
                    feedback_id=feedback.feedback_id,
                    signal=feedback.signal.value,
                    error=str(exc),
                )
                result.errors += 1

        # Update rejection rate metric
        total = result.confirmed + result.rejected + result.flagged
        if total > 0:
            feedback_rejection_rate.set(result.rejected / total)

        self._logger.info(
            "Feedback processing complete",
            confirmed=result.confirmed,
            rejected=result.rejected,
            flagged=result.flagged,
            errors=result.errors,
            run_id=run_id,
        )
        return result

    def _handle_confirmed(self, feedback: LinkFeedback) -> None:
        """Confirmed: link stands. Just mark processed."""
        self._storage.mark_feedback_processed(feedback.feedback_id)
        self._logger.info(
            "Feedback confirmed — link unchanged",
            feedback_id=feedback.feedback_id,
            link_id=feedback.link_id,
        )

    def _handle_rejected(self, feedback: LinkFeedback, run_id: str) -> None:
        """
        Rejected: soft-delete the link and re-queue the source entity.
        Steps:
        1. Fetch the link to get source_id + source_type.
        2. Soft-delete the link (is_current=0).
        3. Insert UnmatchedEntity so next run can re-attempt matching.
        4. Mark feedback processed.

        If the link no longer exists (already deleted), skip structural changes
        and just mark the feedback processed.
        """
        link = self._storage.get_link_by_id(feedback.link_id)

        if link is None:
            self._logger.warning(
                "Rejected feedback references non-existent link — marking processed",
                feedback_id=feedback.feedback_id,
                link_id=feedback.link_id,
            )
            self._storage.mark_feedback_processed(feedback.feedback_id)
            return

        # Soft-delete the link
        self._storage.soft_delete_link(link.link_id)

        # Re-queue the source entity for next run's matching
        unmatched = UnmatchedEntity(
            entity_id=link.source_id,
            entity_type=link.source_type,
            target_type=link.target_type,
            reason_code=UnmatchedReasonCode.NO_CANDIDATE_FOUND,
            run_id=run_id,
        )
        self._storage.insert_unmatched(unmatched)

        self._storage.mark_feedback_processed(feedback.feedback_id)
        self._logger.info(
            "Feedback rejected — link soft-deleted, source entity re-queued",
            feedback_id=feedback.feedback_id,
            link_id=feedback.link_id,
            source_id=link.source_id,
            source_type=link.source_type.value,
        )

    def _handle_flagged(self, feedback: LinkFeedback) -> None:
        """
        Flagged: uncertain — log for human/agent investigation.
        No structural change in V1. Mark processed so it isn't re-processed each run.
        """
        self._storage.mark_feedback_processed(feedback.feedback_id)
        self._logger.warning(
            "Feedback flagged — logged for investigation, no automated action in V1",
            feedback_id=feedback.feedback_id,
            link_id=feedback.link_id,
            reason=feedback.reason,
        )
