"""Unit tests for src/feedback_processor.py"""

import pytest

from tests.conftest import make_entity_link, make_feedback
from src.constants import EntityType, FeedbackSignal, FeedbackSource
from src.feedback_processor import FeedbackProcessor, FeedbackResult
from src.models.links import LinkFeedback

RUN_ID = "fb_run_001"


@pytest.fixture
def processor(tmp_db):
    return FeedbackProcessor(tmp_db)


def _seed_link(tmp_db):
    """Insert a link and return it."""
    link = make_entity_link()
    tmp_db.upsert_link(link)
    return link


def _seed_feedback(tmp_db, link_id, signal, source="autonomous"):
    fb = make_feedback(link_id=link_id, signal=signal, source=FeedbackSource(source))
    tmp_db.insert_feedback(fb)
    return fb


class TestFeedbackProcessor:
    def test_no_pending_feedback_returns_zeros(self, processor):
        result = processor.process_all(RUN_ID)
        assert result.confirmed == 0
        assert result.rejected == 0
        assert result.flagged == 0
        assert result.errors == 0

    def test_confirmed_marks_processed_link_unchanged(self, processor, tmp_db):
        link = _seed_link(tmp_db)
        _seed_feedback(tmp_db, link.link_id, FeedbackSignal.CONFIRMED)

        result = processor.process_all(RUN_ID)
        assert result.confirmed == 1

        # Link should still be current
        stored = tmp_db.get_link(link.source_id, link.target_id, is_current=True)
        assert stored is not None
        assert stored.is_current is True

        # Feedback should be processed
        pending = tmp_db.get_unprocessed_feedback()
        assert len(pending) == 0

    def test_rejected_soft_deletes_link_and_requeues(self, processor, tmp_db):
        link = _seed_link(tmp_db)
        _seed_feedback(tmp_db, link.link_id, FeedbackSignal.REJECTED)

        result = processor.process_all(RUN_ID)
        assert result.rejected == 1

        # Link should be soft-deleted
        current = tmp_db.get_link(link.source_id, link.target_id, is_current=True)
        assert current is None

        # Source entity should be re-queued as unmatched
        unmatched = tmp_db.get_unmatched(entity_type=link.source_type.value, v2_processed=False)
        assert any(u.entity_id == link.source_id for u in unmatched)

        # Feedback marked processed
        pending = tmp_db.get_unprocessed_feedback()
        assert len(pending) == 0

    def test_flagged_marks_processed_no_structural_change(self, processor, tmp_db):
        link = _seed_link(tmp_db)
        _seed_feedback(tmp_db, link.link_id, FeedbackSignal.FLAGGED)

        result = processor.process_all(RUN_ID)
        assert result.flagged == 1

        # Link is unchanged
        current = tmp_db.get_link(link.source_id, link.target_id, is_current=True)
        assert current is not None

        # Feedback marked processed
        pending = tmp_db.get_unprocessed_feedback()
        assert len(pending) == 0

    def test_mixed_signals_correct_counts(self, processor, tmp_db):
        link_a = make_entity_link(source_id="txn_a", target_id="msg_a", link_id="link_a")
        link_b = make_entity_link(source_id="txn_b", target_id="msg_b", link_id="link_b")
        link_c = make_entity_link(source_id="txn_c", target_id="msg_c", link_id="link_c")
        tmp_db.upsert_link(link_a)
        tmp_db.upsert_link(link_b)
        tmp_db.upsert_link(link_c)

        _seed_feedback(tmp_db, "link_a", FeedbackSignal.CONFIRMED)
        _seed_feedback(tmp_db, "link_b", FeedbackSignal.REJECTED)
        _seed_feedback(tmp_db, "link_c", FeedbackSignal.FLAGGED)

        result = processor.process_all(RUN_ID)
        assert result.confirmed == 1
        assert result.rejected == 1
        assert result.flagged == 1
        assert result.errors == 0

    def test_rejected_unknown_link_id_continues(self, processor, tmp_db):
        """Feedback for a non-existent link: logs warning, marks processed, errors=0."""
        fb = make_feedback(
            link_id="nonexistent_link",
            signal=FeedbackSignal.REJECTED,
        )
        tmp_db.insert_feedback(fb)

        result = processor.process_all(RUN_ID)
        # Should not crash; rejected count still increments (we processed it)
        assert result.rejected == 1
        assert result.errors == 0

        # Feedback marked processed
        pending = tmp_db.get_unprocessed_feedback()
        assert len(pending) == 0

    def test_returns_feedback_result_dataclass(self, processor):
        result = processor.process_all(RUN_ID)
        assert isinstance(result, FeedbackResult)
