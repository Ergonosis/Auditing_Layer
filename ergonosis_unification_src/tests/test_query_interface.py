"""Unit tests for src/query_interface.py"""

import os
from datetime import datetime, timezone, timedelta

import pytest

from tests.conftest import make_entity_link, make_feedback, make_transaction, make_email, make_calendar_event, TEST_USER_HASH
from src.models.consent import UserConsent
from src.constants import AmbiguityStatus, EntityType, FeedbackSignal, FeedbackSource, RunType, UnmatchedReasonCode
from src.models.links import AmbiguousMatch, UnmatchedEntity
from src.query_interface import LinkedBundle, UnifiedQueryInterface

RUN_ID = "qi_run_001"


class TestGetLinkedEntities:
    def test_returns_bundle_for_source(self, query_interface, tmp_db):
        link = make_entity_link(source_id="txn_001", target_id="msg_001")
        tmp_db.upsert_link(link)

        bundles = query_interface.get_linked_entities("txn_001", "transaction", user_id_hash=TEST_USER_HASH)
        assert len(bundles) == 1
        assert isinstance(bundles[0], LinkedBundle)
        assert bundles[0].linked_entity_id == "msg_001"
        assert bundles[0].linked_entity_type == "email"
        assert bundles[0].confidence == link.confidence

    def test_bidirectional_target_query(self, query_interface, tmp_db):
        link = make_entity_link(source_id="txn_001", target_id="msg_001")
        tmp_db.upsert_link(link)

        # Query from the target side
        bundles = query_interface.get_linked_entities("msg_001", "email", user_id_hash=TEST_USER_HASH)
        assert len(bundles) == 1
        assert bundles[0].linked_entity_id == "txn_001"
        assert bundles[0].linked_entity_type == "transaction"

    def test_empty_when_no_links(self, query_interface):
        bundles = query_interface.get_linked_entities("nonexistent", "transaction", user_id_hash=TEST_USER_HASH)
        assert bundles == []

    def test_soft_deleted_link_not_returned(self, query_interface, tmp_db):
        link = make_entity_link()
        tmp_db.upsert_link(link)
        tmp_db.soft_delete_link(link.link_id)

        bundles = query_interface.get_linked_entities(link.source_id, "transaction", user_id_hash=TEST_USER_HASH)
        assert bundles == []

    def test_match_tier_in_bundle(self, query_interface, tmp_db):
        link = make_entity_link()
        tmp_db.upsert_link(link)
        bundles = query_interface.get_linked_entities(link.source_id, "transaction", user_id_hash=TEST_USER_HASH)
        assert bundles[0].match_tier == link.match_tier.value


class TestGetUnlinkedEntities:
    def test_returns_unmatched_by_type(self, query_interface, tmp_db):
        record = UnmatchedEntity(
            entity_id="txn_001",
            entity_type=EntityType.TRANSACTION,
            reason_code=UnmatchedReasonCode.NO_CANDIDATE_FOUND,
            run_id=RUN_ID,
        )
        tmp_db.insert_unmatched(record)

        results = query_interface.get_unlinked_entities("transaction", user_id_hash=TEST_USER_HASH)
        assert len(results) == 1
        assert results[0].entity_id == "txn_001"

    def test_empty_for_unknown_type(self, query_interface):
        results = query_interface.get_unlinked_entities("calendar_event", user_id_hash=TEST_USER_HASH)
        assert results == []

    def test_date_range_filter_includes(self, query_interface, tmp_db):
        record = UnmatchedEntity(
            entity_id="txn_002",
            entity_type=EntityType.TRANSACTION,
            reason_code=UnmatchedReasonCode.NO_CANDIDATE_FOUND,
            run_id=RUN_ID,
        )
        tmp_db.insert_unmatched(record)

        now = datetime.now(timezone.utc)
        start = now - timedelta(minutes=5)
        end = now + timedelta(minutes=5)
        results = query_interface.get_unlinked_entities("transaction", date_range=(start, end), user_id_hash=TEST_USER_HASH)
        assert any(r.entity_id == "txn_002" for r in results)

    def test_date_range_filter_excludes_old(self, query_interface, tmp_db):
        record = UnmatchedEntity(
            entity_id="txn_old",
            entity_type=EntityType.TRANSACTION,
            reason_code=UnmatchedReasonCode.NO_CANDIDATE_FOUND,
            run_id=RUN_ID,
        )
        tmp_db.insert_unmatched(record)

        # date range in the past that won't include just-created record
        past_start = datetime(2020, 1, 1, tzinfo=timezone.utc)
        past_end = datetime(2020, 1, 2, tzinfo=timezone.utc)
        results = query_interface.get_unlinked_entities("transaction", date_range=(past_start, past_end), user_id_hash=TEST_USER_HASH)
        assert all(r.entity_id != "txn_old" for r in results)


class TestGetAmbiguousMatches:
    def test_returns_pending_ambiguous(self, query_interface, tmp_db):
        record = AmbiguousMatch(
            source_entity_id="txn_001",
            candidate_ids=["msg_a", "msg_b"],
            candidate_scores=[0.82, 0.78],
        )
        tmp_db.insert_ambiguous(record)

        results = query_interface.get_ambiguous_matches(status="pending", user_id_hash=TEST_USER_HASH)
        assert len(results) == 1
        assert results[0].source_entity_id == "txn_001"

    def test_empty_for_resolved(self, query_interface):
        results = query_interface.get_ambiguous_matches(status="resolved", user_id_hash=TEST_USER_HASH)
        assert results == []


class TestWriteFeedback:
    def test_creates_feedback_record(self, query_interface, tmp_db):
        link = make_entity_link()
        tmp_db.upsert_link(link)

        fb = query_interface.write_feedback(
            link_id=link.link_id,
            signal="confirmed",
            source="autonomous",
            reason="Looks correct",
            user_id_hash=TEST_USER_HASH,
        )
        assert fb.link_id == link.link_id
        assert fb.signal == FeedbackSignal.CONFIRMED
        assert fb.source == FeedbackSource.AUTONOMOUS
        assert fb.processed is False

        # Verify in storage
        pending = tmp_db.get_unprocessed_feedback()
        assert any(p.feedback_id == fb.feedback_id for p in pending)

    def test_invalid_signal_raises_value_error(self, query_interface):
        with pytest.raises(ValueError, match="Invalid signal"):
            query_interface.write_feedback("link_id", "nonsense_signal", "autonomous", user_id_hash=TEST_USER_HASH)

    def test_invalid_source_raises_value_error(self, query_interface):
        with pytest.raises(ValueError, match="Invalid source"):
            query_interface.write_feedback("link_id", "confirmed", "robot_overlord", user_id_hash=TEST_USER_HASH)

    def test_all_valid_signals(self, query_interface, tmp_db):
        link = make_entity_link()
        tmp_db.upsert_link(link)
        for signal in ("confirmed", "rejected", "flagged"):
            fb = query_interface.write_feedback(link.link_id, signal, "human", user_id_hash=TEST_USER_HASH)
            assert fb.signal.value == signal


class TestGetLastRunStatus:
    def test_returns_none_when_no_runs(self, query_interface):
        result = query_interface.get_last_run_status()
        assert result is None

    def test_returns_most_recent_run(self, query_interface, tmp_db):
        from src.models.links import RunLog
        from src.constants import RunType, RunStatus
        run = RunLog(run_type=RunType.INCREMENTAL, status=RunStatus.SUCCESS)
        tmp_db.insert_run_log(run)
        result = query_interface.get_last_run_status()
        assert result is not None
        assert result.run_id == run.run_id


# ── End-to-end pipeline integration ───────────────────────────────────────────

@pytest.fixture(autouse=True)
def stub_env(monkeypatch):
    monkeypatch.setenv("STUB_INGESTION", "true")


class TestQueryInterfaceAfterPipeline:
    """Runs the real pipeline and validates query_interface reads from resulting state."""

    def _make_qi(self, tmp_db):
        consent = UserConsent(
            user_id_hash=TEST_USER_HASH, consent_type="data_processing", granted=True, source="test"
        )
        tmp_db.upsert_consent(consent)
        return UnifiedQueryInterface(tmp_db)

    def test_get_unlinked_entities_returns_unmatched_after_run(self, tmp_db):
        from src.pipeline import run_pipeline
        run_pipeline(run_type=RunType.FULL_REFRESH, storage=tmp_db)
        qi = self._make_qi(tmp_db)

        # Stub fixture data produces unmatched entities (cross-field matching deferred to V2)
        unmatched = qi.get_unlinked_entities("transaction", user_id_hash=TEST_USER_HASH)
        assert isinstance(unmatched, list)
        assert len(unmatched) > 0

    def test_get_ambiguous_matches_returns_list_after_run(self, tmp_db):
        from src.pipeline import run_pipeline
        run_pipeline(run_type=RunType.FULL_REFRESH, storage=tmp_db)
        qi = self._make_qi(tmp_db)

        # May be empty with stub data — just verify no error and correct type
        results = qi.get_ambiguous_matches(status="pending", user_id_hash=TEST_USER_HASH)
        assert isinstance(results, list)

    def test_write_feedback_round_trip_with_real_link(self, tmp_db):
        """Write feedback against a link stored after pipeline run; verify it persists."""
        link = make_entity_link(source_id="txn_rt_001", target_id="msg_rt_001")
        tmp_db.upsert_link(link)
        qi = self._make_qi(tmp_db)

        fb = qi.write_feedback(
            link_id=link.link_id,
            signal="confirmed",
            source="autonomous",
            reason="round-trip test",
            user_id_hash=TEST_USER_HASH,
        )
        assert fb.feedback_id is not None

        # Confirm it appears in unprocessed queue
        pending = tmp_db.get_unprocessed_feedback()
        assert any(f.feedback_id == fb.feedback_id for f in pending)

    def test_get_last_run_status_after_pipeline(self, tmp_db):
        from src.pipeline import run_pipeline
        from src.constants import RunStatus
        run_pipeline(run_type=RunType.FULL_REFRESH, storage=tmp_db)
        qi = UnifiedQueryInterface(tmp_db)

        run = qi.get_last_run_status()
        assert run is not None
        assert run.status == RunStatus.SUCCESS


class TestGetEntity:
    def test_get_transaction_returns_model_after_upsert(self, query_interface, tmp_db):
        txn = make_transaction()
        tmp_db.upsert_transaction(txn)
        result = query_interface.get_entity(
            txn.transaction_id, "transaction", user_id_hash=TEST_USER_HASH
        )
        assert result is not None
        assert result.transaction_id == txn.transaction_id
        assert result.amount == txn.amount

    def test_get_email_returns_model_after_upsert(self, query_interface, tmp_db):
        email = make_email()
        tmp_db.upsert_email(email)
        result = query_interface.get_entity(
            email.message_id, "email", user_id_hash=TEST_USER_HASH
        )
        assert result is not None
        assert result.message_id == email.message_id

    def test_get_calendar_event_returns_model_after_upsert(self, query_interface, tmp_db):
        event = make_calendar_event()
        tmp_db.upsert_calendar_event(event)
        result = query_interface.get_entity(
            event.event_id, "calendar_event", user_id_hash=TEST_USER_HASH
        )
        assert result is not None
        assert result.event_id == event.event_id

    def test_returns_none_for_unknown_id(self, query_interface):
        result = query_interface.get_entity(
            "nonexistent", "transaction", user_id_hash=TEST_USER_HASH
        )
        assert result is None

    def test_raises_for_invalid_entity_type(self, query_interface):
        with pytest.raises(ValueError, match="Invalid entity_type"):
            query_interface.get_entity(
                "x", "invalid_type", user_id_hash=TEST_USER_HASH
            )

    def test_requires_consent(self, tmp_db):
        from src.query_interface import UnifiedQueryInterface
        from src.utils.errors import ConsentRequiredError
        qi = UnifiedQueryInterface(tmp_db)
        with pytest.raises(ConsentRequiredError):
            qi.get_entity("x", "transaction", user_id_hash="no_consent_hash")

    def test_logs_access_audit(self, query_interface, tmp_db):
        txn = make_transaction()
        tmp_db.upsert_transaction(txn)
        query_interface.get_entity(
            txn.transaction_id, "transaction", user_id_hash=TEST_USER_HASH
        )
        with tmp_db._connect() as conn:
            row = conn.execute(
                "SELECT * FROM access_audit_log WHERE operation='get_entity'"
            ).fetchone()
        assert row is not None


class TestGetEntitiesByType:
    def test_returns_all_emails(self, query_interface, tmp_db):
        e1 = make_email(message_id="msg_batch_001")
        e2 = make_email(message_id="msg_batch_002", sender="other@example.com")
        tmp_db.upsert_email(e1)
        tmp_db.upsert_email(e2)

        results = query_interface.get_entities_by_type("email", user_id_hash=TEST_USER_HASH)
        ids = {r.message_id for r in results}
        assert "msg_batch_001" in ids
        assert "msg_batch_002" in ids

    def test_returns_all_calendar_events(self, query_interface, tmp_db):
        ev = make_calendar_event(event_id="evt_batch_001")
        tmp_db.upsert_calendar_event(ev)

        results = query_interface.get_entities_by_type("calendar_event", user_id_hash=TEST_USER_HASH)
        ids = {r.event_id for r in results}
        assert "evt_batch_001" in ids

    def test_raises_for_invalid_type(self, query_interface):
        with pytest.raises(ValueError, match="get_entities_by_type supports"):
            query_interface.get_entities_by_type("receipt", user_id_hash=TEST_USER_HASH)

    def test_raises_for_transaction_type(self, query_interface):
        with pytest.raises(ValueError, match="get_entities_by_type supports"):
            query_interface.get_entities_by_type("transaction", user_id_hash=TEST_USER_HASH)


class TestGetAllLocalStore:
    def test_get_all_emails(self, tmp_db):
        from tests.conftest import make_email
        from src.models.email import Email
        e1 = make_email(message_id="store_001")
        e2 = make_email(message_id="store_002")
        tmp_db.upsert_email(e1)
        tmp_db.upsert_email(e2)

        result = tmp_db.get_all_emails()
        assert len(result) >= 2
        assert all(isinstance(r, Email) for r in result)
        ids = {r.message_id for r in result}
        assert "store_001" in ids
        assert "store_002" in ids

    def test_get_all_calendar_events(self, tmp_db):
        from tests.conftest import make_calendar_event
        from src.models.calendar_event import CalendarEvent
        ev = make_calendar_event(event_id="store_evt_001", attendees=["a@b.com"])
        tmp_db.upsert_calendar_event(ev)

        result = tmp_db.get_all_calendar_events()
        assert len(result) >= 1
        assert all(isinstance(r, CalendarEvent) for r in result)
        found = [r for r in result if r.event_id == "store_evt_001"]
        assert len(found) == 1
        assert found[0].attendees == ["a@b.com"]
