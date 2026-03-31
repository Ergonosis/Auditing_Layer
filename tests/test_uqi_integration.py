"""Tests for src/integrations/unification_client.py"""

import hashlib
import os
from unittest.mock import MagicMock, patch

import pytest

TEST_USER_EMAIL = "test@ergonosis.com"
TEST_USER_HASH = hashlib.sha256(TEST_USER_EMAIL.encode()).hexdigest()[:16]


class TestGetUserIdHash:
    def test_returns_sha256_of_email(self, monkeypatch):
        monkeypatch.setenv("UNIFICATION_USER_EMAIL", "foo@bar.com")
        from src.integrations.unification_client import _get_user_id_hash
        assert _get_user_id_hash() == hashlib.sha256(b"foo@bar.com").hexdigest()[:16]

    def test_raises_when_env_var_missing(self, monkeypatch):
        monkeypatch.delenv("UNIFICATION_USER_EMAIL", raising=False)
        from src.integrations.unification_client import _get_user_id_hash
        with pytest.raises(RuntimeError, match="UNIFICATION_USER_EMAIL"):
            _get_user_id_hash()


class TestGetUqi:
    def test_dev_mode_returns_uqi(self, tmp_uqi, monkeypatch):
        """In dev mode (ENVIRONMENT unset), get_uqi() returns a UQI backed by LocalStore."""
        monkeypatch.delenv("ENVIRONMENT", raising=False)
        from src.integrations.unification_client import get_uqi
        get_uqi.cache_clear()
        uqi = get_uqi()
        from src.query_interface import UnifiedQueryInterface
        assert isinstance(uqi, UnifiedQueryInterface)

    def test_production_mode_does_not_raise_not_implemented(self, monkeypatch):
        """Verify the production code path no longer raises NotImplementedError."""
        import inspect
        from src.integrations import unification_client
        src_code = inspect.getsource(unification_client.get_uqi.__wrapped__)
        assert "NotImplementedError" not in src_code
        assert "get_storage_backend" in src_code


class TestGetAllEntities:
    def test_returns_emails(self, tmp_uqi):
        uqi, storage = tmp_uqi
        from src.models.email import Email
        from datetime import datetime, timezone
        email = Email(
            message_id="test_msg_001",
            received_at=datetime(2026, 2, 15, 14, 0, tzinfo=timezone.utc),
            sender="vendor@example.com",
            recipients=["user@ergonosis.com"],
            subject="Invoice from AWS",
            body_preview="Please find attached invoice",
        )
        storage.upsert_email(email)

        from src.integrations.unification_client import get_all_entities
        results = get_all_entities("email")
        assert len(results) >= 1
        assert any(r.message_id == "test_msg_001" for r in results)

    def test_returns_calendar_events(self, tmp_uqi):
        uqi, storage = tmp_uqi
        from src.models.calendar_event import CalendarEvent
        from datetime import datetime, timezone
        ev = CalendarEvent(
            event_id="test_evt_001",
            start_time=datetime(2026, 2, 15, 9, 0, tzinfo=timezone.utc),
            end_time=datetime(2026, 2, 15, 10, 0, tzinfo=timezone.utc),
            organizer="org@ergonosis.com",
            subject="AWS Kickoff",
        )
        storage.upsert_calendar_event(ev)

        from src.integrations.unification_client import get_all_entities
        results = get_all_entities("calendar_event")
        assert len(results) >= 1
        assert any(r.event_id == "test_evt_001" for r in results)

    def test_invalid_type_returns_empty(self):
        """get_entities_by_type raises ValueError for 'transaction', get_all_entities catches → []."""
        from src.integrations.unification_client import get_all_entities
        results = get_all_entities("transaction")
        assert results == []


class TestTryWriteFeedback:
    def test_returns_false_when_no_links(self, tmp_uqi):
        """No entity links for transaction → returns False without writing."""
        from src.integrations.unification_client import try_write_feedback
        result = try_write_feedback("nonexistent_txn", "flagged", "autonomous", reason="test")
        assert result is False

    def test_returns_true_when_link_exists(self, tmp_uqi):
        """When a transaction has a linked entity, try_write_feedback returns True."""
        uqi, storage = tmp_uqi

        from src.models.links import EntityLink
        # Use string values directly to avoid collision with auditing's src.constants
        link = EntityLink(
            source_id="txn_feedback_001",
            target_id="email_001",
            source_type="transaction",
            target_type="email",
            match_type="deterministic",
            match_tier="tier1_exact",
            confidence=0.9,
            linkage_key="transaction_id:txn_feedback_001",
        )
        storage.upsert_link(link)

        from src.integrations.unification_client import try_write_feedback
        result = try_write_feedback("txn_feedback_001", "flagged", "autonomous", reason="test")
        assert result is True


class TestGetAmbiguousMatches:
    def test_returns_empty_list_when_none(self, tmp_uqi):
        from src.integrations.unification_client import get_ambiguous_matches
        results = get_ambiguous_matches()
        assert results == []


class TestLocalStoreSmoke:
    def test_upsert_and_query_transactions(self, tmp_uqi):
        """Insert 3 unmatched transaction records, verify get_unlinked_entities returns 3 and emails are empty."""
        uqi, storage = tmp_uqi

        from src.models.links import UnmatchedEntity
        # Use string values directly to avoid collision with auditing's src.constants
        import uuid

        run_id = str(uuid.uuid4())
        for i in range(1, 4):
            storage.insert_unmatched(UnmatchedEntity(
                entity_id=f"smoke_txn_{i:03d}",
                entity_type="transaction",
                reason_code="no_candidate_found",
                run_id=run_id,
            ))

        # Email entities should be empty (none inserted)
        from src.integrations.unification_client import get_all_entities
        emails = get_all_entities("email")
        assert emails == []

        # Unlinked transactions should return 3
        results = uqi.get_unlinked_entities("transaction", user_id_hash=TEST_USER_HASH)
        assert len(results) == 3
