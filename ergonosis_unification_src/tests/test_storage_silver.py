"""Tests for Silver table (canonical raw record) storage methods."""

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from tests.conftest import make_calendar_event, make_email, make_transaction


# ── LocalStore: Transactions ──────────────────────────────────────────────────


class TestLocalStoreSilverTransactions:
    def test_upsert_then_get_returns_model(self, tmp_db):
        txn = make_transaction()
        tmp_db.upsert_transaction(txn)
        result = tmp_db.get_transaction(txn.transaction_id)
        assert result is not None
        assert result.transaction_id == txn.transaction_id
        assert result.amount == txn.amount

    def test_upsert_is_idempotent(self, tmp_db):
        txn = make_transaction()
        tmp_db.upsert_transaction(txn)
        tmp_db.upsert_transaction(txn)
        result = tmp_db.get_transaction(txn.transaction_id)
        assert result is not None

    def test_upsert_updates_existing_record(self, tmp_db):
        txn = make_transaction(amount=10.0)
        tmp_db.upsert_transaction(txn)
        updated = make_transaction(amount=99.99)
        tmp_db.upsert_transaction(updated)
        result = tmp_db.get_transaction(txn.transaction_id)
        assert result.amount == 99.99

    def test_get_nonexistent_returns_none(self, tmp_db):
        assert tmp_db.get_transaction("nonexistent") is None

    def test_category_json_roundtrip(self, tmp_db):
        txn = make_transaction(category=["Food and Drink", "Restaurants"])
        tmp_db.upsert_transaction(txn)
        result = tmp_db.get_transaction(txn.transaction_id)
        assert result.category == ["Food and Drink", "Restaurants"]

    def test_null_category_roundtrip(self, tmp_db):
        txn = make_transaction(category=None)
        tmp_db.upsert_transaction(txn)
        result = tmp_db.get_transaction(txn.transaction_id)
        assert result.category is None

    def test_null_raw_file_ref_allowed(self, tmp_db):
        txn = make_transaction(raw_file_ref=None)
        tmp_db.upsert_transaction(txn)
        result = tmp_db.get_transaction(txn.transaction_id)
        assert result.raw_file_ref is None


# ── LocalStore: Emails ────────────────────────────────────────────────────────


class TestLocalStoreSilverEmails:
    def test_upsert_then_get_returns_model(self, tmp_db):
        email = make_email()
        tmp_db.upsert_email(email)
        result = tmp_db.get_email(email.message_id)
        assert result is not None
        assert result.message_id == email.message_id
        assert result.sender == email.sender

    def test_recipients_json_roundtrip(self, tmp_db):
        email = make_email(recipients=["a@b.com", "c@d.com"])
        tmp_db.upsert_email(email)
        result = tmp_db.get_email(email.message_id)
        assert result.recipients == ["a@b.com", "c@d.com"]

    def test_upsert_is_idempotent(self, tmp_db):
        email = make_email()
        tmp_db.upsert_email(email)
        tmp_db.upsert_email(email)
        result = tmp_db.get_email(email.message_id)
        assert result is not None

    def test_get_nonexistent_returns_none(self, tmp_db):
        assert tmp_db.get_email("nonexistent") is None


# ── LocalStore: Calendar Events ───────────────────────────────────────────────


class TestLocalStoreSilverCalendarEvents:
    def test_upsert_then_get_returns_model(self, tmp_db):
        event = make_calendar_event()
        tmp_db.upsert_calendar_event(event)
        result = tmp_db.get_calendar_event(event.event_id)
        assert result is not None
        assert result.event_id == event.event_id
        assert result.organizer == event.organizer

    def test_attendees_json_roundtrip(self, tmp_db):
        event = make_calendar_event(attendees=["a@b.com", "c@d.com"])
        tmp_db.upsert_calendar_event(event)
        result = tmp_db.get_calendar_event(event.event_id)
        assert result.attendees == ["a@b.com", "c@d.com"]

    def test_null_attendees_roundtrip(self, tmp_db):
        event = make_calendar_event(attendees=None)
        tmp_db.upsert_calendar_event(event)
        result = tmp_db.get_calendar_event(event.event_id)
        assert result.attendees is None

    def test_upsert_is_idempotent(self, tmp_db):
        event = make_calendar_event()
        tmp_db.upsert_calendar_event(event)
        tmp_db.upsert_calendar_event(event)
        result = tmp_db.get_calendar_event(event.event_id)
        assert result is not None

    def test_get_nonexistent_returns_none(self, tmp_db):
        assert tmp_db.get_calendar_event("nonexistent") is None


# ── DeltaClient: MERGE SQL verification ──────────────────────────────────────


class TestDeltaClientSilverMethods:
    @pytest.fixture
    def mock_client(self):
        with patch("src.storage.delta_client.DeltaClient._connect"):
            from src.storage.delta_client import DeltaClient
            client = DeltaClient.__new__(DeltaClient)
            client.host = "test"
            client.catalog = "ergonosis"
            client.schema = "unification"
            client._connection = MagicMock()
            client._execute = MagicMock()
            client._fetchone = MagicMock(return_value=None)
            yield client

    def test_upsert_transaction_calls_merge(self, mock_client):
        txn = make_transaction()
        mock_client.upsert_transaction(txn)
        sql = mock_client._execute.call_args[0][0]
        assert "MERGE INTO" in sql
        assert "transactions" in sql
        assert "transaction_id" in sql

    def test_upsert_email_calls_merge(self, mock_client):
        email = make_email()
        mock_client.upsert_email(email)
        sql = mock_client._execute.call_args[0][0]
        assert "MERGE INTO" in sql
        assert "emails" in sql
        assert "message_id" in sql

    def test_upsert_calendar_event_calls_merge(self, mock_client):
        event = make_calendar_event()
        mock_client.upsert_calendar_event(event)
        sql = mock_client._execute.call_args[0][0]
        assert "MERGE INTO" in sql
        assert "calendar_events" in sql
        assert "event_id" in sql

    def test_upsert_transaction_null_category(self, mock_client):
        txn = make_transaction(category=None)
        mock_client.upsert_transaction(txn)
        sql = mock_client._execute.call_args[0][0]
        assert "NULL AS category" in sql

    def test_upsert_email_recipients_json_encoded(self, mock_client):
        email = make_email(recipients=["a@b.com"])
        mock_client.upsert_email(email)
        sql = mock_client._execute.call_args[0][0]
        assert '["a@b.com"]' in sql

    def test_get_transaction_returns_none_on_miss(self, mock_client):
        result = mock_client.get_transaction("nonexistent")
        assert result is None

    def test_upsert_transaction_date_cast(self, mock_client):
        txn = make_transaction()
        mock_client.upsert_transaction(txn)
        sql = mock_client._execute.call_args[0][0]
        assert "CAST(" in sql
        assert "AS DATE)" in sql
