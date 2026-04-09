"""Unit tests for src/etl/transformer.py"""

import pytest

from src.etl.transformer import Transformer
from src.models.calendar_event import CalendarEvent
from src.models.email import Email
from src.models.transaction import Transaction
from src.utils.errors import SchemaValidationError


@pytest.fixture
def transformer():
    return Transformer()


class TestTransformTransaction:
    def test_valid_full_record(self, transformer):
        raw = {
            "transaction_id": "txn_001",
            "account_id": "acc_001",
            "amount": 42.50,
            "date": "2026-02-15",
            "merchant_name": "Whole Foods Market",
            "name": "WHOLEFDS #123",
            "payment_channel": "in store",
            "category": ["Food and Drink"],
        }
        result = transformer.transform_transaction(raw)
        assert isinstance(result, Transaction)
        assert result.transaction_id == "txn_001"
        assert result.amount == 42.50
        assert result.source == "plaid"
        assert result.merchant_name == "Whole Foods Market"

    def test_missing_required_field_raises(self, transformer):
        raw = {
            "transaction_id": "txn_001",
            "account_id": "acc_001",
            # amount is missing
            "date": "2026-02-15",
        }
        with pytest.raises(SchemaValidationError):
            transformer.transform_transaction(raw)

    def test_null_optional_field_is_none(self, transformer):
        raw = {
            "transaction_id": "txn_001",
            "account_id": "acc_001",
            "amount": 42.50,
            "date": "2026-02-15",
            "merchant_name": None,
        }
        result = transformer.transform_transaction(raw)
        assert result.merchant_name is None

    def test_ingested_at_is_set(self, transformer):
        raw = {
            "transaction_id": "txn_001",
            "account_id": "acc_001",
            "amount": 9.99,
            "date": "2026-02-18",
        }
        result = transformer.transform_transaction(raw)
        assert result.ingested_at is not None


class TestTransformEmail:
    def test_valid_full_record(self, transformer):
        raw = {
            "message_id": "msg_001",
            "received_at": "2026-02-15T14:32:00Z",
            "sender": "receipts@wholefoods.com",
            "recipients": ["dylan@ergonosis.com"],
            "subject": "Your Whole Foods receipt",
        }
        result = transformer.transform_email(raw)
        assert isinstance(result, Email)
        assert result.message_id == "msg_001"
        assert result.source == "microsoft_graph"

    def test_missing_recipients_raises(self, transformer):
        raw = {
            "message_id": "msg_001",
            "received_at": "2026-02-15T14:32:00Z",
            "sender": "test@example.com",
            # recipients missing
        }
        with pytest.raises(SchemaValidationError):
            transformer.transform_email(raw)

    def test_missing_message_id_raises(self, transformer):
        raw = {
            "received_at": "2026-02-15T14:32:00Z",
            "sender": "test@example.com",
            "recipients": ["user@example.com"],
        }
        with pytest.raises(SchemaValidationError):
            transformer.transform_email(raw)

    def test_null_subject_allowed(self, transformer):
        raw = {
            "message_id": "msg_001",
            "received_at": "2026-02-15T14:32:00Z",
            "sender": "test@example.com",
            "recipients": ["user@example.com"],
            "subject": None,
        }
        result = transformer.transform_email(raw)
        assert result.subject is None


class TestTransformCalendarEvent:
    def test_valid_full_record(self, transformer):
        raw = {
            "event_id": "evt_001",
            "start_time": "2026-02-15T09:00:00Z",
            "end_time": "2026-02-15T10:00:00Z",
            "organizer": "dylan@ergonosis.com",
            "subject": "Team lunch",
        }
        result = transformer.transform_calendar_event(raw)
        assert isinstance(result, CalendarEvent)
        assert result.event_id == "evt_001"
        assert result.source == "microsoft_graph"

    def test_end_before_start_raises(self, transformer):
        raw = {
            "event_id": "evt_001",
            "start_time": "2026-02-15T10:00:00Z",
            "end_time": "2026-02-15T09:00:00Z",  # end before start
            "organizer": "dylan@ergonosis.com",
        }
        with pytest.raises(SchemaValidationError):
            transformer.transform_calendar_event(raw)

    def test_missing_organizer_raises(self, transformer):
        raw = {
            "event_id": "evt_001",
            "start_time": "2026-02-15T09:00:00Z",
            "end_time": "2026-02-15T10:00:00Z",
            # organizer missing
        }
        with pytest.raises(SchemaValidationError):
            transformer.transform_calendar_event(raw)


class TestTransformBatch:
    def test_all_valid_returns_successes(self, transformer, raw_transactions):
        successful, failed = transformer.transform_batch(raw_transactions, "transaction")
        assert len(successful) == len(raw_transactions)
        assert len(failed) == 0

    def test_hard_fail_propagates(self, transformer):
        records = [
            {"transaction_id": "txn_001", "account_id": "acc", "amount": 10.0, "date": "2026-01-01"},
            {"account_id": "acc", "amount": 10.0, "date": "2026-01-01"},  # missing transaction_id
        ]
        with pytest.raises(SchemaValidationError):
            transformer.transform_batch(records, "transaction")

    def test_unknown_entity_type_raises(self, transformer):
        with pytest.raises(ValueError, match="Unknown entity_type"):
            transformer.transform_batch([], "banana")

    def test_email_batch(self, transformer, raw_emails):
        successful, failed = transformer.transform_batch(raw_emails, "email")
        assert len(successful) == len(raw_emails)

    def test_calendar_batch(self, transformer, raw_calendar_events):
        successful, failed = transformer.transform_batch(raw_calendar_events, "calendar_event")
        assert len(successful) == len(raw_calendar_events)
