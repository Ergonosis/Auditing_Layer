"""Tests for src/etl/validator.py."""

import pytest

from src.etl.validator import OPTIONAL_FIELDS, REQUIRED_FIELDS, check_optional_fields, validate_required_fields
from src.utils.errors import SchemaValidationError


class TestValidateRequiredFields:
    def test_all_fields_present_no_exception(self):
        raw = {"transaction_id": "t1", "account_id": "a1", "amount": 10.0, "date": "2026-01-01"}
        validate_required_fields(raw, "transaction")  # must not raise

    def test_single_missing_field_raises(self):
        raw = {"transaction_id": "t1", "account_id": "a1", "amount": 10.0}  # missing "date"
        with pytest.raises(SchemaValidationError, match="date"):
            validate_required_fields(raw, "transaction")

    def test_multiple_missing_fields_all_listed(self):
        raw = {"transaction_id": "t1"}  # missing account_id, amount, date
        with pytest.raises(SchemaValidationError) as exc_info:
            validate_required_fields(raw, "transaction")
        message = str(exc_info.value)
        assert "account_id" in message
        assert "amount" in message
        assert "date" in message

    def test_none_value_counts_as_missing(self):
        raw = {"transaction_id": "t1", "account_id": None, "amount": 10.0, "date": "2026-01-01"}
        with pytest.raises(SchemaValidationError, match="account_id"):
            validate_required_fields(raw, "transaction")

    def test_email_entity_type(self):
        raw = {"message_id": "m1", "received_at": "2026-01-01", "sender": "a@b.com", "recipients": ["c@d.com"]}
        validate_required_fields(raw, "email")  # must not raise

    def test_calendar_event_entity_type(self):
        raw = {"event_id": "e1", "start_time": "2026-01-01T09:00:00", "end_time": "2026-01-01T10:00:00", "organizer": "o@e.com"}
        validate_required_fields(raw, "calendar_event")  # must not raise

    def test_unknown_entity_type_no_required_fields(self):
        """Unknown type has no required fields — should never raise."""
        validate_required_fields({"any": "value"}, "unknown_type")


class TestCheckOptionalFields:
    def test_optional_fields_present_returns_names(self):
        raw = {"merchant_name": "Starbucks", "name": "SBUX", "payment_channel": "in store"}
        result = check_optional_fields(raw, "transaction")
        assert "merchant_name" in result
        assert "name" in result
        assert "payment_channel" in result

    def test_no_optional_fields_returns_empty_list(self):
        raw = {"transaction_id": "t1", "account_id": "a1", "amount": 10.0, "date": "2026-01-01"}
        result = check_optional_fields(raw, "transaction")
        assert result == []

    def test_none_values_not_counted(self):
        raw = {"merchant_name": None, "name": "SBUX"}
        result = check_optional_fields(raw, "transaction")
        assert "merchant_name" not in result
        assert "name" in result

    def test_unknown_entity_type_returns_empty_list(self):
        result = check_optional_fields({"foo": "bar"}, "unknown_type")
        assert result == []

    def test_email_optional_fields(self):
        raw = {"subject": "Hello", "body_preview": "Hi there", "thread_id": "th1"}
        result = check_optional_fields(raw, "email")
        assert set(result) == {"subject", "body_preview", "thread_id"}
