"""Tests for Pydantic model validators in src/models/."""

import math
from datetime import date, datetime, timezone

import pytest
from pydantic import ValidationError

from src.models.calendar_event import CalendarEvent
from src.models.email import Email
from src.models.links import AmbiguousMatch
from src.models.transaction import Transaction


class TestTransactionValidators:
    def test_nan_amount_raises(self):
        with pytest.raises(ValidationError, match="finite"):
            Transaction(transaction_id="t1", account_id="a1", amount=float("nan"), date=date(2026, 1, 1))

    def test_inf_amount_raises(self):
        with pytest.raises(ValidationError, match="finite"):
            Transaction(transaction_id="t1", account_id="a1", amount=math.inf, date=date(2026, 1, 1))

    def test_iso_string_date_coerced(self):
        txn = Transaction(transaction_id="t1", account_id="a1", amount=10.0, date="2026-03-01")
        assert txn.date == date(2026, 3, 1)

    def test_date_object_accepted(self):
        txn = Transaction(transaction_id="t1", account_id="a1", amount=10.0, date=date(2026, 3, 1))
        assert txn.date == date(2026, 3, 1)

    def test_valid_negative_amount(self):
        txn = Transaction(transaction_id="t1", account_id="a1", amount=-50.0, date=date(2026, 1, 1))
        assert txn.amount == -50.0

    def test_round_trip_model_dump(self):
        txn = Transaction(transaction_id="t1", account_id="a1", amount=42.0, date=date(2026, 3, 1))
        dumped = txn.model_dump()
        restored = Transaction(**dumped)
        assert restored.transaction_id == txn.transaction_id
        assert restored.amount == txn.amount
        assert restored.date == txn.date


class TestEmailValidators:
    def test_sender_without_at_raises_validation_error(self):
        """Validator returns None for invalid sender, but field is str — raises ValidationError."""
        with pytest.raises(ValidationError):
            Email(
                message_id="m1",
                received_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
                sender="notanemail",
                recipients=["a@b.com"],
            )

    def test_valid_sender_preserved(self):
        email = Email(
            message_id="m1",
            received_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            sender="valid@example.com",
            recipients=["a@b.com"],
        )
        assert email.sender == "valid@example.com"

    def test_body_preview_truncated_to_255(self):
        long_body = "x" * 400
        email = Email(
            message_id="m1",
            received_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            sender="a@b.com",
            recipients=["c@d.com"],
            body_preview=long_body,
        )
        assert len(email.body_preview) == 255

    def test_body_preview_under_255_unchanged(self):
        short_body = "Hello world"
        email = Email(
            message_id="m1",
            received_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            sender="a@b.com",
            recipients=["c@d.com"],
            body_preview=short_body,
        )
        assert email.body_preview == short_body

    def test_empty_recipients_raises(self):
        with pytest.raises(ValidationError):
            Email(
                message_id="m1",
                received_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
                sender="a@b.com",
                recipients=[],
            )

    def test_round_trip_model_dump(self):
        email = Email(
            message_id="m1",
            received_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            sender="a@b.com",
            recipients=["c@d.com"],
        )
        dumped = email.model_dump()
        restored = Email(**dumped)
        assert restored.message_id == email.message_id


class TestCalendarEventValidators:
    def test_end_before_start_raises(self):
        with pytest.raises(ValidationError, match="end_time"):
            CalendarEvent(
                event_id="e1",
                start_time=datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc),
                end_time=datetime(2026, 1, 1, 9, 0, tzinfo=timezone.utc),
                organizer="org@example.com",
            )

    def test_end_equal_to_start_accepted(self):
        ts = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
        event = CalendarEvent(event_id="e1", start_time=ts, end_time=ts, organizer="org@example.com")
        assert event.start_time == event.end_time

    def test_valid_event_accepted(self):
        event = CalendarEvent(
            event_id="e1",
            start_time=datetime(2026, 1, 1, 9, 0, tzinfo=timezone.utc),
            end_time=datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc),
            organizer="org@example.com",
        )
        assert event.event_id == "e1"

    def test_round_trip_model_dump(self):
        event = CalendarEvent(
            event_id="e1",
            start_time=datetime(2026, 1, 1, 9, 0, tzinfo=timezone.utc),
            end_time=datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc),
            organizer="org@example.com",
        )
        dumped = event.model_dump()
        restored = CalendarEvent(**dumped)
        assert restored.event_id == event.event_id


class TestAmbiguousMatchValidators:
    def test_misaligned_candidate_lists_raises(self):
        with pytest.raises(ValidationError, match="must equal"):
            AmbiguousMatch(
                source_entity_id="src1",
                candidate_ids=["a", "b"],
                candidate_scores=[0.9],  # length mismatch
            )

    def test_single_candidate_raises(self):
        """min_length=2 on candidate_ids."""
        with pytest.raises(ValidationError):
            AmbiguousMatch(
                source_entity_id="src1",
                candidate_ids=["a"],
                candidate_scores=[0.9],
            )

    def test_valid_ambiguous_match(self):
        match = AmbiguousMatch(
            source_entity_id="src1",
            candidate_ids=["a", "b"],
            candidate_scores=[0.9, 0.8],
        )
        assert len(match.candidate_ids) == 2
