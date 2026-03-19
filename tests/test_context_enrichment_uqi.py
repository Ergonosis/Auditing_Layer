"""Tests for context_tools.py after UQI migration."""

from datetime import datetime, timezone
from unittest.mock import patch

import pytest


def _make_email(**overrides):
    from src.models.email import Email
    defaults = dict(
        message_id="msg_test_001",
        received_at=datetime(2026, 2, 15, 14, 0, tzinfo=timezone.utc),
        sender="vendor@example.com",
        recipients=["user@ergonosis.com"],
        subject="Invoice",
        body_preview="Here is your invoice",
    )
    defaults.update(overrides)
    return Email(**defaults)


def _make_calendar_event(**overrides):
    from src.models.calendar_event import CalendarEvent
    defaults = dict(
        event_id="evt_test_001",
        start_time=datetime(2026, 2, 15, 9, 0, tzinfo=timezone.utc),
        end_time=datetime(2026, 2, 15, 10, 0, tzinfo=timezone.utc),
        organizer="org@ergonosis.com",
        subject="Meeting",
    )
    defaults.update(overrides)
    return CalendarEvent(**defaults)


class TestSearchEmailsBatch:
    def test_match_found(self):
        emails = [
            _make_email(message_id="e1", subject="AWS Invoice Feb", body_preview="Details here"),
            _make_email(message_id="e2", subject="Lunch plan", body_preview="Let's eat"),
            _make_email(message_id="e3", subject="Other", body_preview="AWS stuff"),
        ]
        with patch("src.tools.context_tools.get_all_entities", return_value=emails):
            from src.tools.context_tools import search_emails_batch
            import json
            txns = json.dumps([{"txn_id": "txn_001", "vendor": "AWS", "amount": 500, "date": "2026-02-15"}])
            result = search_emails_batch.run(txns)
        assert "txn_001" in result
        assert result["txn_001"]["match_count"] >= 1
        # e1 matches in subject, e3 matches in body_preview
        ids = [m["email_id"] for m in result["txn_001"]["email_matches"]]
        assert "e1" in ids

    def test_no_match(self):
        emails = [_make_email(message_id="e1", subject="Lunch", body_preview="Food")]
        with patch("src.tools.context_tools.get_all_entities", return_value=emails):
            from src.tools.context_tools import search_emails_batch
            import json
            txns = json.dumps([{"txn_id": "txn_001", "vendor": "AWS", "amount": 500, "date": "2026-02-15"}])
            result = search_emails_batch.run(txns)
        assert result["txn_001"]["match_count"] == 0


class TestSearchCalendarEvents:
    def test_match_found(self):
        events = [_make_calendar_event(event_id="evt1", subject="AWS Kickoff Meeting")]
        with patch("src.tools.context_tools.get_all_entities", return_value=events):
            from src.tools.context_tools import search_calendar_events
            result = search_calendar_events.run("2026-02-15", "AWS")
        assert len(result) == 1
        assert result[0]["event_id"] == "evt1"

    def test_no_match(self):
        events = [_make_calendar_event(event_id="evt1", subject="Lunch")]
        with patch("src.tools.context_tools.get_all_entities", return_value=events):
            from src.tools.context_tools import search_calendar_events
            result = search_calendar_events.run("2026-02-15", "AWS")
        assert result == []


class TestExtractApprovalChains:
    def test_approval_found(self):
        emails = [
            _make_email(message_id="e1", thread_id="thread_1", body_preview="Waiting to hear back"),
            _make_email(message_id="e2", thread_id="thread_1", body_preview="Approved for payment",
                        sender="boss@example.com",
                        received_at=datetime(2026, 2, 15, 15, 0, tzinfo=timezone.utc)),
        ]
        with patch("src.tools.context_tools.get_all_entities", return_value=emails):
            from src.tools.context_tools import extract_approval_chains
            result = extract_approval_chains.run("thread_1")
        assert result["approved"] is True
        assert result["approver"] == "boss@example.com"
        # "approved" keyword appears before "approved for payment" in the keyword list
        assert any(kw in result["approval_keywords"] for kw in ("approved", "approved for payment"))

    def test_no_approval(self):
        emails = [
            _make_email(message_id="e1", thread_id="thread_2", body_preview="Let me check on this"),
        ]
        with patch("src.tools.context_tools.get_all_entities", return_value=emails):
            from src.tools.context_tools import extract_approval_chains
            result = extract_approval_chains.run("thread_2")
        assert result["approved"] is False


class TestStubs:
    def test_find_receipt_images_returns_empty(self):
        from src.tools.context_tools import find_receipt_images
        result = find_receipt_images.run("AWS", 500.0, '["2026-01-01", "2026-12-31"]')
        assert result == []

    def test_semantic_search_returns_empty(self):
        from src.tools.context_tools import semantic_search_documents
        result = semantic_search_documents.run("AWS infrastructure")
        assert result == []
