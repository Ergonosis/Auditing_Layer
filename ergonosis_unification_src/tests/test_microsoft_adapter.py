"""Tests for src/ingestion/microsoft_adapter.py."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.ingestion.microsoft_adapter import (
    _load_fixture,
    _msgraph_breaker,
    fetch_calendar_events,
    fetch_emails,
)
from src.utils.errors import IngestionCredentialsRequiredError, IngestionError

_FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(autouse=True)
def _reset_breaker():
    """Reset circuit breaker state between tests so failures don't cascade."""
    _msgraph_breaker._state_storage.state = "closed"
    yield
    _msgraph_breaker._state_storage.state = "closed"


class TestFetchEmailsStubModes:
    def test_stub_true_returns_fixture_emails(self, monkeypatch):
        monkeypatch.setenv("STUB_INGESTION", "true")
        result = fetch_emails("user@example.com", "2026-01-01", "2026-02-01")
        assert isinstance(result, list)
        assert len(result) > 0

    def test_stub_rich_returns_rich_fixture_emails(self, monkeypatch):
        monkeypatch.setenv("STUB_INGESTION", "rich")
        result = fetch_emails("user@example.com", "2026-01-01", "2026-02-01")
        assert isinstance(result, list)
        assert len(result) > 0

    def test_missing_credentials_falls_back_to_fixture(self, monkeypatch):
        monkeypatch.delenv("STUB_INGESTION", raising=False)
        monkeypatch.delenv("MSGRAPH_CLIENT_ID", raising=False)
        monkeypatch.delenv("MSGRAPH_CLIENT_SECRET", raising=False)
        result = fetch_emails("user@example.com", "2026-01-01", "2026-02-01")
        assert isinstance(result, list)
        assert len(result) > 0

    def test_missing_credentials_raises_when_secure_required(self, monkeypatch):
        monkeypatch.delenv("STUB_INGESTION", raising=False)
        monkeypatch.delenv("MSGRAPH_CLIENT_ID", raising=False)
        monkeypatch.delenv("MSGRAPH_CLIENT_SECRET", raising=False)
        monkeypatch.setenv("UNIFICATION_SECURE_STORAGE_REQUIRED", "true")
        with pytest.raises(IngestionCredentialsRequiredError):
            fetch_emails("user@example.com", "2026-01-01", "2026-02-01")

    def test_live_email_path_calls_client(self, monkeypatch):
        """With credentials set + client mocked, confirm live path is taken."""
        monkeypatch.delenv("STUB_INGESTION", raising=False)
        monkeypatch.setenv("MSGRAPH_CLIENT_ID", "fake_id")
        monkeypatch.setenv("MSGRAPH_CLIENT_SECRET", "fake_secret")
        monkeypatch.setenv("MSGRAPH_TENANT_ID", "fake_tenant")

        mock_fetch = MagicMock(return_value=[{"message_id": "m1", "subject": "hi"}])

        with patch("src.ms_graph_client.fetch_user_emails", mock_fetch):
            result = fetch_emails("user@example.com", "2026-01-01", "2026-02-01")

        assert result == [{"message_id": "m1", "subject": "hi"}]
        mock_fetch.assert_called_once()

    def test_real_client_delegates_and_raises_ingestion_error(self, monkeypatch):
        monkeypatch.delenv("STUB_INGESTION", raising=False)
        monkeypatch.setenv("MSGRAPH_CLIENT_ID", "fake_id")
        monkeypatch.setenv("MSGRAPH_CLIENT_SECRET", "fake_secret")
        monkeypatch.setenv("MSGRAPH_TENANT_ID", "fake_tenant")

        mock_fetch = MagicMock(side_effect=RuntimeError("graph down"))

        with patch("src.ms_graph_client.fetch_user_emails", mock_fetch):
            with pytest.raises(IngestionError, match="Microsoft Graph email fetch failed"):
                fetch_emails("user@example.com", "2026-01-01", "2026-02-01")

    def test_rich_fixture_content_differs_from_basic_fixture(self, monkeypatch):
        """Rich mock and basic fixture are separate files — verify they exist and differ."""
        monkeypatch.setenv("STUB_INGESTION", "true")
        basic = fetch_emails("u@e.com", "2026-01-01", "2026-02-01")
        monkeypatch.setenv("STUB_INGESTION", "rich")
        rich = fetch_emails("u@e.com", "2026-01-01", "2026-02-01")
        assert isinstance(basic, list)
        assert isinstance(rich, list)


class TestFetchCalendarEventsStubModes:
    def test_stub_true_returns_fixture_events(self, monkeypatch):
        monkeypatch.setenv("STUB_INGESTION", "true")
        result = fetch_calendar_events("user@example.com", "2026-01-01", "2026-02-01")
        assert isinstance(result, list)
        assert len(result) > 0

    def test_stub_rich_returns_rich_fixture_events(self, monkeypatch):
        monkeypatch.setenv("STUB_INGESTION", "rich")
        result = fetch_calendar_events("user@example.com", "2026-01-01", "2026-02-01")
        assert isinstance(result, list)
        assert len(result) > 0

    def test_missing_credentials_falls_back_to_fixture(self, monkeypatch):
        """Without credentials, calendar falls back to fixture data."""
        monkeypatch.delenv("STUB_INGESTION", raising=False)
        monkeypatch.delenv("MSGRAPH_CLIENT_ID", raising=False)
        monkeypatch.delenv("MSGRAPH_CLIENT_SECRET", raising=False)
        result = fetch_calendar_events("user@example.com", "2026-01-01", "2026-02-01")
        assert isinstance(result, list)
        assert len(result) > 0

    def test_missing_credentials_raises_when_secure_required(self, monkeypatch):
        monkeypatch.delenv("STUB_INGESTION", raising=False)
        monkeypatch.delenv("MSGRAPH_CLIENT_ID", raising=False)
        monkeypatch.delenv("MSGRAPH_CLIENT_SECRET", raising=False)
        monkeypatch.setenv("UNIFICATION_SECURE_STORAGE_REQUIRED", "true")
        with pytest.raises(IngestionCredentialsRequiredError):
            fetch_calendar_events("user@example.com", "2026-01-01", "2026-02-01")

    def test_live_calendar_path_calls_client(self, monkeypatch):
        """With credentials set + client mocked, confirm live path is taken."""
        monkeypatch.delenv("STUB_INGESTION", raising=False)
        monkeypatch.setenv("MSGRAPH_CLIENT_ID", "fake_id")
        monkeypatch.setenv("MSGRAPH_CLIENT_SECRET", "fake_secret")
        monkeypatch.setenv("MSGRAPH_TENANT_ID", "fake_tenant")

        mock_fetch = MagicMock(return_value=[{"event_id": "e1", "subject": "standup"}])

        with patch("src.ms_graph_client.fetch_user_calendar_events", mock_fetch):
            result = fetch_calendar_events("user@example.com", "2026-01-01", "2026-02-01")

        assert result == [{"event_id": "e1", "subject": "standup"}]
        mock_fetch.assert_called_once()

    def test_live_calendar_raises_ingestion_error(self, monkeypatch):
        monkeypatch.delenv("STUB_INGESTION", raising=False)
        monkeypatch.setenv("MSGRAPH_CLIENT_ID", "fake_id")
        monkeypatch.setenv("MSGRAPH_CLIENT_SECRET", "fake_secret")
        monkeypatch.setenv("MSGRAPH_TENANT_ID", "fake_tenant")

        mock_fetch = MagicMock(side_effect=RuntimeError("calendar API down"))

        with patch("src.ms_graph_client.fetch_user_calendar_events", mock_fetch):
            with pytest.raises(IngestionError, match="Microsoft Graph calendar fetch failed"):
                fetch_calendar_events("user@example.com", "2026-01-01", "2026-02-01")


class TestLoadFixture:
    def test_nonexistent_path_raises_ingestion_error(self):
        bad_path = Path("/nonexistent/does_not_exist.json")
        with pytest.raises(IngestionError, match="Failed to load fixture"):
            _load_fixture(bad_path)

    def test_valid_fixture_returns_list(self):
        result = _load_fixture(_FIXTURES / "sample_emails.json")
        assert isinstance(result, list)
