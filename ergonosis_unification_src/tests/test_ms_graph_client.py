"""Tests for src/ms_graph_client.py — MS Graph client unit tests."""

import os
from unittest.mock import MagicMock, patch

import pytest

from src.ms_graph_client import (
    MSGraphClient,
    _ensure_datetime,
    _html_to_text,
    fetch_user_calendar_events,
    fetch_user_emails,
)


@pytest.fixture
def mock_msal():
    """Patch MSAL to return a fake access token without network calls."""
    mock_app = MagicMock()
    mock_app.acquire_token_for_client.return_value = {"access_token": "fake-token"}
    with patch("src.ms_graph_client.msal.ConfidentialClientApplication", return_value=mock_app) as m:
        yield m


def _make_client(mock_msal) -> MSGraphClient:
    return MSGraphClient("cid", "csecret", "tid")


class TestEnsureDatetime:
    def test_bare_date_gets_time_appended(self):
        assert _ensure_datetime("2026-03-01") == "2026-03-01T00:00:00Z"

    def test_full_datetime_unchanged(self):
        assert _ensure_datetime("2026-03-01T14:00:00Z") == "2026-03-01T14:00:00Z"

    def test_datetime_without_z_unchanged(self):
        assert _ensure_datetime("2026-03-01T14:00:00") == "2026-03-01T14:00:00"


class TestHtmlToText:
    def test_strips_html_tags(self):
        html = "<html><body><p>Hello <b>world</b></p></body></html>"
        assert _html_to_text(html) == "Hello world"

    def test_empty_string(self):
        assert _html_to_text("") == ""

    def test_none_returns_empty(self):
        assert _html_to_text(None) == ""

    def test_normalizes_whitespace(self):
        html = "<body><p>Hello</p>  \n  <p>world</p></body>"
        result = _html_to_text(html)
        assert "  " not in result


class TestMSGraphClientTokenAcquisition:
    def test_msal_called_with_correct_params(self, mock_msal):
        _make_client(mock_msal)
        mock_msal.assert_called_once_with(
            client_id="cid",
            authority="https://login.microsoftonline.com/tid",
            client_credential="csecret",
        )

    def test_raises_on_token_failure(self):
        mock_app = MagicMock()
        mock_app.acquire_token_for_client.return_value = {
            "error_description": "Invalid credentials"
        }
        with patch("src.ms_graph_client.msal.ConfidentialClientApplication", return_value=mock_app):
            with pytest.raises(RuntimeError, match="Failed to acquire MS Graph token"):
                MSGraphClient("cid", "csecret", "tid")


class TestFetchMessages:
    def test_single_page(self, mock_msal):
        client = _make_client(mock_msal)
        fake_response = MagicMock()
        fake_response.status_code = 200
        fake_response.json.return_value = {
            "value": [
                {
                    "id": "msg1",
                    "subject": "Test email",
                    "from": {"emailAddress": {"address": "a@b.com"}},
                    "toRecipients": [{"emailAddress": {"address": "c@d.com"}}],
                    "receivedDateTime": "2026-03-01T10:00:00Z",
                    "body": {"content": "Hello", "contentType": "text"},
                    "conversationId": "conv1",
                }
            ],
        }

        with patch("src.ms_graph_client.requests.get", return_value=fake_response):
            result = client.fetch_messages("u@e.com", "2026-03-01", "2026-03-02")

        assert len(result) == 1
        msg = result[0]
        assert msg["message_id"] == "msg1"
        assert msg["sender"] == "a@b.com"
        assert msg["recipients"] == ["c@d.com"]
        assert msg["received_at"] == "2026-03-01T10:00:00Z"
        assert msg["subject"] == "Test email"
        assert msg["body_preview"] == "Hello"
        assert msg["thread_id"] == "conv1"

    def test_pagination(self, mock_msal):
        client = _make_client(mock_msal)

        page1 = MagicMock()
        page1.status_code = 200
        page1.json.return_value = {
            "value": [{"id": "msg1", "subject": "P1", "from": {"emailAddress": {"address": "a@b.com"}},
                        "toRecipients": [{"emailAddress": {"address": "u@e.com"}}], "receivedDateTime": "2026-03-01T10:00:00Z",
                        "body": {"content": "", "contentType": "text"}, "conversationId": None}],
            "@odata.nextLink": "https://graph.microsoft.com/v1.0/next-page",
        }
        page2 = MagicMock()
        page2.status_code = 200
        page2.json.return_value = {
            "value": [{"id": "msg2", "subject": "P2", "from": {"emailAddress": {"address": "a@b.com"}},
                        "toRecipients": [{"emailAddress": {"address": "u@e.com"}}], "receivedDateTime": "2026-03-01T11:00:00Z",
                        "body": {"content": "", "contentType": "text"}, "conversationId": None}],
        }

        with patch("src.ms_graph_client.requests.get", side_effect=[page1, page2]):
            result = client.fetch_messages("u@e.com", "2026-03-01", "2026-03-02")

        assert len(result) == 2

    def test_max_pages_limits_fetching(self, mock_msal):
        client = _make_client(mock_msal)

        page = MagicMock()
        page.status_code = 200
        page.json.return_value = {
            "value": [{"id": "msg1", "subject": "P1", "from": {"emailAddress": {"address": "a@b.com"}},
                        "toRecipients": [{"emailAddress": {"address": "u@e.com"}}], "receivedDateTime": "2026-03-01T10:00:00Z",
                        "body": {"content": "", "contentType": "text"}, "conversationId": None}],
            "@odata.nextLink": "https://graph.microsoft.com/v1.0/next-page",
        }

        with patch("src.ms_graph_client.requests.get", return_value=page):
            result = client.fetch_messages("u@e.com", "2026-03-01", "2026-03-02", max_pages=1)

        assert len(result) == 1

    def test_html_body_stripped(self, mock_msal):
        client = _make_client(mock_msal)
        fake_response = MagicMock()
        fake_response.status_code = 200
        fake_response.json.return_value = {
            "value": [
                {
                    "id": "msg1",
                    "subject": "HTML email",
                    "from": {"emailAddress": {"address": "a@b.com"}},
                    "toRecipients": [{"emailAddress": {"address": "u@e.com"}}],
                    "receivedDateTime": "2026-03-01T10:00:00Z",
                    "body": {"content": "<html><body><b>Bold</b> text</body></html>", "contentType": "html"},
                    "conversationId": None,
                }
            ],
        }

        with patch("src.ms_graph_client.requests.get", return_value=fake_response):
            result = client.fetch_messages("u@e.com", "2026-03-01", "2026-03-02", strip_html=True)

        assert "<b>" not in result[0]["body_preview"]
        assert "Bold" in result[0]["body_preview"]

    def test_raises_on_api_error(self, mock_msal):
        client = _make_client(mock_msal)
        fake_response = MagicMock()
        fake_response.status_code = 401
        fake_response.text = "Unauthorized"

        with patch("src.ms_graph_client.requests.get", return_value=fake_response):
            with pytest.raises(RuntimeError, match="Graph API error 401"):
                client.fetch_messages("u@e.com", "2026-03-01", "2026-03-02")


class TestFetchCalendarEvents:
    def test_single_page(self, mock_msal):
        client = _make_client(mock_msal)
        fake_response = MagicMock()
        fake_response.status_code = 200
        fake_response.json.return_value = {
            "value": [
                {
                    "id": "evt1",
                    "subject": "Standup",
                    "start": {"dateTime": "2026-03-01T09:00:00.0000000", "timeZone": "UTC"},
                    "end": {"dateTime": "2026-03-01T09:30:00.0000000", "timeZone": "UTC"},
                    "organizer": {"emailAddress": {"address": "boss@e.com"}},
                    "attendees": [
                        {"emailAddress": {"address": "a@e.com"}},
                        {"emailAddress": {"address": "b@e.com"}},
                    ],
                    "location": {"displayName": "Room 1"},
                }
            ],
        }

        with patch("src.ms_graph_client.requests.get", return_value=fake_response):
            result = client.fetch_calendar_events("u@e.com", "2026-03-01", "2026-03-02")

        assert len(result) == 1
        evt = result[0]
        assert evt["event_id"] == "evt1"
        assert evt["subject"] == "Standup"
        assert evt["start_time"] == "2026-03-01T09:00:00.0000000"
        assert evt["end_time"] == "2026-03-01T09:30:00.0000000"
        assert evt["organizer"] == "boss@e.com"
        assert evt["attendees"] == ["a@e.com", "b@e.com"]
        assert evt["location"] == "Room 1"

    def test_empty_location_returns_none(self, mock_msal):
        client = _make_client(mock_msal)
        fake_response = MagicMock()
        fake_response.status_code = 200
        fake_response.json.return_value = {
            "value": [
                {
                    "id": "evt2",
                    "subject": "Call",
                    "start": {"dateTime": "2026-03-01T10:00:00.0000000"},
                    "end": {"dateTime": "2026-03-01T10:30:00.0000000"},
                    "organizer": {"emailAddress": {"address": "boss@e.com"}},
                    "attendees": [],
                    "location": {"displayName": ""},
                }
            ],
        }

        with patch("src.ms_graph_client.requests.get", return_value=fake_response):
            result = client.fetch_calendar_events("u@e.com", "2026-03-01", "2026-03-02")

        assert result[0]["location"] is None
        assert result[0]["attendees"] is None  # empty list becomes None

    def test_raises_on_api_error(self, mock_msal):
        client = _make_client(mock_msal)
        fake_response = MagicMock()
        fake_response.status_code = 403
        fake_response.text = "Forbidden"

        with patch("src.ms_graph_client.requests.get", return_value=fake_response):
            with pytest.raises(RuntimeError, match="Graph API error 403"):
                client.fetch_calendar_events("u@e.com", "2026-03-01", "2026-03-02")


class TestTokenRefreshOn401:
    def test_token_refreshed_on_401_retry(self, mock_msal):
        client = _make_client(mock_msal)
        msal_app = mock_msal.return_value

        response_401 = MagicMock()
        response_401.status_code = 401
        response_401.text = "Unauthorized"

        response_200 = MagicMock()
        response_200.status_code = 200
        response_200.json.return_value = {"value": []}

        with patch("src.ms_graph_client.requests.get", side_effect=[response_401, response_200]):
            result = client.fetch_messages("u@e.com", "2026-03-01", "2026-03-02")

        assert result == []
        # Once at init + once on 401 retry
        assert msal_app.acquire_token_for_client.call_count == 2

    def test_error_response_does_not_leak_raw_text(self, mock_msal):
        client = _make_client(mock_msal)
        error_body = "token=supersecret Bearer abc123longtoken and user@private.com details"

        fake_response = MagicMock()
        fake_response.status_code = 403
        fake_response.text = error_body

        with patch("src.ms_graph_client.requests.get", return_value=fake_response):
            with pytest.raises(RuntimeError, match="Graph API error 403") as exc_info:
                client.fetch_messages("u@e.com", "2026-03-01", "2026-03-02")

        error_msg = str(exc_info.value)
        assert "supersecret" not in error_msg
        assert "abc123longtoken" not in error_msg


class TestConvenienceFunctions:
    def test_fetch_user_emails_reads_env(self, monkeypatch, mock_msal):
        monkeypatch.setenv("MSGRAPH_CLIENT_ID", "cid")
        monkeypatch.setenv("MSGRAPH_CLIENT_SECRET", "csecret")
        monkeypatch.setenv("MSGRAPH_TENANT_ID", "tid")

        fake_response = MagicMock()
        fake_response.status_code = 200
        fake_response.json.return_value = {"value": []}

        with patch("src.ms_graph_client.requests.get", return_value=fake_response):
            result = fetch_user_emails("u@e.com", "2026-03-01", "2026-03-02")

        assert result == []

    def test_fetch_user_calendar_events_reads_env(self, monkeypatch, mock_msal):
        monkeypatch.setenv("MSGRAPH_CLIENT_ID", "cid")
        monkeypatch.setenv("MSGRAPH_CLIENT_SECRET", "csecret")
        monkeypatch.setenv("MSGRAPH_TENANT_ID", "tid")

        fake_response = MagicMock()
        fake_response.status_code = 200
        fake_response.json.return_value = {"value": []}

        with patch("src.ms_graph_client.requests.get", return_value=fake_response):
            result = fetch_user_calendar_events("u@e.com", "2026-03-01", "2026-03-02")

        assert result == []

    def test_missing_env_raises_key_error(self, monkeypatch):
        monkeypatch.delenv("MSGRAPH_CLIENT_ID", raising=False)
        monkeypatch.delenv("MSGRAPH_CLIENT_SECRET", raising=False)
        monkeypatch.delenv("MSGRAPH_TENANT_ID", raising=False)
        with pytest.raises(KeyError):
            fetch_user_emails("u@e.com", "2026-03-01", "2026-03-02")
