"""
Microsoft Graph client — emails and calendar events.

Vendored and adapted from Ergonosis/Data-Aggregation-Unifying-Layer
(microsoft/ms_graph_email_client.py). Key changes:
- Added calendar event fetching via /calendarView endpoint
- Convenience functions read credentials from os.environ (Secret Manager)
- Normalized output keys to match pipeline models (Email, CalendarEvent)
- Uses html.parser instead of lxml to avoid extra dependency
"""

import os
import re
from typing import Any, Dict, List, Optional

import msal
import requests
from bs4 import BeautifulSoup

from src.utils.logging import get_logger
from src.utils.sanitize import sanitize_exception

logger = get_logger(__name__)

GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
GRAPH_SCOPE = ["https://graph.microsoft.com/.default"]


class MSGraphClient:
    """Microsoft Graph client using client credentials (app-only) flow."""

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        tenant_id: str,
        timeout: int = 30,
    ) -> None:
        self._authority = f"https://login.microsoftonline.com/{tenant_id}"
        self._timeout = timeout
        self._app = msal.ConfidentialClientApplication(
            client_id=client_id,
            authority=self._authority,
            client_credential=client_secret,
        )
        self._access_token = self._acquire_token()

    def _acquire_token(self) -> str:
        result = self._app.acquire_token_for_client(scopes=GRAPH_SCOPE)
        if "access_token" not in result:
            raise RuntimeError(
                f"Failed to acquire MS Graph token: {result.get('error', 'unknown_error')}"
            )
        return result["access_token"]

    def _get(self, url: str) -> requests.Response:
        """GET with automatic token refresh on 401 (single retry)."""
        response = requests.get(url, headers=self._headers, timeout=self._timeout)
        if response.status_code == 401:
            self._access_token = self._acquire_token()
            response = requests.get(url, headers=self._headers, timeout=self._timeout)
        return response

    @property
    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json",
        }

    # ── Email fetching ────────────────────────────────────────────────────

    def fetch_messages(
        self,
        user_email: str,
        start_datetime: str,
        end_datetime: str,
        select_fields: Optional[List[str]] = None,
        page_size: int = 50,
        max_pages: Optional[int] = None,
        strip_html: bool = True,
    ) -> List[Dict[str, Any]]:
        if select_fields is None:
            select_fields = [
                "id",
                "subject",
                "from",
                "toRecipients",
                "receivedDateTime",
                "body",
                "conversationId",
            ]

        # Ensure datetimes have time component for Graph API filtering
        start_dt = _ensure_datetime(start_datetime)
        end_dt = _ensure_datetime(end_datetime)

        filter_query = (
            f"receivedDateTime ge {start_dt} and "
            f"receivedDateTime le {end_dt}"
        )
        select_query = ",".join(select_fields)
        url: Optional[str] = (
            f"{GRAPH_BASE_URL}/users/{user_email}/messages"
            f"?$filter={filter_query}"
            f"&$select={select_query}"
            f"&$top={page_size}"
        )

        messages: List[Dict[str, Any]] = []
        page_count = 0

        while url:
            response = self._get(url)
            if response.status_code != 200:
                raise RuntimeError(
                    f"Graph API error {response.status_code}: {_safe_graph_error(response.status_code, response.text)}"
                )
            data = response.json()
            for msg in data.get("value", []):
                normalized = self._normalize_message(msg, strip_html=strip_html)
                # Skip system/internal messages with no sender or no recipients
                if normalized.get("sender") and normalized.get("recipients"):
                    messages.append(normalized)
            url = data.get("@odata.nextLink")
            page_count += 1
            if max_pages and page_count >= max_pages:
                break

        return messages

    @staticmethod
    def _normalize_message(msg: Dict[str, Any], strip_html: bool = True) -> Dict[str, Any]:
        """Map Graph API message to keys expected by the pipeline Email model."""
        body = msg.get("body", {})
        content = body.get("content", "")

        if strip_html and body.get("contentType") == "html":
            content = _html_to_text(content)

        return {
            "message_id": msg.get("id"),
            "received_at": msg.get("receivedDateTime"),
            "sender": msg.get("from", {}).get("emailAddress", {}).get("address"),
            "recipients": [
                r.get("emailAddress", {}).get("address")
                for r in msg.get("toRecipients", [])
            ],
            "subject": msg.get("subject"),
            "body_preview": content[:255] if content else None,
            "thread_id": msg.get("conversationId"),
        }

    # ── Calendar fetching ─────────────────────────────────────────────────

    def fetch_calendar_events(
        self,
        user_email: str,
        start_datetime: str,
        end_datetime: str,
        page_size: int = 50,
        max_pages: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        start_dt = _ensure_datetime(start_datetime)
        end_dt = _ensure_datetime(end_datetime)

        url: Optional[str] = (
            f"{GRAPH_BASE_URL}/users/{user_email}/calendarView"
            f"?startDateTime={start_dt}"
            f"&endDateTime={end_dt}"
            f"&$select=id,subject,start,end,organizer,attendees,location"
            f"&$top={page_size}"
        )

        events: List[Dict[str, Any]] = []
        page_count = 0

        while url:
            response = self._get(url)
            if response.status_code != 200:
                raise RuntimeError(
                    f"Graph API error {response.status_code}: {_safe_graph_error(response.status_code, response.text)}"
                )
            data = response.json()
            for evt in data.get("value", []):
                events.append(self._normalize_event(evt))
            url = data.get("@odata.nextLink")
            page_count += 1
            if max_pages and page_count >= max_pages:
                break

        return events

    @staticmethod
    def _normalize_event(evt: Dict[str, Any]) -> Dict[str, Any]:
        """Map Graph API event to keys expected by the pipeline CalendarEvent model."""
        organizer = (
            evt.get("organizer", {}).get("emailAddress", {}).get("address")
        )
        attendees = [
            a.get("emailAddress", {}).get("address")
            for a in evt.get("attendees", [])
            if a.get("emailAddress", {}).get("address")
        ]
        location = evt.get("location", {}).get("displayName") or None

        return {
            "event_id": evt.get("id"),
            "start_time": evt.get("start", {}).get("dateTime"),
            "end_time": evt.get("end", {}).get("dateTime"),
            "organizer": organizer,
            "subject": evt.get("subject"),
            "attendees": attendees or None,
            "location": location,
        }


def _safe_graph_error(status_code: int, response_text: str) -> str:
    """Sanitize a Graph API error response before including in an exception."""
    truncated = response_text[:200] if response_text else ""
    sanitized = sanitize_exception(RuntimeError(truncated))
    # strip the "RuntimeError: " prefix added by sanitize_exception
    return sanitized.removeprefix("RuntimeError: ")


# ── Module-level helpers ──────────────────────────────────────────────────

def _ensure_datetime(dt_str: str) -> str:
    """Append T00:00:00Z if the string looks like a bare date (YYYY-MM-DD)."""
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", dt_str):
        return f"{dt_str}T00:00:00Z"
    return dt_str


def _html_to_text(html: str) -> str:
    """Extract visible text from HTML email body."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    body = soup.body
    if body:
        text = body.get_text(separator=" ", strip=True)
    else:
        text = soup.get_text(separator=" ", strip=True)
    return re.sub(r"\s+", " ", text)


# ── Convenience functions (credentials from environment) ──────────────────

def fetch_user_emails(
    user_email: str,
    start_datetime: str,
    end_datetime: str,
    strip_html: bool = True,
    page_size: int = 50,
    max_pages: int = 10,
) -> List[Dict[str, Any]]:
    """Fetch emails for a user. Reads MSGRAPH_* credentials from os.environ."""
    client = MSGraphClient(
        client_id=os.environ["MSGRAPH_CLIENT_ID"],
        client_secret=os.environ["MSGRAPH_CLIENT_SECRET"],
        tenant_id=os.environ["MSGRAPH_TENANT_ID"],
    )
    return client.fetch_messages(
        user_email=user_email,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        strip_html=strip_html,
        page_size=page_size,
        max_pages=max_pages,
    )


def fetch_user_calendar_events(
    user_email: str,
    start_datetime: str,
    end_datetime: str,
    max_pages: int = 10,
) -> List[Dict[str, Any]]:
    """Fetch calendar events for a user. Reads MSGRAPH_* credentials from os.environ."""
    client = MSGraphClient(
        client_id=os.environ["MSGRAPH_CLIENT_ID"],
        client_secret=os.environ["MSGRAPH_CLIENT_SECRET"],
        tenant_id=os.environ["MSGRAPH_TENANT_ID"],
    )
    return client.fetch_calendar_events(
        user_email=user_email,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        max_pages=max_pages,
    )
