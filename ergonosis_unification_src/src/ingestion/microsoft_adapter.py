"""Microsoft Graph ingestion adapter — emails and calendar events."""

import json
import os
from pathlib import Path
from typing import Any, Dict, List

from pybreaker import CircuitBreaker, CircuitBreakerError
from tenacity import retry, stop_after_attempt, wait_exponential

from src.constants import ENV_SECURE_STORAGE_REQUIRED
from src.utils.errors import IngestionCredentialsRequiredError, IngestionError
from src.utils.logging import get_logger
from src.utils.sanitize import sanitize_exception

logger = get_logger(__name__)

_msgraph_breaker = CircuitBreaker(fail_max=5, reset_timeout=60, name="msgraph")

_FIXTURE_EMAILS = Path(__file__).parent.parent.parent / "tests" / "fixtures" / "sample_emails.json"
_FIXTURE_CALENDAR = Path(__file__).parent.parent.parent / "tests" / "fixtures" / "sample_calendar_events.json"
_RICH_FIXTURE_EMAILS = Path(__file__).parent.parent.parent / "tests" / "fixtures" / "rich_mock_emails.json"
_RICH_FIXTURE_CALENDAR = Path(__file__).parent.parent.parent / "tests" / "fixtures" / "rich_mock_calendar_events.json"
_SMB_FIXTURE_EMAILS = Path(__file__).parent.parent.parent / "tests" / "fixtures" / "smb_emails.json"
_SMB_FIXTURE_CALENDAR = Path(__file__).parent.parent.parent / "tests" / "fixtures" / "smb_calendar_events.json"


def _is_stub_mode() -> bool:
    return os.getenv("STUB_INGESTION") == "true"


def _is_rich_mode() -> bool:
    return os.getenv("STUB_INGESTION") == "rich"


def _is_smb_mode() -> bool:
    return os.getenv("STUB_INGESTION") == "smb"


def _missing_credentials() -> bool:
    return not os.getenv("MSGRAPH_CLIENT_ID") or not os.getenv("MSGRAPH_CLIENT_SECRET")


def _load_fixture(path: Path) -> List[Dict[str, Any]]:
    try:
        with open(path) as f:
            return json.load(f)
    except Exception as exc:
        raise IngestionError(f"Failed to load fixture {path}: {exc}") from exc


def fetch_emails(
    user_email: str,
    start_datetime: str,
    end_datetime: str,
    strip_html: bool = True,
    page_size: int = 50,
    max_pages: int = 10,
) -> List[Dict[str, Any]]:
    """
    Wraps ms_graph_email_client.fetch_user_emails.
    Falls back to fixture stub when credentials are absent.

    Returns: list of raw email dicts as returned by the Microsoft module.
    Raises: IngestionError on fetch failure.
    """
    if _is_smb_mode():
        logger.info("STUB_INGESTION=smb — returning SMB email fixture data")
        return _load_fixture(_SMB_FIXTURE_EMAILS)
    if _is_rich_mode():
        logger.info("STUB_INGESTION=rich — returning email rich mock fixture data")
        return _load_fixture(_RICH_FIXTURE_EMAILS)
    if _is_stub_mode():
        logger.info("STUB_INGESTION=true — returning email fixture data")
        return _load_fixture(_FIXTURE_EMAILS)

    if _missing_credentials():
        if os.getenv(ENV_SECURE_STORAGE_REQUIRED, "").lower() == "true":
            raise IngestionCredentialsRequiredError(
                "Microsoft Graph credentials (MSGRAPH_CLIENT_ID, MSGRAPH_CLIENT_SECRET) are required "
                "when UNIFICATION_SECURE_STORAGE_REQUIRED=true."
            )
        logger.warning("Microsoft Graph credentials not set — returning email fixture data")
        return _load_fixture(_FIXTURE_EMAILS)

    from src.ms_graph_client import fetch_user_emails as _fetch_emails

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=30), reraise=True)
    @_msgraph_breaker
    def _fetch_with_retry():
        try:
            return _fetch_emails(
                user_email=user_email,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                strip_html=strip_html,
                page_size=page_size,
                max_pages=max_pages,
            )
        except CircuitBreakerError:
            raise IngestionError("Microsoft Graph circuit breaker open")

    try:
        return _fetch_with_retry()
    except Exception as exc:
        raise IngestionError(f"Microsoft Graph email fetch failed: {sanitize_exception(exc)}") from exc


def fetch_calendar_events(
    user_email: str,
    start_datetime: str,
    end_datetime: str,
    max_pages: int = 10,
) -> List[Dict[str, Any]]:
    """
    Fetch calendar events from Microsoft Graph.
    Falls back to fixture stub when credentials are absent.

    Returns: list of raw calendar event dicts.
    Raises: IngestionError on fetch failure.
    """
    if _is_smb_mode():
        logger.info("STUB_INGESTION=smb — returning SMB calendar fixture data")
        return _load_fixture(_SMB_FIXTURE_CALENDAR)
    if _is_rich_mode():
        logger.info("STUB_INGESTION=rich — returning calendar rich mock fixture data")
        return _load_fixture(_RICH_FIXTURE_CALENDAR)
    if _is_stub_mode():
        logger.info("STUB_INGESTION=true — returning calendar fixture data")
        return _load_fixture(_FIXTURE_CALENDAR)

    if _missing_credentials():
        if os.getenv(ENV_SECURE_STORAGE_REQUIRED, "").lower() == "true":
            raise IngestionCredentialsRequiredError(
                "Microsoft Graph credentials (MSGRAPH_CLIENT_ID, MSGRAPH_CLIENT_SECRET) are required "
                "when UNIFICATION_SECURE_STORAGE_REQUIRED=true."
            )
        logger.warning("Microsoft Graph credentials not set — returning calendar fixture data")
        return _load_fixture(_FIXTURE_CALENDAR)

    from src.ms_graph_client import fetch_user_calendar_events as _fetch_cal

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=30), reraise=True)
    @_msgraph_breaker
    def _fetch_with_retry():
        try:
            return _fetch_cal(
                user_email=user_email,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                max_pages=max_pages,
            )
        except CircuitBreakerError:
            raise IngestionError("Microsoft Graph circuit breaker open")

    try:
        return _fetch_with_retry()
    except Exception as exc:
        raise IngestionError(f"Microsoft Graph calendar fetch failed: {sanitize_exception(exc)}") from exc
