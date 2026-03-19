"""Context enrichment tools for finding supporting documentation"""

from crewai.tools import tool
import pandas as pd
from typing import Dict, Any, List
from datetime import timedelta
from src.integrations.unification_client import get_all_entities
from src.utils.logging import get_logger
import re

logger = get_logger(__name__)


def sanitize_sql_value(value: str) -> str:
    """Sanitize a string for safe interpolation into a SQL query.

    Escapes single quotes (doubling them per SQL standard) and rejects strings
    containing SQL metacharacters that cannot be safely escaped: semicolons,
    inline comment sequences (-- and /*).

    Raises:
        ValueError: If the value contains rejected metacharacters.
    """
    s = str(value)
    if re.search(r";|--|/\*", s):
        raise ValueError(f"SQL value contains disallowed metacharacters: {s!r}")
    return s.replace("'", "''")


def validate_numeric(value) -> float:
    """Validate that a value is numeric before SQL interpolation.

    Raises:
        ValueError: If the value cannot be converted to a float.
    """
    try:
        return float(value)
    except (ValueError, TypeError) as exc:
        raise ValueError(
            f"Expected a numeric value for SQL interpolation, got {value!r}"
        ) from exc


@tool("search_emails_batch")
def search_emails_batch(transactions_json: str = "[]") -> dict[str, Any]:
    """
    Batch search emails for mentions of vendors/amounts/dates

    Args:
        transactions_json: JSON array string of suspicious transactions like '[{"txn_id": "x", "vendor": "AWS", "amount": 500, "date": "2025-02-01"}, ...]'
            Required keys: txn_id, vendor, amount, date

    Returns:
        {
            'txn_x': {
                'email_matches': [
                    {'email_id': 'e1', 'subject': 'AWS Invoice', 'confidence': 0.9},
                    ...
                ]
            },
            ...
        }
    """
    import json
    transactions = json.loads(transactions_json) if transactions_json else []

    logger.info(f"Searching emails for {len(transactions)} transactions")

    try:
        results = {}
        all_emails = get_all_entities("email")

        for txn in transactions:
            vendor = txn['vendor']
            txn_date = pd.to_datetime(txn['date'])

            start_date = txn_date - timedelta(days=3)
            end_date = txn_date + timedelta(days=3)

            filtered = [
                e for e in all_emails
                if start_date <= pd.to_datetime(e.received_at).tz_localize(None) <= end_date
                and (
                    vendor.lower() in (e.subject or "").lower()
                    or vendor.lower() in (e.body_preview or "").lower()
                )
            ][:5]

            email_matches = [
                {
                    "email_id": e.message_id,
                    "subject": e.subject,
                    "sender": e.sender,
                    "confidence": 0.9 if vendor.lower() in (e.subject or "").lower() else 0.7,
                }
                for e in filtered
            ]

            results[txn['txn_id']] = {
                'email_matches': email_matches,
                'match_count': len(email_matches)
            }

        logger.info(f"Email search complete: found matches for {sum(1 for r in results.values() if r['match_count'] > 0)} transactions")
        return results

    except Exception as e:
        logger.error(f"Email search failed: {e}")
        return {}


@tool("search_calendar_events")
def search_calendar_events(transaction_date: str, vendor: str) -> list:
    """
    Search calendar for events matching transaction date

    Args:
        transaction_date: Transaction date (ISO format)
        vendor: Vendor name

    Returns:
        List of matching events [
            {'event_id': 'cal_123', 'title': 'Client dinner', 'date': '2025-02-01'},
            ...
        ]
    """
    logger.info(f"Searching calendar for {vendor} on {transaction_date}")

    try:
        txn_date = pd.to_datetime(transaction_date)
        start_date = txn_date - timedelta(days=3)
        end_date = txn_date + timedelta(days=3)

        all_events = get_all_entities("calendar_event")
        filtered = [
            ev for ev in all_events
            if start_date <= pd.to_datetime(ev.start_time).tz_localize(None) <= end_date
            and vendor.lower() in (ev.subject or "").lower()
        ][:5]

        if not filtered:
            logger.info(f"No calendar events found for {vendor}")
            return []

        return [
            {"event_id": ev.event_id, "title": ev.subject, "event_date": str(ev.start_time)}
            for ev in filtered
        ]

    except Exception as e:
        logger.error(f"Calendar search failed: {e}")
        return []


@tool("extract_approval_chains")
def extract_approval_chains(email_thread_id: str) -> dict:
    """
    Extract approval information from email thread

    Args:
        email_thread_id: Email thread ID

    Returns:
        {
            'approved': bool,
            'approver': str,
            'timestamp': str,
            'approval_keywords': list
        }
    """
    logger.info(f"Extracting approval chain from thread {email_thread_id}")

    try:
        all_emails = get_all_entities("email")
        thread_emails = sorted(
            [e for e in all_emails if e.thread_id == email_thread_id],
            key=lambda e: e.received_at,
        )

        if not thread_emails:
            return {
                'approved': False,
                'approver': None,
                'timestamp': None,
                'approval_keywords': []
            }

        approval_keywords = [
            'approved', 'authorize', 'authorized', 'go ahead', 'proceed',
            'looks good', 'lgtm', 'approved for payment', 'please pay'
        ]

        for email in thread_emails:
            body_lower = (email.body_preview or "").lower()

            for keyword in approval_keywords:
                if keyword in body_lower:
                    return {
                        'approved': True,
                        'approver': email.sender,
                        'timestamp': str(email.received_at),
                        'approval_keywords': [keyword]
                    }

        return {
            'approved': False,
            'approver': None,
            'timestamp': None,
            'approval_keywords': []
        }

    except Exception as e:
        logger.error(f"Approval extraction failed: {e}")
        return {
            'approved': False,
            'approver': None,
            'timestamp': None,
            'approval_keywords': []
        }


@tool("find_receipt_images")
def find_receipt_images(vendor: str, amount: float, date_range: tuple[str, str]) -> list[dict[str, str]]:
    """
    Find receipt images matching vendor/amount/date

    Args:
        vendor: Vendor name
        amount: Transaction amount
        date_range: (start_date, end_date)

    Returns:
        List of receipt file paths ['s3://bucket/receipt1.jpg', ...]
    """
    logger.debug("receipts_ocr not available via UQI — returning empty")
    return []


@tool("semantic_search_documents")
def semantic_search_documents(query: str, top_k: int = 5) -> list:
    """
    Semantic search across documents using LLM embeddings (EXPENSIVE - use sparingly!)

    Args:
        query: Search query (e.g., "AWS infrastructure approval")
        top_k: Number of results to return

    Returns:
        List of relevant documents [
            {'doc_id': 'd1', 'title': '...', 'snippet': '...', 'relevance': 0.85},
            ...
        ]
    """
    logger.debug("documents not available via UQI — returning empty")
    return []
