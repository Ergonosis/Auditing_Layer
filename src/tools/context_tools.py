"""Context enrichment tools for finding supporting documentation"""

from crewai.tools import tool
import pandas as pd
from typing import Dict, Any, List
from datetime import timedelta
from src.tools.databricks_client import query_gold_tables
from src.tools.llm_client import call_llm
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
    # Parse JSON string to list
    import json
    transactions = json.loads(transactions_json) if transactions_json else []

    logger.info(f"Searching emails for {len(transactions)} transactions")

    try:
        results = {}

        for txn in transactions:
            vendor = txn['vendor']
            amount = txn['amount']
            txn_date = pd.to_datetime(txn['date'])

            # Query emails ±3 days from transaction
            start_date = txn_date - timedelta(days=3)
            end_date = txn_date + timedelta(days=3)

            safe_vendor = sanitize_sql_value(vendor)
            safe_start = sanitize_sql_value(start_date)
            safe_end = sanitize_sql_value(end_date)

            emails = query_gold_tables(f"""
                SELECT email_id, subject, sender, email_date
                FROM gold.emails
                WHERE email_date BETWEEN '{safe_start}' AND '{safe_end}'
                  AND (subject LIKE '%{safe_vendor}%' OR body LIKE '%{safe_vendor}%')
                LIMIT 5
            """)

            email_matches = []
            for _, email in emails.iterrows():
                # Calculate confidence based on match quality
                confidence = 0.9 if vendor.lower() in email['subject'].lower() else 0.7

                email_matches.append({
                    'email_id': email['email_id'],
                    'subject': email['subject'],
                    'sender': email['sender'],
                    'confidence': confidence
                })

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

        safe_vendor = sanitize_sql_value(vendor)
        safe_start = sanitize_sql_value(start_date)
        safe_end = sanitize_sql_value(end_date)

        events = query_gold_tables(f"""
            SELECT event_id, title, event_date, description
            FROM gold.calendar_events
            WHERE event_date BETWEEN '{safe_start}' AND '{safe_end}'
              AND (title LIKE '%{safe_vendor}%' OR description LIKE '%{safe_vendor}%')
            LIMIT 5
        """)

        if events.empty:
            logger.info(f"No calendar events found for {vendor}")
            return []

        return events[['event_id', 'title', 'event_date']].to_dict('records')

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
        # Query email thread
        safe_thread_id = sanitize_sql_value(email_thread_id)
        emails = query_gold_tables(f"""
            SELECT email_id, sender, body, email_date
            FROM gold.emails
            WHERE thread_id = '{safe_thread_id}'
            ORDER BY email_date ASC
        """)

        if emails.empty:
            return {
                'approved': False,
                'approver': None,
                'timestamp': None,
                'approval_keywords': []
            }

        # Approval keywords
        approval_keywords = [
            'approved', 'authorize', 'authorized', 'go ahead', 'proceed',
            'looks good', 'lgtm', 'approved for payment', 'please pay'
        ]

        # Search for approval keywords in email bodies
        for _, email in emails.iterrows():
            body_lower = email['body'].lower()

            for keyword in approval_keywords:
                if keyword in body_lower:
                    return {
                        'approved': True,
                        'approver': email['sender'],
                        'timestamp': str(email['email_date']),
                        'approval_keywords': [keyword]
                    }

        # No approval found
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
    logger.info(f"Finding receipts for {vendor}, ${amount}")

    try:
        # Parse JSON array to tuple
        import json
        date_range_parsed = json.loads(date_range) if date_range else ["2025-01-01", "2025-12-31"]
        start_date, end_date = date_range_parsed

        safe_vendor = sanitize_sql_value(vendor)
        safe_start = sanitize_sql_value(start_date)
        safe_end = sanitize_sql_value(end_date)
        safe_amount = validate_numeric(amount)

        receipts = query_gold_tables(f"""
            SELECT receipt_id, file_path, ocr_vendor, ocr_amount, ocr_date
            FROM gold.receipts_ocr
            WHERE ocr_date BETWEEN '{safe_start}' AND '{safe_end}'
              AND ocr_vendor LIKE '%{safe_vendor}%'
              AND ocr_amount BETWEEN {safe_amount * 0.95} AND {safe_amount * 1.05}
            LIMIT 5
        """)

        if receipts.empty:
            logger.info(f"No receipts found for {vendor}")
            return []

        return receipts['file_path'].tolist()

    except Exception as e:
        logger.error(f"Receipt search failed: {e}")
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
    logger.info(f"Semantic search for: {query}")

    try:
        # Use LLM to reformulate query and search
        prompt = f"""
You are searching a document database for information about: "{query}"

Given this context, generate 3 specific search keywords (comma-separated) that would find relevant documents.

Example:
Query: "AWS infrastructure approval"
Keywords: AWS invoice, infrastructure purchase, cloud services approval

Respond with ONLY the keywords, no explanation:
"""

        keywords_response = call_llm(prompt, agent_name="ContextEnrichment")
        keywords = [k.strip() for k in keywords_response.split(',')]

        logger.info(f"Expanded query to keywords: {keywords}")

        # Search documents using keywords (SQL)
        results = []
        for keyword in keywords[:3]:  # Limit to 3 keywords
            safe_keyword = sanitize_sql_value(keyword)
            docs = query_gold_tables(f"""
                SELECT doc_id, title, snippet
                FROM gold.documents
                WHERE title LIKE '%{safe_keyword}%' OR content LIKE '%{safe_keyword}%'
                LIMIT 2
            """)

            for _, doc in docs.iterrows():
                results.append({
                    'doc_id': doc['doc_id'],
                    'title': doc['title'],
                    'snippet': doc['snippet'][:200],
                    'relevance': 0.8  # Simplified relevance score
                })

        return results[:top_k]

    except Exception as e:
        logger.error(f"Semantic search failed: {e}")
        return []
