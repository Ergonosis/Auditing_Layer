"""Schema registry — single source of truth for required/optional fields per entity type."""

from typing import Any, Dict, List

from src.utils.errors import SchemaValidationError

REQUIRED_FIELDS: Dict[str, List[str]] = {
    "transaction": ["transaction_id", "account_id", "amount", "date"],
    "email": ["message_id", "received_at", "sender", "recipients"],
    "calendar_event": ["event_id", "start_time", "end_time", "organizer"],
}

OPTIONAL_FIELDS: Dict[str, List[str]] = {
    "transaction": ["merchant_name", "name", "payment_channel", "category"],
    "email": ["subject", "body_preview", "thread_id"],
    "calendar_event": ["subject", "attendees", "location"],
}

ETL_ADDED_FIELDS: Dict[str, str] = {
    "transaction": "plaid",
    "email": "microsoft_graph",
    "calendar_event": "microsoft_graph",
}


def validate_required_fields(raw: Dict[str, Any], entity_type: str) -> None:
    """
    Check all required fields are present and non-None.
    Raises SchemaValidationError listing ALL missing fields, not just the first.
    """
    required = REQUIRED_FIELDS.get(entity_type, [])
    missing = [f for f in required if raw.get(f) is None]
    if missing:
        raise SchemaValidationError(
            f"[{entity_type}] Missing required fields: {missing}"
        )


def check_optional_fields(raw: Dict[str, Any], entity_type: str) -> List[str]:
    """
    Returns list of optional fields that are present and have valid (non-None) values.
    Does not raise — caller handles soft fail logic.
    """
    optional = OPTIONAL_FIELDS.get(entity_type, [])
    return [f for f in optional if raw.get(f) is not None]
