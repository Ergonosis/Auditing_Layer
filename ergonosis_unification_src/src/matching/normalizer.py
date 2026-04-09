"""String normalization utilities for fuzzy matching."""

import re
from datetime import date, datetime
from typing import Optional, Union

# Plaid merchant name abbreviation expansions — applied before any other normalization.
# Each entry is (compiled_regex, replacement_string). Applied in order; first match wins
# only for that specific pattern (all patterns are applied independently).
_MERCHANT_EXPANSIONS = [
    # Whole Foods: WHOLEFDS MKT #0523 → "Whole Foods Market", WFM #0523 → "Whole Foods"
    (re.compile(r'\bWHOLEFDS\s+MKT\b.*', re.IGNORECASE), 'Whole Foods Market'),
    (re.compile(r'\bWHOLEFDS\b.*', re.IGNORECASE), 'Whole Foods'),
    (re.compile(r'\bWFM\b.*', re.IGNORECASE), 'Whole Foods'),
    # Starbucks: STARBUCKS #12345 SF CA, STARBUCKS STORE 12345
    (re.compile(r'\bSTARBUCKS\s*(?:STORE\s*)?\s*#?\d+.*', re.IGNORECASE), 'Starbucks'),
    # Delta: DELTA AIR LINES 006, DELTA AIR 0062234556778
    (re.compile(r'\bDELTA AIR(?:\s+LINES?)?\s*\d+.*', re.IGNORECASE), 'Delta'),
    # Uber: UBER * TRIP, UBER * RIDE (abbreviated with asterisk — not canonical "Uber Technologies")
    (re.compile(r'\bUBER\s*\*\s*\w+.*', re.IGNORECASE), 'Uber'),
    # Lyft: LYFT *RIDE SUN 9PM
    (re.compile(r'\bLYFT\s*\*\w+.*', re.IGNORECASE), 'Lyft'),
    # Marriott: MARRIOTT SF DOWNTOWN, MARRIOTT INTL HOTELS (abbreviated with location/suffix)
    (re.compile(r'\bMARRIOTT\s+(?:INTL|SF|NYC|LA|CHICAGO|DOWNTOWN|HOTELS?)\b.*', re.IGNORECASE), 'Marriott'),
    # Airbnb: AIRBNB * HMWM2ABCDE (abbreviated with asterisk code), AIRBNB INC
    (re.compile(r'\bAIRBNB\s*\*\s*\w+.*', re.IGNORECASE), 'Airbnb'),
    (re.compile(r'\bAIRBNB\s+INC\b.*', re.IGNORECASE), 'Airbnb'),
    # AWS: AWS EMEA, AWS EMEA LLC
    (re.compile(r'\bAWS\s+EMEA\b.*', re.IGNORECASE), 'Amazon Web Services'),
    # WeWork: WEWORK 340 PINE ST SF, WEWORK COMPANIES INC
    (re.compile(r'\bWEWORK\b.*', re.IGNORECASE), 'WeWork'),
    # DoorDash: DOORDASH*CHIPOTLE, DOORDASH INC
    (re.compile(r'\bDOORDASH\b.*', re.IGNORECASE), 'DoorDash'),
    # Intuit QuickBooks: INTUIT *QUICKBOOKS
    (re.compile(r'\bINTUIT\s*\*\s*QUICKBOOKS\b.*', re.IGNORECASE), 'QuickBooks'),
    # Costco: COSTCO WHSE #0456
    (re.compile(r'\bCOSTCO\s+WHSE\b.*', re.IGNORECASE), 'Costco'),
    # Trader Joe's: TRADER JOE S #127
    (re.compile(r'\bTRADER\s+JOE\s+S\b.*', re.IGNORECASE), 'Trader Joes'),
]


def _is_plaid_raw_name(name: str) -> bool:
    """Return True if name looks like a raw Plaid name (≥70% uppercase letters)."""
    letters = [c for c in name if c.isalpha()]
    if not letters:
        return False
    return sum(1 for c in letters if c.isupper()) / len(letters) >= 0.7


def normalize_merchant_name(name: str) -> str:
    """
    Normalize a merchant/vendor name for fuzzy comparison.
    Steps (in order):
    0. Expand Plaid merchant abbreviations (e.g. WHOLEFDS → Whole Foods) — raw names only
    1. Lowercase
    2. Remove common legal suffixes: LLC, Inc, Corp, Ltd, Co (with word boundary)
    3. Remove punctuation except spaces
    4. Collapse multiple whitespace to single space
    5. Strip leading/trailing whitespace
    6. Remove common noise words: "the", "a", "and", "&"

    Examples:
      "Whole Foods Market, LLC" → "whole foods market"
      "WHOLEFDS MKT #0523" → "whole foods"
      "Amazon.com*12345" → "amazoncom 12345"
    """
    # Step 0: expand Plaid abbreviations — only applied when name is mostly uppercase
    if _is_plaid_raw_name(name):
        for pattern, replacement in _MERCHANT_EXPANSIONS:
            expanded = pattern.sub(replacement, name)
            if expanded != name:
                name = expanded
                break  # stop after first matching expansion
    s = name.lower()
    s = re.sub(r'\b(llc|inc|corp|ltd|co)\b\.?', '', s)
    s = re.sub(r'[^\w\s]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    s = re.sub(r'\b(the|a|and|&)\b', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def normalize_subject(subject: str) -> str:
    """
    Normalize an email/calendar subject for fuzzy comparison.
    Steps:
    1. Lowercase
    2. Remove common prefixes: Re:, Fwd:, FW:, RE:, FWD: (case-insensitive)
    3. Remove currency amounts (e.g. $42.50) — these shift but shouldn't dominate similarity
    4. Remove punctuation except spaces
    5. Collapse whitespace, strip
    """
    s = subject.lower()
    s = re.sub(r'^(re|fwd|fw)\s*:\s*', '', s, flags=re.IGNORECASE)
    s = re.sub(r'\$[\d,]+(?:\.\d{1,2})?', '', s)
    s = re.sub(r'[^\w\s]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def normalize_email_address(email: str) -> Optional[str]:
    """Lowercase and strip. Basic format validation — return None if no '@'."""
    normalized = email.lower().strip()
    if '@' not in normalized:
        return None
    return normalized


def date_within_window(
    date1: Union[date, datetime, str],
    date2: Union[date, datetime, str],
    window_days: int,
) -> bool:
    """
    Returns True if abs(date1 - date2) <= window_days.
    Accepts date, datetime, or ISO string for either argument.
    """
    def _to_date(d: Union[date, datetime, str]) -> date:
        if isinstance(d, datetime):
            return d.date()
        if isinstance(d, date):
            return d
        parsed = datetime.fromisoformat(d)
        return parsed.date()

    d1 = _to_date(date1)
    d2 = _to_date(date2)
    return abs((d1 - d2).days) <= window_days


def amount_matches(amount1: float, amount2: float, tolerance_pct: float = 0.0) -> bool:
    """
    Returns True if amounts are equal within tolerance_pct (0.0 = exact match).
    For Tier 2 composite matching, tolerance is 0 (exact amount required).
    """
    if tolerance_pct == 0.0:
        return amount1 == amount2
    if amount1 == 0.0 and amount2 == 0.0:
        return True
    base = max(abs(amount1), abs(amount2))
    return abs(amount1 - amount2) / base <= tolerance_pct
