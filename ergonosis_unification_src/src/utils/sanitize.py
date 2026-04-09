"""Sanitize exception messages before persistent storage."""

import re

_SECRET_PATTERNS = [
    re.compile(r"Bearer\s+\S+", re.IGNORECASE),
    re.compile(r"Basic\s+[A-Za-z0-9+/=]+", re.IGNORECASE),
    re.compile(r"\bdapi[a-f0-9]{32}\b", re.IGNORECASE),
    re.compile(r"(token|key|password|secret|credential)\s*=\s*\S+", re.IGNORECASE),
    re.compile(r"eyJ[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+"),  # JWT tokens
    re.compile(r"[A-Za-z0-9+/=]{40,}"),  # long base64-like strings
]


def sanitize_exception(exc: Exception, max_length: int = 500) -> str:
    """Extract class name + first line of message, strip secrets, truncate."""
    cls_name = type(exc).__name__
    msg = str(exc).split("\n")[0]
    for pattern in _SECRET_PATTERNS:
        msg = pattern.sub("[REDACTED]", msg)
    result = f"{cls_name}: {msg}"
    if len(result) > max_length:
        result = result[: max_length - 3] + "..."
    return result
