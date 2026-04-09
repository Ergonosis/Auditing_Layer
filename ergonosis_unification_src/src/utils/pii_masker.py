import re

_PII_PATTERNS = [
    (re.compile(r"\b\d{3}-\d{2}-\d{4}\b"), "[SSN]"),
    (re.compile(r"\b(?:\d{4}[- ]?){3}\d{4}\b"), "[CC]"),
    (re.compile(r"\b\+?1?\s*\(?\d{3}\)?[\s.\-]\d{3}[\s.\-]\d{4}\b"), "[PHONE]"),
    (re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b"), "[EMAIL]"),
]


def mask_pii(text: str) -> str:
    if not text:
        return text
    for pattern, replacement in _PII_PATTERNS:
        text = pattern.sub(replacement, text)
    return text
