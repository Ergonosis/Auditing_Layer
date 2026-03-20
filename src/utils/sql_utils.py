"""SQL input sanitization utilities for safe query construction.

These helpers must be applied to ALL user-supplied or LLM-supplied values
before interpolation into SQL strings.  Databricks does not support
positional-parameter binding for identifiers (table/schema names), so we
use strict allowlists instead.
"""

import re


# ---------------------------------------------------------------------------
# String literals
# ---------------------------------------------------------------------------

_DISALLOWED_RE = re.compile(r";|--|/\*")


def sanitize_sql_value(value: str) -> str:
    """Sanitize a string value for safe interpolation into a SQL literal.

    Escapes single quotes (ANSI SQL standard doubling) and rejects strings
    containing metacharacters that cannot be safely escaped:
      - semicolons  (statement terminator / stacking)
      - ``--``      (inline comment)
      - ``/*``      (block comment open)

    Usage::

        safe = sanitize_sql_value(user_input)
        query = f"SELECT * FROM t WHERE col = '{safe}'"

    Raises:
        ValueError: If the value contains disallowed metacharacters.
    """
    s = str(value)
    if _DISALLOWED_RE.search(s):
        raise ValueError(f"SQL value contains disallowed metacharacters: {s!r}")
    return s.replace("'", "''")


# ---------------------------------------------------------------------------
# Numeric values
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Identifiers (table names, schema names, source tags)
# ---------------------------------------------------------------------------

# Allows letters, digits, underscores, and dots (for catalog.schema.table).
_SAFE_IDENTIFIER_RE = re.compile(r"^[a-zA-Z0-9_.]+$")


def validate_identifier(value: str) -> str:
    """Validate a SQL identifier (table name, schema, source tag).

    Only permits alphanumeric characters, underscores, and dots.
    This is an allowlist — anything outside that set raises immediately.

    Usage::

        safe_table = validate_identifier(table_name)
        query = f"SELECT * FROM {safe_table}"

    Raises:
        ValueError: If the identifier contains characters outside the allowlist.
    """
    s = str(value)
    if not _SAFE_IDENTIFIER_RE.match(s):
        raise ValueError(
            f"SQL identifier contains disallowed characters: {s!r}. "
            "Only alphanumeric characters, underscores, and dots are permitted."
        )
    return s
