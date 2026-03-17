"""Tests for the SQL sanitization helpers in context_tools."""

import pytest


def test_sanitize_single_quote_escaping():
    """Single quotes in vendor names must be doubled, not rejected."""
    from src.tools.context_tools import sanitize_sql_value

    assert sanitize_sql_value("O'Reilly") == "O''Reilly"


def test_sanitize_rejects_sql_comment():
    """Strings containing -- (inline comment) must be rejected."""
    from src.tools.context_tools import sanitize_sql_value

    with pytest.raises(ValueError):
        sanitize_sql_value("AWS -- drop table")


def test_sanitize_rejects_semicolon():
    """Strings containing semicolons must be rejected."""
    from src.tools.context_tools import sanitize_sql_value

    with pytest.raises(ValueError):
        sanitize_sql_value("vendor; DELETE FROM emails")
