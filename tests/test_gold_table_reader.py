"""Tests for GoldTableReader — mocks Databricks connection."""

import os
from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.utils.errors import DatabricksConnectionError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_cursor(fetchall_rows=None, fetchone_row=None, description=None):
    """Return a mock cursor context-manager whose fetchall/fetchone are pre-set."""
    cursor = MagicMock()
    cursor.fetchall.return_value = fetchall_rows or []
    cursor.fetchone.return_value = fetchone_row
    cursor.description = description or []

    @contextmanager
    def _cursor_ctx():
        yield cursor

    return cursor, _cursor_ctx


def _make_conn(cursor, cursor_ctx):
    conn = MagicMock()
    conn.cursor = cursor_ctx
    return conn


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

TXN_ROWS = [
    ("txn_001", "Vendor A", 150.0, "2026-01-01", "food", "plaid", "acc1",
     datetime(2026, 1, 1, tzinfo=timezone.utc)),
    ("txn_002", "Vendor B", 6000.0, "2026-01-02", "transport", "plaid", "acc2",
     datetime(2026, 1, 2, tzinfo=timezone.utc)),
]
TXN_DESCRIPTION = [
    ("transaction_id",), ("vendor",), ("amount",), ("date",),
    ("category",), ("source",), ("account_id",), ("ingested_at",),
]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestGoldTableReaderGetTransactions:
    def test_returns_dataframe_with_correct_columns(self, monkeypatch):
        cursor, cursor_ctx = _make_cursor(
            fetchall_rows=TXN_ROWS,
            description=TXN_DESCRIPTION,
        )
        conn = _make_conn(cursor, cursor_ctx)

        monkeypatch.setenv("DATABRICKS_HOST", "test.azuredatabricks.net")
        monkeypatch.setenv("DATABRICKS_TOKEN", "test-token")
        monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/test")

        with patch("databricks.sql.connect", return_value=conn):
            from src.db.gold_table_reader import GoldTableReader
            reader = GoldTableReader()
            df = reader.get_transactions()

        assert isinstance(df, pd.DataFrame)
        assert "txn_id" in df.columns
        assert "vendor" in df.columns
        assert "amount" in df.columns
        assert "date" in df.columns
        assert "category" in df.columns
        assert "source" in df.columns
        assert "account_id" in df.columns
        assert "ingested_at" in df.columns
        assert len(df) == 2
        assert df.iloc[0]["txn_id"] == "txn_001"

    def test_returns_empty_dataframe_on_db_error(self, monkeypatch):
        cursor = MagicMock()
        cursor.execute.side_effect = Exception("DB error")
        conn = MagicMock()

        @contextmanager
        def _ctx():
            yield cursor

        conn.cursor = _ctx

        monkeypatch.setenv("DATABRICKS_HOST", "test.azuredatabricks.net")
        monkeypatch.setenv("DATABRICKS_TOKEN", "test-token")
        monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/test")

        with patch("databricks.sql.connect", return_value=conn):
            from importlib import import_module, reload
            import src.db.gold_table_reader as mod
            reload(mod)
            reader = mod.GoldTableReader.__new__(mod.GoldTableReader)
            reader.catalog = "ergonosis"
            reader._connection = conn
            df = reader.get_transactions()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0


class TestGoldTableReaderLinkedIds:
    def _reader_with_conn(self, conn, monkeypatch):
        monkeypatch.setenv("DATABRICKS_HOST", "test.azuredatabricks.net")
        monkeypatch.setenv("DATABRICKS_TOKEN", "test-token")
        monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/test")
        with patch("databricks.sql.connect", return_value=conn):
            from importlib import reload
            import src.db.gold_table_reader as mod
            reload(mod)
            return mod.GoldTableReader()

    def test_returns_set_of_matched_ids(self, monkeypatch):
        cursor, cursor_ctx = _make_cursor(fetchall_rows=[("txn_001",), ("txn_002",)])
        conn = _make_conn(cursor, cursor_ctx)
        reader = self._reader_with_conn(conn, monkeypatch)
        result = reader.get_linked_transaction_ids()
        assert isinstance(result, set)
        assert result == {"txn_001", "txn_002"}

    def test_returns_empty_set_on_error(self, monkeypatch):
        cursor = MagicMock()
        cursor.execute.side_effect = Exception("table not found")
        conn = MagicMock()

        @contextmanager
        def _ctx():
            yield cursor

        conn.cursor = _ctx
        reader = self._reader_with_conn(conn, monkeypatch)
        result = reader.get_linked_transaction_ids()
        assert isinstance(result, set)
        assert len(result) == 0


class TestGoldTableReaderUnmatchedIds:
    def _reader_with_conn(self, conn, monkeypatch):
        monkeypatch.setenv("DATABRICKS_HOST", "test.azuredatabricks.net")
        monkeypatch.setenv("DATABRICKS_TOKEN", "test-token")
        monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/test")
        with patch("databricks.sql.connect", return_value=conn):
            from importlib import reload
            import src.db.gold_table_reader as mod
            reload(mod)
            return mod.GoldTableReader()

    def test_returns_set_of_unmatched_ids(self, monkeypatch):
        rows = [("txn_003",), ("txn_004",), ("txn_005",)]
        cursor, cursor_ctx = _make_cursor(fetchall_rows=rows)
        conn = _make_conn(cursor, cursor_ctx)
        reader = self._reader_with_conn(conn, monkeypatch)
        result = reader.get_unmatched_transaction_ids()
        assert isinstance(result, set)
        assert result == {"txn_003", "txn_004", "txn_005"}

    def test_returns_empty_set_on_error(self, monkeypatch):
        cursor = MagicMock()
        cursor.execute.side_effect = Exception("table missing")
        conn = MagicMock()

        @contextmanager
        def _ctx():
            yield cursor

        conn.cursor = _ctx
        reader = self._reader_with_conn(conn, monkeypatch)
        result = reader.get_unmatched_transaction_ids()
        assert isinstance(result, set)
        assert len(result) == 0


class TestGoldTableReaderRunTimestamp:
    def _reader_with_conn(self, conn, monkeypatch):
        monkeypatch.setenv("DATABRICKS_HOST", "test.azuredatabricks.net")
        monkeypatch.setenv("DATABRICKS_TOKEN", "test-token")
        monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/test")
        with patch("databricks.sql.connect", return_value=conn):
            from importlib import reload
            import src.db.gold_table_reader as mod
            reload(mod)
            return mod.GoldTableReader()

    def test_returns_none_when_no_completed_runs(self, monkeypatch):
        cursor, cursor_ctx = _make_cursor(fetchone_row=(None,))
        conn = _make_conn(cursor, cursor_ctx)
        reader = self._reader_with_conn(conn, monkeypatch)
        result = reader.get_last_unification_run_timestamp()
        assert result is None

    def test_returns_utc_aware_datetime_when_run_exists(self, monkeypatch):
        ts = datetime(2026, 3, 19, 12, 0, 0, tzinfo=timezone.utc)
        cursor, cursor_ctx = _make_cursor(fetchone_row=(ts,))
        conn = _make_conn(cursor, cursor_ctx)
        reader = self._reader_with_conn(conn, monkeypatch)
        result = reader.get_last_unification_run_timestamp()
        assert isinstance(result, datetime)
        assert result.tzinfo is not None
        assert result == ts

    def test_adds_utc_to_naive_datetime(self, monkeypatch):
        ts_naive = datetime(2026, 3, 19, 12, 0, 0)  # no tzinfo
        cursor, cursor_ctx = _make_cursor(fetchone_row=(ts_naive,))
        conn = _make_conn(cursor, cursor_ctx)
        reader = self._reader_with_conn(conn, monkeypatch)
        result = reader.get_last_unification_run_timestamp()
        assert result is not None
        assert result.tzinfo is not None

    def test_returns_none_when_run_log_table_missing(self, monkeypatch):
        cursor = MagicMock()
        cursor.execute.side_effect = Exception("Table not found: run_log")
        conn = MagicMock()

        @contextmanager
        def _ctx():
            yield cursor

        conn.cursor = _ctx
        reader = self._reader_with_conn(conn, monkeypatch)
        result = reader.get_last_unification_run_timestamp()
        assert result is None


class TestGoldTableReaderConnectionErrors:
    def test_raises_on_missing_host(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_HOST", raising=False)
        monkeypatch.setenv("DATABRICKS_TOKEN", "test-token")
        monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/test")
        with pytest.raises(DatabricksConnectionError):
            from importlib import reload
            import src.db.gold_table_reader as mod
            reload(mod)
            mod.GoldTableReader()

    def test_raises_on_missing_token(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_HOST", "test.azuredatabricks.net")
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/test")
        with pytest.raises(DatabricksConnectionError):
            from importlib import reload
            import src.db.gold_table_reader as mod
            reload(mod)
            mod.GoldTableReader()
