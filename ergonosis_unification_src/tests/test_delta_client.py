"""Tests for src/storage/delta_client.py — enforcement gate, SQL escaping, identifier validation."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.constants import (
    ENV_DATABRICKS_HOST,
    ENV_DATABRICKS_TOKEN,
    ENV_SECURE_STORAGE_REQUIRED,
)
from src.storage.delta_client import DeltaClient, _esc, get_storage_backend
from src.storage.local_store import LocalStore
from src.utils.errors import ConfigurationError, SecureStorageRequiredError


# ── _esc helper ────────────────────────────────────────────────────────────────

class TestEscHelper:
    def test_normal_string(self):
        assert _esc("hello") == "'hello'"

    def test_single_quote_doubled(self):
        assert _esc("it's") == "'it''s'"

    def test_sql_injection_attempt(self):
        result = _esc("x' OR '1'='1")
        # Exact expected output — all embedded quotes are doubled
        assert result == "'x'' OR ''1''=''1'"

    def test_empty_string(self):
        assert _esc("") == "''"

    def test_uuid_unchanged(self):
        uid = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        assert _esc(uid) == f"'{uid}'"


# ── get_storage_backend enforcement gate ──────────────────────────────────────

class TestStorageBackendGate:
    def test_no_host_no_secure_mode_returns_local_store(self, monkeypatch, tmp_path):
        monkeypatch.delenv(ENV_DATABRICKS_HOST, raising=False)
        monkeypatch.delenv(ENV_SECURE_STORAGE_REQUIRED, raising=False)
        backend = get_storage_backend()
        assert isinstance(backend, LocalStore)
        backend.close()

    def test_no_host_secure_mode_raises(self, monkeypatch):
        monkeypatch.delenv(ENV_DATABRICKS_HOST, raising=False)
        monkeypatch.setenv(ENV_SECURE_STORAGE_REQUIRED, "true")
        with pytest.raises(SecureStorageRequiredError, match="DATABRICKS_HOST"):
            get_storage_backend()

    def test_no_token_secure_mode_raises(self, monkeypatch):
        monkeypatch.setenv(ENV_DATABRICKS_HOST, "https://fake.databricks.com")
        monkeypatch.delenv(ENV_DATABRICKS_TOKEN, raising=False)
        monkeypatch.setenv(ENV_SECURE_STORAGE_REQUIRED, "true")
        with pytest.raises(SecureStorageRequiredError, match="DATABRICKS_TOKEN"):
            get_storage_backend()

    def test_empty_token_secure_mode_raises(self, monkeypatch):
        monkeypatch.setenv(ENV_DATABRICKS_HOST, "https://fake.databricks.com")
        monkeypatch.setenv(ENV_DATABRICKS_TOKEN, "")
        monkeypatch.setenv(ENV_SECURE_STORAGE_REQUIRED, "true")
        with pytest.raises(SecureStorageRequiredError, match="DATABRICKS_TOKEN"):
            get_storage_backend()

    def test_connection_failure_secure_mode_raises(self, monkeypatch):
        monkeypatch.setenv(ENV_DATABRICKS_HOST, "https://fake.databricks.com")
        monkeypatch.setenv(ENV_DATABRICKS_TOKEN, "fake-token")
        monkeypatch.setenv(ENV_SECURE_STORAGE_REQUIRED, "true")
        with patch("src.storage.delta_client.DeltaClient._connect", side_effect=Exception("conn refused")):
            with pytest.raises(SecureStorageRequiredError, match="connection failed"):
                get_storage_backend()

    def test_connection_failure_non_secure_returns_local_store(self, monkeypatch):
        monkeypatch.setenv(ENV_DATABRICKS_HOST, "https://fake.databricks.com")
        monkeypatch.setenv(ENV_DATABRICKS_TOKEN, "fake-token")
        monkeypatch.delenv(ENV_SECURE_STORAGE_REQUIRED, raising=False)
        with patch("src.storage.delta_client.DeltaClient._connect", side_effect=Exception("conn refused")):
            backend = get_storage_backend()
        assert isinstance(backend, LocalStore)
        backend.close()

    def test_secure_mode_case_insensitive(self, monkeypatch):
        """UNIFICATION_SECURE_STORAGE_REQUIRED=True (capital T) still activates secure mode."""
        monkeypatch.delenv(ENV_DATABRICKS_HOST, raising=False)
        monkeypatch.setenv(ENV_SECURE_STORAGE_REQUIRED, "True")
        with pytest.raises(SecureStorageRequiredError):
            get_storage_backend()


# ── DeltaClient identifier validation ─────────────────────────────────────────

class TestIdentifierValidation:
    def _make_client(self, catalog="ergonosis", schema="unification"):
        mock_conn = MagicMock()
        with patch("src.storage.delta_client.DeltaClient._connect", return_value=mock_conn):
            return DeltaClient(host="h", token="t", catalog=catalog, schema=schema)

    def test_valid_catalog_and_schema_accepted(self):
        client = self._make_client(catalog="ergonosis", schema="unification")
        assert client.catalog == "ergonosis"

    def test_catalog_with_path_separator_raises(self):
        with pytest.raises(ConfigurationError, match="catalog"):
            self._make_client(catalog="ergonosis/../other")

    def test_schema_with_special_chars_raises(self):
        with pytest.raises(ConfigurationError, match="schema"):
            self._make_client(schema="unification; DROP TABLE entity_links--")

    def test_schema_with_hyphen_raises(self):
        with pytest.raises(ConfigurationError, match="schema"):
            self._make_client(schema="my-schema")

    def test_underscore_allowed(self):
        client = self._make_client(catalog="my_catalog", schema="my_schema_v2")
        assert client.schema == "my_schema_v2"


# ── upsert_link SQL escaping ───────────────────────────────────────────────────

class TestUpsertLinkSQLEscaping:
    def _make_client(self):
        mock_conn = MagicMock()
        with patch("src.storage.delta_client.DeltaClient._connect", return_value=mock_conn):
            client = DeltaClient(host="h", token="t")
        return client

    def test_single_quote_in_link_id_is_escaped(self):
        from tests.conftest import make_entity_link
        link = make_entity_link(source_id="txn' OR '1'='1", target_id="msg_001")
        client = self._make_client()
        captured_sql = []

        def fake_execute(sql, params=()):
            captured_sql.append(sql)
            mock_cursor = MagicMock()
            return mock_cursor

        client._execute = fake_execute
        client.upsert_link(link)

        assert len(captured_sql) == 1
        sql = captured_sql[0]
        # Injection attempt must be escaped — original unescaped pattern must not appear
        assert "txn' OR '1'='1" not in sql
        # Escaped form must appear
        assert "txn'' OR ''1''=''1" in sql

    def test_rationale_with_single_quote_is_escaped(self):
        from tests.conftest import make_entity_link
        link = make_entity_link(rationale="merchant's match")
        client = self._make_client()
        captured_sql = []
        client._execute = lambda sql, params=(): captured_sql.append(sql)
        client.upsert_link(link)
        assert "merchant''s match" in captured_sql[0]


# ── Parameterized DELETE statements ──────────────────────────────────────────

class TestParameterizedDeletes:
    """Verify that DELETE statements use ? placeholders instead of string interpolation."""

    def _make_client(self):
        client = object.__new__(DeltaClient)
        client.catalog = "ergonosis"
        client.schema = "unification"
        client._table = lambda name: f"ergonosis.unification.{name}"
        return client

    def test_acquire_pipeline_lock_parameterizes_timestamp(self):
        client = self._make_client()

        calls = []
        def mock_execute(sql, params=()):
            calls.append((sql, params))
            return MagicMock(rowcount=0)

        client._execute = mock_execute
        client._fetchone = lambda sql, params=(): {"run_id": "run_A"}

        client.acquire_pipeline_lock("run_A", ttl_seconds=3600)

        delete_calls = [(s, p) for s, p in calls if "DELETE" in s]
        assert len(delete_calls) >= 1
        sql, params = delete_calls[0]
        assert "?" in sql, "Timestamp should use ? placeholder"
        assert len(params) >= 1, "Timestamp value should be passed as parameter"

    def test_purge_old_records_parameterizes_cutoff(self):
        client = self._make_client()

        calls = []
        def mock_execute(sql, params=()):
            calls.append((sql, params))
            return MagicMock(rowcount=0)

        client._execute = mock_execute

        client.purge_old_records("entity_links", "created_at", 86400)

        assert len(calls) == 1
        sql, params = calls[0]
        assert "?" in sql, "Cutoff should use ? placeholder"
        assert len(params) == 1, "Cutoff value should be passed as parameter"
