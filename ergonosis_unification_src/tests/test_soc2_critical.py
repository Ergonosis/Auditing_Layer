"""SOC 2 Phase 1 — Critical findings tests."""
import hashlib
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch


# Test 1.1: DeltaClient does not store token as instance variable
def test_delta_client_no_token_attribute():
    from src.storage.delta_client import DeltaClient

    mock_conn = MagicMock()

    with patch("databricks.sql.connect", return_value=mock_conn):
        client = DeltaClient(host="fake.host", token="super_secret_token")

    assert not hasattr(client, "token"), "DeltaClient must not store token as instance variable"


# Test 1.2: Microsoft Graph raises IngestionCredentialsRequiredError in secure mode
def test_msgraph_raises_in_secure_mode_without_credentials(monkeypatch):
    from src.utils.errors import IngestionCredentialsRequiredError
    from src.ingestion.microsoft_adapter import fetch_emails

    monkeypatch.setenv("UNIFICATION_SECURE_STORAGE_REQUIRED", "true")
    monkeypatch.delenv("MSGRAPH_CLIENT_ID", raising=False)
    monkeypatch.delenv("MSGRAPH_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("STUB_INGESTION", raising=False)

    with pytest.raises(IngestionCredentialsRequiredError):
        fetch_emails(
            user_email="test@example.com",
            start_datetime="2025-01-01T00:00:00Z",
            end_datetime="2025-01-31T23:59:59Z",
        )


# Test 1.3: Plaid raises IngestionCredentialsRequiredError in secure mode
def test_plaid_raises_in_secure_mode_without_credentials(monkeypatch):
    from src.utils.errors import IngestionCredentialsRequiredError
    from src.ingestion.plaid_adapter import fetch_plaid_transactions

    monkeypatch.setenv("UNIFICATION_SECURE_STORAGE_REQUIRED", "true")
    monkeypatch.delenv("PLAID_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("STUB_INGESTION", raising=False)

    with pytest.raises(IngestionCredentialsRequiredError):
        fetch_plaid_transactions(
            account_id="acc_test",
            date_range=("2025-01-01", "2025-01-31"),
        )


# Test 4.2: Sender email is hashed at ETL time
def test_sender_email_hashed_at_etl():
    from src.etl.transformer import Transformer

    raw_email = {
        "message_id": "AAMktest001",
        "subject": "Test invoice",
        "sender": "vendor@example.com",
        "recipients": ["user@company.com"],
        "received_at": "2025-01-15T10:30:00Z",
        "body_preview": "Please find attached",
        "thread_id": "thread001",
    }
    transformer = Transformer()
    result = transformer.transform_email(raw_email)
    expected_hash = hashlib.sha256("vendor@example.com".encode()).hexdigest()
    assert result.sender == expected_hash, f"Expected hashed sender, got: {result.sender}"
    assert "@" not in result.sender, "Sender should not contain @ after ETL"


# Test 5.1: purge_old_records removes expired rows
def test_purge_old_records_removes_expired(tmp_db):
    old_time = (datetime.now(timezone.utc) - timedelta(days=400)).isoformat()

    with tmp_db._connect() as conn:
        conn.execute(
            "INSERT INTO entity_links "
            "(link_id, source_id, target_id, source_type, target_type, match_type, match_tier, "
            "confidence, linkage_key, rule_version, created_at, effective_from, is_current) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("link_old", "s1", "t1", "transaction", "email",
             "deterministic", "tier1_exact", 0.9, "key", "1.0",
             old_time, old_time, 1),
        )

    deleted = tmp_db.purge_old_records("entity_links", "created_at", 365 * 24 * 3600)
    assert deleted >= 1

    with tmp_db._connect() as conn:
        rows = conn.execute(
            "SELECT * FROM entity_links WHERE link_id='link_old'"
        ).fetchall()
    assert len(rows) == 0


# Test 5.2: hard_delete_entity_data removes all references and writes deletion_audit
def test_hard_delete_entity_data(tmp_db):
    now = datetime.now(timezone.utc).isoformat()

    with tmp_db._connect() as conn:
        conn.execute(
            "INSERT INTO entity_links "
            "(link_id, source_id, target_id, source_type, target_type, match_type, match_tier, "
            "confidence, linkage_key, rule_version, created_at, effective_from, is_current) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("link_del1", "entity_to_delete", "other_entity", "transaction", "email",
             "deterministic", "tier1_exact", 0.9, "key", "1.0", now, now, 1),
        )
        conn.execute(
            "INSERT INTO unmatched_entities "
            "(entity_id, entity_type, target_type, reason_code, run_id, logged_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("entity_to_delete", "transaction", "email", "NO_CANDIDATE_FOUND", "run_test", now),
        )

    total = tmp_db.hard_delete_entity_data("entity_to_delete", "transaction", "test")
    assert total >= 2

    with tmp_db._connect() as conn:
        links = conn.execute(
            "SELECT * FROM entity_links WHERE source_id='entity_to_delete'"
        ).fetchall()
        assert len(links) == 0

        unmatched = conn.execute(
            "SELECT * FROM unmatched_entities WHERE entity_id='entity_to_delete'"
        ).fetchall()
        assert len(unmatched) == 0

        audit = conn.execute(
            "SELECT * FROM deletion_audit WHERE entity_id='entity_to_delete'"
        ).fetchall()
        assert len(audit) == 1
        audit_row = dict(audit[0])
        assert audit_row["status"] == "completed"


# Test 1.4: LocalStore raises SecureStorageRequiredError when UNIFICATION_SECURE_STORAGE_REQUIRED=true
def test_local_store_raises_in_secure_mode(monkeypatch, tmp_path):
    from src.utils.errors import SecureStorageRequiredError
    from src.storage.local_store import LocalStore

    monkeypatch.setenv("UNIFICATION_SECURE_STORAGE_REQUIRED", "true")
    with pytest.raises(SecureStorageRequiredError):
        LocalStore(db_path=str(tmp_path / "secure_test.db"))


# Test 1.5: check_workspace_encryption raises StorageError in secure mode when CMK is not enabled
def test_check_workspace_encryption_raises_in_secure_mode(monkeypatch):
    from unittest.mock import patch, MagicMock
    import json
    from src.storage.delta_client import DeltaClient
    from src.utils.errors import StorageError

    monkeypatch.setenv("UNIFICATION_SECURE_STORAGE_REQUIRED", "true")

    mock_conn = MagicMock()
    mock_response = MagicMock()
    mock_response.read.return_value = json.dumps({"enableCustomerManagedKey": "false"}).encode()
    mock_response.__enter__ = lambda s: s
    mock_response.__exit__ = MagicMock(return_value=False)

    with patch("databricks.sql.connect", return_value=mock_conn):
        client = DeltaClient(host="fake.host", token="tok")

    with patch("urllib.request.urlopen", return_value=mock_response):
        with pytest.raises(StorageError, match="CMK"):
            client.check_workspace_encryption("tok")
