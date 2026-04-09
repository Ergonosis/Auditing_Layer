"""SOC 2 Phase 3 — Medium severity findings tests."""
import pytest
from pydantic import ValidationError


# Test 1.4: Field length limits enforced on Email.subject
def test_email_subject_length_limit():
    from src.models.email import Email

    with pytest.raises(ValidationError):
        Email(
            message_id="msg001",
            received_at="2025-01-15T10:30:00Z",
            sender="vendor@example.com",
            recipients=["user@company.com"],
            subject="x" * 999,  # exceeds max_length=998
        )


# Test 1.4: Field length limits enforced on LinkFeedback.reason
def test_link_feedback_reason_length_limit():
    from src.models.links import LinkFeedback

    with pytest.raises(ValidationError):
        LinkFeedback(
            link_id="l1",
            signal="confirmed",
            source="human",
            reason="r" * 1001,  # exceeds max_length=1000
        )


# Test 1.7: sanitize.py redacts Basic auth
def test_sanitize_redacts_basic_auth():
    from src.utils.sanitize import sanitize_exception

    try:
        raise ValueError("Authorization: Basic dXNlcjpwYXNzd29yZA==")
    except ValueError as e:
        result = sanitize_exception(e)
    assert "dXNlcjpwYXNzd29yZA==" not in result
    assert "[REDACTED]" in result


# Test 1.7: sanitize.py redacts Databricks dapi tokens
def test_sanitize_redacts_databricks_token():
    from src.utils.sanitize import sanitize_exception

    try:
        raise ValueError("token=dapi1234567890abcdef1234567890abcd")
    except ValueError as e:
        result = sanitize_exception(e)
    assert "dapi1234567890abcdef1234567890abcd" not in result


# Test 3.3: AmbiguousMatch rejects out-of-range confidence scores
def test_ambiguous_match_rejects_out_of_range_score():
    from src.models.links import AmbiguousMatch

    with pytest.raises(ValidationError):
        AmbiguousMatch(
            source_entity_id="e1",
            source_type="transaction",
            target_type="email",
            candidate_ids=["c1", "c2"],
            candidate_scores=[1.5, 0.8],  # 1.5 > 1.0 — invalid
            run_id="run1",
        )


# Test 3.4: transform_batch aggregates hard failures and raises SchemaValidationError
def test_transform_batch_aggregates_errors():
    from src.etl.transformer import Transformer
    from src.utils.errors import SchemaValidationError

    transformer = Transformer()

    bad_records = [
        {},  # missing required fields
        {},  # another bad record
        {},  # third bad record
    ]
    with pytest.raises(SchemaValidationError) as exc_info:
        transformer.transform_batch(bad_records, "email")
    # Should mention multiple failures
    assert "3" in str(exc_info.value) or "record" in str(exc_info.value).lower()


# Test 3.5: Config loader rejects invalid amount_tolerance_pct
def test_config_loader_rejects_invalid_amount_tolerance(tmp_path):
    from src.utils.config_loader import load_config
    from src.utils.errors import ConfigurationError
    import yaml

    bad_config = {
        "rule_version": "1.0",
        "match_rules": {
            "transaction_email": {
                "amount_tolerance_pct": -0.5,  # invalid — must be in [0.0, 1.0]
            }
        },
    }
    config_file = tmp_path / "bad_config.yaml"
    config_file.write_text(yaml.dump(bad_config))

    with pytest.raises((ConfigurationError, Exception)):
        load_config(str(config_file))


# Test 4.5: PII masker redacts phone numbers
def test_pii_masker_redacts_phone():
    from src.utils.pii_masker import mask_pii

    result = mask_pii("Call me at 555-867-5309 for details")
    assert "555-867-5309" not in result
    assert "[PHONE]" in result


# Test 4.5: PII masker redacts SSN
def test_pii_masker_redacts_ssn():
    from src.utils.pii_masker import mask_pii

    result = mask_pii("SSN: 123-45-6789")
    assert "123-45-6789" not in result
    assert "[SSN]" in result


# Test 4.5: PII masker redacts email addresses in text
def test_pii_masker_redacts_email_in_text():
    from src.utils.pii_masker import mask_pii

    result = mask_pii("Contact vendor@example.com for invoice")
    assert "vendor@example.com" not in result
    assert "[EMAIL]" in result


# Test 5.5: set_user_preference and get_user_preference work correctly
def test_user_preference_opt_out(tmp_db):
    tmp_db.set_user_preference("hash_user1", opted_out=True)
    pref = tmp_db.get_user_preference("hash_user1")
    assert pref is not None
    assert pref.get("opted_out") is True


# Test 5.6: COLLECT_BODY_PREVIEW=false suppresses body_preview
def test_data_minimization_suppresses_body_preview(monkeypatch):
    from src.etl.transformer import Transformer

    monkeypatch.setenv("COLLECT_BODY_PREVIEW", "false")
    raw_email = {
        "message_id": "AAMktest002",
        "subject": "Invoice",
        "sender": "vendor@example.com",
        "recipients": ["user@company.com"],
        "received_at": "2025-01-15T10:30:00Z",
        "body_preview": "This is the preview text",
        "thread_id": "thread002",
    }
    transformer = Transformer()
    result = transformer.transform_email(raw_email)
    assert result.body_preview is None or result.body_preview == ""


# Test 3.1: LocalStore purge_old_records rejects invalid table name
def test_purge_old_records_rejects_invalid_table(tmp_db):
    from src.utils.errors import StorageError

    with pytest.raises(StorageError, match="Unsafe identifier"):
        tmp_db.purge_old_records("bad; DROP TABLE entity_links", "created_at", 3600)


# Test 3.2: LocalStore purge_old_records rejects invalid column name
def test_purge_old_records_rejects_invalid_col(tmp_db):
    from src.utils.errors import StorageError

    with pytest.raises(StorageError, match="Unsafe identifier"):
        tmp_db.purge_old_records("entity_links", "col--inject", 3600)


# Test 3.3: purge_old_audit_logs removes old rows from access_audit_log
def test_purge_old_audit_logs_respects_ttl(tmp_db):
    from datetime import datetime, timezone, timedelta
    from src.models.audit import AccessAuditRecord

    old_record = AccessAuditRecord(
        operation="get_linked_entities",
        entity_id_hash="abc123",
        entity_type="transaction",
        result_count=1,
        success=True,
    )
    # Backdate event_time to 8 years ago
    old_time = (datetime.now(timezone.utc) - timedelta(days=8 * 365)).isoformat()
    with tmp_db._connect() as conn:
        conn.execute(
            "INSERT INTO access_audit_log "
            "(audit_id, event_time, operation, caller, entity_id_hash, entity_type, run_id, result_count, success) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (old_record.audit_id, old_time, old_record.operation, "test",
             old_record.entity_id_hash, old_record.entity_type, None, 1, 1),
        )

    deleted = tmp_db.purge_old_audit_logs(ttl_seconds=7 * 365 * 24 * 3600)
    assert deleted >= 1

    with tmp_db._connect() as conn:
        rows = conn.execute(
            "SELECT * FROM access_audit_log WHERE audit_id=?", (old_record.audit_id,)
        ).fetchall()
    assert len(rows) == 0
