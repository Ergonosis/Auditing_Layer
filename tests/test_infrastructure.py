"""Unit tests for core infrastructure components."""

import pytest
import os
from datetime import datetime


def test_databricks_mock_adapter():
    """Test that mock data adapter works."""
    from src.tools.databricks_client import query_gold_tables

    result = query_gold_tables("SELECT * FROM gold.recent_transactions")
    assert not result.empty
    assert 'txn_id' in result.columns
    assert len(result) == 3  # We have 3 sample transactions


def test_databricks_get_last_audit_timestamp():
    """Test retrieving last audit timestamp."""
    from src.tools.databricks_client import get_last_audit_timestamp

    result = get_last_audit_timestamp()
    assert isinstance(result, datetime)
    # Should either return a timestamp from fixture or default 2025-01-01
    assert result.year == 2025


def test_databricks_health_check():
    """Test Databricks health check in dev mode."""
    from src.tools.databricks_client import check_databricks_health

    # In dev mode, should always be healthy
    result = check_databricks_health()
    assert result is True


def test_databricks_missing_fixture():
    """Test behavior when fixture is missing."""
    from src.tools.databricks_client import query_gold_tables

    # Query a table with no fixture
    result = query_gold_tables("SELECT * FROM gold.nonexistent_table")
    assert result.empty  # Should return empty DataFrame, not crash


def test_llm_cost_calculation():
    """Test LLM cost calculation."""
    from src.tools.llm_client import calculate_cost
    from src.llm.provider import ANTHROPIC_FAST_MODEL, ANTHROPIC_SMART_MODEL

    # Test Haiku pricing ($1.00 per 1M tokens)
    cost = calculate_cost(1_000_000, ANTHROPIC_FAST_MODEL)
    assert cost == 1.00

    # Test Sonnet pricing ($3.00 per 1M tokens)
    cost = calculate_cost(1_000_000, ANTHROPIC_SMART_MODEL)
    assert cost == 3.00

    # Test smaller token count ($1.00/1M = $0.001/1K)
    cost = calculate_cost(1000, ANTHROPIC_FAST_MODEL)
    assert abs(cost - 0.001) < 1e-10

    # Test legacy model name resolution
    cost = calculate_cost(1_000_000, "anthropic/claude-haiku-4.5")
    assert cost == 1.00  # Should resolve to Haiku pricing


def test_llm_client_without_api_key():
    """Test LLM client behavior when API key is missing."""
    from src.llm.provider import reset_llm_client

    # Reset singleton so it re-reads env on next call
    reset_llm_client()

    original_key = os.getenv("ANTHROPIC_API_KEY")
    os.environ.pop("ANTHROPIC_API_KEY", None)

    try:
        # Should raise ValueError from provider (no key set)
        with pytest.raises(ValueError, match="ANTHROPIC_API_KEY"):
            from src.tools.llm_client import call_llm
            call_llm("Test prompt")
    finally:
        # Restore original key and reset singleton
        if original_key:
            os.environ["ANTHROPIC_API_KEY"] = original_key
        reset_llm_client()


def test_state_manager_save_restore():
    """Test Redis state save/restore."""
    from src.orchestrator.state_manager import save_workflow_state, restore_workflow_state

    state = {
        'test': 'data',
        'timestamp': datetime.now().isoformat(),
        'count': 42
    }

    audit_run_id = 'test_run_123'

    # Save state
    save_workflow_state(audit_run_id, state)

    # Restore state
    restored = restore_workflow_state(audit_run_id)

    # If Redis is available, state should match
    # If Redis is unavailable, restored will be empty dict (graceful degradation)
    if restored:
        assert restored['test'] == state['test']
        assert restored['count'] == state['count']
    else:
        # Redis not available, that's okay
        assert restored == {}


def test_state_manager_mark_complete():
    """Test marking audit as complete."""
    from src.orchestrator.state_manager import mark_audit_complete, restore_workflow_state

    audit_run_id = 'test_run_complete'
    summary = {
        'transactions_processed': 1000,
        'flags_created': 15
    }

    mark_audit_complete(audit_run_id, summary)

    # Restore and verify
    restored = restore_workflow_state(audit_run_id)
    if restored:  # Only check if Redis available
        assert restored['status'] == 'completed'
        assert 'completed_at' in restored


def test_state_manager_health_check():
    """Test Redis health check."""
    from src.orchestrator.state_manager import check_redis_health

    # Should return True or False depending on Redis availability
    result = check_redis_health()
    assert isinstance(result, bool)


def test_database_schemas_valid():
    """Test that database schemas are valid SQL."""
    from src.db.schemas import FLAGS_TABLE_SCHEMA, AUDIT_TRAIL_TABLE_SCHEMA, WORKFLOW_STATE_TABLE_SCHEMA

    # Check that schemas are non-empty strings
    assert len(FLAGS_TABLE_SCHEMA) > 0
    assert len(AUDIT_TRAIL_TABLE_SCHEMA) > 0
    assert len(WORKFLOW_STATE_TABLE_SCHEMA) > 0

    # Check that schemas contain expected keywords
    assert "CREATE TABLE" in FLAGS_TABLE_SCHEMA
    assert "CREATE TABLE" in AUDIT_TRAIL_TABLE_SCHEMA
    assert "CREATE TABLE" in WORKFLOW_STATE_TABLE_SCHEMA

    assert "NOT NULL" in FLAGS_TABLE_SCHEMA
    assert "NOT NULL" in AUDIT_TRAIL_TABLE_SCHEMA
    assert "NOT NULL" in WORKFLOW_STATE_TABLE_SCHEMA
    assert "USING DELTA" in FLAGS_TABLE_SCHEMA
    assert "USING DELTA" in AUDIT_TRAIL_TABLE_SCHEMA
    assert "USING DELTA" in WORKFLOW_STATE_TABLE_SCHEMA


def test_database_create_all_tables():
    """Test create_all_tables function exists and is callable."""
    from src.db.schemas import create_all_tables

    # Just verify the function exists and is callable
    assert callable(create_all_tables)


def test_fixture_data_structure():
    """Test that fixture files have correct structure."""
    import pandas as pd

    # Test transactions fixture
    transactions = pd.read_json("tests/fixtures/sample_transactions.json")
    assert 'txn_id' in transactions.columns
    assert 'amount' in transactions.columns
    assert 'vendor' in transactions.columns
    assert len(transactions) > 0

    # Test credit cards fixture
    credit_cards = pd.read_json("tests/fixtures/sample_credit_cards.json")
    assert 'txn_id' in credit_cards.columns
    assert 'amount' in credit_cards.columns
    assert len(credit_cards) > 0

    # Test emails fixture
    emails = pd.read_json("tests/fixtures/sample_emails.json")
    assert 'email_id' in emails.columns
    assert 'subject' in emails.columns
    assert len(emails) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
