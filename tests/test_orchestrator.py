"""Tests for orchestrator and main workflow"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.orchestrator.orchestrator_agent import AuditOrchestrator
from src.orchestrator.retry_handler import retry_with_exponential_backoff
from src.utils.errors import AuditSystemError


def test_orchestrator_initialization():
    """Test that orchestrator initializes correctly"""
    orchestrator = AuditOrchestrator()
    assert orchestrator.audit_run_id is not None
    assert orchestrator.config is not None
    assert orchestrator.start_time is None


def test_full_audit_cycle_skipped_when_no_run_log(monkeypatch):
    """run_audit_cycle() returns 'skipped' when Gold tables have no completed run."""
    with patch("src.db.gold_table_reader.GoldTableReader") as MockReader:
        MockReader.return_value.get_last_unification_run_timestamp.return_value = None
        orchestrator = AuditOrchestrator()
        results = orchestrator.run_audit_cycle()

    assert results["status"] == "skipped"
    assert results["transaction_count"] == 0


def test_run_cycle_stale_store(monkeypatch):
    """run_audit_cycle() returns 'skipped' when Gold table last run is >24h old."""
    stale_ts = datetime.now(timezone.utc) - timedelta(hours=25)
    with patch("src.db.gold_table_reader.GoldTableReader") as MockReader:
        MockReader.return_value.get_last_unification_run_timestamp.return_value = stale_ts
        orchestrator = AuditOrchestrator()
        results = orchestrator.run_audit_cycle()

    assert results["status"] == "skipped"


def test_retry_handler():
    """Test retry logic with exponential backoff"""
    attempts = []

    def failing_func():
        attempts.append(1)
        if len(attempts) < 3:
            raise Exception("Test failure")
        return "success"

    result = retry_with_exponential_backoff(failing_func, max_retries=5, base_delay=0)
    assert result == "success"
    assert len(attempts) == 3


def test_retry_handler_exhaustion():
    """Test that retry handler raises error after max attempts"""
    def always_fail():
        raise Exception("Always fails")

    with pytest.raises(AuditSystemError):
        retry_with_exponential_backoff(always_fail, max_retries=3, base_delay=0)


def test_merge_suspicious_results():
    """Test merging results from parallel agents (4-agent pipeline: no anomaly key)"""
    orchestrator = AuditOrchestrator()

    parallel_results = {
        'reconciliation': {
            'unmatched_transactions': [
                {'txn_id': 'txn_001'},
                {'txn_id': 'txn_002'}
            ]
        },
        'data_quality': {
            'incomplete_records': ['txn_005'],
            'duplicates': {
                'duplicate_groups': [
                    {'ids': ['txn_003', 'txn_004'], 'count': 2}
                ]
            }
        }
    }

    transactions = pd.DataFrame({
        'txn_id': ['txn_001', 'txn_002', 'txn_003', 'txn_004', 'txn_005', 'txn_006'],
        'amount': [100, 200, 300, 400, 500, 600]
    })

    # Patch _get_uqi_unmatched to return None (fallback to parallel_results)
    with patch.object(orchestrator, '_get_uqi_unmatched', return_value=None):
        suspicious = orchestrator._merge_suspicious_results(parallel_results, transactions)

    suspicious_ids = [t['txn_id'] for t in suspicious]
    assert 'txn_001' in suspicious_ids
    assert 'txn_002' in suspicious_ids
    assert 'txn_003' in suspicious_ids
    assert 'txn_004' in suspicious_ids
    assert 'txn_005' in suspicious_ids
    assert 'txn_006' not in suspicious_ids


def test_context_enrichment_failure_nonfatal():
    """_run_context_enrichment returns {} on failure, not raising."""
    orchestrator = AuditOrchestrator()
    with patch("src.orchestrator.orchestrator_agent.Crew", side_effect=Exception("LLM down")):
        result = orchestrator._run_context_enrichment([{"txn_id": "t1"}])
    assert result == {}


def test_full_audit_cycle_happy_path():
    """run_audit_cycle() processes transactions when Gold tables have fresh data."""
    fresh_ts = datetime.now(timezone.utc) - timedelta(hours=1)
    txns = pd.DataFrame({
        'txn_id': ['txn_001', 'txn_002'],
        'vendor': ['AWS', 'Stripe'],
        'amount': [150.0, 300.0],
        'date': ['2026-03-18', '2026-03-18'],
        'category': ['software', 'payments'],
        'source': ['plaid', 'plaid'],
        'account_id': ['acc1', 'acc2'],
        'ingested_at': [datetime.now(timezone.utc), datetime.now(timezone.utc)],
    })

    with patch("src.db.gold_table_reader.GoldTableReader") as MockReader, \
         patch.object(AuditOrchestrator, "_run_parallel_agents",
                      return_value={"data_quality": {}, "reconciliation": {}}), \
         patch.object(AuditOrchestrator, "_augment_with_direct_analysis",
                      return_value={"data_quality": {}, "reconciliation": {}}), \
         patch.object(AuditOrchestrator, "_resolve_ambiguous_matches", return_value=[]), \
         patch.object(AuditOrchestrator, "_merge_suspicious_results", return_value=[]), \
         patch.object(AuditOrchestrator, "_run_context_enrichment", return_value={}), \
         patch.object(AuditOrchestrator, "_run_escalation_direct", return_value=0):
        MockReader.return_value.get_last_unification_run_timestamp.return_value = fresh_ts
        MockReader.return_value.get_transactions.return_value = txns
        MockReader.return_value.get_linked_transaction_ids.return_value = {'txn_001'}
        MockReader.return_value.get_unmatched_transaction_ids.return_value = {'txn_002'}
        orchestrator = AuditOrchestrator()
        results = orchestrator.run_audit_cycle()

    assert results["transaction_count"] == 2
    assert results["status"] != "skipped"


def test_run_audit_cycle_uses_gold_tables():
    """run_audit_cycle() reads from GoldTableReader for primary data load."""
    fresh_ts = datetime.now(timezone.utc) - timedelta(hours=1)
    txns = pd.DataFrame({
        'txn_id': ['txn_001', 'txn_002'],
        'vendor': ['Vendor A', 'Vendor B'],
        'amount': [100.0, 200.0],
        'date': ['2026-01-01', '2026-01-02'],
        'category': ['food', 'transport'],
        'source': ['plaid', 'plaid'],
        'account_id': ['acc1', 'acc2'],
        'ingested_at': [datetime.now(timezone.utc), datetime.now(timezone.utc)],
    })

    with patch("src.db.gold_table_reader.GoldTableReader") as MockReader, \
         patch.object(AuditOrchestrator, "_run_parallel_agents",
                      return_value={"data_quality": {"incomplete_records": [], "duplicates": {"duplicate_groups": []}},
                                    "reconciliation": {"unmatched_transactions": []}}), \
         patch.object(AuditOrchestrator, "_augment_with_direct_analysis",
                      side_effect=lambda r, _: r), \
         patch.object(AuditOrchestrator, "_resolve_ambiguous_matches", return_value=[]), \
         patch.object(AuditOrchestrator, "_merge_suspicious_results", return_value=[]), \
         patch.object(AuditOrchestrator, "_run_context_enrichment", return_value={}), \
         patch.object(AuditOrchestrator, "_run_escalation_direct", return_value=0):
        mock_reader = MockReader.return_value
        mock_reader.get_last_unification_run_timestamp.return_value = fresh_ts
        mock_reader.get_transactions.return_value = txns
        mock_reader.get_linked_transaction_ids.return_value = {'txn_001'}
        mock_reader.get_unmatched_transaction_ids.return_value = {'txn_002'}

        orchestrator = AuditOrchestrator()
        result = orchestrator.run_audit_cycle()

    mock_reader.get_last_unification_run_timestamp.assert_called_once()
    mock_reader.get_transactions.assert_called_once()
    mock_reader.get_linked_transaction_ids.assert_called_once()
    mock_reader.get_unmatched_transaction_ids.assert_called_once()
    assert result["transaction_count"] == 2
    assert result["status"] != "skipped"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
