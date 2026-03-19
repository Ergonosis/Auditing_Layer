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
    """run_audit_cycle() returns 'skipped' when UQI has no run_log."""
    mock_uqi = MagicMock()
    mock_uqi.get_last_run_status.return_value = None

    # get_uqi is imported inside run_audit_cycle — patch at the source module
    with patch("src.integrations.unification_client.get_uqi", return_value=mock_uqi):
        orchestrator = AuditOrchestrator()
        results = orchestrator.run_audit_cycle()

    assert results["status"] == "skipped"
    assert results["transaction_count"] == 0


def test_run_cycle_stale_store(monkeypatch):
    """run_audit_cycle() returns 'skipped' when last run is >24h old."""
    mock_run = MagicMock()
    mock_run.start_time = datetime.now(timezone.utc) - timedelta(hours=25)

    mock_uqi = MagicMock()
    mock_uqi.get_last_run_status.return_value = mock_run

    with patch("src.integrations.unification_client.get_uqi", return_value=mock_uqi):
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
    """run_audit_cycle() processes transactions when UQI has a fresh run_log."""
    mock_run = MagicMock()
    mock_run.start_time = datetime.now(timezone.utc) - timedelta(hours=1)

    def _make_entity(txn_id, amount, vendor):
        e = MagicMock()
        e.transaction_id = txn_id
        e.merchant_name = vendor
        e.name = vendor
        e.amount = amount
        e.date = "2026-03-18"
        e.source = "credit_card"
        return e

    mock_record_1 = MagicMock()
    mock_record_1.entity_id = "e1"
    mock_record_2 = MagicMock()
    mock_record_2.entity_id = "e2"

    mock_uqi = MagicMock()
    mock_uqi.get_last_run_status.return_value = mock_run
    mock_uqi.get_unlinked_entities.return_value = [mock_record_1, mock_record_2]
    mock_uqi.get_entity.side_effect = lambda eid, *a, **kw: {
        "e1": _make_entity("txn_001", 150.0, "AWS"),
        "e2": _make_entity("txn_002", 300.0, "Stripe"),
    }[eid]

    with patch("src.integrations.unification_client.get_uqi", return_value=mock_uqi), \
         patch.object(AuditOrchestrator, "_run_parallel_agents",
                      return_value={"data_quality": {}, "reconciliation": {}}), \
         patch.object(AuditOrchestrator, "_augment_with_direct_analysis",
                      return_value={"data_quality": {}, "reconciliation": {}}), \
         patch.object(AuditOrchestrator, "_resolve_ambiguous_matches", return_value=[]), \
         patch.object(AuditOrchestrator, "_merge_suspicious_results", return_value=[]), \
         patch.object(AuditOrchestrator, "_run_context_enrichment", return_value={}), \
         patch.object(AuditOrchestrator, "_run_escalation_direct", return_value=0):
        orchestrator = AuditOrchestrator()
        results = orchestrator.run_audit_cycle()

    assert results["transaction_count"] == 2
    assert results["status"] != "skipped"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
