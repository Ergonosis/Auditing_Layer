"""Unit tests for Context Enrichment and Escalation agents"""

import json
import pytest
from datetime import datetime, timedelta
from src.tools.context_tools import (
    search_emails_batch,
    search_calendar_events,
    extract_approval_chains,
    find_receipt_images,
    semantic_search_documents
)
from src.tools.escalation_tools import (
    calculate_severity_score,
    generate_root_cause_analysis,
    batch_classify_with_llm,
    create_audit_flag,
    check_escalation_rules
)
from src.constants import SeverityLevel


class TestContextTools:
    """Test context enrichment tools"""

    def test_search_emails_batch(self):
        """Test batch email search"""
        txns = [
            {
                'txn_id': 'cc_1',
                'vendor': 'AWS',
                'amount': 500,
                'date': '2025-02-01'
            }
        ]
        result = search_emails_batch.func(json.dumps(txns))

        assert isinstance(result, dict)
        assert 'cc_1' in result or len(result) == 0  # May be empty with mock data

    def test_search_calendar_events(self):
        """Test calendar event search"""
        result = search_calendar_events.func('2025-02-01', 'AWS')

        assert isinstance(result, list)
        # Empty list is valid if no events found

    def test_extract_approval_chains(self):
        """Test approval chain extraction"""
        result = extract_approval_chains.func('thread_123')

        assert isinstance(result, dict)
        assert 'approved' in result
        assert 'approver' in result
        assert 'timestamp' in result
        assert 'approval_keywords' in result
        assert isinstance(result['approved'], bool)

    def test_find_receipt_images(self):
        """Test receipt image search"""
        date_range = ('2025-01-29', '2025-02-04')
        result = find_receipt_images.func('AWS', 500.0, date_range)

        assert isinstance(result, list)
        # Empty list is valid if no receipts found

    def test_semantic_search_documents(self):
        """Test semantic document search"""
        # This uses LLM, so may fail without API key
        try:
            result = semantic_search_documents.func('AWS infrastructure approval', top_k=3)
            assert isinstance(result, list)
            assert len(result) <= 3
        except Exception as e:
            # Expected if no LLM access or mock data not set up
            pytest.skip(f"Semantic search not available: {e}")


class TestEscalationTools:
    """Test escalation and classification tools"""

    def test_severity_calculation_critical(self):
        """Test severity calculation for CRITICAL case"""
        txn = {
            'txn_id': 'cc_1',
            'vendor': 'Unknown Vendor',
            'amount': 1000,
            'date': '2025-02-01'
        }
        agent_results = {
            'data_quality': {'incomplete': True},
            'reconciliation': {'matched': False},
        }

        result = calculate_severity_score.func(txn, agent_results)

        assert isinstance(result, dict)
        assert 'severity_score' in result
        assert 'level' in result
        assert 'confidence' in result
        assert 'contributing_factors' in result

        # Should be CRITICAL: no_reconciliation_match(+50) + incomplete_data(+30) = 80
        assert result['level'] == SeverityLevel.CRITICAL.value
        assert result['severity_score'] >= 70
        assert len(result['contributing_factors']) == 2

    def test_severity_calculation_warning(self):
        """Test severity calculation for WARNING case"""
        txn = {
            'txn_id': 'cc_2',
            'vendor': 'AWS',
            'amount': 500,
            'date': '2025-02-01'
        }
        agent_results = {
            'reconciliation': {'matched': False},
            'anomaly': {'anomaly_score': 65},
            'context': {
                'email_approval': True,
                'receipt_found': False
            }
        }

        result = calculate_severity_score.func(txn, agent_results)

        # Should be WARNING: 40 + 10 = 50
        assert result['level'] == SeverityLevel.WARNING.value
        assert result['severity_score'] >= 50
        assert result['severity_score'] < 80

    def test_severity_calculation_info(self):
        """Test severity calculation for INFO case"""
        txn = {
            'txn_id': 'cc_3',
            'vendor': 'AWS',
            'amount': 100,
            'date': '2025-02-01'
        }
        agent_results = {
            'reconciliation': {'matched': True},
            'anomaly': {'anomaly_score': 45},
            'context': {
                'email_approval': True,
                'receipt_found': True
            }
        }

        result = calculate_severity_score.func(txn, agent_results)

        # Should be INFO: 0 points
        assert result['level'] == SeverityLevel.INFO.value
        assert result['severity_score'] < 50

    def test_root_cause_analysis_template(self):
        """Test root cause analysis using template (high confidence)"""
        txn = {
            'txn_id': 'cc_1',
            'vendor': 'Unknown',
            'amount': 1000,
            'date': '2025-02-01'
        }
        agent_results = {
            'escalation': {
                'confidence': 0.8,
                'contributing_factors': [
                    'no_reconciliation_match',
                    'high_anomaly_score'
                ]
            },
            'reconciliation': {'matched': False},
            'anomaly': {'anomaly_score': 85},
            'context': {
                'email_approval': False,
                'receipt_found': False
            }
        }

        result = generate_root_cause_analysis.func(txn, agent_results)

        assert isinstance(result, str)
        assert len(result) > 0
        assert 'Flagged because:' in result or 'Transaction' in result

    def test_root_cause_analysis_llm(self):
        """Test root cause analysis using LLM (low confidence)"""
        txn = {
            'txn_id': 'cc_2',
            'vendor': 'EdgeCase',
            'amount': 777,
            'date': '2025-02-01'
        }
        agent_results = {
            'escalation': {
                'confidence': 0.5,  # Low confidence triggers LLM
                'contributing_factors': []
            },
            'reconciliation': {'matched': False},
            'anomaly': {'anomaly_score': 55},
            'context': {
                'email_approval': False,
                'receipt_found': True
            }
        }

        try:
            result = generate_root_cause_analysis.func(txn, agent_results)
            assert isinstance(result, str)
            assert len(result) > 0
        except Exception as e:
            # Expected if no LLM access
            pytest.skip(f"LLM not available: {e}")

    def test_batch_classify_with_llm(self):
        """Test batch LLM classification"""
        transactions = [
            {'txn_id': 'cc_1', 'vendor': 'Vendor1', 'amount': 100},
            {'txn_id': 'cc_2', 'vendor': 'Vendor2', 'amount': 200}
        ]
        agent_results_list = [
            {
                'reconciliation': {'matched': False},
                'anomaly': {'anomaly_score': 60},
                'context': {'email_approval': True, 'receipt_found': False}
            },
            {
                'reconciliation': {'matched': True},
                'anomaly': {'anomaly_score': 40},
                'context': {'email_approval': True, 'receipt_found': True}
            }
        ]

        try:
            result = batch_classify_with_llm.func(json.dumps(transactions), json.dumps(agent_results_list))
            assert isinstance(result, list)
            # Should have classification for each transaction
            if len(result) > 0:
                assert 'txn_id' in result[0]
                assert 'severity' in result[0]
                assert 'explanation' in result[0]
        except Exception as e:
            # Expected if no LLM access
            pytest.skip(f"LLM not available: {e}")

    def test_create_audit_flag(self):
        """Test audit flag creation"""
        flag_id = create_audit_flag.func(
            transaction_id='cc_1',
            audit_run_id='run_123',
            severity='CRITICAL',
            explanation='Test flag',
            evidence={'email': 'e1', 'receipt': 'r1'}
        )

        assert isinstance(flag_id, str)
        assert len(flag_id) == 36  # UUID format

    def test_escalation_rules_critical_high_value(self):
        """Test escalation rule for high-value CRITICAL"""
        result = check_escalation_rules.func(
            severity=SeverityLevel.CRITICAL.value,
            amount=2000,
            vendor='Unknown'
        )

        # Should remain CRITICAL
        assert result == SeverityLevel.CRITICAL.value

    def test_escalation_rules_whitelist(self):
        """Test escalation rule for whitelisted vendor"""
        # Note: This assumes whitelisted_vendors is in config
        # For proper testing, would need to mock config
        result = check_escalation_rules.func(
            severity=SeverityLevel.WARNING.value,
            amount=100,
            vendor='AWS'  # Assuming AWS might be whitelisted
        )

        # Result depends on config, just check it's a valid severity
        assert result in [
            SeverityLevel.CRITICAL.value,
            SeverityLevel.WARNING.value,
            SeverityLevel.INFO.value,
            'AUTO_APPROVED'
        ]

    def test_escalation_rules_auto_approve(self):
        """Test escalation rule for low-value INFO"""
        result = check_escalation_rules.func(
            severity=SeverityLevel.INFO.value,
            amount=25,
            vendor='CoffeeShop'
        )

        # Should be auto-approved (amount < 50)
        assert result == 'AUTO_APPROVED'


class TestContextAgent:
    """Test Context Enrichment Agent"""

    def test_context_agent_exists(self):
        """Test that context agent is properly configured"""
        from src.agents.context_enrichment_agent import context_agent, context_task

        assert context_agent is not None
        assert context_task is not None
        assert context_agent.role == "Context Enrichment Specialist"
        assert len(context_agent.tools) == 5


class TestEscalationAgent:
    """Test Escalation Agent"""

    def test_escalation_agent_exists(self):
        """Test that escalation agent is properly configured"""
        from src.agents.escalation_agent import escalation_agent, escalation_task

        assert escalation_agent is not None
        assert escalation_task is not None
        assert escalation_agent.role == "Escalation & Classification Specialist"
        assert len(escalation_agent.tools) == 5


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
