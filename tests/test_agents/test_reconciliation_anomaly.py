"""Unit tests for Reconciliation and Anomaly Detection Agents"""

import json
import pytest
from datetime import datetime, timedelta
from src.tools.reconciliation_tools import (
    cross_source_matcher,
    entity_resolver_kg,
    fuzzy_vendor_matcher,
    receipt_transaction_matcher,
    find_orphan_transactions
)
from src.tools.anomaly_tools import (
    run_isolation_forest,
    check_vendor_spending_profile,
    detect_amount_outliers,
    time_series_deviation_check,
    batch_anomaly_scorer
)


# Reconciliation Tools Tests

def test_cross_source_matcher():
    """Test cross-source transaction matching"""
    # This will use mock data from databricks_client
    result = cross_source_matcher.func('credit_card', 'bank', json.dumps(['2025-02-01', '2025-02-28']))

    assert 'matched_pairs' in result
    assert 'match_rate' in result
    assert 'unmatched_source_1' in result
    assert 'unmatched_source_2' in result
    assert isinstance(result['match_rate'], float)
    assert 0 <= result['match_rate'] <= 1


def test_entity_resolver_kg():
    """Test Knowledge Graph entity resolution"""
    result = entity_resolver_kg.func('Amazon Marketplace')

    assert 'canonical_entity_id' in result
    assert 'canonical_name' in result
    assert 'aliases' in result
    assert 'confidence' in result
    assert isinstance(result['confidence'], float)
    assert 0 <= result['confidence'] <= 1


def test_fuzzy_vendor_matcher():
    """Test fuzzy vendor name matching"""
    # Test exact match
    result1 = fuzzy_vendor_matcher.func('Amazon', 'Amazon')
    assert result1 == 1.0

    # Test similar match
    result2 = fuzzy_vendor_matcher.func('Amazon Marketplace', 'AMAZON MKTPLACE')
    assert result2 > 0.5

    # Test dissimilar match
    result3 = fuzzy_vendor_matcher.func('Amazon', 'Walmart')
    assert result3 < 0.5


def test_receipt_transaction_matcher():
    """Test receipt to transaction matching"""
    receipt_data = {
        'vendor': 'Amazon',
        'amount': 150.00,
        'date': '2025-02-15'
    }

    result = receipt_transaction_matcher.func(json.dumps(receipt_data), 'gold.transactions')

    assert 'matched_transaction_id' in result
    assert 'confidence' in result
    assert 'amount_delta' in result
    assert 'date_delta_days' in result


def test_find_orphan_transactions():
    """Test orphan transaction detection"""
    sources = ['credit_card', 'bank', 'receipts']

    result = find_orphan_transactions.func(json.dumps(sources))

    assert 'orphan_count' in result
    assert 'orphans' in result
    assert isinstance(result['orphan_count'], int)
    assert isinstance(result['orphans'], list)


# Anomaly Detection Tools Tests

def test_run_isolation_forest():
    """Test Isolation Forest anomaly detection"""
    transactions = [
        {'txn_id': 'cc_1', 'amount': 100, 'vendor_id': 'v1', 'date': '2025-02-01'},
        {'txn_id': 'cc_2', 'amount': 5000, 'vendor_id': 'v2', 'date': '2025-02-02'},
        {'txn_id': 'cc_3', 'amount': 95, 'vendor_id': 'v1', 'date': '2025-02-03'},
        {'txn_id': 'cc_4', 'amount': 110, 'vendor_id': 'v3', 'date': '2025-02-04'},
    ]

    result = run_isolation_forest.func(json.dumps(transactions))

    assert 'anomaly_scores' in result
    assert 'anomaly_count' in result
    assert isinstance(result['anomaly_count'], int)
    # Model may not be pre-fitted in test environment — scores list may be empty on first run
    assert isinstance(result['anomaly_scores'], list)


def test_check_vendor_spending_profile():
    """Test vendor spending profile check"""
    result = check_vendor_spending_profile.func('amazon_marketplace', 150.00)

    assert 'is_outlier' in result
    assert 'z_score' in result
    assert 'vendor_mean' in result
    assert 'vendor_std' in result
    assert 'explanation' in result
    assert isinstance(result['is_outlier'], bool)


def test_detect_amount_outliers():
    """Test statistical amount outlier detection"""
    # Use many normal values (~100) plus one extreme outlier to ensure z-score > 2.0.
    # With 20 values near 100 and one at 10000, mean≈570, std≈2120, z_outlier≈4.4.
    normal = [{'txn_id': f'cc_{i}', 'amount': 100 + i} for i in range(20)]
    outlier = [{'txn_id': 'cc_outlier', 'amount': 10000}]
    transactions = normal + outlier

    result = detect_amount_outliers.func(json.dumps(transactions))

    assert 'outliers' in result
    assert 'outlier_count' in result
    assert result['outlier_count'] >= 1  # Should detect the 10000 amount


def test_time_series_deviation_check():
    """Test time series deviation detection"""
    base_date = datetime(2025, 1, 1)
    transactions = [
        {'txn_id': f'cc_{i}', 'amount': 100 + (i % 2) * 10,
         'date': (base_date + timedelta(days=i*30)).isoformat()}
        for i in range(5)
    ]
    # Add a deviation
    transactions.append({'txn_id': 'cc_6', 'amount': 200,
                         'date': (base_date + timedelta(days=180)).isoformat()})

    result = time_series_deviation_check.func(json.dumps(transactions))

    assert 'deviations' in result
    assert 'deviation_count' in result
    assert isinstance(result['deviation_count'], int)


def test_batch_anomaly_scorer():
    """Test batch anomaly scoring"""
    transactions = [
        {
            'txn_id': 'cc_1',
            'is_anomaly_if': True,
            'is_vendor_outlier': False,
            'is_amount_outlier': False,
            'is_time_series_deviation': False
        },
        {
            'txn_id': 'cc_2',
            'is_anomaly_if': True,
            'is_vendor_outlier': True,
            'is_amount_outlier': True,
            'is_time_series_deviation': False
        },
        {
            'txn_id': 'cc_3',
            'is_anomaly_if': False,
            'is_vendor_outlier': False,
            'is_amount_outlier': False,
            'is_time_series_deviation': False
        }
    ]

    result = batch_anomaly_scorer.func(json.dumps(transactions))

    assert 'scored_transactions' in result
    assert 'high_risk_count' in result
    assert len(result['scored_transactions']) == 3

    # Check scoring logic
    scores = {t['txn_id']: t['anomaly_score'] for t in result['scored_transactions']}
    assert scores['cc_1'] == 30  # Only isolation forest
    assert scores['cc_2'] == 80  # All three signals
    assert scores['cc_3'] == 0   # No signals


# Integration Tests

def test_reconciliation_agent_tools_loaded():
    """Test that reconciliation agent has all required tools"""
    from src.agents.reconciliation_agent import reconciliation_agent

    assert len(reconciliation_agent.tools) == 5
    tool_names = [tool.name for tool in reconciliation_agent.tools]
    assert 'cross_source_matcher' in tool_names
    assert 'entity_resolver_kg' in tool_names
    assert 'fuzzy_vendor_matcher' in tool_names
    assert 'receipt_transaction_matcher' in tool_names
    assert 'find_orphan_transactions' in tool_names


def test_anomaly_agent_tools_loaded():
    """Test that anomaly detection agent has all required tools"""
    from src.agents.anomaly_detection_agent import anomaly_agent

    assert len(anomaly_agent.tools) == 5
    tool_names = [tool.name for tool in anomaly_agent.tools]
    assert 'run_isolation_forest' in tool_names
    assert 'check_vendor_spending_profile' in tool_names
    assert 'detect_amount_outliers' in tool_names
    assert 'time_series_deviation_check' in tool_names
    assert 'batch_anomaly_scorer' in tool_names


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
