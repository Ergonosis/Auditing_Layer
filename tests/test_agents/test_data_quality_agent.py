"""Unit tests for Data Quality Agent and tools"""

import json
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from src.agents.data_quality_agent import data_quality_agent, data_quality_task
from src.tools.data_quality_tools import (
    check_data_completeness,
    validate_schema_conformity,
    detect_duplicate_records,
    infer_domain_freshness,
    check_data_quality_gates
)


# Mock data fixtures
@pytest.fixture
def mock_transaction_data():
    """Create mock transaction data for testing"""
    return pd.DataFrame({
        'txn_id': ['txn_001', 'txn_002', 'txn_003', 'txn_004', 'txn_005'],
        'vendor': ['Amazon', 'Walmart', None, 'Target', 'Costco'],
        'amount': [100.50, 250.75, 50.00, 300.00, 150.25],
        'date': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05']),
        'source': ['credit_card', 'bank', 'credit_card', None, 'bank']
    })


@pytest.fixture
def mock_duplicate_data():
    """Create mock data with duplicates"""
    return pd.DataFrame({
        'txn_id': ['txn_001', 'txn_001', 'txn_002', 'txn_003', 'txn_003'],
        'vendor': ['Amazon', 'Amazon', 'Walmart', 'Target', 'Target'],
        'amount': [100.50, 100.50, 250.75, 50.00, 50.00],
        'date': pd.to_datetime(['2024-01-01'] * 5),
        'source': ['credit_card'] * 5
    })


def test_check_completeness(mock_transaction_data):
    """Test completeness check with mock data"""
    txn_json = mock_transaction_data.to_json(orient='records')
    result = check_data_completeness.func(txn_json)

    assert 'total_records' in result
    assert result['total_records'] == 5
    assert 'completeness_score' in result
    assert 0 <= result['completeness_score'] <= 1
    assert 'missing_vendor' in result
    assert result['missing_vendor'] == 1  # One null vendor
    assert 'missing_source' in result
    assert result['missing_source'] == 1  # One null source


def test_check_completeness_empty_table():
    """Test completeness check with empty data"""
    result = check_data_completeness.func("[]")

    assert result['total_records'] == 0
    assert result['completeness_score'] == 0.0
    assert result['missing_vendor'] == 0
    assert result['missing_amount'] == 0


def test_schema_validation(mock_transaction_data):
    """Test schema validation"""
    expected_schema = {
        'amount': 'float',
        'vendor': 'str',
        'txn_id': 'str'
    }
    txn_json = mock_transaction_data.to_json(orient='records')

    result = validate_schema_conformity.func(txn_json, json.dumps(expected_schema))

    assert isinstance(result, list)
    # Should pass - all types match (date excluded: JSON round-trip loses datetime type)
    assert len(result) == 0


def test_schema_validation_missing_field(mock_transaction_data):
    """Test schema validation with missing field"""
    expected_schema = {
        'amount': 'float',
        'vendor': 'str',
        'missing_field': 'str'
    }
    txn_json = mock_transaction_data.to_json(orient='records')

    result = validate_schema_conformity.func(txn_json, json.dumps(expected_schema))

    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0]['field'] == 'missing_field'
    assert result[0]['error'] == 'Missing field'


def test_schema_validation_type_mismatch(mock_transaction_data):
    """Test schema validation with type mismatch"""
    expected_schema = {
        'amount': 'str',  # Should be float
        'vendor': 'str'
    }
    txn_json = mock_transaction_data.to_json(orient='records')

    result = validate_schema_conformity.func(txn_json, json.dumps(expected_schema))

    assert isinstance(result, list)
    assert len(result) >= 1
    assert any(err['field'] == 'amount' for err in result)


def test_duplicate_detection(mock_duplicate_data):
    """Test duplicate detection"""
    txn_json = mock_duplicate_data.to_json(orient='records')
    result = detect_duplicate_records.func(txn_json, '["txn_id"]')

    assert 'duplicate_count' in result
    assert 'duplicate_groups' in result
    assert result['duplicate_count'] == 4  # 4 records in duplicate groups
    assert len(result['duplicate_groups']) == 2  # 2 unique duplicate groups


def test_duplicate_detection_no_duplicates(mock_transaction_data):
    """Test duplicate detection with no duplicates"""
    txn_json = mock_transaction_data.to_json(orient='records')
    result = detect_duplicate_records.func(txn_json, '["txn_id"]')

    assert result['duplicate_count'] == 0
    assert len(result['duplicate_groups']) == 0


def test_duplicate_detection_empty_table():
    """Test duplicate detection with empty data"""
    result = detect_duplicate_records.func("[]", '["txn_id"]')

    assert result['duplicate_count'] == 0
    assert result['duplicate_groups'] == []


def test_domain_inference_manual_config():
    """Test domain inference with manual config"""
    mock_config = {
        'domains': {
            'default': {
                'max_age_hours': 48,
                'critical_amount_threshold': 1000
            }
        }
    }

    pattern = {
        'domain': 'default',
        'frequency': 'daily'
    }

    with patch('src.tools.data_quality_tools.load_config', return_value=mock_config), \
         patch('src.tools.data_quality_tools.get_domain_config', return_value=mock_config['domains']['default']):
        result = infer_domain_freshness.func(json.dumps(pattern))

        assert 'domain' in result
        assert result['domain'] == 'default'
        assert 'max_age_hours' in result
        assert result['max_age_hours'] == 48
        assert result['source'] == 'manual_config'
        assert result['confidence'] == 1.0


def test_domain_inference_no_config_returns_default():
    """Test domain inference returns hardcoded default when no manual config exists"""
    pattern = {
        'frequency': 'daily',
        'vendor_type': 'service',
        'avg_amount': 500.0
    }

    with patch('src.tools.data_quality_tools.load_config', return_value={}), \
         patch('src.tools.data_quality_tools.get_domain_config', return_value=None):
        result = infer_domain_freshness.func(json.dumps(pattern))

        assert result['domain'] == 'default'
        assert result['max_age_hours'] == 48
        assert result['source'] == 'default_config'
        assert result['confidence'] == 0.5


def test_domain_inference_error_fallback():
    """Test domain inference with error fallback"""
    pattern = {'frequency': 'daily'}

    with patch('src.tools.data_quality_tools.load_config', side_effect=Exception("Config error")):
        result = infer_domain_freshness.func(json.dumps(pattern))

        assert result['domain'] == 'default'
        assert result['max_age_hours'] == 48
        assert result['source'] == 'error_fallback'
        assert result['confidence'] == 0.0


def test_quality_gates_pass():
    """Test quality gates with passing metrics"""
    quality_metrics = {
        'total_records': 1000,
        'completeness_score': 0.95
    }

    thresholds = {
        'completeness_threshold': 0.90
    }

    result = check_data_quality_gates.func(json.dumps(quality_metrics), json.dumps(thresholds))
    assert result is True


def test_quality_gates_fail():
    """Test quality gates with failing metrics"""
    quality_metrics = {
        'total_records': 1000,
        'completeness_score': 0.85
    }

    thresholds = {
        'completeness_threshold': 0.90
    }

    result = check_data_quality_gates.func(json.dumps(quality_metrics), json.dumps(thresholds))
    assert result is False


def test_quality_gates_default_threshold():
    """Test quality gates with default threshold"""
    quality_metrics = {
        'completeness_score': 0.92
    }

    thresholds = {}

    result = check_data_quality_gates.func(json.dumps(quality_metrics), json.dumps(thresholds))
    assert result is True


def test_agent_definition():
    """Test that agent is properly defined"""
    assert data_quality_agent is not None
    assert data_quality_agent.role == "Data Quality Specialist"
    assert len(data_quality_agent.tools) == 5
    assert data_quality_agent.allow_delegation is False


def test_task_definition():
    """Test that task is properly defined"""
    assert data_quality_task is not None
    assert data_quality_task.agent == data_quality_agent
    assert "completeness" in data_quality_task.description.lower()
    assert "schema" in data_quality_task.description.lower()
    assert "duplicate" in data_quality_task.description.lower()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
