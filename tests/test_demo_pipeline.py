"""Integration tests for demo pipeline using RIA CSV data"""

import pytest
import os
import pandas as pd
from pathlib import Path


# Set demo mode for all tests in this module
@pytest.fixture(scope="module", autouse=True)
def setup_demo_mode():
    """Set environment variables for demo mode"""
    os.environ["DEMO_MODE"] = "true"
    os.environ["ENVIRONMENT"] = "demo"
    os.environ["STATE_BACKEND"] = "memory"
    os.environ["DEMO_DATA_DIR"] = "ria_data"
    yield
    # Cleanup
    os.environ.pop("DEMO_MODE", None)
    os.environ.pop("ENVIRONMENT", None)
    os.environ.pop("STATE_BACKEND", None)


class TestDemoDataLoader:
    """Test demo data loader functionality"""

    def test_demo_data_files_exist(self):
        """Verify all required demo data files exist"""
        data_dir = Path("ria_data")
        assert data_dir.exists(), "Demo data directory not found"

        required_files = [
            "ria_clients.csv",
            "ria_bank_transactions.csv",
            "ria_credit_card_expenses_with_cardholders.csv",
            "ria_receipts_travel_and_business_dev.csv"
        ]

        for filename in required_files:
            filepath = data_dir / filename
            assert filepath.exists(), f"Required file not found: {filename}"

    def test_load_demo_data(self):
        """Test loading demo data via CSV loader"""
        from src.demo.csv_data_loader import DemoDataLoader

        loader = DemoDataLoader()

        # Test clients
        clients = loader.clients
        assert not clients.empty, "Clients data should not be empty"
        assert 'client_id' in clients.columns

        # Test credit card expenses
        cc_expenses = loader.credit_card_expenses
        assert not cc_expenses.empty, "Credit card expenses should not be empty"
        assert 'expense_id' in cc_expenses.columns
        assert 'merchant' in cc_expenses.columns

        # Test bank transactions
        bank = loader.bank_transactions
        assert not bank.empty, "Bank transactions should not be empty"

        # Test receipts
        receipts = loader.receipts
        assert not receipts.empty, "Receipts should not be empty"

    def test_get_transactions_for_audit(self):
        """Test transaction transformation for audit"""
        from src.demo.csv_data_loader import DemoDataLoader

        loader = DemoDataLoader()
        transactions = loader.get_transactions_for_audit()

        assert not transactions.empty, "Transactions should not be empty"

        # Verify schema transformation
        required_cols = ['txn_id', 'source', 'amount', 'vendor', 'date']
        for col in required_cols:
            assert col in transactions.columns, f"Missing required column: {col}"

        # Verify source is credit_card
        assert all(transactions['source'] == 'credit_card')

        # Verify data types
        assert pd.api.types.is_datetime64_any_dtype(transactions['date'])
        assert pd.api.types.is_numeric_dtype(transactions['amount'])

    def test_demo_data_summary(self):
        """Test summary statistics"""
        from src.demo.csv_data_loader import DemoDataLoader

        loader = DemoDataLoader()
        stats = loader.get_summary_stats()

        assert stats['clients'] > 0
        assert stats['credit_card_expenses'] > 0
        assert stats['bank_transactions'] > 0
        assert stats['receipts'] > 0

        # RIA dataset should have ~2,168 credit card expenses
        assert stats['credit_card_expenses'] > 2000
        # And ~53% receipt coverage
        receipt_coverage = stats['receipts'] / stats['credit_card_expenses']
        assert 0.4 < receipt_coverage < 0.6  # Between 40% and 60%


class TestDemoUqiClient:
    """Test UQI returns valid instance in dev/demo mode"""

    def test_uqi_returns_instance_in_dev_mode(self, monkeypatch, tmp_path):
        """In dev/demo mode (ENVIRONMENT unset), get_uqi() returns a real UQI."""
        monkeypatch.delenv("ENVIRONMENT", raising=False)
        monkeypatch.setenv("UNIFICATION_DB_PATH", str(tmp_path / "demo.db"))
        from src.integrations.unification_client import get_uqi
        get_uqi.cache_clear()
        uqi = get_uqi()
        from src.query_interface import UnifiedQueryInterface
        assert isinstance(uqi, UnifiedQueryInterface)
        get_uqi.cache_clear()

    def test_query_gold_tables_demo(self):
        """Test query_gold_tables routes to demo data"""
        from src.tools.databricks_client import query_gold_tables

        # Query should route to demo CSV data
        result = query_gold_tables("SELECT * FROM gold.recent_transactions")

        assert not result.empty, "Should return demo data"
        assert 'txn_id' in result.columns
        assert 'vendor' in result.columns


class TestDemoConfiguration:
    """Test demo configuration loading"""

    def test_demo_config_loads(self):
        """Test that demo configuration file loads correctly"""
        from src.utils.config_loader import load_config

        config = load_config()  # Should auto-load rules_demo.yaml

        assert 'version' in config
        assert config['version'] == "1.0.0-demo"
        assert 'rules' in config
        assert 'domain_configs' in config

    def test_demo_config_overrides(self):
        """Test that demo config has appropriate overrides"""
        from src.utils.config_loader import load_config

        config = load_config()

        # Demo should have whitelisted vendors
        assert len(config.get('whitelisted_vendors', [])) > 0
        assert 'AWS' in config['whitelisted_vendors']
        assert 'Slack' in config['whitelisted_vendors']

        # Demo should have adjusted thresholds
        data_quality_threshold = config['rules']['data_quality']['completeness_threshold']
        assert data_quality_threshold == 0.85  # Lower than production 0.90


class TestDemoStateManager:
    """Test in-memory state backend"""

    def test_in_memory_state_save_restore(self):
        """Test saving and restoring state in memory"""
        from src.orchestrator.state_manager import save_workflow_state, restore_workflow_state
        import src.orchestrator.state_manager as sm

        test_audit_id = "test-demo-123"
        test_state = {
            'status': 'in_progress',
            'transaction_count': 100,
            'test_data': 'demo'
        }

        # STATE_BACKEND is a module-level constant read at import time;
        # patch it directly rather than relying on the env var fixture.
        original = sm.STATE_BACKEND
        sm.STATE_BACKEND = "memory"
        try:
            save_workflow_state(test_audit_id, test_state)
            restored = restore_workflow_state(test_audit_id)
        finally:
            sm.STATE_BACKEND = original

        assert restored == test_state
        assert restored['test_data'] == 'demo'


@pytest.mark.slow
class TestDemoEndToEnd:
    """End-to-end integration tests for demo pipeline"""

    def test_demo_pipeline_dry_run(self):
        """Test that demo data can be loaded without running full audit"""
        from src.demo.csv_data_loader import load_demo_data

        # Simulate orchestrator query
        transactions = load_demo_data("SELECT * FROM gold.recent_transactions")

        assert not transactions.empty
        assert len(transactions) > 2000  # RIA dataset has 2,168 expenses

    @pytest.mark.skip(reason="Full audit is slow and may require LLM API keys")
    def test_full_demo_audit_cycle(self):
        """
        Test full audit cycle with demo data.

        This test is skipped by default because:
        - It's slow (2-5 minutes)
        - It requires LLM API keys
        - It's more suitable for manual testing

        To run: pytest tests/test_demo_pipeline.py::TestDemoEndToEnd::test_full_demo_audit_cycle -v
        """
        from src.orchestrator.orchestrator_agent import AuditOrchestrator

        orchestrator = AuditOrchestrator()
        results = orchestrator.run_audit_cycle()

        # Verify results structure
        assert 'audit_run_id' in results
        assert 'status' in results
        assert results['status'] == 'completed'
        assert 'transaction_count' in results
        assert results['transaction_count'] > 0

        # Verify some flags were created
        assert 'flags_created' in results
        # Based on RIA data, expect 50-100 flags
        assert results['flags_created'] > 0
