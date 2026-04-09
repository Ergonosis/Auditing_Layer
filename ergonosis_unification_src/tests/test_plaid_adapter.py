"""Unit tests for src/ingestion/plaid_adapter.py — all mocked, no real API calls."""

from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.utils.errors import IngestionError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_txn(**overrides):
    """Return a SimpleNamespace that quacks like a Plaid Transaction object."""
    defaults = {
        "transaction_id": "txn_001",
        "account_id": "acct_001",
        "amount": 42.50,
        "date": "2026-01-15",
        "merchant_name": "Acme Corp",
        "name": "ACME CORP PURCHASE",
        "payment_channel": "online",
        "category": ["Shopping", "Electronics"],
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_mock_response(transactions, total_transactions=None):
    """Return a SimpleNamespace that quacks like a TransactionsGetResponse."""
    if total_transactions is None:
        total_transactions = len(transactions)
    return SimpleNamespace(
        transactions=transactions,
        total_transactions=total_transactions,
    )


def _clear_plaid_env(monkeypatch):
    """Remove all Plaid env vars so tests start clean."""
    for var in [
        "STUB_INGESTION",
        "PLAID_ACCESS_TOKEN",
        "PLAID_CLIENT_ID",
        "PLAID_SECRET",
        "PLAID_ENV",
    ]:
        monkeypatch.delenv(var, raising=False)


# ===========================================================================
# TestStubFallback
# ===========================================================================


class TestStubFallback:
    def test_stub_ingestion_env_returns_fixture(self, monkeypatch):
        _clear_plaid_env(monkeypatch)
        monkeypatch.setenv("STUB_INGESTION", "true")

        from src.ingestion.plaid_adapter import fetch_plaid_transactions

        result = fetch_plaid_transactions(
            account_id="acct_test",
            date_range=("2026-01-01", "2026-01-31"),
        )
        assert "transactions" in result
        assert len(result["transactions"]) == 6

    def test_missing_access_token_returns_fixture(self, monkeypatch):
        _clear_plaid_env(monkeypatch)

        from src.ingestion.plaid_adapter import fetch_plaid_transactions

        result = fetch_plaid_transactions(
            account_id="acct_test",
            date_range=("2026-01-01", "2026-01-31"),
        )
        assert "transactions" in result
        assert len(result["transactions"]) == 6


# ===========================================================================
# TestBuildPlaidClient
# ===========================================================================


class TestBuildPlaidClient:
    def test_missing_client_id_raises_ingestion_error(self, monkeypatch):
        _clear_plaid_env(monkeypatch)
        monkeypatch.setenv("PLAID_ACCESS_TOKEN", "access-sandbox-test")
        # CLIENT_ID deliberately not set

        from src.ingestion.plaid_adapter import _build_plaid_client

        with pytest.raises(IngestionError, match="PLAID_CLIENT_ID"):
            _build_plaid_client()

    def test_missing_secret_raises_ingestion_error(self, monkeypatch):
        _clear_plaid_env(monkeypatch)
        monkeypatch.setenv("PLAID_ACCESS_TOKEN", "access-sandbox-test")
        monkeypatch.setenv("PLAID_CLIENT_ID", "client-id-test")
        # SECRET deliberately not set

        from src.ingestion.plaid_adapter import _build_plaid_client

        with pytest.raises(IngestionError, match="PLAID_SECRET"):
            _build_plaid_client()

    def test_invalid_plaid_env_raises_ingestion_error(self, monkeypatch):
        _clear_plaid_env(monkeypatch)
        monkeypatch.setenv("PLAID_ACCESS_TOKEN", "access-sandbox-test")
        monkeypatch.setenv("PLAID_CLIENT_ID", "client-id-test")
        monkeypatch.setenv("PLAID_SECRET", "secret-test")
        monkeypatch.setenv("PLAID_ENV", "invalid")

        from src.ingestion.plaid_adapter import _build_plaid_client

        with pytest.raises(IngestionError, match="Invalid"):
            _build_plaid_client()


# ===========================================================================
# TestFetchAllTransactions
# ===========================================================================


class TestFetchAllTransactions:
    def test_single_page_returns_normalized_dicts(self):
        from src.ingestion.plaid_adapter import _fetch_all_transactions

        txn1 = _make_mock_txn(transaction_id="txn_001")
        txn2 = _make_mock_txn(transaction_id="txn_002", amount=99.99)
        mock_client = MagicMock()
        mock_client.transactions_get.return_value = _make_mock_response([txn1, txn2])

        result = _fetch_all_transactions(
            client=mock_client,
            access_token="access-test",
            account_id="acct_001",
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 31),
        )

        assert len(result) == 2
        expected_keys = {
            "transaction_id", "account_id", "amount", "date",
            "merchant_name", "name", "payment_channel", "category",
        }
        assert set(result[0].keys()) == expected_keys
        assert result[0]["transaction_id"] == "txn_001"
        assert result[1]["amount"] == 99.99

    def test_pagination_fetches_all_pages(self):
        from src.ingestion.plaid_adapter import _fetch_all_transactions

        txn1 = _make_mock_txn(transaction_id="txn_001")
        txn2 = _make_mock_txn(transaction_id="txn_002")
        txn3 = _make_mock_txn(transaction_id="txn_003")

        page1 = _make_mock_response([txn1, txn2], total_transactions=3)
        page2 = _make_mock_response([txn3], total_transactions=3)

        mock_client = MagicMock()
        mock_client.transactions_get.side_effect = [page1, page2]

        result = _fetch_all_transactions(
            client=mock_client,
            access_token="access-test",
            account_id="acct_001",
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 31),
        )

        assert len(result) == 3
        assert mock_client.transactions_get.call_count == 2

    def test_payment_channel_as_string_handled(self):
        from src.ingestion.plaid_adapter import _fetch_all_transactions

        txn = _make_mock_txn(payment_channel="online")
        mock_client = MagicMock()
        mock_client.transactions_get.return_value = _make_mock_response([txn])

        result = _fetch_all_transactions(
            client=mock_client,
            access_token="access-test",
            account_id="acct_001",
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 31),
        )

        assert result[0]["payment_channel"] == "online"

    def test_payment_channel_as_enum_handled(self):
        from src.ingestion.plaid_adapter import _fetch_all_transactions

        enum_channel = SimpleNamespace(value="in store")
        txn = _make_mock_txn(payment_channel=enum_channel)
        mock_client = MagicMock()
        mock_client.transactions_get.return_value = _make_mock_response([txn])

        result = _fetch_all_transactions(
            client=mock_client,
            access_token="access-test",
            account_id="acct_001",
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 31),
        )

        assert result[0]["payment_channel"] == "in store"

    def test_null_merchant_name_preserved(self):
        from src.ingestion.plaid_adapter import _fetch_all_transactions

        txn = _make_mock_txn(merchant_name=None)
        mock_client = MagicMock()
        mock_client.transactions_get.return_value = _make_mock_response([txn])

        result = _fetch_all_transactions(
            client=mock_client,
            access_token="access-test",
            account_id="acct_001",
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 31),
        )

        assert result[0]["merchant_name"] is None


# ===========================================================================
# TestFetchPlaidTransactionsLive
# ===========================================================================


class TestFetchPlaidTransactionsLive:
    def test_api_exception_raises_ingestion_error(self, monkeypatch):
        _clear_plaid_env(monkeypatch)
        monkeypatch.setenv("PLAID_ACCESS_TOKEN", "access-sandbox-test")

        import plaid

        mock_exc = plaid.ApiException(status=400, reason="Bad Request")
        mock_client = MagicMock()
        mock_client.transactions_get.side_effect = mock_exc

        monkeypatch.setattr(
            "src.ingestion.plaid_adapter._build_plaid_client",
            lambda: mock_client,
        )

        from src.ingestion.plaid_adapter import fetch_plaid_transactions

        with pytest.raises(IngestionError, match="Plaid API error"):
            fetch_plaid_transactions(
                account_id="acct_test",
                date_range=("2026-01-01", "2026-01-31"),
            )

    def test_generic_exception_raises_ingestion_error(self, monkeypatch):
        _clear_plaid_env(monkeypatch)
        monkeypatch.setenv("PLAID_ACCESS_TOKEN", "access-sandbox-test")

        mock_client = MagicMock()
        mock_client.transactions_get.side_effect = RuntimeError("connection refused")

        monkeypatch.setattr(
            "src.ingestion.plaid_adapter._build_plaid_client",
            lambda: mock_client,
        )

        from src.ingestion.plaid_adapter import fetch_plaid_transactions

        with pytest.raises(IngestionError, match="Plaid fetch failed"):
            fetch_plaid_transactions(
                account_id="acct_test",
                date_range=("2026-01-01", "2026-01-31"),
            )

    def test_successful_live_call_returns_transactions_dict(self, monkeypatch):
        _clear_plaid_env(monkeypatch)
        monkeypatch.setenv("PLAID_ACCESS_TOKEN", "access-sandbox-test")

        expected_txns = [{"transaction_id": "txn_mock", "amount": 10.0}]

        monkeypatch.setattr(
            "src.ingestion.plaid_adapter._build_plaid_client",
            lambda: MagicMock(),
        )
        monkeypatch.setattr(
            "src.ingestion.plaid_adapter._fetch_all_transactions",
            lambda **kwargs: expected_txns,
        )

        from src.ingestion.plaid_adapter import fetch_plaid_transactions

        result = fetch_plaid_transactions(
            account_id="acct_test",
            date_range=("2026-01-01", "2026-01-31"),
        )

        assert result == {"transactions": expected_txns}
