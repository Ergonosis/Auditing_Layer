"""Plaid ingestion adapter — fetches transaction data in-memory, never writes to disk."""

import json
import os
from datetime import date as date_type
from pathlib import Path
from typing import Any, Dict, List

from pybreaker import CircuitBreaker, CircuitBreakerError
from tenacity import retry, stop_after_attempt, wait_exponential

from src.constants import (
    ENV_PLAID_ACCESS_TOKEN,
    ENV_PLAID_CLIENT_ID,
    ENV_PLAID_ENV,
    ENV_PLAID_SECRET,
    PLAID_TXN_PAGE_SIZE,
)
from src.utils.errors import IngestionCredentialsRequiredError, IngestionError
from src.utils.logging import get_logger

logger = get_logger(__name__)

_plaid_breaker = CircuitBreaker(fail_max=5, reset_timeout=60, name="plaid")

_FIXTURE_PATH = Path(__file__).parent.parent.parent / "tests" / "fixtures" / "sample_transactions.json"
_RICH_FIXTURE_PATH = Path(__file__).parent.parent.parent / "tests" / "fixtures" / "rich_mock_transactions.json"
_SMB_FIXTURE_PATH = Path(__file__).parent.parent.parent / "tests" / "fixtures" / "smb_transactions.json"


def _load_fixture() -> Dict[str, Any]:
    try:
        with open(_FIXTURE_PATH) as f:
            return {"transactions": json.load(f)}
    except Exception as exc:
        raise IngestionError(f"Failed to load Plaid fixture: {exc}") from exc


def _load_rich_fixture() -> Dict[str, Any]:
    try:
        with open(_RICH_FIXTURE_PATH) as f:
            return {"transactions": json.load(f)}
    except Exception as exc:
        raise IngestionError(f"Failed to load Plaid rich fixture: {exc}") from exc


def _load_smb_fixture() -> Dict[str, Any]:
    try:
        with open(_SMB_FIXTURE_PATH) as f:
            return {"transactions": json.load(f)}
    except Exception as exc:
        raise IngestionError(f"Failed to load Plaid SMB fixture: {exc}") from exc


def _build_plaid_client():
    """Construct a PlaidApi client from environment variables. Lazy-imported."""
    import plaid
    from plaid.api import plaid_api

    client_id = os.getenv(ENV_PLAID_CLIENT_ID)
    secret = os.getenv(ENV_PLAID_SECRET)
    if not client_id or not secret:
        raise IngestionError(
            f"PLAID_ACCESS_TOKEN is set but {ENV_PLAID_CLIENT_ID} and/or "
            f"{ENV_PLAID_SECRET} are missing. Both are required for live Plaid calls."
        )

    env_name = os.getenv(ENV_PLAID_ENV, "development").lower()
    env_map = {
        "sandbox": plaid.Environment.Sandbox,
        "development": plaid.Environment.Development,
        "production": plaid.Environment.Production,
    }
    host = env_map.get(env_name)
    if host is None:
        raise IngestionError(
            f"Invalid {ENV_PLAID_ENV}={env_name!r}. Must be sandbox, development, or production."
        )

    configuration = plaid.Configuration(
        host=host,
        api_key={"clientId": client_id, "secret": secret},
    )
    api_client = plaid.ApiClient(configuration)
    return plaid_api.PlaidApi(api_client)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=30), reraise=True)
@_plaid_breaker
def _transactions_get_with_retry(client, request):
    """Calls client.transactions_get with tenacity retry."""
    try:
        return client.transactions_get(request)
    except CircuitBreakerError:
        raise IngestionError("Plaid circuit breaker open")


def _fetch_all_transactions(
    client, access_token: str, account_id: str, start_date: date_type, end_date: date_type
) -> List[Dict[str, Any]]:
    """Paginated fetch of all transactions in the date range."""
    from plaid.model.transactions_get_request import TransactionsGetRequest
    from plaid.model.transactions_get_request_options import TransactionsGetRequestOptions

    all_txns: List[Dict[str, Any]] = []
    offset = 0

    while True:
        opts_kwargs: dict = {"count": PLAID_TXN_PAGE_SIZE, "offset": offset}
        if account_id:
            opts_kwargs["account_ids"] = [account_id]
        request = TransactionsGetRequest(
            access_token=access_token,
            start_date=start_date,
            end_date=end_date,
            options=TransactionsGetRequestOptions(**opts_kwargs),
        )
        response = _transactions_get_with_retry(client, request)

        for t in response.transactions:
            all_txns.append({
                "transaction_id": t.transaction_id,
                "account_id": t.account_id,
                "amount": t.amount,
                "date": str(t.date),
                "merchant_name": t.merchant_name,
                "name": t.name,
                "payment_channel": t.payment_channel if isinstance(t.payment_channel, str) else (t.payment_channel.value if t.payment_channel else None),
                "category": t.category,
            })

        if len(all_txns) >= response.total_transactions:
            break
        offset = len(all_txns)

    return all_txns


def fetch_plaid_transactions(
    account_id: str,
    date_range: tuple,
    hard_pull: bool = False,
    strip_balances: bool = True,
) -> Dict[str, Any]:
    """
    Fetch Plaid transaction data in-memory. Never writes to disk.

    Stub mode (no credentials or STUB_INGESTION=true): returns fixture data.
    Live mode: calls the official plaid-python SDK's /transactions/get endpoint.

    Args:
        account_id: Plaid account ID to filter transactions.
        date_range: Tuple of (start_date_iso, end_date_iso) strings.
        hard_pull: Unused — kept for signature compatibility.
        strip_balances: Unused — transaction dicts never include balances.

    Returns: {"transactions": [dict, ...]}
    Raises: IngestionError on fetch failure or missing credentials.
    """
    import plaid

    # Stub detection — checked in order per spec
    stub_mode = os.getenv("STUB_INGESTION", "")
    if stub_mode == "smb":
        logger.info("STUB_INGESTION=smb — returning Plaid SMB fixture data")
        return _load_smb_fixture()
    if stub_mode == "rich":
        logger.info("STUB_INGESTION=rich — returning Plaid rich mock fixture data")
        return _load_rich_fixture()
    if stub_mode == "true":
        logger.info("STUB_INGESTION=true — returning Plaid fixture data")
        return _load_fixture()

    if not os.getenv(ENV_PLAID_ACCESS_TOKEN):
        if os.getenv("UNIFICATION_SECURE_STORAGE_REQUIRED", "").lower() == "true":
            raise IngestionCredentialsRequiredError(
                "PLAID_ACCESS_TOKEN is required when UNIFICATION_SECURE_STORAGE_REQUIRED=true."
            )
        logger.warning("PLAID_ACCESS_TOKEN not set — returning Plaid fixture data")
        return _load_fixture()

    try:
        client = _build_plaid_client()
        txns = _fetch_all_transactions(
            client=client,
            access_token=os.getenv(ENV_PLAID_ACCESS_TOKEN),
            account_id=account_id,
            start_date=date_type.fromisoformat(date_range[0]),
            end_date=date_type.fromisoformat(date_range[1]),
        )
        logger.info("Plaid live fetch complete", transaction_count=len(txns))
        return {"transactions": txns}
    except plaid.ApiException as exc:
        body = getattr(exc, "body", "") or ""
        raise IngestionError(f"Plaid API error: {exc.status} {exc.reason} — {body[:500]}") from exc
    except IngestionError:
        raise
    except Exception as exc:
        raise IngestionError(f"Plaid fetch failed: {exc}") from exc
