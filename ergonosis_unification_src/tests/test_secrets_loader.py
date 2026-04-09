"""Tests for GCP Secret Manager loader."""

import logging
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.utils.errors import ConfigurationError
from src.utils.secrets_loader import load_secrets_to_env


def _make_secret_response(value: str):
    """Build a response object matching Secret Manager's access_secret_version return."""
    return SimpleNamespace(payload=SimpleNamespace(data=value.encode("utf-8")))


@pytest.fixture(autouse=True)
def _mock_secretmanager():
    """Inject a fake google.cloud.secretmanager so the import succeeds."""
    mock_sm = MagicMock()
    mock_google_cloud = MagicMock()
    mock_google_cloud.secretmanager = mock_sm
    with patch.dict(sys.modules, {
        "google": MagicMock(cloud=mock_google_cloud),
        "google.cloud": mock_google_cloud,
        "google.cloud.secretmanager": mock_sm,
    }):
        yield mock_sm


class TestSecretsLoaderCriticalFailures:
    def test_critical_secret_failure_raises(self, monkeypatch, _mock_secretmanager):
        monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
        for s in ["DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_HTTP_PATH",
                   "PLAID_ACCESS_TOKEN", "PLAID_CLIENT_ID", "PLAID_SECRET",
                   "MSGRAPH_CLIENT_ID", "MSGRAPH_CLIENT_SECRET", "MSGRAPH_TENANT_ID",
                   "UNIFICATION_PLAID_ACCOUNT_ID", "UNIFICATION_USER_EMAIL"]:
            monkeypatch.delenv(s, raising=False)

        mock_client = _mock_secretmanager.SecretManagerServiceClient.return_value

        def access_secret(request):
            if "DATABRICKS_TOKEN" in request["name"]:
                raise RuntimeError("Permission denied")
            return _make_secret_response("fake-value")

        mock_client.access_secret_version.side_effect = access_secret

        with pytest.raises(ConfigurationError, match="DATABRICKS_TOKEN"):
            load_secrets_to_env()

    def test_non_critical_secret_failure_warns(self, monkeypatch, _mock_secretmanager, caplog):
        monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
        for s in ["DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_HTTP_PATH",
                   "PLAID_ACCESS_TOKEN", "PLAID_CLIENT_ID", "PLAID_SECRET",
                   "MSGRAPH_CLIENT_ID", "MSGRAPH_CLIENT_SECRET", "MSGRAPH_TENANT_ID",
                   "UNIFICATION_PLAID_ACCOUNT_ID", "UNIFICATION_USER_EMAIL"]:
            monkeypatch.delenv(s, raising=False)

        mock_client = _mock_secretmanager.SecretManagerServiceClient.return_value

        def access_secret(request):
            if "PLAID_SECRET" in request["name"]:
                raise RuntimeError("Not found")
            return _make_secret_response("fake-value")

        mock_client.access_secret_version.side_effect = access_secret

        with caplog.at_level(logging.WARNING):
            load_secrets_to_env()

        assert any("PLAID_SECRET" in r.message for r in caplog.records)
