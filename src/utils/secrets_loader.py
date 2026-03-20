"""
GCP Secret Manager loader.

Fetches secrets and injects them into os.environ so all existing os.getenv()
calls in the pipeline work without modification.

No-op when GCP_PROJECT_ID is not set (local dev uses .env file as normal).
"""

import logging
import os

from src.utils.errors import ConfigurationError

logger = logging.getLogger(__name__)

_SECRETS = [
    "DATABRICKS_HOST",
    "DATABRICKS_TOKEN",
    "DATABRICKS_HTTP_PATH",
    "PLAID_ACCESS_TOKEN",
    "PLAID_CLIENT_ID",
    "PLAID_SECRET",
    "MSGRAPH_CLIENT_ID",
    "MSGRAPH_CLIENT_SECRET",
    "MSGRAPH_TENANT_ID",
    "UNIFICATION_PLAID_ACCOUNT_ID",
    "UNIFICATION_USER_EMAIL",
    "PLAID_ENV",
]

_CRITICAL_SECRETS = {"DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_HTTP_PATH"}


def load_secrets_to_env(project_id: str | None = None) -> None:
    """
    If GCP_PROJECT_ID is set (or project_id is passed), fetch secrets from
    Secret Manager and inject into os.environ. No-op otherwise.

    Skips any secret whose env var is already set, allowing local overrides.
    """
    resolved_project = project_id or os.environ.get("GCP_PROJECT_ID")
    if not resolved_project:
        return

    try:
        from google.cloud import secretmanager
    except ImportError as exc:
        raise ConfigurationError(
            "GCP_PROJECT_ID is set but google-cloud-secret-manager is not installed. "
            "Run: pip install google-cloud-secret-manager>=2.16"
        ) from exc

    client = secretmanager.SecretManagerServiceClient()

    for secret_name in _SECRETS:
        if os.environ.get(secret_name):
            continue  # already set — local override wins

        resource = f"projects/{resolved_project}/secrets/{secret_name}/versions/latest"
        try:
            response = client.access_secret_version(request={"name": resource})
            os.environ[secret_name] = response.payload.data.decode("utf-8").strip()
        except Exception as exc:
            if secret_name in _CRITICAL_SECRETS:
                raise ConfigurationError(
                    f"Failed to load required secret {secret_name} from Secret Manager"
                ) from exc
            logger.warning("Could not load secret %s: %s", secret_name, exc)
