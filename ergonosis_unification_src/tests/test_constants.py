"""Tests for src/constants.py ENV_* constants."""

import pytest

from src.constants import (
    ENV_CONFIG_PATH,
    ENV_DATABRICKS_CATALOG,
    ENV_DATABRICKS_HOST,
    ENV_DATABRICKS_HTTP_PATH,
    ENV_DATABRICKS_SCHEMA,
    ENV_DATABRICKS_TOKEN,
    ENV_PLAID_ACCESS_TOKEN,
    ENV_PLAID_CLIENT_ID,
    ENV_PLAID_ENV,
    ENV_PLAID_SECRET,
    ENV_SECURE_STORAGE_REQUIRED,
    ENV_STUB_INGESTION,
)


class TestEnvConstants:
    def test_all_env_constants_are_non_empty_strings(self):
        for name, val in [
            ("ENV_DATABRICKS_HOST", ENV_DATABRICKS_HOST),
            ("ENV_DATABRICKS_TOKEN", ENV_DATABRICKS_TOKEN),
            ("ENV_DATABRICKS_HTTP_PATH", ENV_DATABRICKS_HTTP_PATH),
            ("ENV_DATABRICKS_CATALOG", ENV_DATABRICKS_CATALOG),
            ("ENV_DATABRICKS_SCHEMA", ENV_DATABRICKS_SCHEMA),
            ("ENV_SECURE_STORAGE_REQUIRED", ENV_SECURE_STORAGE_REQUIRED),
            ("ENV_CONFIG_PATH", ENV_CONFIG_PATH),
            ("ENV_STUB_INGESTION", ENV_STUB_INGESTION),
            ("ENV_PLAID_ACCESS_TOKEN", ENV_PLAID_ACCESS_TOKEN),
            ("ENV_PLAID_CLIENT_ID", ENV_PLAID_CLIENT_ID),
            ("ENV_PLAID_SECRET", ENV_PLAID_SECRET),
            ("ENV_PLAID_ENV", ENV_PLAID_ENV),
        ]:
            assert isinstance(val, str), f"{name} should be a str"
            assert val, f"{name} should be non-empty"

    def test_exact_values(self):
        assert ENV_DATABRICKS_HOST == "DATABRICKS_HOST"
        assert ENV_DATABRICKS_TOKEN == "DATABRICKS_TOKEN"
        assert ENV_DATABRICKS_HTTP_PATH == "DATABRICKS_HTTP_PATH"
        assert ENV_DATABRICKS_CATALOG == "DATABRICKS_CATALOG"
        assert ENV_DATABRICKS_SCHEMA == "DATABRICKS_SCHEMA"
        assert ENV_SECURE_STORAGE_REQUIRED == "UNIFICATION_SECURE_STORAGE_REQUIRED"
        assert ENV_CONFIG_PATH == "UNIFICATION_CONFIG_PATH"
        assert ENV_STUB_INGESTION == "STUB_INGESTION"
        assert ENV_PLAID_ACCESS_TOKEN == "PLAID_ACCESS_TOKEN"
        assert ENV_PLAID_CLIENT_ID == "PLAID_CLIENT_ID"
        assert ENV_PLAID_SECRET == "PLAID_SECRET"
        assert ENV_PLAID_ENV == "PLAID_ENV"
