"""Tests for src/main.py production startup validation."""

import pytest
from unittest.mock import patch, MagicMock

REQUIRED_VARS = [
    "ANTHROPIC_API_KEY",
    "DATABRICKS_HOST",
    "DATABRICKS_TOKEN",
    "DATABRICKS_HTTP_PATH",
    "UNIFICATION_USER_EMAIL",
]


@pytest.mark.parametrize("missing_var", REQUIRED_VARS)
def test_missing_env_var_raises_in_production(missing_var):
    """Each missing required env var raises RuntimeError in production mode."""
    def fake_getenv(key, default=None):
        if key == "ENVIRONMENT":
            return "production"
        if key == missing_var:
            return None
        return "dummy"

    import src.main as _main_mod
    with patch.object(_main_mod.os, "getenv", side_effect=fake_getenv), \
         patch("src.main.AuditOrchestrator"):
        with pytest.raises(RuntimeError, match=missing_var):
            _main_mod.main()


def test_all_vars_present_does_not_raise():
    """When all required vars are set in production mode, no RuntimeError is raised."""
    def fake_getenv(key, default=None):
        mapping = {v: "dummy" for v in REQUIRED_VARS}
        mapping["ENVIRONMENT"] = "production"
        return mapping.get(key, default)

    mock_orchestrator = MagicMock()
    mock_orchestrator.return_value.run_audit_cycle.return_value = {
        "audit_run_id": "run_1",
        "status": "completed",
        "transaction_count": 0,
        "flags_created": 0,
    }
    import src.main as _main_mod
    with patch.object(_main_mod.os, "getenv", side_effect=fake_getenv), \
         patch("src.main.AuditOrchestrator", mock_orchestrator):
        result = _main_mod.main()
    assert result["status"] == "completed"
