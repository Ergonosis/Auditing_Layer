"""Tests for src/utils/config_loader.py — path traversal guard and env var override."""

import os
from pathlib import Path

import pytest

from src.utils.config_loader import _validate_config_path, load_config
from src.utils.errors import ConfigurationError
from src.constants import ENV_CONFIG_PATH


class TestValidateConfigPath:
    def test_relative_path_within_project_root(self):
        """Relative path that stays inside the project root is accepted."""
        result = _validate_config_path("unification_config.yaml")
        assert result.name == "unification_config.yaml"

    def test_absolute_path_accepted(self, tmp_path):
        """Absolute path is accepted regardless of location (operator responsibility)."""
        abs_path = str(tmp_path / "some_config.yaml")
        result = _validate_config_path(abs_path)
        assert result == Path(abs_path)

    def test_traversal_rejected(self):
        """Relative path that escapes project root raises ConfigurationError."""
        with pytest.raises(ConfigurationError, match="path traversal"):
            _validate_config_path("../../etc/passwd")

    def test_traversal_with_extra_nesting_rejected(self):
        """Deeper traversal also rejected."""
        with pytest.raises(ConfigurationError, match="path traversal"):
            _validate_config_path("../../../etc/shadow")

    def test_traversal_in_subdirectory_rejected(self):
        """Traversal starting from a subdirectory still rejected."""
        with pytest.raises(ConfigurationError, match="path traversal"):
            _validate_config_path("tests/../../etc/passwd")


class TestLoadConfigEnvVar:
    def test_env_var_overrides_argument(self, monkeypatch):
        """UNIFICATION_CONFIG_PATH env var takes precedence over the config_path argument."""
        monkeypatch.setenv(ENV_CONFIG_PATH, "unification_config.yaml")
        # Should load successfully (the real config exists at this path)
        config = load_config("nonexistent_will_not_be_used.yaml")
        assert "entity_types" in config

    def test_traversal_in_env_var_rejected(self, monkeypatch):
        """Path traversal via env var is rejected before file open."""
        monkeypatch.setenv(ENV_CONFIG_PATH, "../../etc/passwd")
        with pytest.raises(ConfigurationError, match="path traversal"):
            load_config()

    def test_valid_relative_path_loads_config(self):
        """Default relative path loads the real config without error."""
        config = load_config("unification_config.yaml")
        assert "match_rules" in config
        assert "confidence_bands" in config


class TestLoadConfigEdgeCases:
    def test_missing_file_raises_configuration_error(self, tmp_path):
        """Non-existent absolute path raises ConfigurationError."""
        missing = str(tmp_path / "does_not_exist.yaml")
        with pytest.raises(ConfigurationError, match="not found"):
            load_config(missing)

    def test_empty_file_raises_configuration_error(self, tmp_path):
        """Empty YAML file (safe_load returns None) raises ConfigurationError."""
        empty = tmp_path / "empty.yaml"
        empty.write_text("")
        with pytest.raises(ConfigurationError, match="empty"):
            load_config(str(empty))

    def test_invalid_yaml_raises_configuration_error(self, tmp_path):
        """Malformed YAML raises ConfigurationError."""
        bad = tmp_path / "bad.yaml"
        bad.write_text("key: [unclosed\n")
        with pytest.raises(ConfigurationError, match="Invalid YAML"):
            load_config(str(bad))

    def test_missing_required_key_raises_configuration_error(self, tmp_path):
        """Config missing a required key raises ConfigurationError naming the key."""
        from src.constants import CONFIG_REQUIRED_KEYS
        # Build a config that has all required keys except the last one
        partial = {k: [] for k in CONFIG_REQUIRED_KEYS[:-1]}
        missing_key = CONFIG_REQUIRED_KEYS[-1]
        import yaml
        cfg_file = tmp_path / "partial.yaml"
        cfg_file.write_text(yaml.dump(partial))
        with pytest.raises(ConfigurationError, match=missing_key):
            load_config(str(cfg_file))

    def test_valid_minimal_config_passes(self, tmp_path):
        """A config with all required keys loads without error."""
        from src.constants import CONFIG_REQUIRED_KEYS
        import yaml
        minimal = {k: [] for k in CONFIG_REQUIRED_KEYS}
        cfg_file = tmp_path / "minimal.yaml"
        cfg_file.write_text(yaml.dump(minimal))
        result = load_config(str(cfg_file))
        assert all(k in result for k in CONFIG_REQUIRED_KEYS)
