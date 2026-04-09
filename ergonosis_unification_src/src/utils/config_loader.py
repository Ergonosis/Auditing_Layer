"""Configuration file loader with validation for the unification layer"""

import os
import yaml
from pathlib import Path
from typing import Any, Dict

from .errors import ConfigurationError
from ..constants import CONFIG_REQUIRED_KEYS, ENV_CONFIG_PATH

_PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()


def _validate_config_path(path_str: str) -> Path:
    """
    Returns a safe resolved Path for the config file.
    Absolute paths are accepted as-is (operator responsibility).
    Relative paths are resolved against the project root; if the result escapes
    the project root (path traversal), ConfigurationError is raised.
    """
    candidate = Path(path_str)
    if candidate.is_absolute():
        return candidate
    resolved = (_PROJECT_ROOT / candidate).resolve()
    try:
        resolved.relative_to(_PROJECT_ROOT)
    except ValueError:
        raise ConfigurationError(
            f"Config path escapes the project root (path traversal rejected): {path_str!r}"
        )
    return resolved


def _validate_matching_config(config: dict) -> None:
    errors = []
    match_rules = config.get("match_rules", {})
    if not isinstance(match_rules, dict):
        return
    for rule_key, rule in match_rules.items():
        tol = rule.get("amount_tolerance_pct")
        if tol is not None and not (0.0 <= float(tol) <= 1.0):
            errors.append(f"{rule_key}.amount_tolerance_pct={tol} must be in [0.0, 1.0]")
        fuzzy = rule.get("tier3_fuzzy", {})
        min_score = fuzzy.get("min_similarity_score")
        if min_score is not None and not (0.0 <= float(min_score) <= 1.0):
            errors.append(f"{rule_key}.tier3_fuzzy.min_similarity_score={min_score} must be in [0.0, 1.0]")
        date_win = fuzzy.get("date_window_days")
        if date_win is not None and (not isinstance(date_win, int) or date_win < 0):
            errors.append(f"{rule_key}.tier3_fuzzy.date_window_days={date_win} must be non-negative int")
    if errors:
        raise ConfigurationError("Invalid matching config: " + "; ".join(errors))


def load_config(config_path: str = "unification_config.yaml") -> Dict[str, Any]:
    """
    Load unification config from YAML.
    Raises ConfigurationError if file missing, invalid YAML, or required keys absent.
    Supports UNIFICATION_CONFIG_PATH env var override.
    Relative paths that escape the project root via traversal are rejected.
    """
    resolved_path = os.getenv(ENV_CONFIG_PATH, config_path)

    try:
        config_file = _validate_config_path(resolved_path)

        if not config_file.exists():
            raise ConfigurationError(f"Configuration file not found: {resolved_path}")

        with open(config_file, "r") as f:
            config = yaml.safe_load(f)

        if config is None:
            raise ConfigurationError(f"Configuration file is empty: {resolved_path}")

        missing_keys = [key for key in CONFIG_REQUIRED_KEYS if key not in config]
        if missing_keys:
            raise ConfigurationError(
                f"Missing required configuration keys: {missing_keys}"
            )

        _validate_matching_config(config)

        return config

    except yaml.YAMLError as e:
        raise ConfigurationError(f"Invalid YAML in configuration file: {e}")
    except ConfigurationError:
        raise
    except Exception as e:
        raise ConfigurationError(f"Error loading configuration: {e}")
