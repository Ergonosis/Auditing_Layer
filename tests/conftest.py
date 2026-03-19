"""Shared pytest fixtures for the auditing layer test suite."""

import hashlib
import os
import sys
from pathlib import Path

# Set env vars BEFORE any src imports (agent modules create LLMs at import time)
if not os.getenv("ANTHROPIC_API_KEY"):
    os.environ["ANTHROPIC_API_KEY"] = "sk-test-dummy-key-for-unit-tests"
if not os.getenv("UNIFICATION_USER_EMAIL"):
    os.environ["UNIFICATION_USER_EMAIL"] = "test@ergonosis.com"

# Make the unification repo's modules importable alongside auditing's `src`.
# Both repos use `src` as their top-level package. We extend the __path__ of
# every overlapping subpackage so that unification-only modules
# (src.models.email, src.storage.*, src.constants, src.utils.classification,
# src.query_interface, etc.) are discoverable without evicting auditing modules.
_UNIFICATION_SRC = Path(__file__).resolve().parents[1].parent / "ergonosis_unification" / "src"
if _UNIFICATION_SRC.is_dir():
    import importlib
    import src as _src_pkg
    _uni_str = str(_UNIFICATION_SRC)

    # Top-level src.__path__
    if _uni_str not in _src_pkg.__path__:
        _src_pkg.__path__.append(_uni_str)

    # For each subdirectory in unification's src/, if a matching src.X package
    # is already loaded in sys.modules, extend its __path__ to include the
    # unification version of that directory.
    for _subdir in _UNIFICATION_SRC.iterdir():
        if not _subdir.is_dir() or not (_subdir / "__init__.py").exists():
            continue
        _pkg_name = f"src.{_subdir.name}"
        _uni_subdir = str(_subdir)
        # Ensure the subpackage is loaded
        try:
            _sub_pkg = importlib.import_module(_pkg_name)
        except ImportError:
            continue
        if hasattr(_sub_pkg, "__path__") and _uni_subdir not in list(_sub_pkg.__path__):
            _sub_pkg.__path__.append(_uni_subdir)

import pytest

TEST_USER_EMAIL = "test@ergonosis.com"
TEST_USER_HASH = hashlib.sha256(TEST_USER_EMAIL.encode()).hexdigest()


@pytest.fixture(autouse=True)
def _set_test_env(monkeypatch):
    """Ensure the test env has UNIFICATION_USER_EMAIL set and lru_cache cleared."""
    monkeypatch.setenv("UNIFICATION_USER_EMAIL", TEST_USER_EMAIL)
    monkeypatch.delenv("UNIFICATION_SECURE_STORAGE_REQUIRED", raising=False)
    monkeypatch.delenv("ENVIRONMENT", raising=False)
    # Set a dummy ANTHROPIC_API_KEY so agent modules can import without error
    if not os.getenv("ANTHROPIC_API_KEY"):
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-dummy-key-for-unit-tests")
    # Clear the cached UQI singleton between tests
    from src.integrations.unification_client import get_uqi
    get_uqi.cache_clear()
    yield
    get_uqi.cache_clear()


@pytest.fixture
def tmp_uqi(tmp_path, monkeypatch):
    """Create a real LocalStore + UQI backed by a temp SQLite DB with consent."""
    db_path = str(tmp_path / "test.db")
    monkeypatch.setenv("UNIFICATION_DB_PATH", db_path)

    from src.integrations.unification_client import get_uqi
    get_uqi.cache_clear()
    uqi = get_uqi()

    # uqi._storage is the LocalStore instance; import consent model via extended src.__path__
    from src.models.consent import UserConsent
    consent = UserConsent(
        user_id_hash=TEST_USER_HASH,
        consent_type="data_processing",
        granted=True,
        source="test",
    )
    uqi._storage.upsert_consent(consent)
    get_uqi.cache_clear()

    return uqi, uqi._storage
