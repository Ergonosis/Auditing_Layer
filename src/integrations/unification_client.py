"""Adapter for the ergonosis-unification query interface.

This is the single import point for all UQI calls in the auditing repo.
"""

import contextlib
import hashlib
import importlib.util
import os
import sys
from functools import lru_cache
from pathlib import Path
from typing import Optional

from src.utils.logging import get_logger

logger = get_logger(__name__)

# Path to the sibling unification repo (dev/test only; ignored in Docker)
_UNIFICATION_SRC = Path(__file__).resolve().parents[3] / "ergonosis_unification" / "src"

# Default DB path
_DEFAULT_DB_PATH = str(
    Path(__file__).resolve().parents[3] / "ergonosis_unification" / ".local_store" / "unification.db"
)


@contextlib.contextmanager
def _unification_imports():
    """Context manager that temporarily makes unification repo's src modules importable.

    Both repos share the `src` top-level package. This context manager prepends
    unification's src/ to src.__path__ so unification submodules are found first,
    then removes it afterward so auditing's src.* modules keep working.

    In Docker (unification installed as wheel), _UNIFICATION_SRC won't exist
    so this is a no-op context manager.
    """
    if not _UNIFICATION_SRC.is_dir():
        yield
        return

    uni_str = str(_UNIFICATION_SRC)
    import src as _src_pkg

    # Prepend unification's src/ to src.__path__ (highest priority)
    _src_pkg.__path__.insert(0, uni_str)

    # Evict cached src.* submodules that resolve to auditing's versions,
    # so they get re-imported from unification on next access.
    # Only evict modules whose __file__ is inside the auditing src — modules
    # from the unification repo (or unknown origin) are left as-is.
    auditing_src = str(Path(__file__).resolve().parents[2])
    evicted = {}
    for key in list(sys.modules.keys()):
        if not key.startswith("src."):
            continue
        mod = sys.modules[key]
        file_ = getattr(mod, "__file__", None)
        if file_ and auditing_src in file_:
            evicted[key] = sys.modules.pop(key)

    try:
        yield
    finally:
        # Remove unification's src/ from front of __path__
        try:
            _src_pkg.__path__.remove(uni_str)
        except ValueError:
            pass
        # Restore auditing's cached modules.
        # Do NOT evict unification modules that were loaded during the context —
        # they persist in sys.modules so subsequent imports (e.g. _make_email)
        # keep working. Since auditing has no src.models.email / src.storage.*
        # equivalents, there's no conflict.
        sys.modules.update(evicted)


def _get_user_id_hash() -> str:
    """Derive the consent-gate user_id_hash from the UNIFICATION_USER_EMAIL env var."""
    email = os.getenv("UNIFICATION_USER_EMAIL", "")
    if not email:
        raise RuntimeError("UNIFICATION_USER_EMAIL env var is required")
    return hashlib.sha256(email.encode()).hexdigest()


@lru_cache(maxsize=1)
def get_uqi():
    """Return a UnifiedQueryInterface backed by the appropriate storage backend.

    Dev/test/demo: LocalStore (SQLite).
    Production: DeltaClient (Databricks).
    """
    with _unification_imports():
        if os.getenv("ENVIRONMENT") == "production":
            from src.storage.delta_client import get_storage_backend
            from src.query_interface import UnifiedQueryInterface

            storage = get_storage_backend()
            return UnifiedQueryInterface(storage)

        from src.storage.local_store import LocalStore
        from src.query_interface import UnifiedQueryInterface

        db_path = os.getenv("UNIFICATION_DB_PATH", _DEFAULT_DB_PATH)
        logger.info("Connecting to unification store", db_path=db_path)
        storage = LocalStore(db_path)
        return UnifiedQueryInterface(storage)


def try_write_feedback(
    transaction_id: str,
    signal: str,
    source: str,
    reason: Optional[str] = None,
) -> bool:
    """Best-effort feedback write. Returns True on success, False on any failure.

    Looks up entity links for the transaction and writes feedback on the
    highest-confidence link. If no links exist (unification hasn't run for
    this txn), skips silently.
    """
    try:
        uqi = get_uqi()
        user_hash = _get_user_id_hash()
        bundles = uqi.get_linked_entities(transaction_id, "transaction", user_id_hash=user_hash)
        if not bundles:
            logger.debug(
                "No unification links for transaction — skipping feedback",
                transaction_id=transaction_id,
            )
            return False

        # Pick the highest-confidence link
        best = max(bundles, key=lambda b: b.confidence)
        uqi.write_feedback(
            link_id=best.link.link_id,
            signal=signal,
            source=source,
            reason=reason,
            user_id_hash=user_hash,
        )
        logger.info(
            "Feedback written to unification store",
            transaction_id=transaction_id,
            link_id=best.link.link_id,
            signal=signal,
            source=source,
        )
        return True

    except Exception as exc:
        logger.warning(
            "Failed to write feedback to unification store — continuing without it",
            transaction_id=transaction_id,
            error=str(exc),
        )
        return False


def get_ambiguous_matches() -> list:
    """Fetch pending ambiguous match records from the unification store.

    Returns:
        List of AmbiguousMatch records with status='pending', or [] on any failure.
    """
    try:
        uqi = get_uqi()
        return uqi.get_ambiguous_matches(status="pending", user_id_hash=_get_user_id_hash())
    except Exception as exc:
        logger.warning(
            "Failed to fetch ambiguous matches from unification store — returning empty list",
            error=str(exc),
        )
        return []


def resolve_ambiguous_match(
    ambiguity_id: str,
    chosen_link_id: str,
    reason: Optional[str] = None,
) -> bool:
    """Submit a resolution for an ambiguous match record.

    Writes a 'confirmed' feedback signal on chosen_link_id. The ambiguity_id
    is logged for traceability but is not passed to write_feedback (which takes
    the link_id of the chosen candidate, not the ambiguity record ID).

    Returns:
        True on success, False on any failure.
    """
    try:
        uqi = get_uqi()
        uqi.write_feedback(
            link_id=chosen_link_id,
            signal="confirmed",
            source="autonomous",
            reason=reason,
            user_id_hash=_get_user_id_hash(),
        )
        logger.info(
            "Ambiguous match resolved",
            ambiguity_id=ambiguity_id,
            chosen_link_id=chosen_link_id,
            reason=reason,
        )
        return True
    except Exception as exc:
        logger.warning(
            "Failed to resolve ambiguous match in unification store — continuing without it",
            ambiguity_id=ambiguity_id,
            chosen_link_id=chosen_link_id,
            error=str(exc),
        )
        return False


def get_all_entities(entity_type: str) -> list:
    """Fetch all Silver records of the given type. Returns [] on any failure."""
    try:
        uqi = get_uqi()
        return uqi.get_entities_by_type(entity_type, user_id_hash=_get_user_id_hash())
    except Exception as exc:
        logger.warning("Failed to fetch entities", entity_type=entity_type, error=str(exc))
        return []
