"""Adapter for the ergonosis-unification query interface.

This is the single import point for all UQI calls in the auditing repo.
"""

import os
from functools import lru_cache
from pathlib import Path
from typing import Optional

from src.utils.logging import get_logger

logger = get_logger(__name__)

# Default path assumes the two repos live side-by-side under ~/projects/
_DEFAULT_DB_PATH = str(
    Path(__file__).resolve().parents[3] / "ergonosis_unification" / ".local_store" / "unification.db"
)


@lru_cache(maxsize=1)
def get_uqi():
    """Return a UnifiedQueryInterface backed by the appropriate storage backend.

    Dev/test/demo: LocalStore (SQLite).
    Production: DeltaClient (Databricks) — not yet implemented.
    """
    if os.getenv("ENVIRONMENT") == "production":
        raise NotImplementedError(
            "Production storage backend (DeltaClient) is not wired yet. "
            "Set UNIFICATION_DB_PATH to use LocalStore in the meantime."
        )

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
        bundles = uqi.get_linked_entities(transaction_id, "transaction")
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
