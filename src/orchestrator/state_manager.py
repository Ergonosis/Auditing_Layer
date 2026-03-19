"""Redis-backed state management for workflow resumability."""

import json
import os
from typing import Dict, Any
from datetime import datetime
from src.utils.errors import StateManagerError
from src.utils.logging import get_logger

logger = get_logger(__name__)

# Try to import redis, but don't fail if not available
try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    logger.warning("Redis module not available, will use in-memory backend")

# In-memory state backend for demo mode
_in_memory_state = {}

# Determine state backend
STATE_BACKEND = os.getenv("STATE_BACKEND", "redis")  # "redis" or "memory"

# Redis client setup
redis_client = None
if STATE_BACKEND == "redis" and REDIS_AVAILABLE:
    try:
        redis_host, redis_port = os.getenv("REDIS_HOST", "localhost:6379").split(':')
        redis_client = redis.Redis(
            host=redis_host,
            port=int(redis_port),
            db=int(os.getenv("REDIS_DB", 0)),
            decode_responses=True,
            socket_keepalive=True,
            socket_connect_timeout=5
        )
        redis_client.ping()  # Test connection
        logger.info("Connected to Redis", host=redis_host, port=redis_port)
    except Exception as e:
        logger.warning(f"Redis connection failed, falling back to in-memory: {e}")
        redis_client = None
elif STATE_BACKEND == "redis" and not REDIS_AVAILABLE:
    logger.warning("Redis backend requested but redis module not installed, using in-memory")
else:
    logger.info(f"Using in-memory state backend ({STATE_BACKEND} mode)")


def _persist_state_to_databricks(audit_run_id: str, state: Dict[str, Any]) -> None:
    """Best-effort Databricks persistence.  No-ops silently outside production."""
    try:
        from src.db.databricks_writer import write_workflow_state
        write_workflow_state(audit_run_id, state)
    except Exception as exc:
        logger.warning(
            "Failed to persist workflow state to Databricks — continuing",
            audit_run_id=audit_run_id,
            error=str(exc),
        )


def save_workflow_state(audit_run_id: str, state: Dict[str, Any]) -> None:
    """
    Save workflow state to Redis or in-memory store, and additionally
    persist to Databricks in production (best-effort).

    Args:
        audit_run_id: Unique audit run ID
        state: State dictionary to save

    Raises:
        StateManagerError: If save fails
    """
    # In-memory backend
    if STATE_BACKEND == "memory":
        _in_memory_state[audit_run_id] = state
        logger.info(f"Saved workflow state (in-memory)", audit_run_id=audit_run_id)
        _persist_state_to_databricks(audit_run_id, state)
        return

    # Redis backend
    if not redis_client:
        logger.warning("Redis unavailable, state not saved")
        _persist_state_to_databricks(audit_run_id, state)
        return

    try:
        key = f"audit:{audit_run_id}:state"
        value = json.dumps(state, default=str)  # default=str handles datetime
        redis_client.setex(key, 86400, value)  # 24 hour TTL
        logger.info(f"Saved workflow state", audit_run_id=audit_run_id)
    except Exception as e:
        raise StateManagerError(f"Failed to save workflow state: {e}")

    _persist_state_to_databricks(audit_run_id, state)


def restore_workflow_state(audit_run_id: str) -> Dict[str, Any]:
    """
    Restore workflow state from Redis or in-memory store.

    Args:
        audit_run_id: Unique audit run ID

    Returns:
        State dictionary, or empty dict if not found
    """
    # In-memory backend
    if STATE_BACKEND == "memory":
        state = _in_memory_state.get(audit_run_id, {})
        if state:
            logger.info(f"Restored workflow state (in-memory)", audit_run_id=audit_run_id)
        else:
            logger.warning(f"No saved state found for {audit_run_id} (in-memory)")
        return state

    # Redis backend
    if not redis_client:
        logger.warning("Redis unavailable, returning empty state")
        return {}

    try:
        key = f"audit:{audit_run_id}:state"
        value = redis_client.get(key)

        if value:
            state = json.loads(value)
            logger.info(f"Restored workflow state", audit_run_id=audit_run_id)
            return state
        else:
            logger.warning(f"No saved state found for {audit_run_id}")
            return {}

    except Exception as e:
        logger.error(f"Failed to restore workflow state: {e}")
        return {}


def mark_audit_complete(audit_run_id: str, summary: Dict[str, Any]) -> None:
    """
    Mark audit as completed and save final summary.

    Args:
        audit_run_id: Unique audit run ID
        summary: Final summary data
    """
    state = {
        'status': 'completed',
        'summary': summary,
        'completed_at': datetime.now().isoformat()
    }
    save_workflow_state(audit_run_id, state)


def check_redis_health() -> bool:
    """
    Check if Redis connection is healthy.

    Returns:
        True if Redis is reachable, False otherwise
    """
    if not redis_client:
        return False

    try:
        redis_client.ping()
        return True
    except Exception:
        return False
