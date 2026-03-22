"""Databricks client with automatic fallback to mock JSON data for local development."""

from functools import lru_cache
import os
import pandas as pd
from datetime import datetime
from typing import Optional
from src.utils.errors import DatabricksConnectionError
from src.utils.logging import get_logger

logger = get_logger(__name__)


@lru_cache(maxsize=1)
def get_databricks_connection():
    """
    Singleton connection pool with automatic retry.
    Returns None if in dev mode (uses mock data instead).
    Returns "DEMO_MODE" if in demo mode (uses CSV data instead).

    Returns:
        Databricks connection object, "DEMO_MODE" sentinel, or None for mock mode

    Raises:
        DatabricksConnectionError: If connection fails in production mode
    """
    # Check for demo mode first
    if os.getenv("DEMO_MODE") == "true" or os.getenv("ENVIRONMENT") == "demo":
        logger.info("Using CSV demo data loader")
        return "DEMO_MODE"  # Sentinel value

    # Production mode
    elif os.getenv("DATABRICKS_HOST") and os.getenv("ENVIRONMENT") == "production":
        try:
            from databricks import sql
            http_path = (
                os.getenv("DATABRICKS_HTTP_PATH")
                or f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}"
            )
            conn = sql.connect(
                server_hostname=os.getenv("DATABRICKS_HOST"),
                http_path=http_path,
                auth_type="pat",
                token=os.getenv("DATABRICKS_TOKEN"),
                timeout_seconds=300
            )
            logger.info("Connected to Databricks", host=os.getenv("DATABRICKS_HOST"))
            return conn
        except Exception as e:
            raise DatabricksConnectionError(f"Failed to connect to Databricks: {e}")

    # Development/test mode (mock JSON fixtures)
    else:
        logger.info("Using mock data adapter (dev mode)")
        return None


def query_gold_tables(sql_query: str) -> pd.DataFrame:
    """
    Execute SQL query with automatic fallback to mock or demo data.

    Args:
        sql_query: SQL query string

    Returns:
        DataFrame with query results

    Raises:
        DatabricksConnectionError: If real connection fails
    """
    conn = get_databricks_connection()

    if conn == "DEMO_MODE":
        # Demo mode - load from CSV files
        try:
            from src.demo.csv_data_loader import load_demo_data
            return load_demo_data(sql_query)
        except Exception as e:
            logger.error(f"Demo data loading failed: {e}")
            raise DatabricksConnectionError(f"Demo data loading failed: {e}")

    elif conn:
        # Real Databricks query
        try:
            cursor = conn.cursor()
            cursor.execute(sql_query)
            result = cursor.fetchall_arrow().to_pandas()
            logger.info(f"Query returned {len(result)} rows")
            return result
        except Exception as e:
            raise DatabricksConnectionError(f"Query failed: {e}")

    else:
        # Mock data adapter (dev/test mode)
        return load_mock_data_from_json(sql_query)


def load_mock_data_from_json(sql_query: str) -> pd.DataFrame:
    """
    Mock data adapter for local development.
    Parses SQL query to determine which fixture to load.

    Args:
        sql_query: SQL query (used to infer which fixture to load)

    Returns:
        DataFrame from JSON fixture
    """
    import json
    from pathlib import Path

    # Parse table name from query (simple heuristic)
    query_lower = sql_query.lower()

    fixture_map = {
        "gold.recent_transactions": "tests/fixtures/sample_transactions.json",
        "gold.credit_cards": "tests/fixtures/sample_credit_cards.json",
        "gold.bank_accounts": "tests/fixtures/sample_bank_accounts.json",
        "gold.emails": "tests/fixtures/sample_emails.json",
        "workflow_state": "tests/fixtures/sample_workflow_state.json"
    }

    for table, fixture_path in fixture_map.items():
        if table in query_lower:
            try:
                return pd.read_json(fixture_path)
            except FileNotFoundError:
                logger.warning(f"Fixture not found: {fixture_path}, returning empty DataFrame")
                return pd.DataFrame()

    # Default: return empty DataFrame with expected schema
    logger.warning(f"No fixture found for query, returning empty DataFrame")
    return pd.DataFrame(columns=['txn_id', 'source', 'amount', 'vendor', 'date'])


def get_last_audit_timestamp() -> datetime:
    """
    Retrieve timestamp of last completed audit.

    Returns:
        Datetime of last audit, or default (2025-01-01) if none found
    """
    try:
        result = query_gold_tables("""
            SELECT MAX(created_at) as last_audit
            FROM workflow_state
            WHERE workflow_status = 'completed'
        """)
        if not result.empty and result['last_audit'][0]:
            return pd.to_datetime(result['last_audit'][0])
    except Exception as e:
        logger.warning(f"Could not get last audit timestamp: {e}")

    return datetime(2025, 1, 1)


def _prewarm_warehouse() -> None:
    """Fire a trivial query to wake the Serverless warehouse before real work starts.
    No-ops in dev/test. Never raises — failure is logged as a warning only."""
    if os.getenv("ENVIRONMENT") != "production":
        return
    try:
        import time
        t0 = time.time()
        conn = get_databricks_connection()
        if conn is None or conn == "DEMO_MODE":
            return
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        logger.info(f"Warehouse pre-warm complete ({time.time() - t0:.1f}s)")
    except Exception as e:
        logger.warning(f"Warehouse pre-warm failed (non-fatal): {e}")


# ── Shared production connection (Option B) ──────────────────────────────

_shared_conn = None


def get_shared_production_connection():
    """Singleton production connection shared by GoldTableReader and AuditDatabricksWriter.
    Returns None in dev/test.

    Passes _retry_stop_after_attempts_duration=60 to the SDK to cap the built-in
    retry loop at 60s. The default (900s) causes the pipeline to silently hang for
    15 minutes on every transient connection error from Cloud Run.
    """
    global _shared_conn
    if os.getenv("ENVIRONMENT") != "production":
        return None
    if _shared_conn is None:
        host = os.getenv("DATABRICKS_HOST", "")
        token = os.getenv("DATABRICKS_TOKEN", "")
        http_path = os.getenv("DATABRICKS_HTTP_PATH", "")
        from databricks import sql as _dbsql
        import time as _time
        t0 = _time.time()
        logger.info("Connecting to Databricks shared connection...")
        _shared_conn = _dbsql.connect(
            server_hostname=host,
            http_path=http_path,
            access_token=token,
            _retry_stop_after_attempts_duration=60,
        )
        logger.info(f"Shared Databricks connection established ({_time.time() - t0:.1f}s)")
    return _shared_conn


def close_shared_connection() -> None:
    """Close the shared production connection. Safe to call multiple times."""
    global _shared_conn
    if _shared_conn is not None:
        try:
            _shared_conn.close()
        except Exception:
            pass
        _shared_conn = None


def check_databricks_health() -> bool:
    """
    Check if Databricks connection is healthy.

    Returns:
        True if connection is healthy, False otherwise
    """
    try:
        conn = get_databricks_connection()
        if conn:
            query_gold_tables("SELECT 1")
            return True
        return True  # Mock mode, always "healthy"
    except Exception:
        return False
