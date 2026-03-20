"""Databricks reader for ergonosis.unification Gold tables.

Reads the full transaction population and match-status signals from the
upstream unification layer. Connection pattern mirrors AuditDatabricksWriter.
"""

import concurrent.futures
import os
import re
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from src.utils.errors import DatabricksConnectionError
from src.utils.logging import get_logger

logger = get_logger(__name__)

_DEFAULT_CATALOG = "ergonosis"
_UNIFICATION_SCHEMA = "unification"
_SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z0-9_]+$")
_CONNECT_TIMEOUT_SECONDS = 30


class GoldTableReader:
    """Read-only client for ergonosis.unification Gold tables."""

    def __init__(self):
        self.catalog = os.getenv("DATABRICKS_CATALOG", _DEFAULT_CATALOG)
        if not _SAFE_IDENTIFIER.match(self.catalog):
            raise DatabricksConnectionError(
                f"GoldTableReader catalog must be alphanumeric/underscores only: {self.catalog!r}"
            )
        self._connection = self._connect()

    def _connect(self):
        host = os.getenv("DATABRICKS_HOST", "")
        token = os.getenv("DATABRICKS_TOKEN", "")
        http_path = os.getenv("DATABRICKS_HTTP_PATH", "")
        if not host or not token:
            raise DatabricksConnectionError(
                "DATABRICKS_HOST and DATABRICKS_TOKEN are required"
            )
        try:
            from databricks import sql as databricks_sql
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                future = ex.submit(
                    databricks_sql.connect,
                    server_hostname=host,
                    http_path=http_path,
                    access_token=token,
                )
                try:
                    conn = future.result(timeout=_CONNECT_TIMEOUT_SECONDS)
                except concurrent.futures.TimeoutError:
                    raise DatabricksConnectionError(
                        f"Databricks connection timed out after {_CONNECT_TIMEOUT_SECONDS}s"
                    )
            logger.info("GoldTableReader connected", host=host)
            return conn
        except DatabricksConnectionError:
            raise
        except Exception as e:
            raise DatabricksConnectionError(f"Cannot connect to Databricks: {e}") from e

    def _table(self, name: str) -> str:
        return f"{self.catalog}.{_UNIFICATION_SCHEMA}.{name}"

    def get_transactions(self, since_timestamp=None) -> pd.DataFrame:
        """Read all rows from ergonosis.unification.transactions.

        Args:
            since_timestamp: If provided, filters WHERE ingested_at > since_timestamp.

        Returns:
            DataFrame with columns: txn_id, vendor, amount, date, category,
            source, account_id, ingested_at.
        """
        table = self._table("transactions")
        if since_timestamp is not None:
            sql = (
                f"SELECT transaction_id, "
                f"COALESCE(merchant_name, name, 'Unknown') AS vendor, "
                f"amount, date, category, source, account_id, ingested_at "
                f"FROM {table} WHERE ingested_at > ?"
            )
            params = (since_timestamp,)
        else:
            sql = (
                f"SELECT transaction_id, "
                f"COALESCE(merchant_name, name, 'Unknown') AS vendor, "
                f"amount, date, category, source, account_id, ingested_at "
                f"FROM {table}"
            )
            params = ()

        try:
            with self._connection.cursor() as cursor:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

            df = pd.DataFrame(rows, columns=columns)
            df = df.rename(columns={"transaction_id": "txn_id"})
            logger.info(f"GoldTableReader loaded {len(df)} transactions")
            return df
        except Exception as e:
            logger.error(f"Failed to read transactions: {e}")
            return pd.DataFrame(columns=["txn_id", "vendor", "amount", "date",
                                         "category", "source", "account_id", "ingested_at"])

    def get_linked_transaction_ids(self) -> set:
        """Return set of transaction_ids that have a confirmed match in entity_links."""
        table = self._table("entity_links")
        sql = (
            f"SELECT DISTINCT source_id FROM {table} "
            f"WHERE source_type = 'transaction' AND is_current = 1"
        )
        try:
            with self._connection.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
            return {row[0] for row in rows if row[0] is not None}
        except Exception as e:
            logger.warning(f"Failed to read entity_links: {e}")
            return set()

    def get_unmatched_transaction_ids(self) -> set:
        """Return set of transaction_ids flagged as unmatched in unmatched_entities."""
        table = self._table("unmatched_entities")
        sql = (
            f"SELECT entity_id FROM {table} "
            f"WHERE entity_type = 'transaction'"
        )
        try:
            with self._connection.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
            return {row[0] for row in rows if row[0] is not None}
        except Exception as e:
            logger.warning(f"Failed to read unmatched_entities: {e}")
            return set()

    def get_last_unification_run_timestamp(self) -> Optional[datetime]:
        """Return the most recent completed unification run start_time as UTC datetime.

        Returns:
            UTC-aware datetime if a completed run exists, None otherwise.
        """
        table = self._table("run_log")
        sql = f"SELECT MAX(start_time) FROM {table} WHERE status IN ('success', 'partial')"
        try:
            with self._connection.cursor() as cursor:
                cursor.execute(sql)
                row = cursor.fetchone()

            if row is None or row[0] is None:
                return None

            ts = row[0]
            if isinstance(ts, datetime):
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                return ts
            # Handle string timestamps from some Databricks connector versions
            ts = datetime.fromisoformat(str(ts))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            return ts

        except Exception as e:
            logger.warning(f"Failed to read run_log (table may not exist): {e}")
            return None
