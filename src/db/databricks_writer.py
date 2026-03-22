"""Databricks Delta Lake write client for the auditing pipeline.

Writes flags, audit trail entries, and workflow state to the
``ergonosis.auditing`` catalog.  In non-production environments the
functions silently no-op so that existing local/test behaviour is
preserved.

Connection pattern mirrors the upstream unification layer's
``src/storage/delta_client.py``.
"""

import concurrent.futures
import json
import os
import re
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, Optional

from src.db.schemas import FLAGS_TABLE_SCHEMA, AUDIT_TRAIL_TABLE_SCHEMA, WORKFLOW_STATE_TABLE_SCHEMA
from src.utils.errors import DatabaseError, DatabricksConnectionError
from src.utils.logging import get_logger

logger = get_logger(__name__)

_DEFAULT_CATALOG = "ergonosis"
_DEFAULT_AUDIT_SCHEMA = "auditing"
_SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z0-9_]+$")
_CONNECT_TIMEOUT_SECONDS = 30


def _is_production() -> bool:
    return os.getenv("ENVIRONMENT") == "production"


def _esc(val: str) -> str:
    """ANSI single-quote escaping for SQL literals."""
    return "'" + val.replace("'", "''") + "'"


class AuditDatabricksWriter:
    """Thin write layer for the ``ergonosis.auditing`` Databricks catalog."""

    def __init__(self, connection=None, skip_ensure_tables: bool = False):
        self.catalog = os.getenv("DATABRICKS_CATALOG", _DEFAULT_CATALOG)
        self.schema = os.getenv("DATABRICKS_AUDIT_SCHEMA", _DEFAULT_AUDIT_SCHEMA)
        for name, val in [("catalog", self.catalog), ("schema", self.schema)]:
            if not _SAFE_IDENTIFIER.match(val):
                raise DatabricksConnectionError(
                    f"AuditDatabricksWriter {name} must be alphanumeric/underscores only: {val!r}"
                )
        if connection is not None:
            self._connection = connection
        else:
            self._connection = self._connect()
        if not skip_ensure_tables:
            self._ensure_tables()

    def _connect(self):
        host = os.getenv("DATABRICKS_HOST", "")
        token = os.getenv("DATABRICKS_TOKEN", "")
        http_path = os.getenv("DATABRICKS_HTTP_PATH", "")
        if not host or not token:
            raise DatabricksConnectionError(
                "DATABRICKS_HOST and DATABRICKS_TOKEN are required for production writes"
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
            logger.info("AuditDatabricksWriter connected", host=host)
            return conn
        except DatabricksConnectionError:
            raise
        except Exception as e:
            raise DatabricksConnectionError(f"Cannot connect to Databricks: {e}") from e

    def _table(self, name: str) -> str:
        return f"{self.catalog}.{self.schema}.{name}"

    def _execute(self, sql: str, params: tuple = ()) -> None:
        try:
            with self._connection.cursor() as cursor:
                cursor.execute(sql, params)
        except Exception as e:
            raise DatabaseError(f"Databricks write failed: {e}") from e

    def _ensure_tables(self) -> None:
        """Create tables if they don't exist (idempotent).

        The DDL in schemas.py uses generic SQL.  Databricks requires
        ``USE CATALOG`` / ``USE SCHEMA`` before bare ``CREATE TABLE``
        statements, so we set the context first and then execute each
        DDL block.
        """
        try:
            self._execute(f"USE CATALOG {self.catalog}")
            self._execute(f"USE SCHEMA {self.schema}")
            for ddl_block in (FLAGS_TABLE_SCHEMA, AUDIT_TRAIL_TABLE_SCHEMA, WORKFLOW_STATE_TABLE_SCHEMA):
                # Each block may contain multiple statements (CREATE TABLE + CREATE INDEX).
                # Databricks SQL connector executes one statement at a time, so split on ';'.
                for stmt in ddl_block.split(";"):
                    stmt = stmt.strip()
                    if stmt:
                        try:
                            self._execute(stmt)
                        except DatabaseError:
                            # CREATE INDEX IF NOT EXISTS may not be supported on
                            # all Databricks runtimes — log and continue.
                            if "CREATE INDEX" in stmt.upper():
                                logger.debug("Skipping unsupported CREATE INDEX on Databricks")
                            else:
                                raise
            logger.info(
                "Audit tables verified",
                catalog=self.catalog,
                schema=self.schema,
            )
        except DatabaseError:
            raise
        except Exception as e:
            raise DatabaseError(f"Failed to ensure audit tables: {e}") from e

    # ── writes ────────────────────────────────────────────────────────────────

    def write_flag(self, flag_data: Dict[str, Any]) -> None:
        """Insert a single audit flag row."""
        t = self._table("flags")
        now = datetime.now(timezone.utc).isoformat()
        evidence = flag_data.get("supporting_evidence_links", "")
        if isinstance(evidence, dict):
            evidence = json.dumps(evidence)
        sql = (
            f"INSERT INTO {t} "
            f"(flag_id, transaction_id, audit_run_id, severity_level, "
            f"confidence_score, explanation, supporting_evidence_links, "
            f"created_at, updated_at) "
            f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        self._execute(sql, (
            flag_data["flag_id"],
            flag_data["transaction_id"],
            flag_data["audit_run_id"],
            flag_data["severity_level"],
            float(flag_data.get("confidence_score", 1.0)),
            flag_data["explanation"],
            evidence,
            flag_data.get("created_at", now),
            now,
        ))
        logger.info(
            "Flag persisted to Databricks",
            flag_id=flag_data["flag_id"],
            transaction_id=flag_data["transaction_id"],
        )

    def write_audit_trail_entry(self, entry: Dict[str, Any]) -> None:
        """Insert a single audit trail row."""
        t = self._table("audit_trail")
        sql = (
            f"INSERT INTO {t} "
            f"(audit_run_id, log_sequence_number, agent_name, tool_called, "
            f"timestamp, execution_time_ms, input_data, output_summary, "
            f"llm_model, llm_tokens_used, llm_cost_dollars, "
            f"error_message, error_stack_trace, decision_chain) "
            f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        self._execute(sql, (
            entry["audit_run_id"],
            int(entry["log_sequence_number"]),
            entry["agent_name"],
            entry["tool_called"],
            entry.get("timestamp", datetime.now(timezone.utc).isoformat()),
            entry.get("execution_time_ms"),
            json.dumps(entry.get("input_data")) if entry.get("input_data") else None,
            json.dumps(entry.get("output_summary")) if entry.get("output_summary") else None,
            entry.get("llm_model"),
            entry.get("llm_tokens_used"),
            entry.get("llm_cost_dollars"),
            entry.get("error_message"),
            entry.get("error_stack_trace"),
            json.dumps(entry.get("decision_chain")) if entry.get("decision_chain") else None,
        ))

    def write_workflow_state(self, audit_run_id: str, state: Dict[str, Any]) -> None:
        """Upsert workflow state for an audit run via DELETE + INSERT (parameterized)."""
        t = self._table("workflow_state")
        now = datetime.now(timezone.utc).isoformat()

        params = (
            audit_run_id,
            state.get("status", "in_progress"),
            state.get("current_agent"),
            json.dumps(state.get("completed_agents", [])),
            json.dumps(state.get("pending_agents", [])),
            json.dumps(state.get("intermediate_results", {})),
            now,
            now,
        )

        self._execute(f"DELETE FROM {t} WHERE audit_run_id = ?", (audit_run_id,))
        self._execute(
            f"""INSERT INTO {t} (
                audit_run_id, workflow_status, current_agent,
                completed_agents, pending_agents, intermediate_results,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            params,
        )
        logger.info(
            "Workflow state persisted to Databricks",
            audit_run_id=audit_run_id,
            status=state.get("status"),
        )

    def read_workflow_state(self, audit_run_id: str) -> Optional[Dict[str, Any]]:
        """Read the latest workflow state for an audit run from Databricks.

        Returns the state dict, or None if no record exists.
        """
        t = self._table("workflow_state")
        try:
            with self._connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT workflow_status, current_agent, completed_agents, "
                    f"pending_agents, intermediate_results "
                    f"FROM {t} WHERE audit_run_id = ? LIMIT 1",
                    (audit_run_id,)
                )
                row = cursor.fetchone()
                if row is None:
                    return None
                return {
                    "status": row[0],
                    "current_agent": row[1],
                    "completed_agents": json.loads(row[2] or "[]"),
                    "pending_agents": json.loads(row[3] or "[]"),
                    "intermediate_results": json.loads(row[4] or "{}"),
                }
        except Exception as e:
            logger.error("Failed to read workflow state from Databricks", error=str(e),
                         audit_run_id=audit_run_id)
            return None

    def close(self) -> None:
        try:
            self._connection.close()
        except Exception:
            pass


# ── module-level convenience functions ────────────────────────────────────
#
# These are the public API.  In non-production environments they silently
# no-op, preserving all existing local/test behaviour.

_writer: Optional[AuditDatabricksWriter] = None


def _get_writer() -> Optional[AuditDatabricksWriter]:
    """Lazy singleton — created on first production write, never in dev/test."""
    global _writer
    if not _is_production():
        return None
    if _writer is None:
        from src.tools.databricks_client import get_shared_production_connection
        skip = os.getenv("AUDIT_TABLES_VERIFIED", "").lower() == "true"
        _writer = AuditDatabricksWriter(
            connection=get_shared_production_connection(),
            skip_ensure_tables=skip,
        )
    return _writer


def write_flag(flag_data: Dict[str, Any]) -> None:
    """Persist an audit flag to Databricks.  No-op outside production."""
    writer = _get_writer()
    if writer is None:
        return
    try:
        writer.write_flag(flag_data)
    except Exception as exc:
        logger.error("Failed to persist flag to Databricks", error=str(exc),
                     flag_id=flag_data.get("flag_id"))


def write_audit_trail_entry(entry: Dict[str, Any]) -> None:
    """Persist an audit trail entry to Databricks.  No-op outside production."""
    writer = _get_writer()
    if writer is None:
        return
    try:
        writer.write_audit_trail_entry(entry)
    except Exception as exc:
        logger.error("Failed to persist audit trail entry to Databricks", error=str(exc))


def write_workflow_state(audit_run_id: str, state: Dict[str, Any]) -> None:
    """Persist workflow state to Databricks.  No-op outside production."""
    writer = _get_writer()
    if writer is None:
        return
    try:
        writer.write_workflow_state(audit_run_id, state)
    except Exception as exc:
        logger.error("Failed to persist workflow state to Databricks", error=str(exc),
                     audit_run_id=audit_run_id)
        raise


def read_workflow_state(audit_run_id: str) -> Optional[Dict[str, Any]]:
    """Read workflow state from Databricks.  Returns None outside production or on miss."""
    writer = _get_writer()
    if writer is None:
        return None
    return writer.read_workflow_state(audit_run_id)
