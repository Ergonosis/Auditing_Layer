"""Databricks Delta Lake client and storage backend factory."""

import concurrent.futures
import os
import pathlib
import re
from typing import List, Optional, Union
import urllib.request

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from src.constants import (
    DATABRICKS_CONNECT_TIMEOUT_SECONDS,
    EntityType,
    ENV_DATABRICKS_CATALOG,
    ENV_DATABRICKS_HOST,
    ENV_DATABRICKS_HTTP_PATH,
    ENV_DATABRICKS_SCHEMA,
    ENV_DATABRICKS_TOKEN,
    ENV_SECURE_STORAGE_REQUIRED,
)
from src.models.links import AmbiguousMatch, EntityLink, LinkFeedback, RunLog, UnmatchedEntity
from src.models.run import Watermark
from pybreaker import CircuitBreaker, CircuitBreakerError
from src.utils.errors import ConfigurationError, DatabricksConnectionError, SecureStorageRequiredError, StorageError
from src.utils.logging import get_logger
from src.utils.metrics import databricks_healthy

logger = get_logger(__name__)

_databricks_breaker = CircuitBreaker(fail_max=5, reset_timeout=60, name="databricks")

_DEFAULT_CATALOG = "ergonosis"
_DEFAULT_SCHEMA = "unification"
_SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z0-9_]+$")


def _esc(val: str) -> str:
    """ANSI single-quote escaping for MERGE source literals.
    Databricks MERGE does not support ? placeholders in USING (SELECT ...) subqueries,
    so we escape string values by doubling embedded single quotes — equivalent to
    parameterization for the types used here (UUIDs, enum values, ISO datetimes, rationale text).
    """
    return "'" + val.replace("'", "''") + "'"


def _get_default_db_path() -> str:
    project_root = pathlib.Path(__file__).parent.parent.parent
    db_dir = project_root / ".local_store"
    db_dir.mkdir(exist_ok=True)
    return str(db_dir / "unification.db")


def get_storage_backend() -> Union["DeltaClient", "LocalStore"]:
    """
    Returns DeltaClient if DATABRICKS_HOST is set and connection succeeds.
    Falls back to LocalStore in dev (logs WARNING).

    When UNIFICATION_SECURE_STORAGE_REQUIRED=true:
    - Missing DATABRICKS_HOST → SecureStorageRequiredError (no fallback)
    - Missing/empty DATABRICKS_TOKEN → SecureStorageRequiredError
    - Connection failure → SecureStorageRequiredError (no fallback)

    Updates databricks_healthy Prometheus gauge accordingly.
    """
    from src.storage.local_store import LocalStore

    secure_mode = os.getenv(ENV_SECURE_STORAGE_REQUIRED, "").lower() == "true"
    host = os.getenv(ENV_DATABRICKS_HOST)
    token = os.getenv(ENV_DATABRICKS_TOKEN)

    if not host:
        if secure_mode:
            databricks_healthy.set(0)
            raise SecureStorageRequiredError(
                f"{ENV_DATABRICKS_HOST} is not set. "
                f"Set {ENV_SECURE_STORAGE_REQUIRED}=false to allow local fallback in dev."
            )
        logger.warning(f"{ENV_DATABRICKS_HOST} not set — using local SQLite store")
        databricks_healthy.set(0)
        return LocalStore(db_path=_get_default_db_path())

    if secure_mode and not token:
        databricks_healthy.set(0)
        raise SecureStorageRequiredError(
            f"{ENV_DATABRICKS_TOKEN} is not set or empty. "
            "Refusing to start in secure storage mode without authentication."
        )

    try:
        client = DeltaClient(
            host=host,
            token=token or "",
            catalog=os.getenv(ENV_DATABRICKS_CATALOG, _DEFAULT_CATALOG),
            schema=os.getenv(ENV_DATABRICKS_SCHEMA, _DEFAULT_SCHEMA),
        )
        client.health_check()
        databricks_healthy.set(1)
        client.check_workspace_encryption(token or "")
        return client
    except (SecureStorageRequiredError, ConfigurationError):
        raise
    except Exception as e:
        if secure_mode:
            databricks_healthy.set(0)
            raise SecureStorageRequiredError(
                f"Databricks connection failed in secure mode: {e}. "
                "Local fallback is disabled."
            ) from e
        logger.warning(f"Databricks connection failed ({e}) — falling back to local store")
        databricks_healthy.set(0)
        return LocalStore(db_path=_get_default_db_path())


class DeltaClient:
    """Production Databricks Delta Lake storage backend."""

    def __init__(self, host: str, token: str, catalog: str = _DEFAULT_CATALOG, schema: str = _DEFAULT_SCHEMA):
        for name, val in [("catalog", catalog), ("schema", schema)]:
            if not _SAFE_IDENTIFIER.match(val):
                raise ConfigurationError(
                    f"DeltaClient {name} must be alphanumeric and underscores only: {val!r}"
                )
        self.host = host
        self.catalog = catalog
        self.schema = schema
        self._connection = self._connect(token)

    def _connect(self, token: str):
        try:
            from databricks import sql as databricks_sql
            http_path = os.getenv("DATABRICKS_HTTP_PATH", "")
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                future = ex.submit(
                    databricks_sql.connect,
                    server_hostname=self.host,
                    http_path=http_path,
                    access_token=token,
                )
                try:
                    conn = future.result(timeout=DATABRICKS_CONNECT_TIMEOUT_SECONDS)
                    # Token is intentionally not stored as self.token after connection.
                    # In production, use short-lived tokens or GCP Workload Identity Federation
                    # (once migrated to GCP) to limit credential exposure window.
                except concurrent.futures.TimeoutError:
                    raise StorageError("Databricks connection timed out after 30s")
            return conn
        except StorageError:
            raise
        except Exception as e:
            raise DatabricksConnectionError(f"Cannot connect to Databricks: {e}") from e

    def check_workspace_encryption(self, token: str) -> bool:
        try:
            url = f"https://{self.host}/api/2.0/workspace-conf?keys=enableCustomerManagedKey"
            req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                import json as _json
                data = _json.loads(resp.read().decode())
            enabled = str(data.get("enableCustomerManagedKey", "false")).lower() == "true"
            if not enabled:
                import os as _os
                if _os.getenv("UNIFICATION_SECURE_STORAGE_REQUIRED", "").lower() == "true":
                    raise StorageError(
                        "Databricks workspace does not have Customer-Managed Key (CMK) encryption enabled. "
                        "CMK is required in secure/production mode (UNIFICATION_SECURE_STORAGE_REQUIRED=true). "
                        "Enable CMK via workspace settings."
                    )
                logger.warning(
                    "Databricks workspace does not have Customer-Managed Key (CMK) encryption enabled. "
                    "Enable CMK via workspace settings for SOC 2 compliance."
                )
            return enabled
        except StorageError:
            raise
        except Exception as exc:
            logger.warning("check_workspace_encryption failed (non-fatal)", error=str(exc))
            return False

    def _table(self, name: str) -> str:
        return f"{self.catalog}.{self.schema}.{name}"

    @retry(
        retry=retry_if_exception_type(StorageError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        reraise=True,
    )
    @_databricks_breaker
    def _execute(self, sql: str, params: tuple = ()):
        try:
            with self._connection.cursor() as cursor:
                cursor.execute(sql, params)
                return cursor
        except CircuitBreakerError:
            raise StorageError("Databricks circuit breaker open")
        except Exception as e:
            raise StorageError(f"Databricks query failed: {e}") from e

    def _fetchall(self, sql: str, params: tuple = ()):
        try:
            with self._connection.cursor() as cursor:
                cursor.execute(sql, params)
                cols = [d[0] for d in cursor.description]
                return [dict(zip(cols, row)) for row in cursor.fetchall()]
        except Exception as e:
            raise StorageError(f"Databricks fetch failed: {e}") from e

    def _fetchone(self, sql: str, params: tuple = ()):
        rows = self._fetchall(sql, params)
        return rows[0] if rows else None

    # ── entity_links ──────────────────────────────────────────────────────────

    def upsert_link(self, link: EntityLink) -> None:
        t = self._table("entity_links")
        sql = f"""
            MERGE INTO {t} AS target
            USING (SELECT
                {_esc(link.link_id)} AS link_id,
                {_esc(link.source_id)} AS source_id,
                {_esc(link.target_id)} AS target_id,
                {_esc(link.source_type.value)} AS source_type,
                {_esc(link.target_type.value)} AS target_type,
                {_esc(link.match_type.value)} AS match_type,
                {_esc(link.match_tier.value)} AS match_tier,
                {float(link.confidence)} AS confidence,
                {_esc(link.linkage_key)} AS linkage_key,
                {_esc(link.rationale) if link.rationale else 'NULL'} AS rationale,
                {_esc(link.rule_version)} AS rule_version,
                {_esc(link.created_at.isoformat())} AS created_at,
                {_esc(link.effective_from.isoformat())} AS effective_from,
                {_esc(link.effective_to.isoformat()) if link.effective_to else 'NULL'} AS effective_to,
                {int(link.is_current)} AS is_current,
                {_esc(link.superseded_by_link_id) if link.superseded_by_link_id else 'NULL'} AS superseded_by_link_id,
                {_esc(link.superseded_in_run_id) if link.superseded_in_run_id else 'NULL'} AS superseded_in_run_id
            ) AS source ON target.link_id = source.link_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
        try:
            self._execute(sql)
        except StorageError:
            raise
        except Exception as e:
            raise StorageError(f"upsert_link failed: {e}") from e

    def get_link_by_id(self, link_id: str) -> Optional[EntityLink]:
        t = self._table("entity_links")
        row = self._fetchone(f"SELECT * FROM {t} WHERE link_id=?", (link_id,))
        if row is None:
            return None
        row["is_current"] = bool(row["is_current"])
        return EntityLink.model_validate(row)

    def get_link(self, source_id: str, target_id: str, is_current: bool = True) -> Optional[EntityLink]:
        t = self._table("entity_links")
        row = self._fetchone(
            f"SELECT * FROM {t} WHERE source_id=? AND target_id=? AND is_current=?",
            (source_id, target_id, int(is_current)),
        )
        if row is None:
            return None
        row["is_current"] = bool(row["is_current"])
        return EntityLink.model_validate(row)

    def get_current_links_by_sources(self, source_ids: list) -> list:
        """Fetch all current links for a set of source_ids in one query."""
        if not source_ids:
            return []
        t = self._table("entity_links")
        placeholders = ",".join(_esc(sid) for sid in source_ids)
        rows = self._fetchall(
            f"SELECT * FROM {t} WHERE is_current=1 AND source_id IN ({placeholders})"
        )
        return [EntityLink.model_validate({**r, "is_current": bool(r["is_current"])}) for r in rows]

    def bulk_upsert_links(self, links: list) -> None:
        """Upsert multiple entity links in one MERGE statement."""
        if not links:
            return
        t = self._table("entity_links")
        value_rows = " UNION ALL ".join(
            f"SELECT {_esc(l.link_id)} AS link_id, "
            f"{_esc(l.source_id)} AS source_id, "
            f"{_esc(l.target_id)} AS target_id, "
            f"{_esc(l.source_type.value)} AS source_type, "
            f"{_esc(l.target_type.value)} AS target_type, "
            f"{_esc(l.match_type.value)} AS match_type, "
            f"{_esc(l.match_tier.value)} AS match_tier, "
            f"{float(l.confidence)} AS confidence, "
            f"{_esc(l.linkage_key)} AS linkage_key, "
            f"{'NULL' if not l.rationale else _esc(l.rationale)} AS rationale, "
            f"{_esc(l.rule_version)} AS rule_version, "
            f"{_esc(l.created_at.isoformat())} AS created_at, "
            f"{_esc(l.effective_from.isoformat())} AS effective_from, "
            f"{'NULL' if not l.effective_to else _esc(l.effective_to.isoformat())} AS effective_to, "
            f"{int(l.is_current)} AS is_current, "
            f"{'NULL' if not l.superseded_by_link_id else _esc(l.superseded_by_link_id)} AS superseded_by_link_id, "
            f"{'NULL' if not l.superseded_in_run_id else _esc(l.superseded_in_run_id)} AS superseded_in_run_id"
            for l in links
        )
        self._execute(f"""
            MERGE INTO {t} AS target
            USING ({value_rows}) AS source ON target.link_id = source.link_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    def soft_delete_link(
        self,
        link_id: str,
        superseded_by_link_id: Optional[str] = None,
        superseded_in_run_id: Optional[str] = None,
    ) -> None:
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        t = self._table("entity_links")
        self._execute(
            f"""UPDATE {t}
                SET is_current=0, effective_to=?,
                    superseded_by_link_id=?, superseded_in_run_id=?
                WHERE link_id=?""",
            (now, superseded_by_link_id, superseded_in_run_id, link_id),
        )

    def get_linked_entities(self, entity_id: str, entity_type: str) -> List[EntityLink]:
        t = self._table("entity_links")
        rows = self._fetchall(
            f"""SELECT * FROM {t}
                WHERE is_current=1
                  AND ((source_id=? AND source_type=?) OR (target_id=? AND target_type=?))""",
            (entity_id, entity_type, entity_id, entity_type),
        )
        return [EntityLink.model_validate({**r, "is_current": bool(r["is_current"])}) for r in rows]

    # ── unmatched_entities ────────────────────────────────────────────────────

    def insert_unmatched(self, record: UnmatchedEntity) -> None:
        t = self._table("unmatched_entities")
        self._execute(
            f"INSERT INTO {t} (entity_id, entity_type, target_type, reason_code, run_id, logged_at, v2_processed) VALUES (?,?,?,?,?,?,?)",
            (record.entity_id, record.entity_type.value,
             record.target_type.value if record.target_type else None,
             record.reason_code.value,
             record.run_id, record.logged_at.isoformat(), int(record.v2_processed)),
        )

    def bulk_insert_unmatched(self, records: list) -> None:
        """Insert multiple unmatched records in one MERGE. Skips existing records (idempotent)."""
        if not records:
            return
        t = self._table("unmatched_entities")
        value_rows = " UNION ALL ".join(
            f"SELECT {_esc(r.entity_id)} AS entity_id, "
            f"{_esc(r.entity_type.value)} AS entity_type, "
            f"{'NULL' if not r.target_type else _esc(r.target_type.value)} AS target_type, "
            f"{_esc(r.reason_code.value)} AS reason_code, "
            f"{_esc(r.run_id)} AS run_id, "
            f"{_esc(r.logged_at.isoformat())} AS logged_at, "
            f"{int(r.v2_processed)} AS v2_processed"
            for r in records
        )
        self._execute(f"""
            MERGE INTO {t} AS target
            USING ({value_rows}) AS source
            ON target.entity_id = source.entity_id
               AND target.entity_type = source.entity_type
               AND target.target_type IS NOT DISTINCT FROM source.target_type
            WHEN NOT MATCHED THEN INSERT *
        """)

    def get_unmatched(self, entity_type: str = None, v2_processed: bool = False) -> List[UnmatchedEntity]:
        t = self._table("unmatched_entities")
        if entity_type is not None:
            rows = self._fetchall(
                f"SELECT * FROM {t} WHERE entity_type=? AND v2_processed=?",
                (entity_type, int(v2_processed)),
            )
        else:
            rows = self._fetchall(
                f"SELECT * FROM {t} WHERE v2_processed=?", (int(v2_processed),)
            )
        return [UnmatchedEntity.model_validate({**r, "v2_processed": bool(r["v2_processed"])}) for r in rows]

    # ── ambiguous_matches ─────────────────────────────────────────────────────

    def insert_ambiguous(self, record: AmbiguousMatch) -> None:
        import json
        t = self._table("ambiguous_matches")
        self._execute(
            f"INSERT INTO {t} (ambiguity_id, source_entity_id, source_type, target_type, candidate_ids, candidate_scores, status, resolved_link_id, resolved_by, logged_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (record.ambiguity_id, record.source_entity_id,
             record.source_type.value if record.source_type else None,
             record.target_type.value if record.target_type else None,
             json.dumps(record.candidate_ids), json.dumps(record.candidate_scores),
             record.status.value, record.resolved_link_id,
             record.resolved_by.value if record.resolved_by else None,
             record.logged_at.isoformat()),
        )

    def bulk_insert_ambiguous(self, records: list) -> None:
        """Insert multiple ambiguous records in one MERGE. Skips existing records (idempotent)."""
        if not records:
            return
        import json
        t = self._table("ambiguous_matches")
        value_rows = " UNION ALL ".join(
            f"SELECT {_esc(r.ambiguity_id)} AS ambiguity_id, "
            f"{_esc(r.source_entity_id)} AS source_entity_id, "
            f"{'NULL' if not r.source_type else _esc(r.source_type.value)} AS source_type, "
            f"{'NULL' if not r.target_type else _esc(r.target_type.value)} AS target_type, "
            f"{_esc(json.dumps(r.candidate_ids))} AS candidate_ids, "
            f"{_esc(json.dumps(r.candidate_scores))} AS candidate_scores, "
            f"{_esc(r.status.value)} AS status, "
            f"{'NULL' if not r.resolved_link_id else _esc(r.resolved_link_id)} AS resolved_link_id, "
            f"{'NULL' if not r.resolved_by else _esc(r.resolved_by.value)} AS resolved_by, "
            f"{_esc(r.logged_at.isoformat())} AS logged_at"
            for r in records
        )
        self._execute(f"""
            MERGE INTO {t} AS target
            USING ({value_rows}) AS source
            ON target.source_entity_id = source.source_entity_id
               AND target.target_type IS NOT DISTINCT FROM source.target_type
            WHEN NOT MATCHED THEN INSERT *
        """)

    def get_ambiguous(self, status: str = "pending") -> List[AmbiguousMatch]:
        import json
        t = self._table("ambiguous_matches")
        rows = self._fetchall(f"SELECT * FROM {t} WHERE status=?", (status,))
        result = []
        for r in rows:
            r["candidate_ids"] = json.loads(r["candidate_ids"])
            r["candidate_scores"] = json.loads(r["candidate_scores"])
            result.append(AmbiguousMatch.model_validate(r))
        return result

    # ── link_feedback ─────────────────────────────────────────────────────────

    def insert_feedback(self, feedback: LinkFeedback) -> None:
        t = self._table("link_feedback")
        self._execute(
            f"INSERT INTO {t} (feedback_id, link_id, signal, source, reason, created_at, processed) VALUES (?,?,?,?,?,?,?)",
            (feedback.feedback_id, feedback.link_id, feedback.signal.value,
             feedback.source.value, feedback.reason,
             feedback.created_at.isoformat(), int(feedback.processed)),
        )

    def get_unprocessed_feedback(self) -> List[LinkFeedback]:
        t = self._table("link_feedback")
        rows = self._fetchall(f"SELECT * FROM {t} WHERE processed=0")
        return [LinkFeedback.model_validate({**r, "processed": bool(r["processed"])}) for r in rows]

    def mark_feedback_processed(self, feedback_id: str) -> None:
        t = self._table("link_feedback")
        self._execute(
            f"UPDATE {t} SET processed=1 WHERE feedback_id=?", (feedback_id,)
        )

    # ── run_log ───────────────────────────────────────────────────────────────

    def insert_run_log(self, run: RunLog) -> None:
        t = self._table("run_log")
        self._execute(
            f"INSERT INTO {t} (run_id, run_type, status, start_time, end_time, records_processed, links_created, unmatched_count, ambiguous_count, failure_reason) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (run.run_id, run.run_type.value, run.status.value,
             run.start_time.isoformat(),
             run.end_time.isoformat() if run.end_time else None,
             run.records_processed, run.links_created,
             run.unmatched_count, run.ambiguous_count, run.failure_reason),
        )

    def update_run_log(self, run: RunLog) -> None:
        t = self._table("run_log")
        self._execute(
            f"UPDATE {t} SET status=?, end_time=?, records_processed=?, links_created=?, unmatched_count=?, ambiguous_count=?, failure_reason=? WHERE run_id=?",
            (run.status.value,
             run.end_time.isoformat() if run.end_time else None,
             run.records_processed, run.links_created,
             run.unmatched_count, run.ambiguous_count,
             run.failure_reason, run.run_id),
        )

    def get_last_run(self, run_type: str = None) -> Optional[RunLog]:
        t = self._table("run_log")
        if run_type is not None:
            row = self._fetchone(
                f"SELECT * FROM {t} WHERE run_type=? ORDER BY start_time DESC LIMIT 1",
                (run_type,),
            )
        else:
            row = self._fetchone(f"SELECT * FROM {t} ORDER BY start_time DESC LIMIT 1")
        if row is None:
            return None
        return RunLog.model_validate(row)

    # ── watermarks ────────────────────────────────────────────────────────────

    def get_watermark(self, entity_type: EntityType) -> Optional[Watermark]:
        t = self._table("watermarks")
        row = self._fetchone(
            f"SELECT * FROM {t} WHERE entity_type=?", (entity_type.value,)
        )
        if row is None:
            return None
        return Watermark.model_validate(row)

    def set_watermark(self, watermark: Watermark) -> None:
        t = self._table("watermarks")
        sql = f"""
            MERGE INTO {t} AS target
            USING (SELECT
                {_esc(watermark.entity_type.value)} AS entity_type,
                {_esc(watermark.last_processed_at.isoformat())} AS last_processed_at,
                {_esc(watermark.run_id)} AS run_id,
                {_esc(watermark.updated_at.isoformat())} AS updated_at
            ) AS source ON target.entity_type = source.entity_type
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
        self._execute(sql)

    # ── intent_log ────────────────────────────────────────────────────────────

    def log_intent(self, run_id: str, operation_type: str, payload: dict) -> None:
        import json
        from datetime import datetime, timezone
        t = self._table("intent_log")
        self._execute(
            f"INSERT INTO {t} (run_id, operation_type, payload_json, planned_at, committed) VALUES (?,?,?,?,0)",
            (run_id, operation_type, json.dumps(payload), datetime.now(timezone.utc).isoformat()),
        )

    def bulk_log_intent(self, run_id: str, entries: list) -> None:
        """Insert multiple intent log entries in a single SQL statement.
        entries: list of (operation_type, payload_dict)
        """
        if not entries:
            return
        import json
        from datetime import datetime, timezone
        t = self._table("intent_log")
        now = datetime.now(timezone.utc).isoformat()
        value_rows = " UNION ALL ".join(
            f"SELECT {_esc(run_id)} AS run_id, {_esc(op)} AS operation_type, "
            f"{_esc(json.dumps(payload))} AS payload_json, {_esc(now)} AS planned_at, 0 AS committed"
            for op, payload in entries
        )
        self._execute(
            f"INSERT INTO {t} (run_id, operation_type, payload_json, planned_at, committed) "
            f"SELECT * FROM ({value_rows})"
        )

    def mark_intent_committed(self, run_id: str) -> None:
        t = self._table("intent_log")
        self._execute(f"UPDATE {t} SET committed=1 WHERE run_id=?", (run_id,))

    def get_intent_log(self, run_id: str) -> List[dict]:
        import json
        t = self._table("intent_log")
        rows = self._fetchall(f"SELECT * FROM {t} WHERE run_id=? ORDER BY id ASC", (run_id,))
        result = []
        for r in rows:
            r["payload"] = json.loads(r.pop("payload_json"))
            r["committed"] = bool(r["committed"])
            result.append(r)
        return result

    # ── idempotency guards ────────────────────────────────────────────────────

    def unmatched_exists(self, entity_id: str, entity_type: str, target_type: str = None, run_id: str = None) -> bool:
        """Returns True if any unmatched record exists for this entity."""
        t = self._table("unmatched_entities")
        if target_type is not None and run_id is not None:
            row = self._fetchone(
                f"SELECT 1 FROM {t} WHERE entity_id=? AND entity_type=? AND target_type=? AND run_id=? LIMIT 1",
                (entity_id, entity_type, target_type, run_id),
            )
        elif target_type is not None:
            row = self._fetchone(
                f"SELECT 1 FROM {t} WHERE entity_id=? AND entity_type=? AND target_type=? LIMIT 1",
                (entity_id, entity_type, target_type),
            )
        elif run_id is not None:
            row = self._fetchone(
                f"SELECT 1 FROM {t} WHERE entity_id=? AND entity_type=? AND run_id=? LIMIT 1",
                (entity_id, entity_type, run_id),
            )
        else:
            row = self._fetchone(
                f"SELECT 1 FROM {t} WHERE entity_id=? AND entity_type=? LIMIT 1",
                (entity_id, entity_type),
            )
        return row is not None

    def ambiguous_exists(self, source_entity_id: str, target_type: str = None) -> bool:
        """Returns True if any ambiguous record exists for this source entity (any status)."""
        t = self._table("ambiguous_matches")
        if target_type is not None:
            row = self._fetchone(
                f"SELECT 1 FROM {t} WHERE source_entity_id=? AND target_type=? LIMIT 1",
                (source_entity_id, target_type),
            )
        else:
            row = self._fetchone(
                f"SELECT 1 FROM {t} WHERE source_entity_id=? LIMIT 1",
                (source_entity_id,),
            )
        return row is not None

    def acquire_pipeline_lock(self, run_id: str, ttl_seconds: int = 7200) -> bool:
        from datetime import datetime, timezone, timedelta
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        expires_iso = (now + timedelta(seconds=ttl_seconds)).isoformat()
        t = self._table("pipeline_locks")
        self._execute(f"DELETE FROM {t} WHERE expires_at < ?", (now_iso,))
        self._execute(
            f"INSERT INTO {t} (lock_name, run_id, acquired_at, expires_at) SELECT 'main', ?, ?, ? "
            f"WHERE NOT EXISTS (SELECT 1 FROM {t} WHERE lock_name='main')",
            (run_id, now_iso, expires_iso),
        )
        row = self._fetchone(f"SELECT run_id FROM {t} WHERE lock_name='main'")
        return row is not None and row["run_id"] == run_id

    def release_pipeline_lock(self, run_id: str) -> None:
        t = self._table("pipeline_locks")
        self._execute(
            f"DELETE FROM {t} WHERE lock_name='main' AND run_id=?", (run_id,)
        )

    def get_pipeline_lock(self):
        t = self._table("pipeline_locks")
        return self._fetchone(f"SELECT * FROM {t} WHERE lock_name='main'")

    def upsert_consent(self, consent) -> None:
        t = self._table("user_consents")
        sql = f"""
            MERGE INTO {t} AS target
            USING (SELECT
                {_esc(consent.consent_id)} AS consent_id,
                {_esc(consent.user_id_hash)} AS user_id_hash,
                {_esc(consent.consent_type)} AS consent_type,
                {int(consent.granted)} AS granted,
                {_esc(consent.granted_at.isoformat())} AS granted_at,
                {_esc(consent.expires_at.isoformat()) if consent.expires_at else 'NULL'} AS expires_at,
                {_esc(consent.source)} AS source,
                {_esc(consent.run_id) if consent.run_id else 'NULL'} AS run_id
            ) AS source ON target.consent_id = source.consent_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
        self._execute(sql)

    def has_active_consent(self, user_id_hash: str, consent_type: str) -> bool:
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        t = self._table("user_consents")
        row = self._fetchone(
            f"SELECT granted, expires_at FROM {t} WHERE user_id_hash=? AND consent_type=? ORDER BY granted_at DESC LIMIT 1",
            (user_id_hash, consent_type),
        )
        if row is None:
            return False
        if not row["granted"]:
            return False
        if row["expires_at"] is not None:
            exp = row["expires_at"]
            # Databricks returns TIMESTAMP as datetime; SQLite returns ISO string
            if isinstance(exp, str):
                exp = datetime.fromisoformat(exp)
            if exp.tzinfo is None:
                exp = exp.replace(tzinfo=timezone.utc)
            if exp <= now:
                return False
        return True

    def log_access(self, record) -> None:
        t = self._table("access_audit_log")
        self._execute(
            f"INSERT INTO {t} (audit_id, event_time, operation, caller, entity_id_hash, entity_type, run_id, result_count, success) VALUES (?,?,?,?,?,?,?,?,?)",
            (
                record.audit_id,
                record.event_time.isoformat(),
                record.operation,
                record.caller,
                record.entity_id_hash,
                record.entity_type,
                record.run_id,
                record.result_count,
                int(record.success),
            ),
        )

    def purge_old_intent_logs(self, ttl_seconds: int) -> int:
        """No-op on DeltaClient — Delta Lake handles retention via table properties."""
        logger.warning(
            "purge_old_intent_logs called on DeltaClient — "
            "Delta retention is managed via table properties (TBLPROPERTIES delta.logRetentionDuration). "
            "No rows deleted."
        )
        return 0

    def purge_old_audit_logs(self, ttl_seconds: int) -> int:
        """No-op on DeltaClient — audit log retention on Delta Lake is managed via
        table properties (delta.logRetentionDuration) and a controlled external process.
        On GCP: use BigQuery table expiration or a Cloud Scheduler job with its own audit trail.
        Returns 0."""
        logger.warning(
            "purge_old_audit_logs called on DeltaClient — audit retention must be managed "
            "via Delta table properties or a controlled external process. No rows deleted."
        )
        return 0

    def purge_old_records(self, table: str, timestamp_col: str, ttl_seconds: int) -> int:
        from datetime import datetime, timezone, timedelta
        if not _SAFE_IDENTIFIER.match(table) or not _SAFE_IDENTIFIER.match(timestamp_col):
            raise StorageError(f"purge_old_records: invalid table or column name: {table!r}, {timestamp_col!r}")
        cutoff_iso = (datetime.now(timezone.utc) - timedelta(seconds=ttl_seconds)).isoformat()
        t = self._table(table)
        sql = f"DELETE FROM {t} WHERE {timestamp_col} < ?"
        try:
            cursor = self._execute(sql, (cutoff_iso,))
            count = cursor.rowcount if cursor.rowcount is not None else 0
            if count:
                logger.info("Retention purge", table=table, rows_deleted=count)
            return count
        except StorageError:
            raise
        except Exception as e:
            raise StorageError(f"purge_old_records failed for {table}: {e}") from e

    def hard_delete_entity_data(self, entity_id: str, entity_type: str, performed_by: str = "admin") -> int:
        import json as _json
        import uuid as _uuid
        from datetime import datetime, timezone
        deletion_id = str(_uuid.uuid4())
        requested_at = datetime.now(timezone.utc).isoformat()
        total = 0

        el = self._table("entity_links")
        ue = self._table("unmatched_entities")
        am = self._table("ambiguous_matches")
        lf = self._table("link_feedback")
        da = self._table("deletion_audit")

        cur = self._execute(
            f"DELETE FROM {el} WHERE source_id=? OR target_id=?",
            (entity_id, entity_id),
        )
        total += cur.rowcount if cur.rowcount is not None else 0

        cur = self._execute(
            f"DELETE FROM {ue} WHERE entity_id=?",
            (entity_id,),
        )
        total += cur.rowcount if cur.rowcount is not None else 0

        cur = self._execute(
            f"DELETE FROM {am} WHERE source_entity_id=?",
            (entity_id,),
        )
        total += cur.rowcount if cur.rowcount is not None else 0

        try:
            cur = self._execute(
                f"DELETE FROM {lf} WHERE link_id IN "
                f"(SELECT link_id FROM {el} WHERE source_id=? OR target_id=?)",
                (entity_id, entity_id),
            )
            total += cur.rowcount if cur.rowcount is not None else 0
        except Exception:
            pass

        completed_at = datetime.now(timezone.utc).isoformat()
        self._execute(
            f"INSERT INTO {da} (deletion_id, entity_id, entity_type, requested_at, completed_at, rows_deleted, performed_by, status) "
            f"VALUES (?,?,?,?,?,?,?,'completed')",
            (deletion_id, entity_id, entity_type, requested_at, completed_at, total, performed_by),
        )
        return total

    # ── user_preferences ──────────────────────────────────────────────────────

    def set_user_preference(self, user_id_hash: str, opted_out: bool, source: str = "api") -> None:
        from datetime import datetime, timezone
        from uuid import uuid4
        t = self._table("user_preferences")
        pref_id = str(uuid4())
        updated_at = datetime.now(timezone.utc).isoformat()
        sql = f"""
            MERGE INTO {t} AS target
            USING (SELECT
                {_esc(pref_id)} AS pref_id,
                {_esc(user_id_hash)} AS user_id_hash,
                {int(opted_out)} AS opted_out,
                {_esc(updated_at)} AS updated_at,
                {_esc(source)} AS source
            ) AS source ON target.user_id_hash = source.user_id_hash
            WHEN MATCHED THEN UPDATE SET opted_out=source.opted_out, updated_at=source.updated_at, source=source.source
            WHEN NOT MATCHED THEN INSERT *
        """
        self._execute(sql)

    def get_user_preference(self, user_id_hash: str):
        t = self._table("user_preferences")
        row = self._fetchone(f"SELECT * FROM {t} WHERE user_id_hash=?", (user_id_hash,))
        if row is None:
            return None
        row["opted_out"] = bool(row["opted_out"])
        return row

    # ── silver tables (canonical raw records) ─────────────────────────────────

    def upsert_transaction(self, txn) -> None:
        import json as _json
        t = self._table("transactions")
        sql = f"""
            MERGE INTO {t} AS target
            USING (SELECT
                {_esc(txn.transaction_id)} AS transaction_id,
                {_esc(txn.account_id)} AS account_id,
                {float(txn.amount)} AS amount,
                CAST({_esc(txn.date.isoformat())} AS DATE) AS date,
                {_esc(txn.merchant_name) if txn.merchant_name else 'NULL'} AS merchant_name,
                {_esc(txn.name) if txn.name else 'NULL'} AS name,
                {_esc(txn.payment_channel) if txn.payment_channel else 'NULL'} AS payment_channel,
                {_esc(_json.dumps(txn.category)) if txn.category is not None else 'NULL'} AS category,
                {_esc(txn.source)} AS source,
                {_esc(txn.ingested_at.isoformat())} AS ingested_at,
                {_esc(txn.raw_file_ref) if txn.raw_file_ref else 'NULL'} AS raw_file_ref
            ) AS source ON target.transaction_id = source.transaction_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
        try:
            self._execute(sql)
        except StorageError:
            raise
        except Exception as e:
            raise StorageError(f"upsert_transaction failed: {e}") from e

    def get_transaction(self, transaction_id: str):
        import json as _json
        from src.models.transaction import Transaction
        t = self._table("transactions")
        row = self._fetchone(f"SELECT * FROM {t} WHERE transaction_id=?", (transaction_id,))
        if row is None:
            return None
        if row.get("category") is not None:
            row["category"] = _json.loads(row["category"])
        return Transaction.model_validate(row)

    def upsert_email(self, email) -> None:
        import json as _json
        t = self._table("emails")
        sql = f"""
            MERGE INTO {t} AS target
            USING (SELECT
                {_esc(email.message_id)} AS message_id,
                {_esc(email.received_at.isoformat())} AS received_at,
                {_esc(email.sender) if email.sender else 'NULL'} AS sender,
                {_esc(_json.dumps(email.recipients))} AS recipients,
                {_esc(email.subject) if email.subject else 'NULL'} AS subject,
                {_esc(email.body_preview) if email.body_preview else 'NULL'} AS body_preview,
                {_esc(email.thread_id) if email.thread_id else 'NULL'} AS thread_id,
                {_esc(email.source)} AS source,
                {_esc(email.ingested_at.isoformat())} AS ingested_at,
                {_esc(email.raw_file_ref) if email.raw_file_ref else 'NULL'} AS raw_file_ref
            ) AS source ON target.message_id = source.message_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
        try:
            self._execute(sql)
        except StorageError:
            raise
        except Exception as e:
            raise StorageError(f"upsert_email failed: {e}") from e

    def get_email(self, message_id: str):
        import json as _json
        from src.models.email import Email
        t = self._table("emails")
        row = self._fetchone(f"SELECT * FROM {t} WHERE message_id=?", (message_id,))
        if row is None:
            return None
        row["recipients"] = _json.loads(row["recipients"])
        return Email.model_validate(row)

    def upsert_calendar_event(self, event) -> None:
        import json as _json
        t = self._table("calendar_events")
        sql = f"""
            MERGE INTO {t} AS target
            USING (SELECT
                {_esc(event.event_id)} AS event_id,
                {_esc(event.start_time.isoformat())} AS start_time,
                {_esc(event.end_time.isoformat())} AS end_time,
                {_esc(event.organizer)} AS organizer,
                {_esc(event.subject) if event.subject else 'NULL'} AS subject,
                {_esc(_json.dumps(event.attendees)) if event.attendees is not None else 'NULL'} AS attendees,
                {_esc(event.location) if event.location else 'NULL'} AS location,
                {_esc(event.source)} AS source,
                {_esc(event.ingested_at.isoformat())} AS ingested_at,
                {_esc(event.raw_file_ref) if event.raw_file_ref else 'NULL'} AS raw_file_ref
            ) AS source ON target.event_id = source.event_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
        try:
            self._execute(sql)
        except StorageError:
            raise
        except Exception as e:
            raise StorageError(f"upsert_calendar_event failed: {e}") from e

    def get_calendar_event(self, event_id: str):
        import json as _json
        from src.models.calendar_event import CalendarEvent
        t = self._table("calendar_events")
        row = self._fetchone(f"SELECT * FROM {t} WHERE event_id=?", (event_id,))
        if row is None:
            return None
        if row.get("attendees") is not None:
            row["attendees"] = _json.loads(row["attendees"])
        return CalendarEvent.model_validate(row)

    def get_all_emails(self) -> list:
        import json as _json
        from src.models.email import Email
        t = self._table("emails")
        rows = self._fetchall(f"SELECT * FROM {t}")
        for r in rows:
            r["recipients"] = _json.loads(r["recipients"])
        return [Email.model_validate(r) for r in rows]

    def get_all_calendar_events(self) -> list:
        import json as _json
        from src.models.calendar_event import CalendarEvent
        t = self._table("calendar_events")
        rows = self._fetchall(f"SELECT * FROM {t}")
        for r in rows:
            if r.get("attendees") is not None:
                r["attendees"] = _json.loads(r["attendees"])
        return [CalendarEvent.model_validate(r) for r in rows]

    # ── lifecycle ─────────────────────────────────────────────────────────────

    def health_check(self) -> bool:
        try:
            self._execute("SELECT 1")
            return True
        except Exception:
            return False

    def close(self) -> None:
        try:
            self._connection.close()
        except Exception:
            pass
