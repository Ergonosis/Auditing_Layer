"""SQLite-backed local storage stub — implements the same interface as DeltaClient."""

import json
import re as _re
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import List, Optional

_SAFE_IDENTIFIER = _re.compile(r'^[a-zA-Z0-9_]+$')

import os as _os

from src.constants import EntityType
from src.models.links import AmbiguousMatch, EntityLink, LinkFeedback, RunLog, UnmatchedEntity
from src.models.run import Watermark
from src.utils.errors import SecureStorageRequiredError, StorageError
from src.utils.logging import get_logger

logger = get_logger(__name__)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _serialize(obj) -> str:
    return obj.model_dump_json()


class LocalStore:
    """SQLite-backed local development storage. Mirrors DeltaClient interface exactly."""

    def __init__(self, db_path: str):
        if _os.getenv("UNIFICATION_SECURE_STORAGE_REQUIRED", "").lower() == "true":
            raise SecureStorageRequiredError(
                "LocalStore (SQLite) cannot be used in secure/production mode "
                "(UNIFICATION_SECURE_STORAGE_REQUIRED=true). Use DeltaClient instead."
            )
        self.db_path = db_path
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS entity_links (
                    link_id TEXT PRIMARY KEY,
                    source_id TEXT NOT NULL,
                    target_id TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    target_type TEXT NOT NULL,
                    match_type TEXT NOT NULL,
                    match_tier TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    linkage_key TEXT NOT NULL,
                    rationale TEXT,
                    rule_version TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    effective_from TEXT NOT NULL,
                    effective_to TEXT,
                    is_current INTEGER NOT NULL DEFAULT 1,
                    superseded_by_link_id TEXT,
                    superseded_in_run_id TEXT
                );

                CREATE TABLE IF NOT EXISTS unmatched_entities (
                    entity_id TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    target_type TEXT,
                    reason_code TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    logged_at TEXT NOT NULL,
                    v2_processed INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (entity_id, entity_type, target_type)
                );

                CREATE TABLE IF NOT EXISTS ambiguous_matches (
                    ambiguity_id TEXT PRIMARY KEY,
                    source_entity_id TEXT NOT NULL,
                    source_type TEXT,
                    target_type TEXT,
                    candidate_ids TEXT NOT NULL,
                    candidate_scores TEXT NOT NULL,
                    status TEXT NOT NULL,
                    resolved_link_id TEXT,
                    resolved_by TEXT,
                    logged_at TEXT NOT NULL,
                    UNIQUE (source_entity_id, target_type)
                );

                CREATE TABLE IF NOT EXISTS link_feedback (
                    feedback_id TEXT PRIMARY KEY,
                    link_id TEXT NOT NULL,
                    signal TEXT NOT NULL,
                    source TEXT NOT NULL,
                    reason TEXT,
                    created_at TEXT NOT NULL,
                    processed INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS run_log (
                    run_id TEXT PRIMARY KEY,
                    run_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    start_time TEXT NOT NULL,
                    end_time TEXT,
                    records_processed INTEGER NOT NULL DEFAULT 0,
                    links_created INTEGER NOT NULL DEFAULT 0,
                    unmatched_count INTEGER NOT NULL DEFAULT 0,
                    ambiguous_count INTEGER NOT NULL DEFAULT 0,
                    failure_reason TEXT
                );

                CREATE TABLE IF NOT EXISTS watermarks (
                    entity_type TEXT PRIMARY KEY,
                    last_processed_at TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS intent_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    operation_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    planned_at TEXT NOT NULL,
                    committed INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS deletion_audit (
                    deletion_id  TEXT PRIMARY KEY,
                    entity_id    TEXT NOT NULL,
                    entity_type  TEXT NOT NULL,
                    requested_at TEXT NOT NULL,
                    completed_at TEXT,
                    rows_deleted INTEGER NOT NULL DEFAULT 0,
                    performed_by TEXT NOT NULL,
                    status       TEXT NOT NULL DEFAULT 'pending'
                );

                CREATE TABLE IF NOT EXISTS pipeline_locks (
                    lock_name   TEXT PRIMARY KEY,
                    run_id      TEXT NOT NULL,
                    acquired_at TEXT NOT NULL,
                    expires_at  TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS user_consents (
                    consent_id   TEXT PRIMARY KEY,
                    user_id_hash TEXT NOT NULL,
                    consent_type TEXT NOT NULL,
                    granted      INTEGER NOT NULL,
                    granted_at   TEXT NOT NULL,
                    expires_at   TEXT,
                    source       TEXT NOT NULL,
                    run_id       TEXT
                );

                CREATE TABLE IF NOT EXISTS access_audit_log (
                    audit_id       TEXT PRIMARY KEY,
                    event_time     TEXT NOT NULL,
                    operation      TEXT NOT NULL,
                    caller         TEXT NOT NULL,
                    entity_id_hash TEXT,
                    entity_type    TEXT,
                    run_id         TEXT,
                    result_count   INTEGER,
                    success        INTEGER NOT NULL DEFAULT 1
                );

                CREATE TABLE IF NOT EXISTS user_preferences (
                    pref_id      TEXT PRIMARY KEY,
                    user_id_hash TEXT NOT NULL UNIQUE,
                    opted_out    INTEGER NOT NULL DEFAULT 0,
                    updated_at   TEXT NOT NULL,
                    source       TEXT NOT NULL DEFAULT 'api'
                );

                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    amount REAL NOT NULL,
                    date TEXT NOT NULL,
                    merchant_name TEXT,
                    name TEXT,
                    payment_channel TEXT,
                    category TEXT,
                    source TEXT NOT NULL,
                    ingested_at TEXT NOT NULL,
                    raw_file_ref TEXT
                );

                CREATE TABLE IF NOT EXISTS emails (
                    message_id TEXT PRIMARY KEY,
                    received_at TEXT NOT NULL,
                    sender TEXT,
                    recipients TEXT NOT NULL,
                    subject TEXT,
                    body_preview TEXT,
                    thread_id TEXT,
                    source TEXT NOT NULL,
                    ingested_at TEXT NOT NULL,
                    raw_file_ref TEXT
                );

                CREATE TABLE IF NOT EXISTS calendar_events (
                    event_id TEXT PRIMARY KEY,
                    start_time TEXT NOT NULL,
                    end_time TEXT NOT NULL,
                    organizer TEXT NOT NULL,
                    subject TEXT,
                    attendees TEXT,
                    location TEXT,
                    source TEXT NOT NULL,
                    ingested_at TEXT NOT NULL,
                    raw_file_ref TEXT
                );
            """)

    # ── entity_links ──────────────────────────────────────────────────────────

    def upsert_link(self, link: EntityLink) -> None:
        try:
            with self._connect() as conn:
                conn.execute("""
                    INSERT INTO entity_links
                        (link_id, source_id, target_id, source_type, target_type,
                         match_type, match_tier, confidence, linkage_key, rationale,
                         rule_version, created_at, effective_from, effective_to, is_current,
                         superseded_by_link_id, superseded_in_run_id)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(link_id) DO UPDATE SET
                        source_id=excluded.source_id,
                        target_id=excluded.target_id,
                        source_type=excluded.source_type,
                        target_type=excluded.target_type,
                        match_type=excluded.match_type,
                        match_tier=excluded.match_tier,
                        confidence=excluded.confidence,
                        linkage_key=excluded.linkage_key,
                        rationale=excluded.rationale,
                        rule_version=excluded.rule_version,
                        effective_from=excluded.effective_from,
                        effective_to=excluded.effective_to,
                        is_current=excluded.is_current,
                        superseded_by_link_id=excluded.superseded_by_link_id,
                        superseded_in_run_id=excluded.superseded_in_run_id
                """, (
                    link.link_id, link.source_id, link.target_id,
                    link.source_type.value, link.target_type.value,
                    link.match_type.value, link.match_tier.value,
                    link.confidence, link.linkage_key, link.rationale,
                    link.rule_version,
                    link.created_at.isoformat(), link.effective_from.isoformat(),
                    link.effective_to.isoformat() if link.effective_to else None,
                    int(link.is_current),
                    link.superseded_by_link_id, link.superseded_in_run_id,
                ))
        except sqlite3.Error as e:
            raise StorageError(f"upsert_link failed: {e}") from e

    def get_link_by_id(self, link_id: str) -> Optional[EntityLink]:
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT * FROM entity_links WHERE link_id=?", (link_id,)
                ).fetchone()
            if row is None:
                return None
            return self._row_to_entity_link(row)
        except sqlite3.Error as e:
            raise StorageError(f"get_link_by_id failed: {e}") from e

    def get_link(self, source_id: str, target_id: str, is_current: bool = True) -> Optional[EntityLink]:
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT * FROM entity_links WHERE source_id=? AND target_id=? AND is_current=?",
                    (source_id, target_id, int(is_current)),
                ).fetchone()
            if row is None:
                return None
            return self._row_to_entity_link(row)
        except sqlite3.Error as e:
            raise StorageError(f"get_link failed: {e}") from e

    def get_current_links_by_sources(self, source_ids: list) -> list:
        """Fetch all current links for a set of source_ids in one query."""
        if not source_ids:
            return []
        placeholders = ",".join("?" * len(source_ids))
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    f"SELECT * FROM entity_links WHERE is_current=1 AND source_id IN ({placeholders})",
                    source_ids,
                ).fetchall()
            return [self._row_to_entity_link(r) for r in rows]
        except sqlite3.Error as e:
            raise StorageError(f"get_current_links_by_sources failed: {e}") from e

    def bulk_upsert_links(self, links: list) -> None:
        """Upsert multiple entity links in one executemany call."""
        if not links:
            return
        rows = [
            (l.link_id, l.source_id, l.target_id,
             l.source_type.value, l.target_type.value,
             l.match_type.value, l.match_tier.value,
             l.confidence, l.linkage_key, l.rationale,
             l.rule_version,
             l.created_at.isoformat(), l.effective_from.isoformat(),
             l.effective_to.isoformat() if l.effective_to else None,
             int(l.is_current),
             l.superseded_by_link_id, l.superseded_in_run_id)
            for l in links
        ]
        try:
            with self._connect() as conn:
                conn.executemany("""
                    INSERT INTO entity_links
                        (link_id, source_id, target_id, source_type, target_type,
                         match_type, match_tier, confidence, linkage_key, rationale,
                         rule_version, created_at, effective_from, effective_to, is_current,
                         superseded_by_link_id, superseded_in_run_id)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(link_id) DO UPDATE SET
                        source_id=excluded.source_id, target_id=excluded.target_id,
                        source_type=excluded.source_type, target_type=excluded.target_type,
                        match_type=excluded.match_type, match_tier=excluded.match_tier,
                        confidence=excluded.confidence, linkage_key=excluded.linkage_key,
                        rationale=excluded.rationale, rule_version=excluded.rule_version,
                        effective_from=excluded.effective_from, effective_to=excluded.effective_to,
                        is_current=excluded.is_current,
                        superseded_by_link_id=excluded.superseded_by_link_id,
                        superseded_in_run_id=excluded.superseded_in_run_id
                """, rows)
        except sqlite3.Error as e:
            raise StorageError(f"bulk_upsert_links failed: {e}") from e

    def soft_delete_link(
        self,
        link_id: str,
        superseded_by_link_id: Optional[str] = None,
        superseded_in_run_id: Optional[str] = None,
    ) -> None:
        try:
            with self._connect() as conn:
                conn.execute(
                    """UPDATE entity_links
                       SET is_current=0, effective_to=?,
                           superseded_by_link_id=?, superseded_in_run_id=?
                       WHERE link_id=?""",
                    (_now().isoformat(), superseded_by_link_id, superseded_in_run_id, link_id),
                )
        except sqlite3.Error as e:
            raise StorageError(f"soft_delete_link failed: {e}") from e

    def get_linked_entities(self, entity_id: str, entity_type: str) -> List[EntityLink]:
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    """SELECT * FROM entity_links
                       WHERE is_current=1
                         AND ((source_id=? AND source_type=?) OR (target_id=? AND target_type=?))""",
                    (entity_id, entity_type, entity_id, entity_type),
                ).fetchall()
            return [self._row_to_entity_link(r) for r in rows]
        except sqlite3.Error as e:
            raise StorageError(f"get_linked_entities failed: {e}") from e

    def _row_to_entity_link(self, row: sqlite3.Row) -> EntityLink:
        d = dict(row)
        d["is_current"] = bool(d["is_current"])
        return EntityLink.model_validate(d)

    # ── unmatched_entities ────────────────────────────────────────────────────

    def insert_unmatched(self, record: UnmatchedEntity) -> None:
        try:
            with self._connect() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO unmatched_entities
                        (entity_id, entity_type, target_type, reason_code, run_id, logged_at, v2_processed)
                    VALUES (?,?,?,?,?,?,?)
                """, (
                    record.entity_id, record.entity_type.value,
                    record.target_type.value if record.target_type else None,
                    record.reason_code.value,
                    record.run_id, record.logged_at.isoformat(), int(record.v2_processed),
                ))
        except sqlite3.Error as e:
            raise StorageError(f"insert_unmatched failed: {e}") from e

    def bulk_insert_unmatched(self, records: list) -> None:
        """Insert multiple unmatched records in one executemany. Skips existing records (idempotent)."""
        if not records:
            return
        rows = [
            (r.entity_id, r.entity_type.value,
             r.target_type.value if r.target_type else None,
             r.reason_code.value, r.run_id, r.logged_at.isoformat(), int(r.v2_processed))
            for r in records
        ]
        try:
            with self._connect() as conn:
                conn.executemany(
                    "INSERT OR IGNORE INTO unmatched_entities "
                    "(entity_id, entity_type, target_type, reason_code, run_id, logged_at, v2_processed) "
                    "VALUES (?,?,?,?,?,?,?)",
                    rows,
                )
        except sqlite3.Error as e:
            raise StorageError(f"bulk_insert_unmatched failed: {e}") from e

    def get_unmatched(self, entity_type: str = None, v2_processed: bool = False) -> List[UnmatchedEntity]:
        try:
            with self._connect() as conn:
                if entity_type is not None:
                    rows = conn.execute(
                        "SELECT * FROM unmatched_entities WHERE entity_type=? AND v2_processed=?",
                        (entity_type, int(v2_processed)),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        "SELECT * FROM unmatched_entities WHERE v2_processed=?",
                        (int(v2_processed),),
                    ).fetchall()
            return [UnmatchedEntity.model_validate({**dict(r), "v2_processed": bool(r["v2_processed"])}) for r in rows]
        except sqlite3.Error as e:
            raise StorageError(f"get_unmatched failed: {e}") from e

    # ── ambiguous_matches ─────────────────────────────────────────────────────

    def insert_ambiguous(self, record: AmbiguousMatch) -> None:
        try:
            with self._connect() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO ambiguous_matches
                        (ambiguity_id, source_entity_id, source_type, target_type,
                         candidate_ids, candidate_scores,
                         status, resolved_link_id, resolved_by, logged_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                """, (
                    record.ambiguity_id, record.source_entity_id,
                    record.source_type.value if record.source_type else None,
                    record.target_type.value if record.target_type else None,
                    json.dumps(record.candidate_ids),
                    json.dumps(record.candidate_scores),
                    record.status.value,
                    record.resolved_link_id,
                    record.resolved_by.value if record.resolved_by else None,
                    record.logged_at.isoformat(),
                ))
        except sqlite3.Error as e:
            raise StorageError(f"insert_ambiguous failed: {e}") from e

    def bulk_insert_ambiguous(self, records: list) -> None:
        """Insert multiple ambiguous records in one executemany. Skips existing records (idempotent)."""
        if not records:
            return
        rows = [
            (r.ambiguity_id, r.source_entity_id,
             r.source_type.value if r.source_type else None,
             r.target_type.value if r.target_type else None,
             json.dumps(r.candidate_ids), json.dumps(r.candidate_scores),
             r.status.value, r.resolved_link_id,
             r.resolved_by.value if r.resolved_by else None,
             r.logged_at.isoformat())
            for r in records
        ]
        try:
            with self._connect() as conn:
                conn.executemany(
                    "INSERT OR IGNORE INTO ambiguous_matches "
                    "(ambiguity_id, source_entity_id, source_type, target_type, "
                    "candidate_ids, candidate_scores, status, resolved_link_id, resolved_by, logged_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?)",
                    rows,
                )
        except sqlite3.Error as e:
            raise StorageError(f"bulk_insert_ambiguous failed: {e}") from e

    def get_ambiguous(self, status: str = "pending") -> List[AmbiguousMatch]:
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT * FROM ambiguous_matches WHERE status=?", (status,)
                ).fetchall()
            result = []
            for r in rows:
                d = dict(r)
                d["candidate_ids"] = json.loads(d["candidate_ids"])
                d["candidate_scores"] = json.loads(d["candidate_scores"])
                result.append(AmbiguousMatch.model_validate(d))
            return result
        except sqlite3.Error as e:
            raise StorageError(f"get_ambiguous failed: {e}") from e

    # ── link_feedback ─────────────────────────────────────────────────────────

    def insert_feedback(self, feedback: LinkFeedback) -> None:
        try:
            with self._connect() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO link_feedback
                        (feedback_id, link_id, signal, source, reason, created_at, processed)
                    VALUES (?,?,?,?,?,?,?)
                """, (
                    feedback.feedback_id, feedback.link_id,
                    feedback.signal.value, feedback.source.value,
                    feedback.reason, feedback.created_at.isoformat(),
                    int(feedback.processed),
                ))
        except sqlite3.Error as e:
            raise StorageError(f"insert_feedback failed: {e}") from e

    def get_unprocessed_feedback(self) -> List[LinkFeedback]:
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT * FROM link_feedback WHERE processed=0"
                ).fetchall()
            return [LinkFeedback.model_validate({**dict(r), "processed": bool(r["processed"])}) for r in rows]
        except sqlite3.Error as e:
            raise StorageError(f"get_unprocessed_feedback failed: {e}") from e

    def mark_feedback_processed(self, feedback_id: str) -> None:
        try:
            with self._connect() as conn:
                conn.execute(
                    "UPDATE link_feedback SET processed=1 WHERE feedback_id=?",
                    (feedback_id,),
                )
        except sqlite3.Error as e:
            raise StorageError(f"mark_feedback_processed failed: {e}") from e

    # ── run_log ───────────────────────────────────────────────────────────────

    def insert_run_log(self, run: RunLog) -> None:
        try:
            with self._connect() as conn:
                conn.execute("""
                    INSERT INTO run_log
                        (run_id, run_type, status, start_time, end_time,
                         records_processed, links_created, unmatched_count,
                         ambiguous_count, failure_reason)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                """, (
                    run.run_id, run.run_type.value, run.status.value,
                    run.start_time.isoformat(),
                    run.end_time.isoformat() if run.end_time else None,
                    run.records_processed, run.links_created,
                    run.unmatched_count, run.ambiguous_count, run.failure_reason,
                ))
        except sqlite3.Error as e:
            raise StorageError(f"insert_run_log failed: {e}") from e

    def update_run_log(self, run: RunLog) -> None:
        try:
            with self._connect() as conn:
                conn.execute("""
                    UPDATE run_log SET
                        status=?, end_time=?, records_processed=?, links_created=?,
                        unmatched_count=?, ambiguous_count=?, failure_reason=?
                    WHERE run_id=?
                """, (
                    run.status.value,
                    run.end_time.isoformat() if run.end_time else None,
                    run.records_processed, run.links_created,
                    run.unmatched_count, run.ambiguous_count,
                    run.failure_reason, run.run_id,
                ))
        except sqlite3.Error as e:
            raise StorageError(f"update_run_log failed: {e}") from e

    def get_last_run(self, run_type: str = None) -> Optional[RunLog]:
        try:
            with self._connect() as conn:
                if run_type is not None:
                    row = conn.execute(
                        "SELECT * FROM run_log WHERE run_type=? ORDER BY start_time DESC LIMIT 1",
                        (run_type,),
                    ).fetchone()
                else:
                    row = conn.execute(
                        "SELECT * FROM run_log ORDER BY start_time DESC LIMIT 1"
                    ).fetchone()
            if row is None:
                return None
            return RunLog.model_validate(dict(row))
        except sqlite3.Error as e:
            raise StorageError(f"get_last_run failed: {e}") from e

    # ── watermarks ────────────────────────────────────────────────────────────

    def get_watermark(self, entity_type: EntityType) -> Optional[Watermark]:
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT * FROM watermarks WHERE entity_type=?",
                    (entity_type.value,),
                ).fetchone()
            if row is None:
                return None
            return Watermark.model_validate(dict(row))
        except sqlite3.Error as e:
            raise StorageError(f"get_watermark failed: {e}") from e

    def set_watermark(self, watermark: Watermark) -> None:
        try:
            with self._connect() as conn:
                conn.execute("""
                    INSERT INTO watermarks (entity_type, last_processed_at, run_id, updated_at)
                    VALUES (?,?,?,?)
                    ON CONFLICT(entity_type) DO UPDATE SET
                        last_processed_at=excluded.last_processed_at,
                        run_id=excluded.run_id,
                        updated_at=excluded.updated_at
                """, (
                    watermark.entity_type.value,
                    watermark.last_processed_at.isoformat(),
                    watermark.run_id,
                    watermark.updated_at.isoformat(),
                ))
        except sqlite3.Error as e:
            raise StorageError(f"set_watermark failed: {e}") from e

    # ── intent_log ────────────────────────────────────────────────────────────

    def log_intent(self, run_id: str, operation_type: str, payload: dict) -> None:
        try:
            with self._connect() as conn:
                conn.execute("""
                    INSERT INTO intent_log (run_id, operation_type, payload_json, planned_at, committed)
                    VALUES (?,?,?,?,0)
                """, (run_id, operation_type, json.dumps(payload), _now().isoformat()))
        except sqlite3.Error as e:
            raise StorageError(f"log_intent failed: {e}") from e

    def bulk_log_intent(self, run_id: str, entries: list) -> None:
        """Insert multiple intent log entries in a single executemany call.
        entries: list of (operation_type, payload_dict)
        """
        if not entries:
            return
        rows = [(run_id, op, json.dumps(payload), _now().isoformat(), 0) for op, payload in entries]
        try:
            with self._connect() as conn:
                conn.executemany(
                    "INSERT INTO intent_log (run_id, operation_type, payload_json, planned_at, committed) "
                    "VALUES (?,?,?,?,?)",
                    rows,
                )
        except sqlite3.Error as e:
            raise StorageError(f"bulk_log_intent failed: {e}") from e

    def mark_intent_committed(self, run_id: str) -> None:
        try:
            with self._connect() as conn:
                conn.execute(
                    "UPDATE intent_log SET committed=1 WHERE run_id=?", (run_id,)
                )
        except sqlite3.Error as e:
            raise StorageError(f"mark_intent_committed failed: {e}") from e

    def get_intent_log(self, run_id: str) -> List[dict]:
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT * FROM intent_log WHERE run_id=? ORDER BY id ASC", (run_id,)
                ).fetchall()
            result = []
            for r in rows:
                d = dict(r)
                d["payload"] = json.loads(d.pop("payload_json"))
                d["committed"] = bool(d["committed"])
                result.append(d)
            return result
        except sqlite3.Error as e:
            raise StorageError(f"get_intent_log failed: {e}") from e

    # ── idempotency guards ────────────────────────────────────────────────────

    def unmatched_exists(self, entity_id: str, entity_type: str, target_type: str = None, run_id: str = None) -> bool:
        """Returns True if any unmatched record exists for this entity."""
        try:
            with self._connect() as conn:
                if target_type is not None and run_id is not None:
                    row = conn.execute(
                        "SELECT 1 FROM unmatched_entities WHERE entity_id=? AND entity_type=? AND target_type=? AND run_id=? LIMIT 1",
                        (entity_id, entity_type, target_type, run_id),
                    ).fetchone()
                elif target_type is not None:
                    row = conn.execute(
                        "SELECT 1 FROM unmatched_entities WHERE entity_id=? AND entity_type=? AND target_type=? LIMIT 1",
                        (entity_id, entity_type, target_type),
                    ).fetchone()
                elif run_id is not None:
                    row = conn.execute(
                        "SELECT 1 FROM unmatched_entities WHERE entity_id=? AND entity_type=? AND run_id=? LIMIT 1",
                        (entity_id, entity_type, run_id),
                    ).fetchone()
                else:
                    row = conn.execute(
                        "SELECT 1 FROM unmatched_entities WHERE entity_id=? AND entity_type=? LIMIT 1",
                        (entity_id, entity_type),
                    ).fetchone()
            return row is not None
        except sqlite3.Error as e:
            raise StorageError(f"unmatched_exists failed: {e}") from e

    def ambiguous_exists(self, source_entity_id: str, target_type: str = None) -> bool:
        """Returns True if any ambiguous record exists for this source entity (any status)."""
        try:
            with self._connect() as conn:
                if target_type is not None:
                    row = conn.execute(
                        "SELECT 1 FROM ambiguous_matches WHERE source_entity_id=? AND target_type=? LIMIT 1",
                        (source_entity_id, target_type),
                    ).fetchone()
                else:
                    row = conn.execute(
                        "SELECT 1 FROM ambiguous_matches WHERE source_entity_id=? LIMIT 1",
                        (source_entity_id,),
                    ).fetchone()
            return row is not None
        except sqlite3.Error as e:
            raise StorageError(f"ambiguous_exists failed: {e}") from e

    def acquire_pipeline_lock(self, run_id: str, ttl_seconds: int = 7200) -> bool:
        from datetime import datetime, timezone, timedelta
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        expires_iso = (now + timedelta(seconds=ttl_seconds)).isoformat()
        try:
            with self._connect() as conn:
                conn.execute(
                    "DELETE FROM pipeline_locks WHERE expires_at < ?", (now_iso,)
                )
                conn.execute(
                    "INSERT OR IGNORE INTO pipeline_locks (lock_name, run_id, acquired_at, expires_at) VALUES ('main', ?, ?, ?)",
                    (run_id, now_iso, expires_iso),
                )
                row = conn.execute(
                    "SELECT run_id FROM pipeline_locks WHERE lock_name='main'"
                ).fetchone()
            return row is not None and row["run_id"] == run_id
        except sqlite3.Error as e:
            raise StorageError(f"acquire_pipeline_lock failed: {e}") from e

    def release_pipeline_lock(self, run_id: str) -> None:
        try:
            with self._connect() as conn:
                conn.execute(
                    "DELETE FROM pipeline_locks WHERE lock_name='main' AND run_id=?",
                    (run_id,),
                )
        except sqlite3.Error as e:
            raise StorageError(f"release_pipeline_lock failed: {e}") from e

    def get_pipeline_lock(self):
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT * FROM pipeline_locks WHERE lock_name='main'"
                ).fetchone()
            return dict(row) if row else None
        except sqlite3.Error as e:
            raise StorageError(f"get_pipeline_lock failed: {e}") from e

    def upsert_consent(self, consent) -> None:
        from src.models.consent import UserConsent
        try:
            with self._connect() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO user_consents
                        (consent_id, user_id_hash, consent_type, granted, granted_at, expires_at, source, run_id)
                    VALUES (?,?,?,?,?,?,?,?)
                """, (
                    consent.consent_id, consent.user_id_hash, consent.consent_type,
                    int(consent.granted),
                    consent.granted_at.isoformat(),
                    consent.expires_at.isoformat() if consent.expires_at else None,
                    consent.source, consent.run_id,
                ))
        except sqlite3.Error as e:
            raise StorageError(f"upsert_consent failed: {e}") from e

    def has_active_consent(self, user_id_hash: str, consent_type: str) -> bool:
        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).isoformat()
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT granted, expires_at FROM user_consents WHERE user_id_hash=? AND consent_type=? ORDER BY granted_at DESC LIMIT 1",
                    (user_id_hash, consent_type),
                ).fetchone()
            if row is None:
                return False
            if not row["granted"]:
                return False
            if row["expires_at"] is not None and row["expires_at"] <= now_iso:
                return False
            return True
        except sqlite3.Error as e:
            raise StorageError(f"has_active_consent failed: {e}") from e

    def log_access(self, record) -> None:
        try:
            with self._connect() as conn:
                conn.execute("""
                    INSERT INTO access_audit_log
                        (audit_id, event_time, operation, caller, entity_id_hash, entity_type, run_id, result_count, success)
                    VALUES (?,?,?,?,?,?,?,?,?)
                """, (
                    record.audit_id,
                    record.event_time.isoformat(),
                    record.operation,
                    record.caller,
                    record.entity_id_hash,
                    record.entity_type,
                    record.run_id,
                    record.result_count,
                    int(record.success),
                ))
        except sqlite3.Error as e:
            raise StorageError(f"log_access failed: {e}") from e

    # ── maintenance ───────────────────────────────────────────────────────────

    def purge_old_records(self, table: str, timestamp_col: str, ttl_seconds: int) -> int:
        if not _SAFE_IDENTIFIER.match(table) or not _SAFE_IDENTIFIER.match(timestamp_col):
            raise StorageError(
                f"Unsafe identifier in purge_old_records: table={table!r}, col={timestamp_col!r}"
            )
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=ttl_seconds)).isoformat()
        try:
            with self._connect() as conn:
                cursor = conn.execute(
                    f"DELETE FROM {table} WHERE {timestamp_col} < ?", (cutoff,)
                )
            count = cursor.rowcount
            if count:
                logger.info("Retention purge", table=table, rows_deleted=count)
            return count
        except sqlite3.Error as e:
            raise StorageError(f"purge_old_records failed for {table}: {e}") from e

    def hard_delete_entity_data(self, entity_id: str, entity_type: str, performed_by: str = "admin") -> int:
        import uuid as _uuid
        from datetime import datetime, timezone
        deletion_id = str(_uuid.uuid4())
        requested_at = datetime.now(timezone.utc).isoformat()
        total = 0
        try:
            with self._connect() as conn:
                cur = conn.execute(
                    "DELETE FROM entity_links WHERE source_id=? OR target_id=?",
                    (entity_id, entity_id),
                )
                total += cur.rowcount
                cur = conn.execute(
                    "DELETE FROM unmatched_entities WHERE entity_id=?",
                    (entity_id,),
                )
                total += cur.rowcount
                cur = conn.execute(
                    "DELETE FROM ambiguous_matches WHERE source_entity_id=? OR candidate_ids LIKE ?",
                    (entity_id, f'%"{entity_id}"%'),
                )
                total += cur.rowcount
                cur = conn.execute(
                    "DELETE FROM link_feedback WHERE link_id IN "
                    "(SELECT link_id FROM entity_links WHERE source_id=? OR target_id=?)",
                    (entity_id, entity_id),
                )
                total += cur.rowcount
                completed_at = datetime.now(timezone.utc).isoformat()
                conn.execute(
                    "INSERT INTO deletion_audit (deletion_id, entity_id, entity_type, requested_at, completed_at, rows_deleted, performed_by, status) "
                    "VALUES (?,?,?,?,?,?,?,'completed')",
                    (deletion_id, entity_id, entity_type, requested_at, completed_at, total, performed_by),
                )
            return total
        except sqlite3.Error as e:
            raise StorageError(f"hard_delete_entity_data failed: {e}") from e

    def purge_old_intent_logs(self, ttl_seconds: int) -> int:
        """Delete intent_log rows older than ttl_seconds. Returns count deleted."""
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=ttl_seconds)).isoformat()
        try:
            with self._connect() as conn:
                cursor = conn.execute(
                    "DELETE FROM intent_log WHERE planned_at < ?", (cutoff,)
                )
            return cursor.rowcount
        except sqlite3.Error as e:
            raise StorageError(f"purge_old_intent_logs failed: {e}") from e

    def purge_old_audit_logs(self, ttl_seconds: int) -> int:
        """Delete access_audit_log and deletion_audit rows older than ttl_seconds.
        Must only be called via a controlled retention process, never via the generic purge loop.
        Returns total rows deleted across both tables."""
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=ttl_seconds)).isoformat()
        total = 0
        try:
            with self._connect() as conn:
                cur = conn.execute(
                    "DELETE FROM access_audit_log WHERE event_time < ?", (cutoff,)
                )
                total += cur.rowcount
                cur = conn.execute(
                    "DELETE FROM deletion_audit WHERE requested_at < ?", (cutoff,)
                )
                total += cur.rowcount
            if total:
                logger.info("Audit log retention purge", rows_deleted=total)
            return total
        except sqlite3.Error as e:
            raise StorageError(f"purge_old_audit_logs failed: {e}") from e

    # ── user_preferences ──────────────────────────────────────────────────────

    def set_user_preference(self, user_id_hash: str, opted_out: bool, source: str = "api") -> None:
        from uuid import uuid4
        pref_id = str(uuid4())
        updated_at = _now().isoformat()
        try:
            with self._connect() as conn:
                conn.execute("""
                    INSERT INTO user_preferences (pref_id, user_id_hash, opted_out, updated_at, source)
                    VALUES (?,?,?,?,?)
                    ON CONFLICT(user_id_hash) DO UPDATE SET
                        opted_out=excluded.opted_out,
                        updated_at=excluded.updated_at,
                        source=excluded.source
                """, (pref_id, user_id_hash, int(opted_out), updated_at, source))
        except sqlite3.Error as e:
            raise StorageError(f"set_user_preference failed: {e}") from e

    def get_user_preference(self, user_id_hash: str):
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT * FROM user_preferences WHERE user_id_hash=?",
                    (user_id_hash,),
                ).fetchone()
            if row is None:
                return None
            d = dict(row)
            d["opted_out"] = bool(d["opted_out"])
            return d
        except sqlite3.Error as e:
            raise StorageError(f"get_user_preference failed: {e}") from e

    # ── silver tables (canonical raw records) ─────────────────────────────────

    def upsert_transaction(self, txn) -> None:
        try:
            with self._connect() as conn:
                conn.execute("""
                    INSERT INTO transactions
                        (transaction_id, account_id, amount, date, merchant_name, name,
                         payment_channel, category, source, ingested_at, raw_file_ref)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(transaction_id) DO UPDATE SET
                        account_id=excluded.account_id,
                        amount=excluded.amount,
                        date=excluded.date,
                        merchant_name=excluded.merchant_name,
                        name=excluded.name,
                        payment_channel=excluded.payment_channel,
                        category=excluded.category,
                        source=excluded.source,
                        ingested_at=excluded.ingested_at,
                        raw_file_ref=excluded.raw_file_ref
                """, (
                    txn.transaction_id, txn.account_id, txn.amount,
                    txn.date.isoformat(),
                    txn.merchant_name, txn.name, txn.payment_channel,
                    json.dumps(txn.category) if txn.category is not None else None,
                    txn.source,
                    txn.ingested_at.isoformat(),
                    txn.raw_file_ref,
                ))
        except sqlite3.Error as e:
            raise StorageError(f"upsert_transaction failed: {e}") from e

    def get_transaction(self, transaction_id: str):
        from src.models.transaction import Transaction
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT * FROM transactions WHERE transaction_id=?", (transaction_id,)
                ).fetchone()
            if row is None:
                return None
            d = dict(row)
            if d.get("category") is not None:
                d["category"] = json.loads(d["category"])
            return Transaction.model_validate(d)
        except sqlite3.Error as e:
            raise StorageError(f"get_transaction failed: {e}") from e

    def upsert_email(self, email) -> None:
        try:
            with self._connect() as conn:
                conn.execute("""
                    INSERT INTO emails
                        (message_id, received_at, sender, recipients, subject, body_preview,
                         thread_id, source, ingested_at, raw_file_ref)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(message_id) DO UPDATE SET
                        received_at=excluded.received_at,
                        sender=excluded.sender,
                        recipients=excluded.recipients,
                        subject=excluded.subject,
                        body_preview=excluded.body_preview,
                        thread_id=excluded.thread_id,
                        source=excluded.source,
                        ingested_at=excluded.ingested_at,
                        raw_file_ref=excluded.raw_file_ref
                """, (
                    email.message_id,
                    email.received_at.isoformat(),
                    email.sender,
                    json.dumps(email.recipients),
                    email.subject, email.body_preview, email.thread_id,
                    email.source,
                    email.ingested_at.isoformat(),
                    email.raw_file_ref,
                ))
        except sqlite3.Error as e:
            raise StorageError(f"upsert_email failed: {e}") from e

    def get_email(self, message_id: str):
        from src.models.email import Email
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT * FROM emails WHERE message_id=?", (message_id,)
                ).fetchone()
            if row is None:
                return None
            d = dict(row)
            d["recipients"] = json.loads(d["recipients"])
            return Email.model_validate(d)
        except sqlite3.Error as e:
            raise StorageError(f"get_email failed: {e}") from e

    def upsert_calendar_event(self, event) -> None:
        try:
            with self._connect() as conn:
                conn.execute("""
                    INSERT INTO calendar_events
                        (event_id, start_time, end_time, organizer, subject, attendees,
                         location, source, ingested_at, raw_file_ref)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(event_id) DO UPDATE SET
                        start_time=excluded.start_time,
                        end_time=excluded.end_time,
                        organizer=excluded.organizer,
                        subject=excluded.subject,
                        attendees=excluded.attendees,
                        location=excluded.location,
                        source=excluded.source,
                        ingested_at=excluded.ingested_at,
                        raw_file_ref=excluded.raw_file_ref
                """, (
                    event.event_id,
                    event.start_time.isoformat(),
                    event.end_time.isoformat(),
                    event.organizer,
                    event.subject,
                    json.dumps(event.attendees) if event.attendees is not None else None,
                    event.location,
                    event.source,
                    event.ingested_at.isoformat(),
                    event.raw_file_ref,
                ))
        except sqlite3.Error as e:
            raise StorageError(f"upsert_calendar_event failed: {e}") from e

    def get_calendar_event(self, event_id: str):
        from src.models.calendar_event import CalendarEvent
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT * FROM calendar_events WHERE event_id=?", (event_id,)
                ).fetchone()
            if row is None:
                return None
            d = dict(row)
            if d.get("attendees") is not None:
                d["attendees"] = json.loads(d["attendees"])
            return CalendarEvent.model_validate(d)
        except sqlite3.Error as e:
            raise StorageError(f"get_calendar_event failed: {e}") from e

    def get_all_emails(self) -> list:
        from src.models.email import Email
        try:
            with self._connect() as conn:
                rows = conn.execute("SELECT * FROM emails").fetchall()
            result = []
            for row in rows:
                d = dict(row)
                d["recipients"] = json.loads(d["recipients"])
                result.append(Email.model_validate(d))
            return result
        except sqlite3.Error as e:
            raise StorageError(f"get_all_emails failed: {e}") from e

    def get_all_calendar_events(self) -> list:
        from src.models.calendar_event import CalendarEvent
        try:
            with self._connect() as conn:
                rows = conn.execute("SELECT * FROM calendar_events").fetchall()
            result = []
            for row in rows:
                d = dict(row)
                if d.get("attendees") is not None:
                    d["attendees"] = json.loads(d["attendees"])
                result.append(CalendarEvent.model_validate(d))
            return result
        except sqlite3.Error as e:
            raise StorageError(f"get_all_calendar_events failed: {e}") from e

    # ── lifecycle ─────────────────────────────────────────────────────────────

    def health_check(self) -> bool:
        try:
            with self._connect() as conn:
                conn.execute("SELECT 1")
            return True
        except sqlite3.Error:
            return False

    def close(self) -> None:
        pass  # Connections are opened/closed per operation; nothing to do.
