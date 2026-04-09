"""
Ergonosis Data Unification Pipeline.

Importable function:
    from src.pipeline import run_pipeline
    result = run_pipeline(run_type=RunType.INCREMENTAL)

CLI usage:
    python -m src.pipeline --type incremental
    python -m src.pipeline --type full_refresh
    python -m src.pipeline --type full_refresh --config path/to/config.yaml
"""

import argparse
import hashlib
import os
from datetime import datetime, timedelta, timezone
from typing import Optional
from uuid import uuid4

from src.constants import (
    AMBIGUOUS_TTL_SECONDS,
    COMMIT_BATCH_SIZE,
    ENTITY_LINKS_TTL_SECONDS,
    EntityType,
    ENV_HEALTH_PORT,
    ENV_METRICS_PORT,
    ENV_PLAID_ACCOUNT_ID,
    ENV_USER_EMAIL,
    INTENT_LOG_TTL_SECONDS,
    LINK_FEEDBACK_TTL_SECONDS,
    PIPELINE_LOCK_TTL_SECONDS,
    RUN_LOG_TTL_SECONDS,
    RunStatus,
    RunType,
    UNMATCHED_TTL_SECONDS,
)
from src.etl.transformer import Transformer
from src.etl.watermark import WatermarkManager
from src.feedback_processor import FeedbackProcessor
from src.ingestion.microsoft_adapter import fetch_calendar_events, fetch_emails
from src.ingestion.plaid_adapter import fetch_plaid_transactions
from src.matching.engine import MatchingEngine
from src.models.links import RunLog
from src.storage.delta_client import get_storage_backend
from src.storage.merge_handler import MergeHandler
from src.utils.config_loader import load_config
from src.utils.secrets_loader import load_secrets_to_env
from src.utils.errors import SecureStorageRequiredError  # noqa: F401 — documents startup raises
from src.utils.logging import get_logger
from src.utils.sanitize import sanitize_exception
from src.utils.metrics import run_duration, unmatched_rate_by_source

logger = get_logger(__name__)


def _chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def _commit_in_batches(items, batch_type, commit_fn, storage, run_id, batch_size, bulk=False):
    """
    Commit items in batches. After each batch, write a "batch_checkpoint" entry
    to the intent log so partial failures are observable via get_intent_log().
    Recovery on re-run is handled by idempotency guards in commit_fn.

    If bulk=True, commit_fn receives the full chunk list (for bulk insert paths).
    If bulk=False (default), commit_fn is called once per item (backward-compatible).
    """
    for batch_index, chunk in enumerate(_chunks(items, batch_size)):
        if bulk:
            commit_fn(chunk)
        else:
            for item in chunk:
                commit_fn(item)
        storage.log_intent(run_id, "batch_checkpoint", {
            "batch_type": batch_type,
            "batch_index": batch_index,
            "count": len(chunk),
        })
        logger.debug(
            "Batch checkpoint written",
            run_id=run_id,
            batch_type=batch_type,
            batch_index=batch_index,
            count=len(chunk),
        )


def _run_retention_purge(storage, config: dict) -> None:
    retention = config.get("retention", {})
    tables = [
        ("entity_links", "created_at", retention.get("entity_links_ttl_days", 365) * 86400, ENTITY_LINKS_TTL_SECONDS),
        ("unmatched_entities", "logged_at", retention.get("unmatched_ttl_days", 90) * 86400, UNMATCHED_TTL_SECONDS),
        ("ambiguous_matches", "logged_at", retention.get("ambiguous_ttl_days", 90) * 86400, AMBIGUOUS_TTL_SECONDS),
        ("run_log", "start_time", retention.get("run_log_ttl_days", 180) * 86400, RUN_LOG_TTL_SECONDS),
        ("link_feedback", "created_at", retention.get("link_feedback_ttl_days", 365) * 86400, LINK_FEEDBACK_TTL_SECONDS),
    ]
    total_deleted = 0
    for table, ts_col, cfg_ttl, fallback_ttl in tables:
        ttl = cfg_ttl if cfg_ttl else fallback_ttl
        try:
            deleted = storage.purge_old_records(table, ts_col, ttl)
            total_deleted += deleted
        except Exception as exc:
            logger.warning("Retention purge failed", table=table, error=str(exc))
    if total_deleted:
        logger.info("Retention purge complete", total_rows_deleted=total_deleted)


# Health HTTP server — started once per process if UNIFICATION_HEALTH_PORT is set.
_health_server_started = False


def _maybe_start_health_server(storage) -> None:
    global _health_server_started
    if _health_server_started:
        return
    port_str = os.getenv(ENV_HEALTH_PORT, "")
    if not port_str:
        return
    try:
        port = int(port_str)
        from src.api.health import create_health_app
        import threading
        app = create_health_app(lambda: storage)
        t = threading.Thread(target=lambda: app.run(host="0.0.0.0", port=port), daemon=True)
        t.start()
        _health_server_started = True
        logger.info("Health server started", port=port)
    except Exception as exc:
        logger.warning("Failed to start health server", error=str(exc))


# Prometheus metrics server — started once per process if METRICS_PORT is set.
_metrics_server_started = False


def _maybe_start_metrics_server() -> None:
    global _metrics_server_started
    if _metrics_server_started:
        return
    port_str = os.getenv(ENV_METRICS_PORT, "")
    if not port_str:
        return
    try:
        port = int(port_str)
        from prometheus_client import start_http_server
        start_http_server(port)
        _metrics_server_started = True
        logger.info("Prometheus metrics server started", port=port)
    except Exception as exc:
        logger.warning("Failed to start Prometheus metrics server", error=str(exc))


def run_pipeline(
    run_type: RunType = RunType.INCREMENTAL,
    config_path: str = "unification_config.yaml",
    storage=None,
) -> RunLog:
    """
    Execute the full unification pipeline.

    Flow:
        1. Load config
        2. Get storage backend (injectable for testing)
        3. Concurrent run guard (rejects if a run is already IN_PROGRESS)
        4. Process unprocessed feedback (CONFIRMED/REJECTED/FLAGGED signals)
        5. Full refresh: reset watermarks to epoch sentinel
        6. Ingest raw data (Plaid + Microsoft Graph, stub in dev mode)
        7. ETL transform (hard fail on SchemaValidationError halts run)
        8. Watermark filter (incremental: skip records already processed)
        9. Run three-tier matching cascade
        10. Write-ahead intent log for all planned operations (via storage backend)
        11. Commit: MergeHandler for links, insert_unmatched, insert_ambiguous
        12. Mark intent log committed
        13. Advance watermarks
        14. Finalize run log (SUCCESS)

    On any unrecoverable exception in steps 4–14:
        - Mark run_log FAILED with failure_reason
        - Append a "failed" entry to the storage-backed intent log
        - Re-raise the exception

    Args:
        run_type: INCREMENTAL (default) or FULL_REFRESH.
        config_path: Path to unification_config.yaml. Overridden by
                     UNIFICATION_CONFIG_PATH env var if set.
        storage: Injectable storage backend (LocalStore or DeltaClient).
                 If None, calls get_storage_backend() to resolve from environment.

    Returns:
        Completed RunLog with status=SUCCESS and final counts.

    Raises:
        SecureStorageRequiredError: If UNIFICATION_SECURE_STORAGE_REQUIRED=true and
            Databricks is unavailable. Raised before any run log is written.
        RuntimeError: If another run is already IN_PROGRESS.
        Any exception that causes run failure (run_log marked FAILED first).
    """
    load_secrets_to_env()
    _maybe_start_metrics_server()

    # ── Step 1: Load config ───────────────────────────────────────────────────
    config = load_config(config_path)
    rule_version: str = config.get("rule_version", "1.0")

    # ── Step 2: Storage backend ───────────────────────────────────────────────
    if storage is None:
        storage = get_storage_backend()

    watermark_mgr = WatermarkManager(storage)

    # ── Step 3: Distributed pipeline lock ────────────────────────────────────
    run_id = str(uuid4())
    if not storage.acquire_pipeline_lock(run_id):
        lock_info = storage.get_pipeline_lock()
        logger.warning(
            "Pipeline lock already held — exiting cleanly to stop Cloud Run retry loop",
            lock_info=lock_info,
        )
        return RunLog(
            run_id=run_id,
            run_type=run_type,
            status=RunStatus.SKIPPED,
        )

    # ── Step 3b: Concurrent run guard (belt and suspenders) ───────────────────
    last_run = storage.get_last_run()
    if last_run and last_run.status == RunStatus.IN_PROGRESS:
        start = last_run.start_time
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        age_seconds = (datetime.now(timezone.utc) - start).total_seconds()

        if age_seconds > PIPELINE_LOCK_TTL_SECONDS:
            # Stale run — likely killed by container timeout without graceful shutdown.
            # Auto-resolve so the pipeline can proceed.
            last_run.status = RunStatus.FAILED
            last_run.end_time = datetime.now(timezone.utc)
            last_run.failure_reason = (
                f"auto-resolved: stale in_progress (started {age_seconds:.0f}s ago, "
                f"exceeded lock TTL of {PIPELINE_LOCK_TTL_SECONDS}s)"
            )
            storage.update_run_log(last_run)
            logger.warning(
                "Auto-resolved stale in_progress run",
                stale_run_id=last_run.run_id,
                age_seconds=age_seconds,
            )
        else:
            # Recent in_progress run — genuinely concurrent. Exit cleanly (exit 0).
            storage.release_pipeline_lock(run_id)
            logger.warning(
                "Recent in_progress run detected — exiting cleanly",
                in_progress_run_id=last_run.run_id,
                age_seconds=age_seconds,
            )
            return RunLog(
                run_id=run_id,
                run_type=run_type,
                status=RunStatus.SKIPPED,
            )

    # ── Step 4: Register run as IN_PROGRESS ───────────────────────────────────
    run_log = RunLog(
        run_id=run_id,
        run_type=run_type,
        status=RunStatus.IN_PROGRESS,
    )
    storage.insert_run_log(run_log)

    logger.info(
        "Pipeline run started",
        run_id=run_id,
        run_type=run_type.value,
        rule_version=rule_version,
    )

    start_time = datetime.now(timezone.utc)
    timer = run_duration.labels(run_type=run_type.value)

    try:
        with timer.time():
            _execute_pipeline(
                run_id=run_id,
                run_type=run_type,
                run_log=run_log,
                config=config,
                rule_version=rule_version,
                storage=storage,
                watermark_mgr=watermark_mgr,
            )

    except Exception as exc:
        run_log.status = RunStatus.FAILED
        run_log.failure_reason = sanitize_exception(exc)
        run_log.end_time = datetime.now(timezone.utc)
        try:
            storage.update_run_log(run_log)
        except Exception:
            pass  # Don't mask the original error
        try:
            storage.log_intent(run_id, "failed", {"reason": sanitize_exception(exc)})
        except Exception:
            pass
        logger.error(
            "Pipeline run failed",
            run_id=run_id,
            error=sanitize_exception(exc),
        )
        raise
    finally:
        try:
            storage.release_pipeline_lock(run_id)
        except Exception:
            pass

    logger.info(
        "Pipeline run complete",
        run_id=run_id,
        status=run_log.status.value,
        links_created=run_log.links_created,
        unmatched_count=run_log.unmatched_count,
        ambiguous_count=run_log.ambiguous_count,
        records_processed=run_log.records_processed,
    )
    return run_log


def _execute_pipeline(
    run_id: str,
    run_type: RunType,
    run_log: RunLog,
    config: dict,
    rule_version: str,
    storage,
    watermark_mgr: WatermarkManager,
) -> None:
    """Internal pipeline body. Separated for clean exception handling in run_pipeline."""

    _maybe_start_health_server(storage)

    # ── Step 4: Intent log TTL purge ──────────────────────────────────────────
    purged = storage.purge_old_intent_logs(INTENT_LOG_TTL_SECONDS)
    if purged:
        logger.info("Intent log TTL purge", rows_deleted=purged)

    _run_retention_purge(storage, config)

    # ── Step 5: Feedback processing ───────────────────────────────────────────
    processor = FeedbackProcessor(storage)
    fb_result = processor.process_all(run_id)
    logger.info(
        "Feedback processed",
        confirmed=fb_result.confirmed,
        rejected=fb_result.rejected,
        flagged=fb_result.flagged,
        errors=fb_result.errors,
    )

    # ── Step 5: Full refresh — reset watermarks to epoch sentinel ─────────────
    if run_type == RunType.FULL_REFRESH:
        watermark_mgr.reset_watermarks(run_id)
        logger.info("Watermarks reset for full refresh", run_id=run_id)

    # ── Step 6: Ingest ────────────────────────────────────────────────────────
    ingestion_cfg = config.get("ingestion", {})
    account_id = (
        os.getenv(ENV_PLAID_ACCOUNT_ID)
        or ingestion_cfg.get("plaid_account_id")
        or None
    )
    user_email = (
        os.getenv(ENV_USER_EMAIL)
        or ingestion_cfg.get("user_email")
        or "stub@ergonosis.com"
    )

    today = datetime.now(timezone.utc).date()
    lookback = ingestion_cfg.get("lookback_days", 90)
    if run_type == RunType.INCREMENTAL:
        last_watermark_dt = watermark_mgr.get_watermark(EntityType.TRANSACTION)
        if last_watermark_dt is not None:
            start_date = (
                last_watermark_dt.astimezone(timezone.utc).date()
                if last_watermark_dt.tzinfo
                else last_watermark_dt.date()
            )
        else:
            start_date = today - timedelta(days=lookback)
    else:
        start_date = today - timedelta(days=lookback)

    date_range = (start_date.isoformat(), today.isoformat())

    user_id_hash = hashlib.sha256(user_email.encode()).hexdigest()[:16]

    stub_mode = os.getenv("STUB_INGESTION", "").lower() in ("true", "rich", "smb")
    if not stub_mode and not storage.has_active_consent(user_id_hash, "data_processing"):
        raise RuntimeError(
            f"No active data_processing consent on record for user {user_id_hash}. "
            "Register consent before running the pipeline."
        )
    if not stub_mode:
        pref = storage.get_user_preference(user_id_hash)
        if pref and pref.get("opted_out"):
            raise RuntimeError(f"User {user_id_hash} has opted out of data processing.")

    logger.info(
        "Ingestion params resolved",
        account_id=account_id,
        user_email=user_id_hash,
        date_range=date_range,
    )

    raw_txns = fetch_plaid_transactions(
        account_id=account_id,
        date_range=date_range,
    ).get("transactions", [])

    raw_emails = fetch_emails(
        user_email=user_email,
        start_datetime=date_range[0],
        end_datetime=date_range[1],
    )

    raw_calendar = fetch_calendar_events(
        user_email=user_email,
        start_datetime=date_range[0],
        end_datetime=date_range[1],
    )

    logger.info(
        "Ingestion complete",
        raw_transactions=len(raw_txns),
        raw_emails=len(raw_emails),
        raw_calendar=len(raw_calendar),
    )

    # ── Step 7: ETL transform ─────────────────────────────────────────────────
    # SchemaValidationError (hard fail) propagates up immediately and halts the run.
    transformer = Transformer()
    txns, _txn_failures = transformer.transform_batch(raw_txns, "transaction")
    emails, _email_failures = transformer.transform_batch(raw_emails, "email")
    events, _event_failures = transformer.transform_batch(raw_calendar, "calendar_event")

    logger.info(
        "ETL transform complete",
        transactions=len(txns),
        emails=len(emails),
        calendar_events=len(events),
    )

    # ── Step 7.5: Persist raw entities to Silver tables ──────────────────────
    # Silver writes happen pre-watermark-filter so full_refresh runs backfill
    # Silver tables correctly. Failures are non-blocking (warn + continue).
    if not stub_mode:
        _silver_errors = 0
        for _txn in txns:
            try:
                storage.upsert_transaction(_txn)
            except Exception as _exc:
                _silver_errors += 1
                logger.warning("upsert_transaction failed",
                               transaction_id=_txn.transaction_id,
                               error=sanitize_exception(_exc))
        for _email in emails:
            try:
                storage.upsert_email(_email)
            except Exception as _exc:
                _silver_errors += 1
                logger.warning("upsert_email failed",
                               message_id=_email.message_id,
                               error=sanitize_exception(_exc))
        for _event in events:
            try:
                storage.upsert_calendar_event(_event)
            except Exception as _exc:
                _silver_errors += 1
                logger.warning("upsert_calendar_event failed",
                               event_id=_event.event_id,
                               error=sanitize_exception(_exc))
        storage.log_intent(run_id, "silver_persisted", {
            "transactions": len(txns),
            "emails": len(emails),
            "calendar_events": len(events),
            "errors": _silver_errors,
        })
        logger.info("Silver table persistence complete",
                    transactions=len(txns), emails=len(emails),
                    calendar_events=len(events), errors=_silver_errors)

    # ── Step 8: Watermark filter (incremental only) ───────────────────────────
    if run_type == RunType.INCREMENTAL:
        txn_wm = watermark_mgr.get_watermark(EntityType.TRANSACTION)
        email_wm = watermark_mgr.get_watermark(EntityType.EMAIL)
        cal_wm = watermark_mgr.get_watermark(EntityType.CALENDAR_EVENT)

        def _after_watermark(entity, wm: Optional[datetime]) -> bool:
            if wm is None:
                return True
            # Make both comparable: strip tzinfo if one is naive
            entity_ts = entity.ingested_at
            if entity_ts.tzinfo is None and wm.tzinfo is not None:
                wm = wm.replace(tzinfo=None)
            elif entity_ts.tzinfo is not None and wm.tzinfo is None:
                entity_ts = entity_ts.replace(tzinfo=None)
            return entity_ts > wm

        pre_filter = (len(txns), len(emails), len(events))
        txns = [t for t in txns if _after_watermark(t, txn_wm)]
        emails = [e for e in emails if _after_watermark(e, email_wm)]
        events = [e for e in events if _after_watermark(e, cal_wm)]
        logger.info(
            "Watermark filter applied",
            transactions_before=pre_filter[0],
            transactions_after=len(txns),
            emails_before=pre_filter[1],
            emails_after=len(emails),
            calendar_events_before=pre_filter[2],
            calendar_events_after=len(events),
        )

    # ── Step 9: Matching ──────────────────────────────────────────────────────
    engine = MatchingEngine(config, rule_version=rule_version)
    result = engine.run_matching(txns, emails, events, run_id)

    logger.info(
        "Matching complete",
        links=len(result.links),
        unmatched=len(result.unmatched),
        ambiguous=len(result.ambiguous),
        match_rate=f"{result.match_rate:.2%}",
    )

    # ── Step 10: Intent log (write-ahead, stored in secure backend) ───────────
    intent_entries = []
    for link in result.links:
        intent_entries.append(("planned_link", {
            "source_id": link.source_id,
            "target_id": link.target_id,
            "source_type": link.source_type.value,
            "target_type": link.target_type.value,
            "confidence": link.confidence,
            "tier": link.match_tier.value,
        }))
    for u in result.unmatched:
        intent_entries.append(("planned_unmatched", {
            "entity_id": u.entity_id,
            "entity_type": u.entity_type.value,
            "target_type": u.target_type.value if u.target_type else None,
            "reason_code": u.reason_code.value,
        }))
    for a in result.ambiguous:
        intent_entries.append(("planned_ambiguous", {
            "source_entity_id": a.source_entity_id,
            "source_type": a.source_type.value if a.source_type else None,
            "target_type": a.target_type.value if a.target_type else None,
            "candidate_ids": a.candidate_ids,
            "candidate_scores": a.candidate_scores,
        }))
    storage.bulk_log_intent(run_id, intent_entries)

    # ── Step 11: Commit to storage (batched with checkpoint markers) ─────────────
    merge_handler = MergeHandler(storage)

    _commit_in_batches(
        result.links, "links",
        lambda chunk: merge_handler.batch_merge_links(chunk, run_id=run_id),
        storage, run_id, COMMIT_BATCH_SIZE, bulk=True,
    )
    _commit_in_batches(
        result.unmatched, "unmatched",
        lambda chunk: storage.bulk_insert_unmatched(chunk),
        storage, run_id, COMMIT_BATCH_SIZE, bulk=True,
    )
    _commit_in_batches(
        result.ambiguous, "ambiguous",
        lambda chunk: storage.bulk_insert_ambiguous(chunk),
        storage, run_id, COMMIT_BATCH_SIZE, bulk=True,
    )

    # ── Step 11b: Update unmatched_rate_by_source metric ─────────────────────
    total_entities = len(txns) + len(emails) + len(events)
    if total_entities > 0:
        from collections import defaultdict
        unmatched_by_pair: dict = defaultdict(int)
        for u in result.unmatched:
            src = u.entity_type.value
            tgt = u.target_type.value if u.target_type else "unknown"
            unmatched_by_pair[(src, tgt)] += 1
        for (src, tgt), count in unmatched_by_pair.items():
            unmatched_rate_by_source.labels(source_type=src, target_type=tgt).set(count / total_entities)

    # ── Step 12: Mark intent committed ───────────────────────────────────────
    storage.mark_intent_committed(run_id)

    # ── Step 13: Advance watermarks ───────────────────────────────────────────
    now = datetime.now(timezone.utc)
    for entity_type in EntityType:
        watermark_mgr.set_watermark(entity_type, now, run_id)

    # ── Step 14: Finalize run log ─────────────────────────────────────────────
    soft_failures = _txn_failures + _email_failures + _event_failures
    run_log.status = RunStatus.PARTIAL if soft_failures else RunStatus.SUCCESS
    run_log.end_time = datetime.now(timezone.utc)
    run_log.records_processed = len(txns) + len(emails) + len(events)
    run_log.links_created = len(result.links)
    run_log.unmatched_count = len(result.unmatched)
    run_log.ambiguous_count = len(result.ambiguous)
    storage.update_run_log(run_log)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ergonosis Data Unification Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m src.pipeline --type incremental
  python -m src.pipeline --type full_refresh
  STUB_INGESTION=true python -m src.pipeline --type full_refresh
        """,
    )
    parser.add_argument(
        "--type",
        choices=["incremental", "full_refresh"],
        default="incremental",
        help="Run type: 'incremental' processes only new records since last run, "
             "'full_refresh' processes all records from scratch.",
    )
    parser.add_argument(
        "--config",
        default="unification_config.yaml",
        help="Path to YAML config file (default: unification_config.yaml). "
             "Also overridable via UNIFICATION_CONFIG_PATH env var.",
    )
    args = parser.parse_args()
    _run_type = RunType.INCREMENTAL if args.type == "incremental" else RunType.FULL_REFRESH

    completed = run_pipeline(run_type=_run_type, config_path=args.config)
    print(
        f"Run complete: status={completed.status.value} | "
        f"links={completed.links_created} | "
        f"unmatched={completed.unmatched_count} | "
        f"ambiguous={completed.ambiguous_count} | "
        f"records={completed.records_processed}"
    )
