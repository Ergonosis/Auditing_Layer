"""Transformer — converts raw ingestion dicts into validated Pydantic models."""

import hashlib
import os
from datetime import datetime
from typing import Any, Dict, List, Tuple, Union

from src.etl.validator import OPTIONAL_FIELDS, validate_required_fields
from src.models.calendar_event import CalendarEvent
from src.models.email import Email
from src.models.transaction import Transaction
from src.utils.errors import SchemaValidationError, SoftValidationError
from src.utils.logging import get_logger
from src.utils.metrics import entities_ingested, schema_hard_failures, schema_soft_failures
from src.utils.pii_masker import mask_pii

logger = get_logger(__name__)


class Transformer:
    """
    Transforms raw dicts from ingestion adapters into validated Pydantic models.

    Hard fail (SchemaValidationError): required field missing or wrong type.
      → Increments schema_hard_failures metric.
      → Caller must halt run and alert.

    Soft fail: optional field invalid or missing.
      → Logs WARNING with field name and entity_id.
      → Sets field to None.
      → Increments schema_soft_failures metric.
      → Continues processing.

    ETL-added fields (source, ingested_at, raw_file_ref) are always set here.
    raw_file_ref is always None in in-memory/stub mode. It is only populated by
    Databricks Auto Loader in production, where it contains the cloud object storage
    path (S3/ADLS) — never a local file path.
    """

    def transform_transaction(self, raw: Dict[str, Any], raw_file_ref: str = None) -> Transaction:
        """
        1. validate_required_fields() — hard fail if any missing
        2. Map each optional field, catching individual parse errors as soft fails
        3. Add ETL fields
        4. Construct and return Transaction(**mapped)
        5. Increment entities_ingested counter
        """
        entity_type = "transaction"
        entity_id = raw.get("transaction_id", "<unknown>")

        try:
            validate_required_fields(raw, entity_type)
        except SchemaValidationError:
            schema_hard_failures.labels(entity_type=entity_type).inc()
            raise

        mapped: Dict[str, Any] = {
            "transaction_id": raw["transaction_id"],
            "account_id": raw["account_id"],
            "amount": raw["amount"],
            "date": raw["date"],
            "raw_file_ref": raw_file_ref,
        }

        for field in OPTIONAL_FIELDS[entity_type]:
            try:
                value = raw.get(field)
                if value is not None:
                    mapped[field] = value
            except Exception as exc:
                logger.warning(
                    f"Soft fail on optional field",
                    entity_type=entity_type,
                    entity_id=entity_id,
                    field=field,
                    error=str(exc),
                )
                schema_soft_failures.labels(entity_type=entity_type).inc()
                mapped[field] = None

        try:
            record = Transaction(**mapped)
        except Exception as exc:
            schema_hard_failures.labels(entity_type=entity_type).inc()
            raise SchemaValidationError(
                f"[{entity_type}] Pydantic validation failed for {entity_id}: {exc}"
            ) from exc

        entities_ingested.labels(entity_type=entity_type, source="plaid").inc()
        return record

    def transform_email(self, raw: Dict[str, Any], raw_file_ref: str = None) -> Email:
        """Same pattern as transform_transaction."""
        entity_type = "email"
        entity_id = raw.get("message_id", "<unknown>")

        try:
            validate_required_fields(raw, entity_type)
        except SchemaValidationError:
            schema_hard_failures.labels(entity_type=entity_type).inc()
            raise

        mapped: Dict[str, Any] = {
            "message_id": raw["message_id"],
            "received_at": raw["received_at"],
            "sender": hashlib.sha256(raw["sender"].encode()).hexdigest() if raw["sender"] else raw["sender"],
            "recipients": raw["recipients"],
            "raw_file_ref": raw_file_ref,
        }

        for field in OPTIONAL_FIELDS[entity_type]:
            try:
                value = raw.get(field)
                if value is not None:
                    mapped[field] = value
            except Exception as exc:
                logger.warning(
                    f"Soft fail on optional field",
                    entity_type=entity_type,
                    entity_id=entity_id,
                    field=field,
                    error=str(exc),
                )
                schema_soft_failures.labels(entity_type=entity_type).inc()
                mapped[field] = None

        if mapped.get("subject") is not None:
            mapped["subject"] = mask_pii(mapped["subject"])
        if mapped.get("body_preview") is not None:
            mapped["body_preview"] = mask_pii(mapped["body_preview"])
        if os.getenv("COLLECT_BODY_PREVIEW", "true").lower() != "true":
            mapped["body_preview"] = None

        try:
            record = Email(**mapped)
        except Exception as exc:
            schema_hard_failures.labels(entity_type=entity_type).inc()
            raise SchemaValidationError(
                f"[{entity_type}] Pydantic validation failed for {entity_id}: {exc}"
            ) from exc

        entities_ingested.labels(entity_type=entity_type, source="microsoft_graph").inc()
        return record

    def transform_calendar_event(self, raw: Dict[str, Any], raw_file_ref: str = None) -> CalendarEvent:
        """Same pattern."""
        entity_type = "calendar_event"
        entity_id = raw.get("event_id", "<unknown>")

        try:
            validate_required_fields(raw, entity_type)
        except SchemaValidationError:
            schema_hard_failures.labels(entity_type=entity_type).inc()
            raise

        mapped: Dict[str, Any] = {
            "event_id": raw["event_id"],
            "start_time": raw["start_time"],
            "end_time": raw["end_time"],
            "organizer": raw["organizer"],
            "raw_file_ref": raw_file_ref,
        }

        for field in OPTIONAL_FIELDS[entity_type]:
            try:
                value = raw.get(field)
                if value is not None:
                    mapped[field] = value
            except Exception as exc:
                logger.warning(
                    f"Soft fail on optional field",
                    entity_type=entity_type,
                    entity_id=entity_id,
                    field=field,
                    error=str(exc),
                )
                schema_soft_failures.labels(entity_type=entity_type).inc()
                mapped[field] = None

        if mapped.get("subject") is not None:
            mapped["subject"] = mask_pii(mapped["subject"])
        if os.getenv("COLLECT_CALENDAR_ATTENDEES", "true").lower() != "true":
            mapped["attendees"] = None

        try:
            record = CalendarEvent(**mapped)
        except Exception as exc:
            schema_hard_failures.labels(entity_type=entity_type).inc()
            raise SchemaValidationError(
                f"[{entity_type}] Pydantic validation failed for {entity_id}: {exc}"
            ) from exc

        entities_ingested.labels(entity_type=entity_type, source="microsoft_graph").inc()
        return record

    def transform_batch(
        self,
        records: List[Dict[str, Any]],
        entity_type: str,
        raw_file_ref: str = None,
    ) -> Tuple[List[Union[Transaction, Email, CalendarEvent]], List[Dict]]:
        """
        Transform a list of raw records.
        Returns: (successful_records, failed_records)
        failed_records includes {'raw': raw_dict, 'error': str, 'entity_type': str}

        Hard fail on any record raises SchemaValidationError immediately — caller halts run.
        Soft fails are collected and returned in failed_records (run continues).
        """
        _transform_fn = {
            "transaction": self.transform_transaction,
            "email": self.transform_email,
            "calendar_event": self.transform_calendar_event,
        }

        if entity_type not in _transform_fn:
            raise ValueError(f"Unknown entity_type: {entity_type!r}")

        fn = _transform_fn[entity_type]
        successful: List[Union[Transaction, Email, CalendarEvent]] = []
        failed: List[Dict] = []
        hard_failures: List[str] = []

        for raw in records:
            try:
                successful.append(fn(raw, raw_file_ref=raw_file_ref))
            except SchemaValidationError as exc:
                hard_failures.append(str(exc))
            except SoftValidationError as exc:
                failed.append({"raw": raw, "error": str(exc), "entity_type": entity_type})

        if hard_failures:
            raise SchemaValidationError(
                f"{len(hard_failures)} record(s) failed validation: " +
                "; ".join(hard_failures[:5])
            )

        return successful, failed
