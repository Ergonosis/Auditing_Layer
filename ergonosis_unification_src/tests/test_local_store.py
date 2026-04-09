"""Direct unit tests for LocalStore — intent log API and core CRUD methods."""

import time

import pytest

from src.storage.local_store import LocalStore
from src.utils.errors import StorageError


class TestIntentLog:
    def test_log_intent_then_get_returns_entry(self, tmp_db):
        run_id = "run_il_001"
        tmp_db.log_intent(run_id, "planned_link", {"source": "txn1", "target": "msg1"})
        entries = tmp_db.get_intent_log(run_id)
        assert len(entries) == 1
        assert entries[0]["operation_type"] == "planned_link"
        assert entries[0]["payload"]["source"] == "txn1"
        assert entries[0]["committed"] is False

    def test_multiple_entries_ordered(self, tmp_db):
        run_id = "run_il_002"
        tmp_db.log_intent(run_id, "step_a", {"i": 1})
        tmp_db.log_intent(run_id, "step_b", {"i": 2})
        tmp_db.log_intent(run_id, "step_c", {"i": 3})
        entries = tmp_db.get_intent_log(run_id)
        assert [e["operation_type"] for e in entries] == ["step_a", "step_b", "step_c"]

    def test_get_intent_log_isolated_by_run_id(self, tmp_db):
        tmp_db.log_intent("run_A", "op", {"x": 1})
        tmp_db.log_intent("run_B", "op", {"x": 2})
        assert len(tmp_db.get_intent_log("run_A")) == 1
        assert len(tmp_db.get_intent_log("run_B")) == 1

    def test_mark_intent_committed(self, tmp_db):
        run_id = "run_il_003"
        tmp_db.log_intent(run_id, "batch_checkpoint", {"batch_index": 0})
        tmp_db.mark_intent_committed(run_id)
        entries = tmp_db.get_intent_log(run_id)
        assert entries[0]["committed"] is True

    def test_mark_committed_only_affects_target_run(self, tmp_db):
        tmp_db.log_intent("run_X", "op", {})
        tmp_db.log_intent("run_Y", "op", {})
        tmp_db.mark_intent_committed("run_X")
        assert tmp_db.get_intent_log("run_X")[0]["committed"] is True
        assert tmp_db.get_intent_log("run_Y")[0]["committed"] is False

    def test_get_intent_log_empty_run_returns_empty_list(self, tmp_db):
        assert tmp_db.get_intent_log("nonexistent_run") == []


class TestPurgeOldIntentLogs:
    def test_purge_ttl_zero_deletes_all_entries(self, tmp_db):
        tmp_db.log_intent("run_purge", "op", {})
        time.sleep(0.01)  # ensure planned_at is strictly in the past
        deleted = tmp_db.purge_old_intent_logs(ttl_seconds=0)
        assert deleted >= 1
        assert tmp_db.get_intent_log("run_purge") == []

    def test_purge_large_ttl_keeps_entries(self, tmp_db):
        tmp_db.log_intent("run_keep", "op", {})
        deleted = tmp_db.purge_old_intent_logs(ttl_seconds=86400)
        assert deleted == 0
        assert len(tmp_db.get_intent_log("run_keep")) == 1

    def test_purge_returns_count_deleted(self, tmp_db):
        for i in range(3):
            tmp_db.log_intent(f"run_p{i}", "op", {})
        time.sleep(0.01)
        deleted = tmp_db.purge_old_intent_logs(ttl_seconds=0)
        assert deleted == 3


class TestLocalStoreCRUD:
    def test_health_check_returns_true(self, tmp_db):
        assert tmp_db.health_check() is True

    def test_get_last_run_empty_returns_none(self, tmp_db):
        assert tmp_db.get_last_run() is None

    def test_unmatched_exists_false_on_empty(self, tmp_db):
        assert tmp_db.unmatched_exists("entity_1", "transaction") is False

    def test_ambiguous_exists_false_on_empty(self, tmp_db):
        assert tmp_db.ambiguous_exists("source_1") is False
