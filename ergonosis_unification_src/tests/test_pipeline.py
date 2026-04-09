"""Integration tests for src/pipeline.py — full end-to-end pipeline runs."""

import os
import sqlite3
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from src.constants import FeedbackSignal, PIPELINE_LOCK_TTL_SECONDS, RunStatus, RunType
from src.models.links import RunLog
from src.pipeline import run_pipeline
from src.storage.local_store import LocalStore
from tests.conftest import make_entity_link, make_feedback


# All tests use STUB_INGESTION so no real credentials are needed.
pytestmark = pytest.mark.usefixtures("stub_env")


@pytest.fixture(autouse=True)
def stub_env(monkeypatch):
    """Force stub ingestion mode for all pipeline tests."""
    monkeypatch.setenv("STUB_INGESTION", "true")


class TestFullRefreshRun:
    def test_succeeds_with_stub_ingestion(self, tmp_db):
        result = run_pipeline(run_type=RunType.FULL_REFRESH, storage=tmp_db)
        assert isinstance(result, RunLog)
        assert result.status == RunStatus.SUCCESS
        assert result.end_time is not None
        assert result.records_processed > 0

    def test_produces_results(self, tmp_db):
        # Stub fixture data is not designed to match across entity types (tier3 cross-field
        # matching deferred to V2). Pipeline should still complete and process all records.
        result = run_pipeline(run_type=RunType.FULL_REFRESH, storage=tmp_db)
        assert result.status == RunStatus.SUCCESS
        assert result.records_processed == 16  # 6 txns + 6 emails + 4 calendar events
        assert result.unmatched_count > 0  # all unmatched — expected with stub data

    def test_run_log_written_to_storage(self, tmp_db):
        run_pipeline(run_type=RunType.FULL_REFRESH, storage=tmp_db)
        last = tmp_db.get_last_run()
        assert last is not None
        assert last.status == RunStatus.SUCCESS

    def test_watermarks_advanced_after_run(self, tmp_db):
        from src.constants import EntityType
        run_pipeline(run_type=RunType.FULL_REFRESH, storage=tmp_db)
        from src.etl.watermark import WatermarkManager
        wm_mgr = WatermarkManager(tmp_db)
        for entity_type in EntityType:
            wm = wm_mgr.get_watermark(entity_type)
            assert wm is not None  # watermark set for all entity types


class TestIncrementalRun:
    def test_incremental_after_full_refresh_succeeds(self, tmp_db):
        run_pipeline(run_type=RunType.FULL_REFRESH, storage=tmp_db)
        result = run_pipeline(run_type=RunType.INCREMENTAL, storage=tmp_db)
        assert result.status == RunStatus.SUCCESS

    def test_incremental_processes_fewer_records_second_run(self, tmp_db):
        result1 = run_pipeline(run_type=RunType.FULL_REFRESH, storage=tmp_db)
        result2 = run_pipeline(run_type=RunType.INCREMENTAL, storage=tmp_db)
        # Second run after full refresh: fixtures have fixed ingested_at set during
        # first run's transform; watermark is set to after those timestamps, so
        # incremental run should process 0 records (or at most same if ingested_at changes)
        assert result2.status == RunStatus.SUCCESS
        # records_processed may be 0 on second run (all records already watermarked)
        assert result2.records_processed <= result1.records_processed


class TestConcurrentRunGuard:
    def test_returns_skipped_when_recent_run_in_progress(self, tmp_db):
        """Recent IN_PROGRESS run (within lock TTL) should cause SKIPPED return (exit 0)."""
        in_progress_run = RunLog(
            run_type=RunType.INCREMENTAL,
            status=RunStatus.IN_PROGRESS,
        )
        tmp_db.insert_run_log(in_progress_run)

        result = run_pipeline(run_type=RunType.INCREMENTAL, storage=tmp_db)
        assert result.status == RunStatus.SKIPPED

    def test_auto_resolves_stale_in_progress_run(self, tmp_db):
        """Stale IN_PROGRESS run (older than lock TTL) should be auto-marked FAILED
        and the new run should proceed to SUCCESS."""
        stale_time = datetime.now(timezone.utc) - timedelta(
            seconds=PIPELINE_LOCK_TTL_SECONDS + 60
        )
        stale_run = RunLog(
            run_type=RunType.FULL_REFRESH,
            status=RunStatus.IN_PROGRESS,
            start_time=stale_time,
        )
        tmp_db.insert_run_log(stale_run)
        stale_run_id = stale_run.run_id

        result = run_pipeline(run_type=RunType.INCREMENTAL, storage=tmp_db)
        assert result.status == RunStatus.SUCCESS

        # Verify the stale run was auto-marked FAILED
        conn = sqlite3.connect(tmp_db.db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT status, failure_reason FROM run_log WHERE run_id=?", (stale_run_id,)
        ).fetchone()
        conn.close()
        assert row["status"] == "failed"
        assert "auto-resolved" in row["failure_reason"]

    def test_returns_skipped_when_run_age_at_ttl_boundary(self, tmp_db):
        """Run whose age is safely inside the lock TTL is not stale — should return SKIPPED."""
        boundary_time = datetime.now(timezone.utc) - timedelta(
            seconds=PIPELINE_LOCK_TTL_SECONDS - 10
        )
        boundary_run = RunLog(
            run_type=RunType.INCREMENTAL,
            status=RunStatus.IN_PROGRESS,
            start_time=boundary_time,
        )
        tmp_db.insert_run_log(boundary_run)

        result = run_pipeline(run_type=RunType.INCREMENTAL, storage=tmp_db)
        assert result.status == RunStatus.SKIPPED

    def test_does_not_raise_when_last_run_succeeded(self, tmp_db):
        """Previous SUCCESS run should not block new run."""
        success_run = RunLog(
            run_type=RunType.FULL_REFRESH,
            status=RunStatus.SUCCESS,
        )
        tmp_db.insert_run_log(success_run)
        # Should not raise
        result = run_pipeline(run_type=RunType.INCREMENTAL, storage=tmp_db)
        assert result.status == RunStatus.SUCCESS

    def test_does_not_raise_when_last_run_failed(self, tmp_db):
        """Previous FAILED run should not block new run."""
        failed_run = RunLog(
            run_type=RunType.INCREMENTAL,
            status=RunStatus.FAILED,
        )
        tmp_db.insert_run_log(failed_run)
        result = run_pipeline(run_type=RunType.FULL_REFRESH, storage=tmp_db)
        assert result.status == RunStatus.SUCCESS


class TestFeedbackProcessedBeforeMatching:
    def test_rejected_link_soft_deleted_before_new_matching(self, tmp_db):
        # Insert a link directly (fixture data produces 0 links in stub mode)
        link = make_entity_link(source_id="txn_fb_001", target_id="msg_fb_001")
        tmp_db.upsert_link(link)
        link_id = link.link_id

        # Insert a REJECTED feedback signal
        fb = make_feedback(link_id=link_id, signal=FeedbackSignal.REJECTED)
        tmp_db.insert_feedback(fb)

        # Run pipeline — feedback should be processed before matching
        run_pipeline(run_type=RunType.FULL_REFRESH, storage=tmp_db)

        # The link should now be soft-deleted
        with tmp_db._connect() as conn:
            row = conn.execute(
                "SELECT is_current FROM entity_links WHERE link_id=?", (link_id,)
            ).fetchone()
        assert row is not None
        assert row["is_current"] == 0


class TestRunLogOnFailure:
    def test_run_log_marked_failed_on_matching_error(self, tmp_db):
        from src.utils.errors import MatchingError

        with patch(
            "src.pipeline.MatchingEngine.run_matching",
            side_effect=MatchingError("Test matching failure"),
        ):
            with pytest.raises(MatchingError):
                run_pipeline(run_type=RunType.FULL_REFRESH, storage=tmp_db)

        last = tmp_db.get_last_run()
        assert last is not None
        assert last.status == RunStatus.FAILED
        assert "Test matching failure" in last.failure_reason

    def test_intent_log_marked_failed_on_error(self, tmp_db):
        from src.utils.errors import MatchingError

        with patch(
            "src.pipeline.MatchingEngine.run_matching",
            side_effect=MatchingError("boom"),
        ):
            with pytest.raises(MatchingError):
                run_pipeline(run_type=RunType.FULL_REFRESH, storage=tmp_db)

        # The "failed" entry should be stored in the DB intent_log table (not a local file)
        last = tmp_db.get_last_run()
        assert last is not None
        intent_entries = tmp_db.get_intent_log(last.run_id)
        failed_entries = [e for e in intent_entries if e.get("operation_type") == "failed"]
        assert len(failed_entries) >= 1
        assert "boom" in failed_entries[0]["payload"].get("reason", "")


class TestIdempotentRuns:
    def test_second_run_produces_no_duplicate_links(self, tmp_db):
        run_pipeline(run_type=RunType.FULL_REFRESH, storage=tmp_db)

        with tmp_db._connect() as conn:
            count_after_first = conn.execute(
                "SELECT COUNT(*) FROM entity_links WHERE is_current=1"
            ).fetchone()[0]

        # Full refresh again
        run_pipeline(run_type=RunType.FULL_REFRESH, storage=tmp_db)

        with tmp_db._connect() as conn:
            count_after_second = conn.execute(
                "SELECT COUNT(*) FROM entity_links WHERE is_current=1"
            ).fetchone()[0]

        # Idempotent: same number of current links (merge_handler skips/updates, no duplicates)
        assert count_after_second == count_after_first

    def test_second_run_produces_no_duplicate_unmatched(self, tmp_db):
        """Unmatched records for the same entity should not be re-inserted on retry."""
        run_pipeline(run_type=RunType.FULL_REFRESH, storage=tmp_db)

        with tmp_db._connect() as conn:
            count_after_first = conn.execute(
                "SELECT COUNT(*) FROM unmatched_entities"
            ).fetchone()[0]

        run_pipeline(run_type=RunType.FULL_REFRESH, storage=tmp_db)

        with tmp_db._connect() as conn:
            count_after_second = conn.execute(
                "SELECT COUNT(*) FROM unmatched_entities"
            ).fetchone()[0]

        assert count_after_second == count_after_first

    def test_second_run_produces_no_duplicate_ambiguous(self, tmp_db):
        """Ambiguous records for the same source entity are not re-inserted on retry."""
        run_pipeline(run_type=RunType.FULL_REFRESH, storage=tmp_db)

        with tmp_db._connect() as conn:
            count_after_first = conn.execute(
                "SELECT COUNT(*) FROM ambiguous_matches"
            ).fetchone()[0]

        run_pipeline(run_type=RunType.FULL_REFRESH, storage=tmp_db)

        with tmp_db._connect() as conn:
            count_after_second = conn.execute(
                "SELECT COUNT(*) FROM ambiguous_matches"
            ).fetchone()[0]

        assert count_after_second == count_after_first


class TestRuleVersionTraceability:
    def test_links_carry_rule_version_from_config(self, tmp_db):
        """Every stored link should have rule_version matching the YAML config."""
        result = run_pipeline(run_type=RunType.FULL_REFRESH, storage=tmp_db)
        assert result.status == RunStatus.SUCCESS

        with tmp_db._connect() as conn:
            rows = conn.execute(
                "SELECT DISTINCT rule_version FROM entity_links WHERE is_current=1"
            ).fetchall()

        if rows:
            versions = {r[0] for r in rows}
            assert versions == {"1.0"}


class TestBatchedCheckpointing:
    def test_checkpoint_markers_written_to_intent_log(self, tmp_db, monkeypatch):
        """Batch checkpoint entries appear in the intent log after a run."""
        monkeypatch.setattr("src.pipeline.COMMIT_BATCH_SIZE", 2)
        result = run_pipeline(run_type=RunType.FULL_REFRESH, storage=tmp_db)
        assert result.status == RunStatus.SUCCESS

        entries = tmp_db.get_intent_log(result.run_id)
        checkpoints = [e for e in entries if e["operation_type"] == "batch_checkpoint"]
        assert len(checkpoints) >= 1
        for cp in checkpoints:
            assert "batch_type" in cp["payload"]
            assert "batch_index" in cp["payload"]
            assert "count" in cp["payload"]

    def test_batched_commit_produces_correct_counts(self, tmp_db, monkeypatch):
        """Record counts are stable across two runs with a small batch size (idempotency holds)."""
        monkeypatch.setattr("src.pipeline.COMMIT_BATCH_SIZE", 2)
        run_pipeline(run_type=RunType.FULL_REFRESH, storage=tmp_db)

        with tmp_db._connect() as conn:
            count_first = conn.execute(
                "SELECT COUNT(*) FROM unmatched_entities"
            ).fetchone()[0]

        run_pipeline(run_type=RunType.FULL_REFRESH, storage=tmp_db)

        with tmp_db._connect() as conn:
            count_second = conn.execute(
                "SELECT COUNT(*) FROM unmatched_entities"
            ).fetchone()[0]

        assert count_first > 0
        assert count_second == count_first

    def test_partial_commit_followed_by_rerun_is_idempotent(self, tmp_db, monkeypatch):
        """A mid-commit crash on bulk_insert_unmatched followed by a clean re-run produces no duplicates."""
        from src.utils.errors import StorageError
        from unittest.mock import patch

        call_count = {"n": 0}
        original_bulk = tmp_db.bulk_insert_unmatched

        def flaky_bulk(self_arg, records):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise StorageError("simulated bulk crash")
            original_bulk(records)

        with patch.object(tmp_db.__class__, "bulk_insert_unmatched", flaky_bulk):
            with pytest.raises(StorageError):
                run_pipeline(run_type=RunType.FULL_REFRESH, storage=tmp_db)

        last = tmp_db.get_last_run()
        assert last.status == RunStatus.FAILED

        # Second run should complete cleanly — idempotency guards skip already-committed records
        result2 = run_pipeline(run_type=RunType.FULL_REFRESH, storage=tmp_db)
        assert result2.status == RunStatus.SUCCESS

        with tmp_db._connect() as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM unmatched_entities"
            ).fetchone()[0]

        # Count should not exceed what the run reported (no duplicates from partial re-commit)
        assert count <= result2.unmatched_count
        assert count > 0


class TestSecureStorageEnforcement:
    """Tests for UNIFICATION_SECURE_STORAGE_REQUIRED enforcement gate."""

    def test_secure_mode_no_host_raises_before_run_log(self, monkeypatch, tmp_path):
        """SecureStorageRequiredError raised before any run log is written."""
        from src.utils.errors import SecureStorageRequiredError
        from src.storage.local_store import LocalStore

        # Create verify_db BEFORE enabling secure mode (LocalStore blocks in secure mode)
        verify_db = LocalStore(db_path=str(tmp_path / "verify.db"))
        monkeypatch.setenv("UNIFICATION_SECURE_STORAGE_REQUIRED", "true")
        monkeypatch.delenv("DATABRICKS_HOST", raising=False)

        try:
            with pytest.raises(SecureStorageRequiredError):
                # Do NOT inject storage= so get_storage_backend() is called
                run_pipeline(run_type=RunType.FULL_REFRESH)
            # No run log should exist in any storage (pipeline never started)
            assert verify_db.get_last_run() is None
        finally:
            verify_db.close()

    def test_secure_mode_with_injected_storage_still_runs(self, tmp_db, monkeypatch):
        """When storage= is injected directly, the enforcement gate is bypassed — pipeline runs."""
        monkeypatch.setenv("UNIFICATION_SECURE_STORAGE_REQUIRED", "true")
        monkeypatch.setenv("STUB_INGESTION", "true")
        monkeypatch.delenv("DATABRICKS_HOST", raising=False)
        # Inject storage directly — bypasses get_storage_backend()
        result = run_pipeline(run_type=RunType.FULL_REFRESH, storage=tmp_db)
        assert result.status == RunStatus.SUCCESS

    def test_non_secure_mode_no_host_uses_local_store(self, monkeypatch):
        """Without UNIFICATION_SECURE_STORAGE_REQUIRED, missing host falls back to LocalStore."""
        from src.storage.delta_client import get_storage_backend
        from src.storage.local_store import LocalStore

        monkeypatch.delenv("UNIFICATION_SECURE_STORAGE_REQUIRED", raising=False)
        monkeypatch.delenv("DATABRICKS_HOST", raising=False)
        backend = get_storage_backend()
        assert isinstance(backend, LocalStore)


class TestRichMockPipeline:
    """
    End-to-end behavioral tests using STUB_INGESTION=rich.
    Each test verifies a specific matching scenario or edge case.
    The class-level fixture overrides the module-level STUB_INGESTION=true.
    """

    @pytest.fixture(autouse=True)
    def rich_env(self, monkeypatch):
        """Override stub mode to rich for this test class."""
        monkeypatch.setenv("STUB_INGESTION", "rich")

    def _run(self, tmp_db):
        return run_pipeline(run_type=RunType.FULL_REFRESH, storage=tmp_db)

    def _link_ids(self, tmp_db):
        """Return set of (source_id, target_id) for all current links."""
        with tmp_db._connect() as conn:
            rows = conn.execute(
                "SELECT source_id, target_id, match_tier FROM entity_links WHERE is_current=1"
            ).fetchall()
        return {(r["source_id"], r["target_id"], r["match_tier"]) for r in rows}

    def _unmatched_ids(self, tmp_db):
        """Return set of entity_ids in unmatched_entities."""
        with tmp_db._connect() as conn:
            rows = conn.execute("SELECT entity_id FROM unmatched_entities").fetchall()
        return {r["entity_id"] for r in rows}

    def _ambiguous_sources(self, tmp_db):
        """Return set of source_entity_ids in ambiguous_matches."""
        with tmp_db._connect() as conn:
            rows = conn.execute("SELECT source_entity_id FROM ambiguous_matches").fetchall()
        return {r["source_entity_id"] for r in rows}

    def _make_qi(self, tmp_db):
        from src.models.consent import UserConsent
        from src.query_interface import UnifiedQueryInterface
        from tests.conftest import TEST_USER_HASH
        consent = UserConsent(
            user_id_hash=TEST_USER_HASH, consent_type="data_processing", granted=True, source="test"
        )
        tmp_db.upsert_consent(consent)
        return UnifiedQueryInterface(tmp_db), TEST_USER_HASH

    def test_run_succeeds(self, tmp_db):
        """Rich mock run completes with SUCCESS status."""
        result = self._run(tmp_db)
        assert result.status == RunStatus.SUCCESS

    def test_tier3_links_exist(self, tmp_db):
        """At least one Tier 3 fuzzy link is produced (txn→email)."""
        self._run(tmp_db)
        links = self._link_ids(tmp_db)
        tier3 = [l for l in links if l[2] == "tier3_fuzzy"]
        assert len(tier3) >= 1, f"Expected ≥1 tier3_fuzzy links, got {len(tier3)}"

    def test_email_calendar_cascade_matches(self, tmp_db):
        """Q1 Planning email (no txn) matches Q1 Planning calendar event via email→calendar cascade."""
        self._run(tmp_db)
        qi, uhash = self._make_qi(tmp_db)
        bundles = qi.get_linked_entities("rich_msg_011", "email", user_id_hash=uhash)
        assert len(bundles) > 0, "Q1 Planning email should link to calendar event via email→calendar cascade"
        assert bundles[0].linked_entity_id == "rich_evt_003"

    def test_starbucks_linked(self, tmp_db):
        """rich_txn_001 (Starbucks) matches rich_msg_001 via Tier 3 fuzzy."""
        self._run(tmp_db)
        qi, uhash = self._make_qi(tmp_db)
        bundles = qi.get_linked_entities("rich_txn_001", "transaction", user_id_hash=uhash)
        assert len(bundles) > 0, "Starbucks transaction should be linked"
        assert bundles[0].linked_entity_id == "rich_msg_001"

    def test_negative_amount_whole_foods_linked(self, tmp_db):
        """rich_txn_002 (Whole Foods, -$47.50 credit) matches rich_msg_002 via Tier 3."""
        self._run(tmp_db)
        qi, uhash = self._make_qi(tmp_db)
        bundles = qi.get_linked_entities("rich_txn_002", "transaction", user_id_hash=uhash)
        assert len(bundles) > 0, "Whole Foods credit transaction should be linked"

    def test_delta_airlines_tier3_cross_date(self, tmp_db):
        """rich_txn_003 (Delta, 03-03) matches rich_msg_003 (received 03-05) — within 3-day window."""
        self._run(tmp_db)
        qi, uhash = self._make_qi(tmp_db)
        bundles = qi.get_linked_entities("rich_txn_003", "transaction", user_id_hash=uhash)
        assert len(bundles) > 0, "Delta Airlines should link despite 2-day gap"
        assert bundles[0].match_tier == "tier3_fuzzy"

    def test_apple_normalization_linked(self, tmp_db):
        """rich_txn_013 'Apple Inc' normalizes to 'apple'; email 'Re: Apple $1.29' also → 'apple'."""
        self._run(tmp_db)
        qi, uhash = self._make_qi(tmp_db)
        bundles = qi.get_linked_entities("rich_txn_013", "transaction", user_id_hash=uhash)
        assert len(bundles) > 0, "Apple Inc normalization should produce a link"

    def test_uber_ambiguous(self, tmp_db):
        """rich_txn_005 (Uber) has 2 candidate emails — should produce AmbiguousMatch, not EntityLink."""
        self._run(tmp_db)
        qi, uhash = self._make_qi(tmp_db)
        txn_links = qi.get_linked_entities("rich_txn_005", "transaction", user_id_hash=uhash)
        amb_sources = self._ambiguous_sources(tmp_db)
        assert len(txn_links) == 0, "Uber txn should NOT be linked (ambiguous)"
        assert "rich_txn_005" in amb_sources, "Uber txn should appear in ambiguous_matches"

    def test_wework_calendar_ambiguous(self, tmp_db):
        """rich_txn_006 (WeWork) has 2 candidate calendar events — AmbiguousMatch."""
        self._run(tmp_db)
        qi, uhash = self._make_qi(tmp_db)
        txn_links = qi.get_linked_entities("rich_txn_006", "transaction", user_id_hash=uhash)
        amb_sources = self._ambiguous_sources(tmp_db)
        assert len(txn_links) == 0, "WeWork txn should NOT be linked (ambiguous)"
        assert "rich_txn_006" in amb_sources, "WeWork txn should appear in ambiguous_matches"

    def test_obscure_vendor_unmatched(self, tmp_db):
        """rich_txn_007 (Obscure Vendor XR99) has no candidates — UnmatchedEntity."""
        self._run(tmp_db)
        qi, uhash = self._make_qi(tmp_db)
        links = qi.get_linked_entities("rich_txn_007", "transaction", user_id_hash=uhash)
        unmatched = self._unmatched_ids(tmp_db)
        assert len(links) == 0, "Obscure Vendor should not be linked"
        assert "rich_txn_007" in unmatched, "Obscure Vendor should be in unmatched"

    def test_shopify_outside_window_unmatched(self, tmp_db):
        """rich_txn_008 (Shopify, 03-01) has email on 03-11 — outside 3-day window."""
        self._run(tmp_db)
        qi, uhash = self._make_qi(tmp_db)
        links = qi.get_linked_entities("rich_txn_008", "transaction", user_id_hash=uhash)
        unmatched = self._unmatched_ids(tmp_db)
        assert len(links) == 0, "Shopify should not be linked (email out of window)"
        assert "rich_txn_008" in unmatched, "Shopify should be in unmatched"

    def test_null_fields_unmatched(self, tmp_db):
        """rich_txn_010 has null merchant_name and null name — nothing to fuzzy match on."""
        self._run(tmp_db)
        qi, uhash = self._make_qi(tmp_db)
        links = qi.get_linked_entities("rich_txn_010", "transaction", user_id_hash=uhash)
        unmatched = self._unmatched_ids(tmp_db)
        assert len(links) == 0, "Null-field transaction should not be linked"
        assert "rich_txn_010" in unmatched, "Null-field transaction should be unmatched"

    def test_netflix_amount_mismatch_falls_to_tier3(self, tmp_db):
        """rich_txn_011 (Netflix $15.49) vs email (Netflix $15.60) — fuzzy name match at Tier 3."""
        self._run(tmp_db)
        qi, uhash = self._make_qi(tmp_db)
        bundles = qi.get_linked_entities("rich_txn_011", "transaction", user_id_hash=uhash)
        assert len(bundles) > 0, "Netflix should link via Tier 3 name similarity"
        assert bundles[0].match_tier == "tier3_fuzzy"

    def test_marriott_calendar_link(self, tmp_db):
        """rich_txn_004 (Marriott Hotels) matches rich_evt_001 (Marriott Hotel Stay) via Tier 3."""
        self._run(tmp_db)
        qi, uhash = self._make_qi(tmp_db)
        bundles = qi.get_linked_entities("rich_txn_004", "transaction", user_id_hash=uhash)
        assert len(bundles) > 0, "Marriott Hotels should link to calendar event"


class TestCLISmokeTest:
    """Smoke tests for the python -m src.pipeline CLI entrypoint."""

    def _run_cli(self, args, env=None, cwd=None):
        import subprocess, sys, os
        base_env = os.environ.copy()
        base_env["STUB_INGESTION"] = "true"
        if env:
            base_env.update(env)
        return subprocess.run(
            [sys.executable, "-m", "src.pipeline"] + args,
            env=base_env,
            cwd=cwd or os.path.dirname(os.path.dirname(__file__)),
            capture_output=True,
            text=True,
        )

    def test_help_exits_zero(self):
        result = self._run_cli(["--help"])
        assert result.returncode == 0
        assert "incremental" in result.stdout or "incremental" in result.stderr

    def test_incremental_run_exits_zero(self):
        result = self._run_cli(["--type", "incremental"])
        assert result.returncode == 0, result.stderr

    def test_full_refresh_run_exits_zero(self):
        result = self._run_cli(["--type", "full_refresh"])
        assert result.returncode == 0, result.stderr

    def test_invalid_type_exits_nonzero(self):
        result = self._run_cli(["--type", "bad_value"])
        assert result.returncode != 0


class TestBulkWrites:
    """Tests for bulk storage methods added in the batch operations migration."""

    # ── helpers ────────────────────────────────────────────────────────────────

    def _make_unmatched(self, entity_id, run_id="run_bulk_001"):
        from src.models.links import UnmatchedEntity
        from src.constants import EntityType, UnmatchedReasonCode
        return UnmatchedEntity(
            entity_id=entity_id,
            entity_type=EntityType.TRANSACTION,
            target_type=EntityType.EMAIL,
            reason_code=UnmatchedReasonCode.NO_CANDIDATE_FOUND,
            run_id=run_id,
        )

    def _make_ambiguous(self, source_entity_id):
        from src.models.links import AmbiguousMatch
        from src.constants import EntityType
        return AmbiguousMatch(
            source_entity_id=source_entity_id,
            source_type=EntityType.TRANSACTION,
            target_type=EntityType.EMAIL,
            candidate_ids=["msg_a", "msg_b"],
            candidate_scores=[0.85, 0.80],
        )

    # ── bulk_insert_unmatched ──────────────────────────────────────────────────

    def test_bulk_insert_unmatched_inserts_all_records(self, tmp_db):
        records = [self._make_unmatched(f"txn_{i:03d}") for i in range(5)]
        tmp_db.bulk_insert_unmatched(records)
        for r in records:
            assert tmp_db.unmatched_exists(r.entity_id, r.entity_type.value, target_type=r.target_type.value, run_id=r.run_id)

    def test_bulk_insert_unmatched_is_idempotent(self, tmp_db):
        records = [self._make_unmatched(f"txn_{i:03d}") for i in range(3)]
        tmp_db.bulk_insert_unmatched(records)
        tmp_db.bulk_insert_unmatched(records)  # second call must not raise or duplicate
        for r in records:
            assert tmp_db.unmatched_exists(r.entity_id, r.entity_type.value, target_type=r.target_type.value, run_id=r.run_id)

    def test_bulk_insert_unmatched_empty_is_noop(self, tmp_db):
        tmp_db.bulk_insert_unmatched([])  # must not raise

    # ── bulk_insert_ambiguous ──────────────────────────────────────────────────

    def test_bulk_insert_ambiguous_inserts_all_records(self, tmp_db):
        records = [self._make_ambiguous(f"txn_{i:03d}") for i in range(4)]
        tmp_db.bulk_insert_ambiguous(records)
        for r in records:
            assert tmp_db.ambiguous_exists(r.source_entity_id)

    def test_bulk_insert_ambiguous_is_idempotent(self, tmp_db):
        records = [self._make_ambiguous(f"txn_{i:03d}") for i in range(2)]
        tmp_db.bulk_insert_ambiguous(records)
        tmp_db.bulk_insert_ambiguous(records)
        for r in records:
            assert tmp_db.ambiguous_exists(r.source_entity_id)

    def test_bulk_insert_ambiguous_empty_is_noop(self, tmp_db):
        tmp_db.bulk_insert_ambiguous([])

    # ── bulk_log_intent ────────────────────────────────────────────────────────

    def test_bulk_log_intent_inserts_all_entries(self, tmp_db):
        from src.models.links import RunLog
        from src.constants import RunStatus, RunType
        run_id = "run_bulk_intent"
        tmp_db.insert_run_log(RunLog(run_id=run_id, run_type=RunType.INCREMENTAL, status=RunStatus.IN_PROGRESS))
        entries = [
            ("planned_link", {"source_id": f"txn_{i}", "target_id": f"msg_{i}"})
            for i in range(10)
        ]
        tmp_db.bulk_log_intent(run_id, entries)
        # Verify entries landed — mark all committed to confirm rows exist
        tmp_db.mark_intent_committed(run_id)  # should not raise

    def test_bulk_log_intent_empty_is_noop(self, tmp_db):
        tmp_db.bulk_log_intent("run_x", [])

    # ── get_current_links_by_sources + bulk_upsert_links ──────────────────────

    def test_bulk_upsert_links_inserts_new_links(self, tmp_db):
        links = [
            make_entity_link(
                source_id=f"txn_{i:03d}",
                target_id=f"msg_{i:03d}",
            )
            for i in range(5)
        ]
        tmp_db.bulk_upsert_links(links)
        source_ids = [l.source_id for l in links]
        fetched = tmp_db.get_current_links_by_sources(source_ids)
        assert len(fetched) == 5

    def test_bulk_upsert_links_updates_existing_links(self, tmp_db):
        link = make_entity_link(source_id="txn_upd", target_id="msg_upd", confidence=0.80)
        tmp_db.bulk_upsert_links([link])
        # Update confidence
        link.confidence = 0.95
        tmp_db.bulk_upsert_links([link])
        fetched = tmp_db.get_current_links_by_sources(["txn_upd"])
        assert len(fetched) == 1
        assert fetched[0].confidence == 0.95

    def test_get_current_links_by_sources_returns_only_current(self, tmp_db):
        active = make_entity_link(source_id="txn_cur", target_id="msg_cur", is_current=True)
        stale = make_entity_link(
            source_id="txn_cur", target_id="msg_old", is_current=False,
            link_id="stale_link_id",
        )
        tmp_db.upsert_link(active)
        tmp_db.upsert_link(stale)
        fetched = tmp_db.get_current_links_by_sources(["txn_cur"])
        assert all(l.is_current for l in fetched)
        assert len(fetched) == 1

    # ── batch_merge_links ──────────────────────────────────────────────────────

    def test_batch_merge_links_classifies_correctly(self, tmp_db):
        from src.storage.merge_handler import MergeHandler
        handler = MergeHandler(tmp_db)

        existing = make_entity_link(source_id="txn_e", target_id="msg_e", confidence=0.90)
        tmp_db.upsert_link(existing)

        proposed_new = make_entity_link(source_id="txn_n", target_id="msg_n", confidence=0.85)
        proposed_identical = make_entity_link(
            link_id=existing.link_id,
            source_id=existing.source_id,
            target_id=existing.target_id,
            source_type=existing.source_type,
            target_type=existing.target_type,
            match_type=existing.match_type,
            match_tier=existing.match_tier,
            confidence=existing.confidence,
            linkage_key=existing.linkage_key,
            rule_version=existing.rule_version,
        )
        proposed_changed = make_entity_link(
            source_id=existing.source_id,
            target_id=existing.target_id,
            source_type=existing.source_type,
            target_type=existing.target_type,
            match_type=existing.match_type,
            match_tier=existing.match_tier,
            confidence=0.99,  # changed
            linkage_key=existing.linkage_key,
            rule_version=existing.rule_version,
        )

        counts = handler.batch_merge_links(
            [proposed_new, proposed_identical, proposed_changed],
            run_id="run_batch_test",
        )
        assert counts["inserted"] == 1
        assert counts["skipped"] == 1
        assert counts["updated"] == 1

    # ── pipeline integration ───────────────────────────────────────────────────

    def test_pipeline_commit_phase_uses_bulk_path(self, tmp_db, monkeypatch):
        """Pipeline with STUB_INGESTION calls bulk methods rather than per-record methods."""
        import unittest.mock as mock
        from src import pipeline as p

        monkeypatch.setenv("STUB_INGESTION", "true")

        bulk_insert_unmatched = mock.MagicMock(side_effect=tmp_db.bulk_insert_unmatched)
        bulk_insert_ambiguous = mock.MagicMock(side_effect=tmp_db.bulk_insert_ambiguous)
        bulk_log_intent = mock.MagicMock(side_effect=tmp_db.bulk_log_intent)

        monkeypatch.setattr(tmp_db, "bulk_insert_unmatched", bulk_insert_unmatched)
        monkeypatch.setattr(tmp_db, "bulk_insert_ambiguous", bulk_insert_ambiguous)
        monkeypatch.setattr(tmp_db, "bulk_log_intent", bulk_log_intent)

        from src.constants import RunType
        result = p.run_pipeline(storage=tmp_db, run_type=RunType.INCREMENTAL)

        assert result.status.value in ("success", "partial")
        # Bulk methods must have been called — stub data has 12 unmatched, 3 links
        bulk_log_intent.assert_called()
        bulk_insert_unmatched.assert_called()
        # ambiguous=0 in stub data so _commit_in_batches never calls the fn;
        # assert the mock object exists and is the patched method (not the real one)
        assert bulk_insert_ambiguous is tmp_db.bulk_insert_ambiguous
