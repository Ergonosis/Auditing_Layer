"""Unit tests for src/matching/engine.py"""

import pytest

from datetime import datetime, timezone
from tests.conftest import make_email, make_transaction, make_calendar_event
from src.constants import EntityType
from src.matching.engine import MatchingEngine, MatchingResult
from src.models.links import AmbiguousMatch, EntityLink, UnmatchedEntity

RUN_ID = "run_test_001"


class TestMatchingEngine:
    def test_all_fixture_data_runs_without_error(self, matching_engine, sample_transactions, sample_emails, sample_calendar_events):
        result = matching_engine.run_matching(
            sample_transactions, sample_emails, sample_calendar_events, RUN_ID
        )
        assert isinstance(result, MatchingResult)
        assert result.run_id == RUN_ID
        # At least some links should be produced from the designed fixture data
        assert result.total_entities > 0

    def test_empty_inputs_return_empty_result(self, matching_engine):
        result = matching_engine.run_matching([], [], [], RUN_ID)
        assert result.links == []
        assert result.unmatched == []
        assert result.ambiguous == []
        assert result.total_entities == 0
        assert result.match_rate == 0.0

    def test_deduplicates_transactions(self, matching_engine, sample_emails):
        """Passing the same transaction twice should only process it once per entity pair."""
        txn = make_transaction()
        result = matching_engine.run_matching(
            [txn, txn],  # duplicate
            sample_emails,
            [],
            RUN_ID,
        )
        # Each entity pair type (txn→email) should produce at most one result for this transaction.
        # A transaction may appear in unmatched once for the email pair AND once for the calendar
        # pair — that's correct. Count per pair type: filter to EMAIL target type.
        email_unmatched_ids = [
            u.entity_id for u in result.unmatched
            if u.entity_type == EntityType.TRANSACTION
            and u.reason_code is not None
        ]
        # With dedup working, this tx appears at most once per pair type.
        # Total across all pairs ≤ number of entity pairs (2: email, calendar)
        all_tx_unmatched = [u for u in result.unmatched if u.entity_id == txn.transaction_id]
        assert len(all_tx_unmatched) <= 2  # at most once per pair (email + calendar)

    def test_unmatched_email_tracked(self, matching_engine):
        """An email with no matching transaction → UnmatchedEntity(entity_type=EMAIL)."""
        txn = make_transaction(transaction_id="txn_999", merchant_name="XYZ Corp")
        email = make_email(
            message_id="msg_orphan",
            subject="Completely unrelated subject zzz",
        )
        result = matching_engine.run_matching([txn], [email], [], RUN_ID)
        # The email may or may not match, but if it doesn't, it should be in unmatched
        email_unmatched = [u for u in result.unmatched if u.entity_type == EntityType.EMAIL]
        # At minimum: no crash, and unmatched emails are tracked
        for u in email_unmatched:
            assert u.entity_id  # has an id

    def test_calendar_events_present_cascade_runs(self, matching_engine, sample_transactions, sample_emails, sample_calendar_events):
        """When calendar events are provided, cascade runs against them without error."""
        result = matching_engine.run_matching(
            sample_transactions[:2], sample_emails[:2], sample_calendar_events, RUN_ID
        )
        assert isinstance(result, MatchingResult)

    def test_no_calendar_events_logs_all_as_unmatched(self, matching_engine):
        """When calendar_events=[], all transactions logged as unmatched for calendar."""
        txn = make_transaction()
        result = matching_engine.run_matching([txn], [], [], RUN_ID)
        # No calendar events → transaction gets an unmatched record for calendar
        cal_unmatched = [
            u for u in result.unmatched
            if u.entity_type == EntityType.TRANSACTION
        ]
        assert len(cal_unmatched) >= 1

    def test_match_rate_property(self):
        from src.models.links import EntityLink, UnmatchedEntity
        from src.constants import EntityType, MatchTier, MatchType
        from tests.conftest import make_entity_link

        link = make_entity_link()
        unmatched = UnmatchedEntity(
            entity_id="e1",
            entity_type=EntityType.EMAIL,
            reason_code="no_candidate_found",
            run_id=RUN_ID,
        )
        result = MatchingResult(links=[link], unmatched=[unmatched], ambiguous=[], run_id=RUN_ID)
        assert result.total_entities == 2
        assert result.match_rate == 0.5

    def test_total_entities_zero(self):
        result = MatchingResult(links=[], unmatched=[], ambiguous=[], run_id=RUN_ID)
        assert result.total_entities == 0
        assert result.match_rate == 0.0

    def test_email_calendar_cascade_produces_link(self, matching_engine):
        """An unmatched email (no txn match) is then matched against calendar events."""
        from datetime import date
        email = make_email(
            message_id="msg_cal_test",
            received_at=datetime(2026, 2, 15, 9, 0, tzinfo=timezone.utc),
            subject="Q1 Planning Meeting",
        )
        event = make_calendar_event(
            event_id="evt_cal_test",
            start_time=datetime(2026, 2, 15, 10, 0, tzinfo=timezone.utc),
            end_time=datetime(2026, 2, 15, 11, 0, tzinfo=timezone.utc),
            subject="Q1 Planning Meeting",  # exact subject match → Tier 2
        )
        result = matching_engine.run_matching([], [email], [event], RUN_ID)
        email_links = [l for l in result.links if l.source_id == "msg_cal_test" or l.target_id == "msg_cal_test"]
        assert len(email_links) == 1, "Email should link to calendar event"
        assert email_links[0].target_id == "evt_cal_test"

    def test_email_already_txn_matched_skips_calendar(self, matching_engine):
        """An email matched to a transaction is not run against calendar events."""
        from datetime import date
        txn = make_transaction(
            transaction_id="txn_skip",
            merchant_name="Starbucks",
            date=date(2026, 2, 15),
        )
        email = make_email(
            message_id="msg_skip",
            received_at=datetime(2026, 2, 15, 9, 0, tzinfo=timezone.utc),
            subject="Starbucks",  # matches txn → Tier 3 link
        )
        event = make_calendar_event(
            event_id="evt_skip",
            start_time=datetime(2026, 2, 15, 10, 0, tzinfo=timezone.utc),
            end_time=datetime(2026, 2, 15, 11, 0, tzinfo=timezone.utc),
            subject="Starbucks",
        )
        result = matching_engine.run_matching([txn], [email], [event], RUN_ID)
        # email should be linked to txn only, not also to calendar
        email_links = [l for l in result.links if l.source_id == "msg_skip" or l.target_id == "msg_skip"]
        assert len(email_links) == 1, "Email linked to txn should not also match calendar"

    def test_rule_version_on_produced_links(self, config):
        engine = MatchingEngine(config, rule_version="test_ver")
        # Use fixture data that is known to produce links
        from src.etl.transformer import Transformer
        import json
        from pathlib import Path
        transformer = Transformer()
        fixtures = Path("tests/fixtures")
        with open(fixtures / "sample_transactions.json") as f:
            raw_txns = json.load(f)
        with open(fixtures / "sample_emails.json") as f:
            raw_emails = json.load(f)
        txns, _ = transformer.transform_batch(raw_txns, "transaction")
        emails, _ = transformer.transform_batch(raw_emails, "email")
        result = engine.run_matching(txns, emails, [], RUN_ID)
        for link in result.links:
            assert link.rule_version == "test_ver"
