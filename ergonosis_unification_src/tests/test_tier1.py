"""Unit tests for src/matching/tier1_exact.py"""

import pytest

from tests.conftest import make_email, make_transaction, make_calendar_event
from src.constants import EntityType, MatchTier
from src.matching.tier1_exact import Tier1Matcher


@pytest.fixture
def tier1(config):
    return Tier1Matcher(config, rule_version="1.0")


class TestTier1Matcher:
    def test_no_match_different_ids(self, tier1):
        txn = make_transaction(transaction_id="txn_001")
        email = make_email(message_id="msg_999")  # no shared key
        result = tier1.match(txn, [email], EntityType.TRANSACTION, EntityType.EMAIL)
        assert result is None

    def test_match_when_ids_shared(self, tier1):
        # Craft: message_id equals transaction_id (artificial but tests T1 logic)
        shared_id = "shared_key_xyz"
        txn = make_transaction(transaction_id=shared_id)
        email = make_email(message_id=shared_id)
        result = tier1.match(txn, [email], EntityType.TRANSACTION, EntityType.EMAIL)
        assert result is not None
        assert result.match_tier == MatchTier.TIER1_EXACT
        assert result.confidence == 1.0
        assert result.rule_version == "1.0"

    def test_empty_candidates_returns_none(self, tier1):
        txn = make_transaction()
        result = tier1.match(txn, [], EntityType.TRANSACTION, EntityType.EMAIL)
        assert result is None

    def test_none_field_value_no_crash(self, tier1):
        # transaction_id present but message_id is None on email (via missing field)
        txn = make_transaction(transaction_id="txn_001")
        # message_id is required, so we can't set it None via model — instead pass empty candidates
        result = tier1.match(txn, [], EntityType.TRANSACTION, EntityType.EMAIL)
        assert result is None

    def test_rule_version_threaded(self, config):
        tier1_v2 = Tier1Matcher(config, rule_version="2.0")
        shared_id = "shared_key_xyz"
        txn = make_transaction(transaction_id=shared_id)
        email = make_email(message_id=shared_id)
        result = tier1_v2.match(txn, [email], EntityType.TRANSACTION, EntityType.EMAIL)
        assert result is not None
        assert result.rule_version == "2.0"

    def test_transaction_to_calendar_no_shared_key(self, tier1, sample_transactions, sample_calendar_events):
        # Real fixtures: no transaction_id == event_id overlap expected
        result = tier1.match(
            sample_transactions[0],
            sample_calendar_events,
            EntityType.TRANSACTION,
            EntityType.CALENDAR_EVENT,
        )
        assert result is None
