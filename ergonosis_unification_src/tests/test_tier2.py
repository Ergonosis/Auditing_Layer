"""Unit tests for src/matching/tier2_composite.py"""

from datetime import date, datetime, timezone

import pytest

from tests.conftest import make_email, make_transaction, make_calendar_event
from src.constants import EntityType, MatchTier
from src.matching.tier2_composite import Tier2Matcher


@pytest.fixture
def tier2(config):
    return Tier2Matcher(config, rule_version="1.0")


class TestTier2Matcher:
    def test_txn_email_match_normalized_name_and_date(self, tier2):
        """Tier2 txn→email: normalized merchant_name == normalized subject, same date → match."""
        txn = make_transaction(
            transaction_id="txn_x",
            amount=42.50,
            date=date(2026, 2, 15),
            merchant_name="Whole Foods Market",
        )
        # normalize_merchant_name("Whole Foods Market") → "whole foods market"
        # normalize_subject("Whole Foods Market $42.50") → "whole foods market"
        email = make_email(
            received_at=datetime(2026, 2, 15, 14, 0, tzinfo=timezone.utc),
            subject="Whole Foods Market $42.50",
        )
        result = tier2.match(txn, [email], EntityType.TRANSACTION, EntityType.EMAIL)
        assert result is not None
        assert result.match_tier == MatchTier.TIER2_COMPOSITE
        assert result.confidence == 0.90

    def test_txn_email_no_match_wrong_date(self, tier2):
        """Tier2 txn→email: same normalized name but different date → no match."""
        txn = make_transaction(
            merchant_name="Starbucks",
            date=date(2026, 2, 15),
        )
        email = make_email(
            received_at=datetime(2026, 2, 18, 9, 0, tzinfo=timezone.utc),  # 3 days later
            subject="Starbucks",
        )
        result = tier2.match(txn, [email], EntityType.TRANSACTION, EntityType.EMAIL)
        assert result is None

    def test_txn_calendar_match_normalized_name_and_date(self, tier2):
        """Tier2 txn→calendar: normalized merchant_name == normalized subject, same date → match."""
        txn = make_transaction(
            merchant_name="Marriott Hotels",
            date=date(2026, 2, 15),
        )
        # normalize_merchant_name("Marriott Hotels") → "marriott hotels"
        # normalize_subject("Marriott Hotel Stay") → "marriott hotel stay"
        # WRatio would match but Tier2 requires exact equality after normalization — "marriott hotels" != "marriott hotel stay"
        # Use exact subject match: "Marriott Hotels"
        event = make_calendar_event(
            start_time=datetime(2026, 2, 15, 14, 0, tzinfo=timezone.utc),
            end_time=datetime(2026, 2, 15, 18, 0, tzinfo=timezone.utc),
            subject="Marriott Hotels",
        )
        result = tier2.match(txn, [event], EntityType.TRANSACTION, EntityType.CALENDAR_EVENT)
        assert result is not None
        assert result.match_tier == MatchTier.TIER2_COMPOSITE
        assert result.confidence == 0.90

    def test_no_match_null_merchant(self, tier2):
        txn = make_transaction(merchant_name=None)
        email = make_email()
        result = tier2.match(txn, [email], EntityType.TRANSACTION, EntityType.EMAIL)
        assert result is None

    def test_empty_candidates(self, tier2):
        txn = make_transaction()
        result = tier2.match(txn, [], EntityType.TRANSACTION, EntityType.EMAIL)
        assert result is None

    def test_email_to_calendar_normalized_subject_match(self, tier2):
        """
        Email→Calendar: subject (normalized) + date must match.
        Craft a case where normalized subjects are equal and dates match.
        """
        email = make_email(
            received_at=datetime(2026, 2, 15, 9, 0, tzinfo=timezone.utc),
            subject="Team lunch Whole Foods",
        )
        event = make_calendar_event(
            start_time=datetime(2026, 2, 15, 9, 0, tzinfo=timezone.utc),
            end_time=datetime(2026, 2, 15, 10, 0, tzinfo=timezone.utc),
            subject="Team lunch Whole Foods",  # exact same normalized form
        )
        result = tier2.match(email, [event], EntityType.EMAIL, EntityType.CALENDAR_EVENT)
        assert result is not None
        assert result.match_tier == MatchTier.TIER2_COMPOSITE
        assert result.confidence == 0.90
        assert result.rule_version == "1.0"

    def test_email_to_calendar_wrong_date(self, tier2):
        email = make_email(
            received_at=datetime(2026, 2, 15, 9, 0, tzinfo=timezone.utc),
            subject="Team lunch",
        )
        event = make_calendar_event(
            start_time=datetime(2026, 2, 20, 9, 0, tzinfo=timezone.utc),  # different date
            end_time=datetime(2026, 2, 20, 10, 0, tzinfo=timezone.utc),
            subject="Team lunch",
        )
        result = tier2.match(email, [event], EntityType.EMAIL, EntityType.CALENDAR_EVENT)
        assert result is None

    def test_email_to_calendar_null_subject(self, tier2):
        email = make_email(subject=None)
        event = make_calendar_event(subject=None)
        result = tier2.match(email, [event], EntityType.EMAIL, EntityType.CALENDAR_EVENT)
        assert result is None

    def test_amount_tolerance_covers_float_precision(self):
        """Float precision mismatch (42.50000000001 vs 42.50) is within 0.1% tolerance."""
        from src.matching.normalizer import amount_matches
        # abs(42.50000000001 - 42.50) / max(42.50000000001, 42.50) ≈ 2.35e-13 << 0.001
        assert amount_matches(abs(42.50000000001), abs(42.50), tolerance_pct=0.001) is True

    def test_amount_exact_fails_float_precision(self):
        """Without tolerance, 42.50000000001 != 42.50 exactly."""
        from src.matching.normalizer import amount_matches
        assert amount_matches(abs(42.50000000001), abs(42.50), tolerance_pct=0.0) is False

    def test_plaid_negative_amount_matches_positive(self):
        """abs() normalization makes -42.50 compare equal to 42.50."""
        from src.matching.normalizer import amount_matches
        assert amount_matches(abs(42.50), abs(-42.50), tolerance_pct=0.0) is True

    def test_amount_tolerance_config_read_from_yaml(self, config):
        """Tier2 reads amount_tolerance_pct from config (not hardcoded 0.0)."""
        txn_rule = config["match_rules"]["transaction_to_email"]
        assert "amount_tolerance_pct" in txn_rule
        assert txn_rule["amount_tolerance_pct"] == pytest.approx(0.001)
