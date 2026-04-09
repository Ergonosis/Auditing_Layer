"""Unit tests for src/matching/normalizer.py"""

from datetime import date, datetime, timezone

import pytest

from src.matching.normalizer import (
    amount_matches,
    date_within_window,
    normalize_email_address,
    normalize_merchant_name,
    normalize_subject,
)


class TestNormalizeMerchantName:
    def test_llc_suffix_removed(self):
        assert normalize_merchant_name("Whole Foods Market, LLC") == "whole foods market"

    def test_inc_suffix_removed(self):
        assert normalize_merchant_name("Amazon, Inc") == "amazon"

    def test_corp_suffix_removed(self):
        assert normalize_merchant_name("BigCorp Corp") == "bigcorp"

    def test_hash_number_kept(self):
        # WHOLEFDS is expanded to "Whole Foods"; hash and store number are stripped
        result = normalize_merchant_name("WHOLEFDS #123")
        assert "whole foods" in result

    def test_noise_words_removed(self):
        result = normalize_merchant_name("The Amazon Store")
        assert "the" not in result.split()
        assert "amazon" in result

    def test_ampersand_noise(self):
        result = normalize_merchant_name("Smith & Jones")
        assert "&" not in result
        assert "smith" in result
        assert "jones" in result

    def test_all_lowercase(self):
        result = normalize_merchant_name("STARBUCKS")
        assert result == result.lower()

    def test_multiple_spaces_collapsed(self):
        result = normalize_merchant_name("A  B   C")
        assert "  " not in result

    def test_empty_string(self):
        result = normalize_merchant_name("")
        assert result == ""

    def test_punctuation_removed(self):
        result = normalize_merchant_name("Amazon.com*12345")
        assert "." not in result
        assert "*" not in result


class TestNormalizeSubject:
    def test_re_prefix_stripped(self):
        result = normalize_subject("Re: Your receipt")
        assert result.startswith("your receipt")

    def test_fwd_prefix_stripped(self):
        result = normalize_subject("Fwd: Invoice details")
        assert "fwd" not in result

    def test_fw_prefix_stripped(self):
        result = normalize_subject("FW: Follow up")
        assert result.startswith("follow up")

    def test_currency_amount_stripped(self):
        result = normalize_subject("Whole Foods receipt - $42.50")
        assert "$" not in result
        assert "42.50" not in result

    def test_lowercase(self):
        result = normalize_subject("STARBUCKS RECEIPT")
        assert result == result.lower()

    def test_punctuation_removed(self):
        result = normalize_subject("Hello, World!")
        assert "," not in result
        assert "!" not in result

    def test_whitespace_collapsed(self):
        result = normalize_subject("Hello   World")
        assert "  " not in result


class TestNormalizeEmailAddress:
    def test_lowercases(self):
        assert normalize_email_address("TEST@EXAMPLE.COM") == "test@example.com"

    def test_strips_whitespace(self):
        assert normalize_email_address("  user@example.com  ") == "user@example.com"

    def test_no_at_returns_none(self):
        assert normalize_email_address("notanemail") is None

    def test_valid_email(self):
        assert normalize_email_address("user@ergonosis.com") == "user@ergonosis.com"


class TestDateWithinWindow:
    def test_same_date_true(self):
        d = date(2026, 2, 15)
        assert date_within_window(d, d, 3) is True

    def test_exactly_at_boundary_true(self):
        d1 = date(2026, 2, 15)
        d2 = date(2026, 2, 18)  # 3 days later
        assert date_within_window(d1, d2, 3) is True

    def test_one_day_outside_false(self):
        d1 = date(2026, 2, 15)
        d2 = date(2026, 2, 19)  # 4 days later, window=3
        assert date_within_window(d1, d2, 3) is False

    def test_negative_difference_handled(self):
        d1 = date(2026, 2, 18)
        d2 = date(2026, 2, 15)
        assert date_within_window(d1, d2, 3) is True  # abs() handles direction

    def test_datetime_vs_date(self):
        dt = datetime(2026, 2, 15, 14, 30, tzinfo=timezone.utc)
        d = date(2026, 2, 15)
        assert date_within_window(dt, d, 0) is True

    def test_iso_string_input(self):
        assert date_within_window("2026-02-15", "2026-02-16", 3) is True

    def test_mixed_types(self):
        dt = datetime(2026, 2, 15, 9, 0)
        iso = "2026-02-17"
        assert date_within_window(dt, iso, 3) is True


class TestAmountMatches:
    def test_exact_equal_true(self):
        assert amount_matches(42.50, 42.50) is True

    def test_exact_not_equal_false(self):
        assert amount_matches(42.50, 42.51) is False

    def test_zero_tolerance_exact(self):
        assert amount_matches(10.0, 10.0, tolerance_pct=0.0) is True

    def test_within_tolerance_true(self):
        # 1% tolerance: 100 vs 100.5 is within 1%
        assert amount_matches(100.0, 100.5, tolerance_pct=0.01) is True

    def test_outside_tolerance_false(self):
        # 1% tolerance: 100 vs 102 is outside
        assert amount_matches(100.0, 102.0, tolerance_pct=0.01) is False

    def test_both_zero_with_tolerance(self):
        assert amount_matches(0.0, 0.0, tolerance_pct=0.05) is True

    def test_negative_amounts(self):
        assert amount_matches(-50.0, -50.0) is True
