"""Shared pytest fixtures for the Ergonosis Data Unification Layer test suite."""

import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from src.matching.engine import MatchingEngine
from src.models.calendar_event import CalendarEvent
from src.models.email import Email
from src.models.links import EntityLink, LinkFeedback, UnmatchedEntity
from src.models.transaction import Transaction
from src.query_interface import UnifiedQueryInterface
from src.storage.local_store import LocalStore
from src.storage.merge_handler import MergeHandler
from src.utils.config_loader import load_config
from src.constants import EntityType, FeedbackSignal, FeedbackSource, MatchTier, MatchType

_FIXTURES = Path(__file__).parent / "fixtures"


# ── Storage ────────────────────────────────────────────────────────────────────

@pytest.fixture
def tmp_db(tmp_path):
    """Fresh in-memory SQLite LocalStore per test. Never touches the real DB."""
    db = LocalStore(db_path=str(tmp_path / "test.db"))
    yield db
    db.close()


# ── Config ─────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def config():
    """Loaded unification_config.yaml. Session-scoped — config doesn't change per test."""
    return load_config("unification_config.yaml")


# ── Raw fixture data (dicts) ───────────────────────────────────────────────────

@pytest.fixture(scope="session")
def raw_transactions():
    with open(_FIXTURES / "sample_transactions.json") as f:
        return json.load(f)


@pytest.fixture(scope="session")
def raw_emails():
    with open(_FIXTURES / "sample_emails.json") as f:
        return json.load(f)


@pytest.fixture(scope="session")
def raw_calendar_events():
    with open(_FIXTURES / "sample_calendar_events.json") as f:
        return json.load(f)


# ── Pydantic model fixtures ────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def sample_transactions(raw_transactions):
    """List of Transaction Pydantic models from fixture data."""
    return [Transaction(**r) for r in raw_transactions]


@pytest.fixture(scope="session")
def sample_emails(raw_emails):
    """List of Email Pydantic models from fixture data."""
    return [Email(**r) for r in raw_emails]


@pytest.fixture(scope="session")
def sample_calendar_events(raw_calendar_events):
    """List of CalendarEvent Pydantic models from fixture data."""
    return [CalendarEvent(**r) for r in raw_calendar_events]


# ── Composed objects ───────────────────────────────────────────────────────────

@pytest.fixture
def matching_engine(config):
    return MatchingEngine(config, rule_version="1.0")


@pytest.fixture
def merge_handler(tmp_db):
    return MergeHandler(tmp_db)


TEST_USER_HASH = "testhash_abc123"


@pytest.fixture
def query_interface(tmp_db):
    from src.models.consent import UserConsent
    consent = UserConsent(
        user_id_hash=TEST_USER_HASH,
        consent_type="data_processing",
        granted=True,
        source="test",
    )
    tmp_db.upsert_consent(consent)
    return UnifiedQueryInterface(tmp_db)


# ── Convenience builders ───────────────────────────────────────────────────────

def make_transaction(**overrides) -> Transaction:
    """Build a minimal valid Transaction, overriding any fields."""
    defaults = dict(
        transaction_id="txn_test_001",
        account_id="acc_test",
        amount=42.50,
        date=date(2026, 2, 15),
    )
    defaults.update(overrides)
    return Transaction(**defaults)


def make_email(**overrides) -> Email:
    """Build a minimal valid Email, overriding any fields."""
    defaults = dict(
        message_id="msg_test_001",
        received_at=datetime(2026, 2, 15, 14, 0, tzinfo=timezone.utc),
        sender="test@example.com",
        recipients=["user@ergonosis.com"],
    )
    defaults.update(overrides)
    return Email(**defaults)


def make_calendar_event(**overrides) -> CalendarEvent:
    """Build a minimal valid CalendarEvent, overriding any fields."""
    defaults = dict(
        event_id="evt_test_001",
        start_time=datetime(2026, 2, 15, 9, 0, tzinfo=timezone.utc),
        end_time=datetime(2026, 2, 15, 10, 0, tzinfo=timezone.utc),
        organizer="org@ergonosis.com",
    )
    defaults.update(overrides)
    return CalendarEvent(**defaults)


def make_entity_link(**overrides) -> EntityLink:
    """Build a minimal valid EntityLink, overriding any fields."""
    defaults = dict(
        source_id="txn_test_001",
        target_id="msg_test_001",
        source_type=EntityType.TRANSACTION,
        target_type=EntityType.EMAIL,
        match_type=MatchType.DETERMINISTIC,
        match_tier=MatchTier.TIER2_COMPOSITE,
        confidence=0.95,
        linkage_key="merchant_name+amount+date",
        rule_version="1.0",
    )
    defaults.update(overrides)
    return EntityLink(**defaults)


def make_feedback(**overrides) -> LinkFeedback:
    """Build a minimal valid LinkFeedback, overriding any fields."""
    defaults = dict(
        link_id="link_test_001",
        signal=FeedbackSignal.CONFIRMED,
        source=FeedbackSource.AUTONOMOUS,
    )
    defaults.update(overrides)
    return LinkFeedback(**defaults)
