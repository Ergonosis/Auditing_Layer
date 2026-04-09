"""Unit tests for src/storage/merge_handler.py"""

import pytest

from tests.conftest import make_entity_link
from src.storage.merge_handler import MergeHandler


class TestMergeHandler:
    def test_insert_new_link(self, merge_handler, tmp_db):
        link = make_entity_link()
        outcome = merge_handler.merge_link(link)
        assert outcome == "inserted"
        # Verify it's in storage
        stored = tmp_db.get_link(link.source_id, link.target_id, is_current=True)
        assert stored is not None
        assert stored.link_id == link.link_id

    def test_skip_identical_link(self, merge_handler, tmp_db):
        link = make_entity_link()
        merge_handler.merge_link(link)
        # Insert same link again
        outcome = merge_handler.merge_link(link)
        assert outcome == "skipped"

    def test_update_changed_confidence(self, merge_handler, tmp_db):
        link_v1 = make_entity_link(confidence=0.95)
        merge_handler.merge_link(link_v1)

        link_v2 = make_entity_link(confidence=0.80)  # same source/target, different confidence
        outcome = merge_handler.merge_link(link_v2)
        assert outcome == "updated"

        # Old link should be soft-deleted
        old = tmp_db.get_link(link_v1.source_id, link_v1.target_id, is_current=False)
        assert old is not None
        assert old.is_current is False

        # New link should be current
        current = tmp_db.get_link(link_v2.source_id, link_v2.target_id, is_current=True)
        assert current is not None
        assert current.confidence == 0.80

    def test_update_changed_rule_version(self, merge_handler, tmp_db):
        link_v1 = make_entity_link(rule_version="1.0")
        merge_handler.merge_link(link_v1)

        link_v2 = make_entity_link(rule_version="2.0")
        outcome = merge_handler.merge_link(link_v2)
        assert outcome == "updated"

    def test_update_changed_linkage_key(self, merge_handler, tmp_db):
        link_v1 = make_entity_link(linkage_key="merchant_name+amount+date")
        merge_handler.merge_link(link_v1)

        link_v2 = make_entity_link(linkage_key="fuzzy:merchant_name:0.92")
        outcome = merge_handler.merge_link(link_v2)
        assert outcome == "updated"

    def test_different_source_target_pair_inserts_independently(self, merge_handler, tmp_db):
        link_a = make_entity_link(source_id="txn_a", target_id="msg_a")
        link_b = make_entity_link(source_id="txn_b", target_id="msg_b")
        assert merge_handler.merge_link(link_a) == "inserted"
        assert merge_handler.merge_link(link_b) == "inserted"

    def test_superseded_link_has_lineage_populated(self, merge_handler, tmp_db):
        """Soft-deleted link must have superseded_by_link_id and superseded_in_run_id set."""
        link_v1 = make_entity_link(confidence=0.95)
        merge_handler.merge_link(link_v1, run_id="run_001")

        link_v2 = make_entity_link(confidence=0.80)
        merge_handler.merge_link(link_v2, run_id="run_002")

        with tmp_db._connect() as conn:
            row = conn.execute(
                "SELECT superseded_by_link_id, superseded_in_run_id FROM entity_links WHERE link_id=?",
                (link_v1.link_id,),
            ).fetchone()

        assert row is not None
        assert row["superseded_by_link_id"] == link_v2.link_id
        assert row["superseded_in_run_id"] == "run_002"

    def test_supersession_chain_traceable(self, merge_handler, tmp_db):
        """A chain of supersessions can be traversed via superseded_by_link_id."""
        link_v1 = make_entity_link(confidence=0.95)
        merge_handler.merge_link(link_v1, run_id="run_a")

        link_v2 = make_entity_link(confidence=0.80)
        merge_handler.merge_link(link_v2, run_id="run_b")

        link_v3 = make_entity_link(confidence=0.75)
        merge_handler.merge_link(link_v3, run_id="run_c")

        with tmp_db._connect() as conn:
            v1_row = conn.execute(
                "SELECT superseded_by_link_id FROM entity_links WHERE link_id=?",
                (link_v1.link_id,),
            ).fetchone()
            v2_row = conn.execute(
                "SELECT superseded_by_link_id FROM entity_links WHERE link_id=?",
                (link_v2.link_id,),
            ).fetchone()

        assert v1_row["superseded_by_link_id"] == link_v2.link_id
        assert v2_row["superseded_by_link_id"] == link_v3.link_id
