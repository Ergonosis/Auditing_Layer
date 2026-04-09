"""Idempotency logic for entity_links writes."""

from typing import Union

from src.models.links import EntityLink
from src.utils.logging import get_logger
from src.utils.metrics import links_created, links_soft_deleted

logger = get_logger(__name__)


class MergeHandler:
    """
    Enforces idempotency for entity_links writes.

    Three outcomes for any proposed link:
    1. Identical existing link → SKIP (no write)
    2. Existing link with changed metadata → SOFT DELETE old + INSERT new
    3. No existing link → INSERT

    Updates links_created and links_soft_deleted Prometheus counters.
    """

    def __init__(self, storage):
        self.storage = storage

    def merge_link(self, proposed: EntityLink, run_id: str = "") -> str:
        """
        Returns: 'skipped' | 'updated' | 'inserted'
        """
        existing = self.storage.get_link(proposed.source_id, proposed.target_id, is_current=True)

        if existing is None:
            self.storage.upsert_link(proposed)
            links_created.labels(match_tier=proposed.match_tier.value).inc()
            return "inserted"

        if self._is_identical(existing, proposed):
            return "skipped"

        # Changed metadata: soft delete old, insert new — record supersession lineage
        self.storage.soft_delete_link(
            existing.link_id,
            superseded_by_link_id=proposed.link_id,
            superseded_in_run_id=run_id,
        )
        links_soft_deleted.inc()
        self.storage.upsert_link(proposed)
        links_created.labels(match_tier=proposed.match_tier.value).inc()
        return "updated"

    def batch_merge_links(self, proposed_links: list, run_id: str = "") -> dict:
        """Bulk-aware merge: one read, classify in Python, one bulk write.

        Returns counts dict: {'inserted': N, 'updated': N, 'skipped': N}
        """
        if not proposed_links:
            return {"inserted": 0, "updated": 0, "skipped": 0}

        source_ids = [l.source_id for l in proposed_links]
        existing_map = {
            (l.source_id, l.target_id): l
            for l in self.storage.get_current_links_by_sources(source_ids)
        }

        to_upsert: list = []
        to_soft_delete: list = []
        counts = {"inserted": 0, "updated": 0, "skipped": 0}

        for proposed in proposed_links:
            key = (proposed.source_id, proposed.target_id)
            existing = existing_map.get(key)

            if existing is None:
                to_upsert.append(proposed)
                counts["inserted"] += 1
            elif self._is_identical(existing, proposed):
                counts["skipped"] += 1
            else:
                # Soft-delete existing (mutate in Python, batch-upsert handles the UPDATE)
                existing.is_current = False
                existing.effective_to = proposed.effective_from
                existing.superseded_by_link_id = proposed.link_id
                existing.superseded_in_run_id = run_id
                to_soft_delete.append(existing)
                to_upsert.append(proposed)
                counts["updated"] += 1

        if to_soft_delete:
            self.storage.bulk_upsert_links(to_soft_delete)
        if to_upsert:
            self.storage.bulk_upsert_links(to_upsert)

        # Label as 'bulk' — tier breakdown is captured in intent log
        links_created.labels(match_tier="bulk").inc(counts["inserted"] + counts["updated"])
        links_soft_deleted.inc(len(to_soft_delete))
        return counts

    def _is_identical(self, existing: EntityLink, proposed: EntityLink) -> bool:
        """
        Two links are identical if they share the same source_id, target_id,
        match_tier, confidence, and linkage_key. rule_version differences
        are NOT considered identical (triggers an update).
        """
        return (
            existing.source_id == proposed.source_id
            and existing.target_id == proposed.target_id
            and existing.match_tier == proposed.match_tier
            and existing.confidence == proposed.confidence
            and existing.linkage_key == proposed.linkage_key
            and existing.rule_version == proposed.rule_version
        )
