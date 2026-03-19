"""Tests for _resolve_ambiguous_matches() in the orchestrator."""

from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

import pytest

from src.orchestrator.orchestrator_agent import AuditOrchestrator


def _make_ambiguous(source_entity_id, candidate_ids, candidate_scores, ambiguity_id="amb_001"):
    """Create a mock AmbiguousMatch."""
    m = MagicMock()
    m.source_entity_id = source_entity_id
    m.candidate_ids = candidate_ids
    m.candidate_scores = candidate_scores
    m.ambiguity_id = ambiguity_id
    m.logged_at = datetime(2026, 3, 1, tzinfo=timezone.utc)
    return m


class TestResolveAmbiguousMatches:
    def test_auto_resolves_clear_winner(self):
        """Scores [0.92, 0.65] → gap=0.27 ≥ 0.15 → auto-resolve, no escalation."""
        match = _make_ambiguous("txn_001", ["link_a", "link_b"], [0.92, 0.65])
        orch = AuditOrchestrator()

        with patch("src.integrations.unification_client.get_ambiguous_matches", return_value=[match]):
            with patch("src.integrations.unification_client.resolve_ambiguous_match") as mock_resolve:
                result = orch._resolve_ambiguous_matches()

        mock_resolve.assert_called_once()
        assert mock_resolve.call_args.kwargs["chosen_link_id"] == "link_a"
        assert result == []

    def test_escalates_tied_match(self):
        """Scores [0.80, 0.78] → gap=0.02 < 0.15 → do NOT resolve, escalate."""
        match = _make_ambiguous("txn_002", ["link_a", "link_b"], [0.80, 0.78])
        orch = AuditOrchestrator()

        with patch("src.integrations.unification_client.get_ambiguous_matches", return_value=[match]):
            with patch("src.integrations.unification_client.resolve_ambiguous_match") as mock_resolve:
                result = orch._resolve_ambiguous_matches()

        mock_resolve.assert_not_called()
        assert len(result) == 1
        assert result[0]["txn_id"] == "txn_002"

    def test_empty_ambiguous_list(self):
        """No ambiguous matches → returns []."""
        orch = AuditOrchestrator()
        with patch("src.integrations.unification_client.get_ambiguous_matches", return_value=[]):
            result = orch._resolve_ambiguous_matches()
        assert result == []

    def test_single_candidate_auto_resolves(self):
        """Only 1 candidate → gap = score - 0.0. If ≥ 0.15, auto-resolve."""
        match = _make_ambiguous("txn_003", ["link_only"], [0.75])
        orch = AuditOrchestrator()

        with patch("src.integrations.unification_client.get_ambiguous_matches", return_value=[match]):
            with patch("src.integrations.unification_client.resolve_ambiguous_match") as mock_resolve:
                result = orch._resolve_ambiguous_matches()

        mock_resolve.assert_called_once()
        assert result == []
