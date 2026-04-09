"""
Rich mock pipeline run — exercises all matching tiers and edge cases
using synthetic fixture data.

Usage:
    python scripts/run_rich_mock.py

Sets STUB_INGESTION=rich before importing pipeline modules so the adapters
load from tests/fixtures/rich_mock_*.json. Outputs are kept local (SQLite).

Expected scenario summary:
  Txn→Email (Tier 3 fuzzy, single match → EntityLink):
    rich_txn_001  Starbucks Coffee LLC   → rich_msg_001
    rich_txn_002  Whole Foods Market     → rich_msg_002  (negative amount / credit)
    rich_txn_003  Delta Airlines         → rich_msg_003  (2 days apart)
    rich_txn_011  Netflix                → rich_msg_007  (amount mismatch, fuzzy name)
    rich_txn_012  Spotify                → rich_msg_008
    rich_txn_013  Apple Inc              → rich_msg_009  (Re:/Inc normalization)
    rich_txn_014  Chipotle Mexican Grill → rich_msg_010

  Txn→Calendar (Tier 3 fuzzy, single match → EntityLink):
    rich_txn_004  Marriott Hotels → rich_evt_001

  Txn→Email (Tier 3 fuzzy, multiple matches → AmbiguousMatch):
    rich_txn_005  Uber Technologies → {rich_msg_004a, rich_msg_004b}

  Txn→Calendar (Tier 3 fuzzy, multiple matches → AmbiguousMatch):
    rich_txn_006  WeWork → {rich_evt_002a, rich_evt_002b}

  NOTE: email↔calendar matching is NOT run in V1 (engine only runs txn→email and txn→calendar).
  rich_msg_011 (Q1 Planning) will be unmatched as email→transaction (no txn counterpart).

  Unmatched transactions:
    rich_txn_007  Obscure Vendor XR99  (no candidates)
    rich_txn_008  Shopify              (email 10 days out of window)
    rich_txn_009  ACME Industrial      (low similarity)
    rich_txn_010  null name+merchant   (no fields to compare)
    rich_txn_015  Lyft                 (no email nearby)
"""

import os
import sys
import tempfile
from pathlib import Path

# Must be set before pipeline modules are imported (adapters read env at call time)
os.environ["STUB_INGESTION"] = "rich"

# Ensure project root is on path when running from scripts/
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.pipeline import run_pipeline
from src.constants import MatchTier, RunStatus
from src.query_interface import UnifiedQueryInterface
from src.storage.local_store import LocalStore

# ── Setup ─────────────────────────────────────────────────────────────────────

with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
    db_path = f.name

print(f"\nRich mock run — DB: {db_path}")
print("=" * 60)

storage = LocalStore(db_path)

# ── Run pipeline ──────────────────────────────────────────────────────────────

from src.constants import RunType

run_log = run_pipeline(run_type=RunType.FULL_REFRESH, storage=storage)

print(f"\nRun status   : {run_log.status.value}")
print(f"Run ID       : {run_log.run_id}")
print(f"Links created: {run_log.links_created}")
print(f"Unmatched    : {run_log.unmatched_count}")
print(f"Ambiguous    : {run_log.ambiguous_count}")

# ── Query results ─────────────────────────────────────────────────────────────

qi = UnifiedQueryInterface(storage)

# Collect all links from storage directly for tier breakdown
with storage._connect() as conn:
    rows = conn.execute(
        "SELECT match_tier, COUNT(*) as cnt FROM entity_links WHERE is_current=1 GROUP BY match_tier"
    ).fetchall()

tier_counts = {row["match_tier"]: row["cnt"] for row in rows}

print("\nLinks by tier:")
for tier, cnt in sorted(tier_counts.items()):
    print(f"  {tier}: {cnt}")

# Unmatched by type
with storage._connect() as conn:
    unmatched_rows = conn.execute(
        "SELECT entity_type, target_type, reason_code, entity_id FROM unmatched_entities ORDER BY entity_type, entity_id"
    ).fetchall()

print(f"\nUnmatched ({len(unmatched_rows)} total):")
for row in unmatched_rows:
    print(f"  [{row['entity_type']} → {row['target_type']}] {row['entity_id']}  reason={row['reason_code']}")

# Ambiguous
amb_list = qi.get_ambiguous_matches(status="pending")
print(f"\nAmbiguous ({len(amb_list)} total):")
for a in amb_list:
    print(f"  source={a.source_entity_id}  candidates={a.candidate_ids}  scores={[f'{s:.2f}' for s in a.candidate_scores]}")

# Spot-check: links for specific transactions
print("\nSpot-check linked entities:")
for txn_id in ["rich_txn_001", "rich_txn_002", "rich_txn_003", "rich_txn_004", "rich_txn_011", "rich_txn_012", "rich_txn_013"]:
    bundles = qi.get_linked_entities(txn_id, "transaction")
    if bundles:
        b = bundles[0]
        print(f"  {txn_id} → {b.linked_entity_id} ({b.match_tier}, conf={b.confidence:.2f})")
    else:
        print(f"  {txn_id} → (no link)")

# ── Assertions ────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("Assertions:")

failures = []

def check(label, condition, detail=""):
    if condition:
        print(f"  PASS  {label}")
    else:
        msg = f"  FAIL  {label}" + (f" — {detail}" if detail else "")
        print(msg)
        failures.append(label)

check("Run status is SUCCESS", run_log.status == RunStatus.SUCCESS)
check("At least 1 Tier 2 composite link", tier_counts.get("tier2_composite", 0) >= 1,
      f"got {tier_counts.get('tier2_composite', 0)}")
check("At least 4 Tier 3 fuzzy links", tier_counts.get("tier3_fuzzy", 0) >= 4,
      f"got {tier_counts.get('tier3_fuzzy', 0)}")
check("At least 2 ambiguous matches", len(amb_list) >= 2,
      f"got {len(amb_list)}")
check("At least 4 unmatched entities", len(unmatched_rows) >= 4,
      f"got {len(unmatched_rows)}")

# rich_txn_001 (Starbucks) should be linked
txn_001_links = qi.get_linked_entities("rich_txn_001", "transaction")
check("rich_txn_001 (Starbucks) is linked", len(txn_001_links) > 0)

# rich_txn_002 (Whole Foods, negative amount) should be linked
txn_002_links = qi.get_linked_entities("rich_txn_002", "transaction")
check("rich_txn_002 (Whole Foods credit) is linked", len(txn_002_links) > 0)

# rich_txn_007 (Obscure Vendor) should be unmatched
unmatched_ids = {row["entity_id"] for row in unmatched_rows}
check("rich_txn_007 (Obscure Vendor) is unmatched", "rich_txn_007" in unmatched_ids)

# rich_txn_008 (Shopify, out of window) should be unmatched
check("rich_txn_008 (Shopify, out of window) is unmatched", "rich_txn_008" in unmatched_ids)

# rich_txn_005 (Uber) should be in ambiguous, not linked
txn_005_links = qi.get_linked_entities("rich_txn_005", "transaction")
amb_sources = {a.source_entity_id for a in amb_list}
check("rich_txn_005 (Uber) is ambiguous, not linked",
      "rich_txn_005" in amb_sources and len(txn_005_links) == 0)

# rich_txn_013 (Apple Inc, normalization) should be linked
txn_013_links = qi.get_linked_entities("rich_txn_013", "transaction")
check("rich_txn_013 (Apple Inc normalization) is linked", len(txn_013_links) > 0)

# rich_msg_011 (Q1 Planning, no txn) should link to rich_evt_003 via email→calendar cascade
email_011_links = qi.get_linked_entities("rich_msg_011", "email")
check("rich_msg_011 (Q1 Planning email) links to calendar via email→calendar cascade",
      len(email_011_links) > 0 and email_011_links[0].linked_entity_id == "rich_evt_003")

print("\n" + "=" * 60)
if failures:
    print(f"RESULT: {len(failures)} assertion(s) FAILED: {failures}")
    sys.exit(1)
else:
    print(f"RESULT: All assertions passed.")
