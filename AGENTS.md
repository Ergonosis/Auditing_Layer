# Auditing Layer — Agent Reference

This document describes each agent deployed in the auditing pipeline: its role, inputs, outputs, and how it fits into the overall workflow.

The pipeline is orchestrated by `AuditOrchestrator` (`src/orchestrator/orchestrator_agent.py`), which runs agents in two phases:

1. **Parallel phase** — Data Quality + Reconciliation (sequential in CrewAI, logically independent)
2. **Sequential phase** — Context Enrichment → Escalation (direct/deterministic mode)

The Anomaly Detection and Logging agents are defined but currently disabled in the active pipeline (see status notes below).

---

## 1. Data Quality Agent

**File:** `src/agents/data_quality_agent.py`

**Role:** Validates incoming transaction data before any auditing begins. Acts as a gatekeeper — if data quality falls below threshold, downstream agents are not triggered.

**Tools:**
| Tool | Purpose |
|---|---|
| `check_data_completeness` | Finds records missing required fields (vendor, amount, date) |
| `validate_schema_conformity` | Checks field types, formats, and required keys |
| `detect_duplicate_records` | Identifies transactions that appear more than once |
| `infer_domain_freshness` | Checks whether data was ingested within the configured max age window |
| `check_data_quality_gates` | Applies thresholds (e.g., completeness ≥ 90%) to decide pass/fail |

**Input:** Transaction data is preloaded into a tool-level cache (`_PRELOADED_DATA`). Tools accept `transactions_json="[]"` and load from cache automatically. No raw transaction data is passed through LLM prompts.

**Output (JSON):**
```json
{
  "quality_score": 0.95,
  "incomplete_records": ["EXP_001", "EXP_002"],
  "schema_violations": [],
  "duplicates": {
    "duplicate_count": 3,
    "duplicate_groups": [{"ids": ["TXN_010", "TXN_011"], "count": 2}]
  },
  "domain_config": {"domain": "business_operations", "max_age_hours": 48, "confidence": 1.0},
  "freshness_violations": [],
  "gate_passed": true
}
```

- `incomplete_records` — list of transaction ID strings with missing fields
- `gate_passed` — `false` halts the audit (though results are still returned)

---

## 2. Reconciliation Agent

**File:** `src/agents/reconciliation_agent.py`

**Role:** Matches transactions across sources (credit card vs. bank) and identifies orphan transactions that have no corresponding record. Runs second in the parallel phase and consumes Data Quality results from context.

**Tools:**
| Tool | Purpose |
|---|---|
| `cross_source_matcher` | Matches transactions between two sources (e.g., credit card ↔ bank) |
| `entity_resolver_kg` | Resolves vendor entity aliases using a knowledge graph |
| `fuzzy_vendor_matcher` | Matches vendor names with fuzzy string comparison |
| `receipt_transaction_matcher` | Links receipt images to transactions |
| `find_orphan_transactions` | Identifies transactions present in one source but not another |

**Input:** Same preloaded cache as Data Quality. Also receives Data Quality output via CrewAI task context.

**Output (JSON):** Returns a combined object containing both data quality (from context) and reconciliation results:
```json
{
  "data_quality": {
    "quality_score": 0.95,
    "incomplete_records": ["EXP_001"],
    "duplicates": {"duplicate_count": 3, "duplicate_groups": []},
    "gate_passed": true
  },
  "reconciliation": {
    "matched_pairs": [],
    "unmatched_transactions": [
      {"txn_id": "EXP_001"},
      {"txn_id": "EXP_002"}
    ],
    "match_rate": 0.85,
    "total_unmatched_1": 100
  }
}
```

- `unmatched_transactions` — array of objects with `txn_id` field (not plain strings)
- Reconciliation agent output is the final output of the parallel CrewAI crew

---

## 3. Context Enrichment Agent

**File:** `src/agents/context_enrichment_agent.py`

**Role:** For suspicious transactions identified after the parallel phase, searches email archives, calendar systems, and receipt databases to find supporting documentation (approvals, receipts, meeting context). A confidence score per transaction determines whether escalation score should be reduced.

**Tools:**
| Tool | Purpose |
|---|---|
| `search_emails_batch` | Batch search emails by vendor name and amount |
| `search_calendar_events` | Find calendar events correlated with the transaction date/vendor |
| `extract_approval_chains` | Pull email approval thread for a transaction |
| `find_receipt_images` | Match receipt images to transaction metadata |
| `semantic_search_documents` | Semantic search across documents (used only for high-priority unmatched, <5% of volume) |

**Input:**
```json
{
  "transactions": "[{\"txn_id\": \"EXP_001\", \"vendor\": \"Acme\", \"amount\": 1500.00, \"date\": \"2026-01-10\"}, ...]"
}
```
Capped at 20 transactions per run to manage LLM cost.

**Output (JSON):**
```json
{
  "enriched_transactions": [
    {
      "txn_id": "EXP_001",
      "email_approval": true,
      "calendar_event": {"title": "Client dinner", "date": "2026-01-10"},
      "receipt_found": true,
      "confidence": 0.95
    }
  ]
}
```

- `email_approval: true` reduces the transaction's escalation score by 20 points in the direct escalation step
- If context enrichment fails, the orchestrator continues without it (best-effort)

---

## 4. Escalation Agent

**File:** `src/agents/escalation_agent.py`

**Role:** Classifies each suspicious transaction by severity, generates a root cause explanation, and creates a persisted audit flag. In production, this runs in **direct mode** (deterministic Python, not LLM agent) for reliability at scale. The CrewAI agent definition is used for LLM-driven classification in smaller/test runs.

**Tools (CrewAI agent mode):**
| Tool | Purpose |
|---|---|
| `calculate_severity_score` | Computes a 0–100 severity score from contributing factors |
| `generate_root_cause_analysis` | Produces a natural-language explanation of why the transaction is flagged |
| `batch_classify_with_llm` | LLM-based batch classification for ambiguous cases |
| `create_audit_flag` | Persists a flag record to the database; returns a `flag_id` UUID |
| `check_escalation_rules` | Applies business rules (e.g., whitelisted vendors, amount thresholds) to adjust severity |

**Input (CrewAI agent mode):**
```
suspicious_transactions: [{txn_id, vendor, amount, date, ...}, ...]
parallel_results: { data_quality: {...}, reconciliation: {...} }
audit_run_id: "uuid"
```

**Direct mode scoring logic** (used in production via `_run_escalation_direct`):

| Signal | Score contribution |
|---|---|
| No reconciliation match | +50 |
| Incomplete data | +30 |
| Duplicate transaction | +40 |
| Amount ≥ $5,000 | +20 |
| Email approval found | −20 |

| Score range | Severity |
|---|---|
| ≥ 70 | CRITICAL |
| 50–69 | WARNING |
| < 50 | INFO |

**Output:** A flag record written to `ergonosis.auditing.flags` and returned in-memory:
```json
{
  "flag_id": "uuid",
  "transaction_id": "EXP_001",
  "audit_run_id": "uuid",
  "severity_level": "CRITICAL",
  "confidence_score": 0.67,
  "explanation": "Flagged because: No matching bank transaction found for $8500 to Acme Corp; Transaction amount $8500 exceeds high-value threshold.",
  "supporting_evidence_links": {"contributing_factors": ["no_reconciliation_match", "high_amount"]},
  "created_at": "2026-01-10T12:00:00"
}
```

---

## 5. Anomaly Detection Agent

**File:** `src/agents/anomaly_detection_agent.py`

**Status:** Defined but **not active** in the current pipeline (excluded from `orchestrator_agent.py`).

**Role:** Detects statistical and ML-based anomalies in transaction patterns — intended to run in parallel with Data Quality and Reconciliation.

**Tools:**
| Tool | Purpose |
|---|---|
| `run_isolation_forest` | ML-based outlier detection on transaction feature vectors |
| `check_vendor_spending_profile` | Z-score comparison against historical vendor spend |
| `detect_amount_outliers` | Statistical outlier detection on transaction amounts |
| `time_series_deviation_check` | Detects deviations in recurring transaction patterns |
| `batch_anomaly_scorer` | Aggregates signals from all methods into a single anomaly score |

**Input:** Transaction feature vectors (format determined by tool implementations in `src/tools/anomaly_tools.py`).

**Output (JSON):**
```json
{
  "anomaly_scores": [{"txn_id": "TXN_001", "score": 82}, ...],
  "flagged_transactions": [{"txn_id": "TXN_001", "score": 82}]
}
```

- `flagged_transactions` — transactions with anomaly score > 70

---

## 6. Logging Agent

**File:** `src/agents/logging_agent.py`

**Status:** Defined but **removed from the active pipeline**. Audit trail logging is handled automatically by the structured logger (`src/utils/logging.py`) and the `audit_trail` Databricks table written by `src/db/databricks_writer.py`.

**Role:** Records all agent decisions and actions for compliance and debugging.

**Tools:**
| Tool | Purpose |
|---|---|
| `log_agent_decision` | Records a single agent decision with timestamp and context |
| `create_audit_trail_entry` | Appends an immutable entry to the audit trail |
| `get_audit_trail` | Retrieves audit trail entries for a given run |
| `generate_lineage_trace` | Builds a full data lineage trace for a transaction |

**Input:** Agent name, decision description, tool called, LLM tokens used, execution time.

**Output:** Confirmation string; side effect is an entry appended to `ergonosis.auditing.audit_trail`.

---

## Pipeline Summary

```
Transactions (Gold tables / CSV fixtures)
        │
        ▼
┌─────────────────────────────────────┐
│  PARALLEL PHASE (CrewAI sequential) │
│  1. Data Quality Agent              │
│  2. Reconciliation Agent            │
└─────────────────────────────────────┘
        │ suspicious_ids, quality results
        ▼
┌─────────────────────────────────────┐
│  Context Enrichment Agent           │
│  (capped at 20 txns, best-effort)   │
└─────────────────────────────────────┘
        │ email_approval signals
        ▼
┌─────────────────────────────────────┐
│  Escalation (direct mode)           │
│  Score → Severity → Flag → DB write │
└─────────────────────────────────────┘
        │
        ▼
ergonosis.auditing.flags  (Databricks)
```

**Not in active pipeline:** Anomaly Detection, Logging Agent (see notes above).
