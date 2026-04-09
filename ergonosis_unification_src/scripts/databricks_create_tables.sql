-- Create unification tables in Databricks Delta (Unity Catalog).
-- Run once per catalog/schema (e.g. ergonosis.unification).
-- Replace <catalog> and <schema> with your catalog and schema names, or run:
--   USE CATALOG ergonosis;
--   USE SCHEMA unification;
-- then run the CREATE TABLE statements without the catalog.schema prefix.

-- ── RUN-ONCE MIGRATION (for already-live workspaces) ──────────────────────
-- Execute these ALTERs manually before the next pipeline run if the tables
-- already exist. They are idempotent (ADD COLUMN IF NOT EXISTS).
--
-- ALTER TABLE ergonosis.unification.unmatched_entities
--     ADD COLUMN IF NOT EXISTS target_type STRING;
-- ALTER TABLE ergonosis.unification.ambiguous_matches
--     ADD COLUMN IF NOT EXISTS source_type STRING;
-- ALTER TABLE ergonosis.unification.ambiguous_matches
--     ADD COLUMN IF NOT EXISTS target_type STRING;
-- ──────────────────────────────────────────────────────────────────────────

-- Option A: Set context first (recommended)
-- USE CATALOG ergonosis;
-- USE SCHEMA unification;

-- Option B: Use fully qualified names (replace ergonosis and unification if different)
CREATE TABLE IF NOT EXISTS ergonosis.unification.entity_links (
    link_id STRING NOT NULL,
    source_id STRING NOT NULL,
    target_id STRING NOT NULL,
    source_type STRING NOT NULL,
    target_type STRING NOT NULL,
    match_type STRING NOT NULL,
    match_tier STRING NOT NULL,
    confidence DOUBLE NOT NULL,
    linkage_key STRING NOT NULL,
    rationale STRING,
    rule_version STRING NOT NULL,
    created_at TIMESTAMP NOT NULL,
    effective_from TIMESTAMP NOT NULL,
    effective_to TIMESTAMP,
    is_current INT NOT NULL,
    superseded_by_link_id STRING,
    superseded_in_run_id STRING,
    CONSTRAINT entity_links_pk PRIMARY KEY (link_id)
) USING DELTA;

CREATE TABLE IF NOT EXISTS ergonosis.unification.unmatched_entities (
    entity_id STRING NOT NULL,
    entity_type STRING NOT NULL,
    target_type STRING,
    reason_code STRING NOT NULL,
    run_id STRING NOT NULL,
    logged_at TIMESTAMP NOT NULL,
    v2_processed INT NOT NULL,
    CONSTRAINT unmatched_entities_pk PRIMARY KEY (entity_id, run_id, target_type)
) USING DELTA;

CREATE TABLE IF NOT EXISTS ergonosis.unification.ambiguous_matches (
    ambiguity_id STRING NOT NULL,
    source_entity_id STRING NOT NULL,
    source_type STRING,
    target_type STRING,
    candidate_ids STRING NOT NULL,
    candidate_scores STRING NOT NULL,
    status STRING NOT NULL,
    resolved_link_id STRING,
    resolved_by STRING,
    logged_at TIMESTAMP NOT NULL,
    CONSTRAINT ambiguous_matches_pk PRIMARY KEY (ambiguity_id)
) USING DELTA;

CREATE TABLE IF NOT EXISTS ergonosis.unification.link_feedback (
    feedback_id STRING NOT NULL,
    link_id STRING NOT NULL,
    signal STRING NOT NULL,
    source STRING NOT NULL,
    reason STRING,
    created_at TIMESTAMP NOT NULL,
    processed INT NOT NULL,
    CONSTRAINT link_feedback_pk PRIMARY KEY (feedback_id)
) USING DELTA;

CREATE TABLE IF NOT EXISTS ergonosis.unification.run_log (
    run_id STRING NOT NULL,
    run_type STRING NOT NULL,
    status STRING NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    records_processed INT NOT NULL,
    links_created INT NOT NULL,
    unmatched_count INT NOT NULL,
    ambiguous_count INT NOT NULL,
    failure_reason STRING,
    CONSTRAINT run_log_pk PRIMARY KEY (run_id)
) USING DELTA;

CREATE TABLE IF NOT EXISTS ergonosis.unification.watermarks (
    entity_type STRING NOT NULL,
    last_processed_at TIMESTAMP NOT NULL,
    run_id STRING NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    CONSTRAINT watermarks_pk PRIMARY KEY (entity_type)
) USING DELTA;

CREATE TABLE IF NOT EXISTS ergonosis.unification.intent_log (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id STRING NOT NULL,
    operation_type STRING NOT NULL,
    payload_json STRING NOT NULL,
    planned_at TIMESTAMP NOT NULL,
    committed INT NOT NULL,
    CONSTRAINT intent_log_pk PRIMARY KEY (id)
) USING DELTA;

-- ── Silver tables (canonical raw records) ─────────────────────────────────

CREATE TABLE IF NOT EXISTS ergonosis.unification.transactions (
    transaction_id STRING NOT NULL,
    account_id STRING NOT NULL,
    amount DOUBLE NOT NULL,
    date DATE NOT NULL,
    merchant_name STRING,
    name STRING,
    payment_channel STRING,
    category STRING,
    source STRING NOT NULL,
    ingested_at TIMESTAMP NOT NULL,
    raw_file_ref STRING,
    CONSTRAINT transactions_pk PRIMARY KEY (transaction_id)
) USING DELTA;

CREATE TABLE IF NOT EXISTS ergonosis.unification.emails (
    message_id STRING NOT NULL,
    received_at TIMESTAMP NOT NULL,
    sender STRING,
    recipients STRING NOT NULL,
    subject STRING,
    body_preview STRING,
    thread_id STRING,
    source STRING NOT NULL,
    ingested_at TIMESTAMP NOT NULL,
    raw_file_ref STRING,
    CONSTRAINT emails_pk PRIMARY KEY (message_id)
) USING DELTA;

CREATE TABLE IF NOT EXISTS ergonosis.unification.calendar_events (
    event_id STRING NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    organizer STRING NOT NULL,
    subject STRING,
    attendees STRING,
    location STRING,
    source STRING NOT NULL,
    ingested_at TIMESTAMP NOT NULL,
    raw_file_ref STRING,
    CONSTRAINT calendar_events_pk PRIMARY KEY (event_id)
) USING DELTA;
