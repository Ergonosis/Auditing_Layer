"""SQL schemas for audit database tables (Databricks/Delta Lake compatible)."""

# Flags table - stores audit flags for human review
FLAGS_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS flags (
    flag_id STRING NOT NULL,
    transaction_id STRING NOT NULL,
    audit_run_id STRING NOT NULL,
    severity_level STRING,
    confidence_score DOUBLE,
    explanation STRING NOT NULL,
    supporting_evidence_links STRING,
    reviewed BOOLEAN,
    human_decision STRING,
    reviewer_id STRING,
    review_timestamp TIMESTAMP,
    reviewer_notes STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
"""

# Audit trail table - immutable append-only log
AUDIT_TRAIL_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS audit_trail (
    audit_run_id STRING NOT NULL,
    log_sequence_number BIGINT NOT NULL,
    agent_name STRING NOT NULL,
    tool_called STRING NOT NULL,
    timestamp TIMESTAMP,
    execution_time_ms BIGINT,
    input_data STRING,
    output_summary STRING,
    llm_model STRING,
    llm_tokens_used BIGINT,
    llm_cost_dollars DECIMAL(10, 4),
    error_message STRING,
    error_stack_trace STRING,
    decision_chain STRING
)
USING DELTA
"""

# Workflow state table - tracks audit run state
WORKFLOW_STATE_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS workflow_state (
    audit_run_id STRING NOT NULL,
    workflow_status STRING,
    current_agent STRING,
    completed_agents STRING,
    pending_agents STRING,
    intermediate_results STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
"""


def create_all_tables(cursor):
    """
    Execute all CREATE TABLE statements.

    Args:
        cursor: Database cursor object
    """
    cursor.execute(FLAGS_TABLE_SCHEMA)
    cursor.execute(AUDIT_TRAIL_TABLE_SCHEMA)
    cursor.execute(WORKFLOW_STATE_TABLE_SCHEMA)
