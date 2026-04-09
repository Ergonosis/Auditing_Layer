"""Constants and enums for the Ergonosis Data Unification Layer"""

from enum import Enum


class EntityType(str, Enum):
    TRANSACTION = "transaction"
    EMAIL = "email"
    CALENDAR_EVENT = "calendar_event"


class MatchType(str, Enum):
    DETERMINISTIC = "deterministic"
    EMBEDDING = "embedding"
    LLM_INFERRED = "llm_inferred"


class MatchTier(str, Enum):
    TIER1_EXACT = "tier1_exact"
    TIER2_COMPOSITE = "tier2_composite"
    TIER3_FUZZY = "tier3_fuzzy"


class RunType(str, Enum):
    FULL_REFRESH = "full_refresh"
    INCREMENTAL = "incremental"


class RunStatus(str, Enum):
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"
    SKIPPED = "skipped"


class UnmatchedReasonCode(str, Enum):
    NO_SHARED_KEY = "no_shared_key"
    AMBIGUOUS_MERCHANT = "ambiguous_merchant"
    NO_CANDIDATE_FOUND = "no_candidate_found"


class AmbiguityStatus(str, Enum):
    PENDING = "pending"
    RESOLVED = "resolved"
    DISMISSED = "dismissed"


class FeedbackSignal(str, Enum):
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    FLAGGED = "flagged"


class FeedbackSource(str, Enum):
    AUTONOMOUS = "autonomous"
    HUMAN = "human"


class ResolvedBy(str, Enum):
    AUDITING_AGENT = "auditing_agent"
    HUMAN = "human"


DEFAULT_DATE_WINDOW_DAYS = 3
DEFAULT_MIN_SIMILARITY_SCORE = 0.80
DEFAULT_CONFIDENCE_TIER1 = 1.0
DEFAULT_CONFIDENCE_TIER2 = 0.90
DEFAULT_CONFIDENCE_TIER3_MIN = 0.50
INTENT_LOG_TTL_SECONDS = 86400  # 24 hours
CONFIG_REQUIRED_KEYS = ["entity_types", "match_rules", "confidence_bands"]

# Environment variable names — centralised here for auditability
ENV_DATABRICKS_HOST = "DATABRICKS_HOST"
ENV_DATABRICKS_TOKEN = "DATABRICKS_TOKEN"
ENV_DATABRICKS_HTTP_PATH = "DATABRICKS_HTTP_PATH"
ENV_DATABRICKS_CATALOG = "DATABRICKS_CATALOG"
ENV_DATABRICKS_SCHEMA = "DATABRICKS_SCHEMA"
ENV_SECURE_STORAGE_REQUIRED = "UNIFICATION_SECURE_STORAGE_REQUIRED"
ENV_CONFIG_PATH = "UNIFICATION_CONFIG_PATH"
ENV_STUB_INGESTION = "STUB_INGESTION"
ENV_PLAID_ACCOUNT_ID = "UNIFICATION_PLAID_ACCOUNT_ID"
ENV_USER_EMAIL = "UNIFICATION_USER_EMAIL"
ENV_PLAID_ACCESS_TOKEN = "PLAID_ACCESS_TOKEN"
ENV_PLAID_CLIENT_ID = "PLAID_CLIENT_ID"
ENV_PLAID_SECRET = "PLAID_SECRET"
ENV_PLAID_ENV = "PLAID_ENV"
ENV_MSGRAPH_CLIENT_ID = "MSGRAPH_CLIENT_ID"
ENV_MSGRAPH_CLIENT_SECRET = "MSGRAPH_CLIENT_SECRET"
ENV_MSGRAPH_TENANT_ID = "MSGRAPH_TENANT_ID"
ENV_METRICS_PORT = "METRICS_PORT"
ENV_HEALTH_PORT = "UNIFICATION_HEALTH_PORT"

DATABRICKS_CONNECT_TIMEOUT_SECONDS = 30
API_CALL_TIMEOUT_SECONDS = 30
ENV_COLLECT_BODY_PREVIEW = "COLLECT_BODY_PREVIEW"
ENV_COLLECT_CALENDAR_ATTENDEES = "COLLECT_CALENDAR_ATTENDEES"
ENV_GCP_PROJECT_ID = "GCP_PROJECT_ID"

PLAID_TXN_PAGE_SIZE = 500
COMMIT_BATCH_SIZE = 500  # Max records committed per checkpoint batch in Step 11
PIPELINE_LOCK_TTL_SECONDS = 7200  # 2 hours; must match acquire_pipeline_lock() default

ENTITY_LINKS_TTL_SECONDS = 365 * 24 * 3600
UNMATCHED_TTL_SECONDS = 90 * 24 * 3600
AMBIGUOUS_TTL_SECONDS = 90 * 24 * 3600
RUN_LOG_TTL_SECONDS = 180 * 24 * 3600
LINK_FEEDBACK_TTL_SECONDS = 365 * 24 * 3600

# Audit log retention: 7 years (access_audit_log, deletion_audit).
# These tables are NOT eligible for generic purge_old_records calls.
# Purge only via purge_old_audit_logs() through a controlled process.
AUDIT_LOG_RETENTION_SECONDS = 7 * 365 * 24 * 3600
