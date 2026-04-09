"""Prometheus metrics for the Ergonosis Data Unification Layer"""

from prometheus_client import Counter, Gauge, Histogram

# --- Health metrics (per spec) ---

tier_hit_rate = Gauge(
    "unification_tier_hit_rate",
    "Rate of matches resolved at each tier for a given entity pair",
    labelnames=["tier", "entity_pair"],
)

unmatched_rate = Gauge(
    "unification_unmatched_rate",
    "Rate of entities that could not be matched to any counterpart",
    labelnames=["entity_type"],
)

ambiguity_rate = Gauge(
    "unification_ambiguity_rate",
    "Rate of matches flagged as ambiguous for a given entity pair",
    labelnames=["entity_pair"],
)

feedback_rejection_rate = Gauge(
    "unification_feedback_rejection_rate",
    "Rate of match links rejected via feedback signals",
)

# --- Infrastructure counters ---

entities_ingested = Counter(
    "unification_entities_ingested_total",
    "Total number of raw entities successfully ingested from upstream sources",
    labelnames=["entity_type", "source"],
)

links_created = Counter(
    "unification_links_created_total",
    "Total number of match links created across all tiers",
    labelnames=["match_tier"],
)

links_soft_deleted = Counter(
    "unification_links_soft_deleted_total",
    "Total number of match links soft-deleted (e.g. after rejection feedback)",
)

schema_hard_failures = Counter(
    "unification_schema_hard_failures_total",
    "Total number of records that failed hard schema validation (required field missing)",
    labelnames=["entity_type"],
)

schema_soft_failures = Counter(
    "unification_schema_soft_failures_total",
    "Total number of records that failed soft schema validation (optional field invalid)",
    labelnames=["entity_type"],
)

run_duration = Histogram(
    "unification_run_duration_seconds",
    "Wall-clock duration of unification pipeline runs",
    labelnames=["run_type"],
    buckets=[30, 60, 120, 300, 600, 1800],
)

databricks_healthy = Gauge(
    "unification_databricks_connection_healthy",
    "1 if the Databricks connection is healthy, 0 otherwise",
)

unmatched_rate_by_source = Gauge(
    "unification_unmatched_rate_by_source",
    "Ratio of unmatched to total entities by source and target type",
    labelnames=["source_type", "target_type"],
)

feedback_latency_seconds = Histogram(
    "unification_feedback_latency_seconds",
    "Time between feedback creation and processing",
    buckets=[60, 300, 1800, 7200, 86400],
)

data_freshness_seconds = Gauge(
    "unification_data_freshness_seconds",
    "Seconds since most recent watermark advancement per entity type",
    labelnames=["entity_type"],
)
