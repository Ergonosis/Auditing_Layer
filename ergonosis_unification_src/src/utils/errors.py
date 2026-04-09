"""Custom exceptions for the Ergonosis Data Unification Layer"""


class UnificationError(Exception):
    """Base exception for all unification layer errors"""
    pass


class SchemaValidationError(UnificationError):
    """Hard fail — raised when a required field is missing or has an invalid type.
    Processing of the affected record must stop immediately."""
    pass


class SoftValidationError(UnificationError):
    """Soft fail — raised when an optional field is present but invalid.
    Processing may continue with the field omitted or defaulted."""
    pass


class DatabricksConnectionError(UnificationError):
    """Raised when the Databricks connection cannot be established or is lost
    during a read/write operation."""
    pass


class StorageError(UnificationError):
    """Raised when a read or write operation against the backing store fails
    for reasons other than a connection error (e.g. permission denied,
    table not found, serialisation failure)."""
    pass


class IntentLogError(UnificationError):
    """Raised when writing to or reading from the intent log fails, including
    TTL enforcement errors and log corruption."""
    pass


class IngestionError(UnificationError):
    """Raised when raw entity records cannot be fetched or parsed from an
    upstream source (Plaid, Microsoft Graph, etc.)."""
    pass


class IngestionCredentialsRequiredError(IngestionError):
    """Raised when credentials are missing in secure mode."""


class WatermarkError(UnificationError):
    """Raised when reading or advancing the incremental watermark fails,
    preventing safe incremental runs."""
    pass


class ConfigurationError(UnificationError):
    """Raised when the YAML config file is missing, contains invalid YAML,
    or is missing one or more required keys."""
    pass


class MatchingError(UnificationError):
    """Raised when the three-tier matching cascade encounters an unrecoverable
    error that prevents match evaluation for a record pair."""
    pass


class FeedbackProcessingError(UnificationError):
    """Raised when a feedback signal (confirmed / rejected / flagged) cannot
    be applied to a match link, e.g. due to a missing link ID or storage
    failure during the update."""
    pass


class SecureStorageRequiredError(UnificationError):
    """Raised at pipeline startup when UNIFICATION_SECURE_STORAGE_REQUIRED=true
    but Databricks is not configured or the connection fails. Prevents the
    pipeline from running with local SQLite in a production/secure environment."""
    pass


class ConsentRequiredError(UnificationError):
    """Raised when a query-time consent check fails — user has no active consent
    for the requested operation, or has opted out of data processing."""
    pass
