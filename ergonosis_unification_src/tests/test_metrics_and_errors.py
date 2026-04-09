"""Tests for src/utils/metrics.py and src/utils/errors.py."""

import pytest


class TestErrorHierarchy:
    def test_all_errors_subclass_unification_error(self):
        from src.utils.errors import (
            UnificationError,
            SchemaValidationError,
            SoftValidationError,
            DatabricksConnectionError,
            StorageError,
            IntentLogError,
            IngestionError,
            WatermarkError,
            ConfigurationError,
            MatchingError,
            FeedbackProcessingError,
            SecureStorageRequiredError,
        )
        subclasses = [
            SchemaValidationError,
            SoftValidationError,
            DatabricksConnectionError,
            StorageError,
            IntentLogError,
            IngestionError,
            WatermarkError,
            ConfigurationError,
            MatchingError,
            FeedbackProcessingError,
            SecureStorageRequiredError,
        ]
        for cls in subclasses:
            assert issubclass(cls, UnificationError), f"{cls.__name__} must subclass UnificationError"

    def test_unification_error_subclasses_exception(self):
        from src.utils.errors import UnificationError
        assert issubclass(UnificationError, Exception)

    def test_instantiate_with_message(self):
        from src.utils.errors import (
            SchemaValidationError, IngestionError, WatermarkError,
            ConfigurationError, StorageError, FeedbackProcessingError,
        )
        for cls in [SchemaValidationError, IngestionError, WatermarkError,
                    ConfigurationError, StorageError, FeedbackProcessingError]:
            e = cls("test message")
            assert "test message" in str(e)

    def test_can_be_caught_as_base(self):
        from src.utils.errors import UnificationError, StorageError
        with pytest.raises(UnificationError):
            raise StorageError("storage failed")


class TestMetricsImport:
    def test_module_imports_without_error(self):
        import src.utils.metrics as metrics
        assert metrics is not None

    def test_gauges_accessible(self):
        from src.utils.metrics import (
            tier_hit_rate, unmatched_rate, ambiguity_rate,
            feedback_rejection_rate, databricks_healthy,
        )
        assert tier_hit_rate is not None
        assert unmatched_rate is not None
        assert ambiguity_rate is not None
        assert feedback_rejection_rate is not None
        assert databricks_healthy is not None

    def test_counters_accessible(self):
        from src.utils.metrics import (
            entities_ingested, links_created, links_soft_deleted,
            schema_hard_failures, schema_soft_failures,
        )
        assert entities_ingested is not None
        assert links_created is not None
        assert schema_hard_failures is not None

    def test_histogram_accessible(self):
        from src.utils.metrics import run_duration
        assert run_duration is not None
