"""Incremental run watermark tracking."""

from datetime import datetime
from typing import Dict, Optional

from src.constants import EntityType
from src.models.run import Watermark
from src.utils.errors import WatermarkError
from src.utils.logging import get_logger

logger = get_logger(__name__)


class WatermarkManager:
    """
    Tracks last successfully processed timestamp per entity type.
    Used for incremental runs: only process records after the watermark.
    Stored in the storage backend (entity_links table has its own table).
    """

    def __init__(self, storage):
        self._storage = storage  # LocalStore or DeltaClient

    def get_watermark(self, entity_type: EntityType) -> Optional[datetime]:
        """Returns last_processed_at for this entity type, or None for full refresh."""
        try:
            watermark: Optional[Watermark] = self._storage.get_watermark(entity_type)
            if watermark is None:
                return None
            return watermark.last_processed_at
        except Exception as exc:
            raise WatermarkError(
                f"Failed to read watermark for {entity_type}: {exc}"
            ) from exc

    def set_watermark(self, entity_type: EntityType, timestamp: datetime, run_id: str) -> None:
        """Update watermark after successful run."""
        try:
            watermark = Watermark(
                entity_type=entity_type,
                last_processed_at=timestamp,
                run_id=run_id,
            )
            self._storage.set_watermark(watermark)
            logger.info(
                "Watermark updated",
                entity_type=entity_type.value,
                timestamp=timestamp.isoformat(),
                run_id=run_id,
            )
        except WatermarkError:
            raise
        except Exception as exc:
            raise WatermarkError(
                f"Failed to set watermark for {entity_type}: {exc}"
            ) from exc

    def get_all_watermarks(self) -> Dict[str, Optional[datetime]]:
        """Returns {entity_type: last_processed_at} for all entity types."""
        result: Dict[str, Optional[datetime]] = {}
        for entity_type in EntityType:
            try:
                result[entity_type.value] = self.get_watermark(entity_type)
            except WatermarkError:
                result[entity_type.value] = None
        return result

    def reset_watermarks(self, run_id: str) -> None:
        """
        Reset all watermarks to epoch sentinel (datetime.min) before a full refresh run.
        The pipeline's watermark filter compares ingested_at > watermark.last_processed_at,
        so setting last_processed_at=datetime.min ensures ALL records pass through.
        """
        from datetime import datetime, timezone
        epoch = datetime.min.replace(tzinfo=timezone.utc)
        try:
            for entity_type in EntityType:
                watermark = Watermark(
                    entity_type=entity_type,
                    last_processed_at=epoch,
                    run_id=run_id,
                )
                self._storage.set_watermark(watermark)
                logger.info(
                    "Watermark reset to epoch for full refresh",
                    entity_type=entity_type.value,
                    run_id=run_id,
                )
        except Exception as exc:
            raise WatermarkError(f"Failed to reset watermarks: {exc}") from exc
