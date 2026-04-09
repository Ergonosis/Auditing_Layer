"""Tests for src/etl/watermark.py — WatermarkManager."""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from src.constants import EntityType
from src.etl.watermark import WatermarkManager
from src.models.run import Watermark
from src.utils.errors import WatermarkError


class TestGetWatermark:
    def test_fresh_db_returns_none(self, tmp_db):
        wm = WatermarkManager(tmp_db)
        result = wm.get_watermark(EntityType.TRANSACTION)
        assert result is None

    def test_returns_set_value(self, tmp_db):
        wm = WatermarkManager(tmp_db)
        ts = datetime(2026, 3, 1, 12, 0, 0, tzinfo=timezone.utc)
        wm.set_watermark(EntityType.TRANSACTION, ts, run_id="run_001")
        result = wm.get_watermark(EntityType.TRANSACTION)
        assert result == ts

    def test_storage_error_raises_watermark_error(self):
        mock_storage = MagicMock()
        mock_storage.get_watermark.side_effect = RuntimeError("db down")
        wm = WatermarkManager(mock_storage)
        with pytest.raises(WatermarkError, match="Failed to read watermark"):
            wm.get_watermark(EntityType.EMAIL)


class TestSetWatermark:
    def test_overwrite_updates_value(self, tmp_db):
        wm = WatermarkManager(tmp_db)
        ts1 = datetime(2026, 2, 1, tzinfo=timezone.utc)
        ts2 = datetime(2026, 3, 1, tzinfo=timezone.utc)
        wm.set_watermark(EntityType.TRANSACTION, ts1, run_id="run_001")
        wm.set_watermark(EntityType.TRANSACTION, ts2, run_id="run_002")
        assert wm.get_watermark(EntityType.TRANSACTION) == ts2

    def test_storage_error_raises_watermark_error(self):
        mock_storage = MagicMock()
        mock_storage.set_watermark.side_effect = RuntimeError("db down")
        wm = WatermarkManager(mock_storage)
        with pytest.raises(WatermarkError, match="Failed to set watermark"):
            wm.set_watermark(EntityType.TRANSACTION, datetime.now(timezone.utc), run_id="r1")


class TestGetAllWatermarks:
    def test_all_entity_types_present(self, tmp_db):
        wm = WatermarkManager(tmp_db)
        result = wm.get_all_watermarks()
        for entity_type in EntityType:
            assert entity_type.value in result

    def test_unset_watermarks_are_none(self, tmp_db):
        wm = WatermarkManager(tmp_db)
        result = wm.get_all_watermarks()
        assert all(v is None for v in result.values())

    def test_set_one_watermark_visible_in_all(self, tmp_db):
        wm = WatermarkManager(tmp_db)
        ts = datetime(2026, 3, 5, tzinfo=timezone.utc)
        wm.set_watermark(EntityType.EMAIL, ts, run_id="run_abc")
        result = wm.get_all_watermarks()
        assert result[EntityType.EMAIL.value] == ts
        assert result[EntityType.TRANSACTION.value] is None


class TestResetWatermarks:
    def test_reset_sets_all_to_epoch(self, tmp_db):
        wm = WatermarkManager(tmp_db)
        ts = datetime(2026, 3, 1, tzinfo=timezone.utc)
        for et in EntityType:
            wm.set_watermark(et, ts, run_id="run_001")
        wm.reset_watermarks(run_id="run_refresh")
        epoch = datetime.min.replace(tzinfo=timezone.utc)
        for et in EntityType:
            assert wm.get_watermark(et) == epoch

    def test_storage_error_raises_watermark_error(self):
        mock_storage = MagicMock()
        mock_storage.set_watermark.side_effect = RuntimeError("disk full")
        wm = WatermarkManager(mock_storage)
        with pytest.raises(WatermarkError, match="Failed to reset watermarks"):
            wm.reset_watermarks(run_id="run_x")
