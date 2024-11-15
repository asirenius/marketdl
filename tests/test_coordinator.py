import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from marketdl.coordinator import DownloadCoordinator
from marketdl.logger import TextLogger
from marketdl.models import Artifact, DataType


@pytest.mark.asyncio
async def test_coordinator_empty_downloads(
    mock_data_source, mock_storage, mock_progress, mock_logger
):
    coordinator = DownloadCoordinator(
        data_source=mock_data_source,
        storage=mock_storage,
        logger=mock_logger,
        progress=mock_progress,
        max_workers=2,
    )
    await coordinator.start([])
    mock_progress.set_total.assert_called_once_with(0)


@pytest.mark.asyncio
async def test_coordinator_new_downloads(
    mock_data_source, mock_storage, mock_progress, test_artifact, mock_logger
):
    coordinator = DownloadCoordinator(
        data_source=mock_data_source,
        storage=mock_storage,
        logger=mock_logger,
        progress=mock_progress,
        max_workers=2,
    )
    await coordinator.start([test_artifact])
    mock_progress.set_total.assert_called_once_with(1)


@pytest.mark.asyncio
async def test_coordinator_worker_error(
    mock_data_source, mock_storage, mock_progress, test_artifact, mock_logger
):
    mock_data_source.get_aggregates.side_effect = Exception("Test error")
    coordinator = DownloadCoordinator(
        data_source=mock_data_source,
        storage=mock_storage,
        logger=mock_logger,
        progress=mock_progress,
        max_workers=2,
    )
    await coordinator.start([test_artifact])
    mock_progress.mark_failed.assert_called_once()


@pytest.mark.asyncio
async def test_coordinator_existing_files(
    mock_data_source, mock_storage, mock_progress, test_artifact, mock_logger
):
    mock_storage.exists.return_value = True
    coordinator = DownloadCoordinator(
        data_source=mock_data_source,
        storage=mock_storage,
        logger=mock_logger,
        progress=mock_progress,
        max_workers=2,
    )
    await coordinator.start([test_artifact])
    mock_progress.mark_skipped.assert_called_once_with(test_artifact.id)
