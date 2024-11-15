import asyncio
from datetime import datetime
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from marketdl.market_data import PolygonMarketData
from marketdl.models import Artifact, DataType, DateRange, Frequency, TimeUnit
from marketdl.progress import ConsoleProgress
from marketdl.storage import ParquetStorage
from marketdl.worker import DownloadWorker


@pytest.mark.asyncio
async def test_full_download_flow(tmp_path, mock_http_client, mock_logger):
    storage = ParquetStorage(compress=True, logger=mock_logger)

    mock_response = AsyncMock()
    mock_response.status_code = 200
    mock_response.json = AsyncMock(return_value={"results": [{"data": 1}]})
    mock_http_client.get.return_value = mock_response

    client = PolygonMarketData(
        client=mock_http_client,
        api_key="test_key",
        timeout=30,
        max_retries=3,
        retry_delay=0.1,
        logger=mock_logger,
    )

    progress = ConsoleProgress(logger=mock_logger)
    artifact = Artifact(
        symbol="TEST",
        data_type=DataType.AGGREGATES,
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2024, 1, 2),
        frequency=Frequency(multiplier=1, unit=TimeUnit.MINUTE),
        base_path=tmp_path,
    )

    progress.set_total(1)
    progress.mark_started(artifact.id)

    data = await mock_http_client.get()
    artifact.data = pd.DataFrame([{"data": 1}])

    await storage.save(artifact)
    progress.mark_completed(artifact.id)

    assert artifact.output_path.exists()
    assert progress.stats.completed == 1


@pytest.mark.asyncio
async def test_concurrent_downloads(tmp_path, mock_http_client, mock_logger):
    storage = ParquetStorage(compress=True, logger=mock_logger)
    progress = ConsoleProgress(logger=mock_logger)

    mock_response = AsyncMock()
    mock_response.status_code = 200
    mock_response.json = AsyncMock(return_value={"results": [{"data": 1}]})
    mock_http_client.get.return_value = mock_response

    artifacts = [
        Artifact(
            symbol=f"TEST{i}",
            data_type=DataType.AGGREGATES,
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 2),
            frequency=Frequency(multiplier=1, unit=TimeUnit.MINUTE),
            base_path=tmp_path,
        )
        for i in range(3)
    ]

    progress.set_total(len(artifacts))

    for artifact in artifacts:
        progress.mark_started(artifact.id)
        artifact.data = pd.DataFrame([{"data": 1}])
        await storage.save(artifact)
        progress.mark_completed(artifact.id)

    assert progress.stats.completed == len(artifacts)
    assert all(a.output_path.exists() for a in artifacts)


@pytest.mark.asyncio
async def test_error_handling(mock_http_client, date_range, frequency, mock_logger):
    mock_http_client.get.side_effect = Exception("API Error")
    client = PolygonMarketData(
        client=mock_http_client,
        api_key="test_key",
        timeout=30,
        max_retries=1,
        retry_delay=0.1,
        logger=mock_logger,
    )

    with pytest.raises(RuntimeError):
        await client.get_aggregates("TEST", frequency, date_range)
