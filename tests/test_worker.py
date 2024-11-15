import asyncio

import pytest

from marketdl.worker import DownloadWorker


@pytest.mark.asyncio
async def test_worker_successful_download(
    mock_data_source, mock_storage, mock_progress, test_artifact, mock_logger
):
    queue = asyncio.Queue()
    worker = DownloadWorker(
        id=1,
        download_queue=queue,
        data_source=mock_data_source,
        storage=mock_storage,
        logger=mock_logger,
        progress=mock_progress,
    )
    await queue.put(test_artifact)
    # Add another task to ensure worker exits
    task = asyncio.create_task(worker.run())
    await asyncio.sleep(0.1)  # Give worker time to process
    task.cancel()  # Cancel after processing
    await task
    mock_storage.save.assert_called_once()


@pytest.mark.asyncio
async def test_worker_error_handling(
    mock_data_source, mock_storage, mock_progress, test_artifact, mock_logger
):
    mock_data_source.get_aggregates.side_effect = Exception("Test error")
    queue = asyncio.Queue()
    worker = DownloadWorker(
        id=1,
        download_queue=queue,
        data_source=mock_data_source,
        storage=mock_storage,
        logger=mock_logger,
        progress=mock_progress,
    )
    await queue.put(test_artifact)
    task = asyncio.create_task(worker.run())
    await asyncio.sleep(0.1)
    task.cancel()
    await task
    mock_progress.mark_failed.assert_called_once()


@pytest.mark.asyncio
async def test_worker_cancellation(
    mock_data_source, mock_storage, mock_progress, test_artifact, mock_logger
):
    queue = asyncio.Queue()
    worker = DownloadWorker(
        id=1,
        download_queue=queue,
        data_source=mock_data_source,
        storage=mock_storage,
        logger=mock_logger,
        progress=mock_progress,
    )
    task = asyncio.create_task(worker.run())
    # Add queue timeout to trigger queue.get exception
    worker._queue_timeout = 0.1
    await asyncio.sleep(0.2)  # Wait for timeout
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        mock_logger.info.assert_any_call("Worker shutting down", id=1)
        mock_logger.info.assert_any_call("Worker cancelled", id=1)
