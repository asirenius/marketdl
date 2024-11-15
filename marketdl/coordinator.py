import asyncio
from typing import List

from marketdl.interfaces import DataSource, Logger, ProgressTracker, Storage
from marketdl.models import Artifact
from marketdl.worker import DownloadWorker


class DownloadCoordinator:
    """Coordinates downloads"""

    def __init__(
        self,
        data_source: DataSource,
        storage: Storage,
        logger: Logger,
        progress: ProgressTracker,
        max_workers: int,
    ):
        self.data_source = data_source
        self.storage = storage
        self.logger = logger
        self.progress = progress
        self.max_workers = max_workers
        self.download_queue: asyncio.Queue[Artifact] = asyncio.Queue()
        self.workers: List[asyncio.Task] = []

    async def start(self, downloads: List[Artifact]) -> None:
        """Start the download process"""
        try:
            self.progress.set_total(len(downloads))

            self.logger.info(
                "Starting download process",
                download_count=len(downloads),
                worker_count=self.max_workers,
            )

            new_count = await self._queue_new_downloads(downloads)

            if new_count == 0:
                self.logger.info("No new downloads to process")
                return

            self.workers = await self._start_worker_pool()

            await self._wait_for_completion()

            self.logger.info("Download process completed")

        except Exception as e:
            self.logger.error("Download process failed", error=str(e))
            raise
        finally:
            self.progress.close()

    async def _queue_new_downloads(self, downloads: List[Artifact]) -> int:
        """Queue downloads that don't exist in storage"""
        new_count = 0
        for download in downloads:
            if not self.storage.exists(download):
                await self.download_queue.put(download)
                new_count += 1
            else:
                self.progress.mark_skipped(download.id)
        return new_count

    async def _start_worker_pool(self) -> List[asyncio.Task]:
        """Start the worker pool"""
        return [
            asyncio.create_task(
                DownloadWorker(
                    id=i,
                    download_queue=self.download_queue,
                    data_source=self.data_source,
                    storage=self.storage,
                    logger=self.logger,
                    progress=self.progress,
                ).run()
            )
            for i in range(self.max_workers)
        ]

    async def _wait_for_completion(self) -> None:
        """Wait for downloads to complete and cleanup workers"""
        await self.download_queue.join()
        for worker in self.workers:
            worker.cancel()
        await asyncio.gather(*self.workers, return_exceptions=True)
