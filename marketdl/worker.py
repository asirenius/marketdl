import asyncio

import pandas as pd

from marketdl.interfaces import DataSource, Logger, ProgressTracker, Storage
from marketdl.models import Artifact, DataType, DateRange


class DownloadWorker:
    """Worker process for downloading market data from queue."""

    def __init__(
        self,
        id: int,
        download_queue: asyncio.Queue,
        data_source: DataSource,
        storage: Storage,
        logger: Logger,
        progress: ProgressTracker,
    ):
        self.id = id
        self.download_queue = download_queue
        self.data_source = data_source
        self.storage = storage
        self.logger = logger
        self.progress = progress
        self._queue_timeout = 5.0

    async def run(self) -> None:
        """Process downloads from queue until cancelled."""
        self.logger.info("Starting worker", id=self.id)

        while True:
            try:
                try:
                    download = await asyncio.wait_for(
                        self.download_queue.get(), timeout=self._queue_timeout
                    )
                except asyncio.TimeoutError:
                    continue
                except asyncio.CancelledError:
                    self.logger.info(
                        "Worker cancelled while waiting for task", id=self.id
                    )
                    break

                try:
                    self.progress.mark_started(download.id)
                    await self._process_download(download)
                    self.progress.mark_completed(download.id)
                except asyncio.CancelledError:
                    self.logger.info(
                        "Worker cancelled while processing download",
                        id=self.id,
                        download_id=download.id,
                    )
                    break
                except Exception as e:
                    self.logger.error(
                        "Failed to process download",
                        id=download.id,
                        error=str(e),
                    )
                    self.progress.mark_failed(download.id, str(e))
                finally:
                    self.download_queue.task_done()

            except asyncio.CancelledError:
                self.logger.info("Worker cancelled", id=self.id)
                break
            except Exception as e:
                self.logger.error(
                    "Worker error",
                    id=self.id,
                    error=str(e),
                )
                continue

        self.logger.info("Worker shutting down", id=self.id)

    async def _process_download(self, download: Artifact) -> None:
        """Process single download artifact."""
        try:
            market_data = await self._fetch_market_data(download)
            if market_data.empty:
                self.logger.info("No data available", id=download.id)
                return

            download.data = market_data
            await self.storage.save(download)

        except asyncio.CancelledError:
            self.logger.info("Download cancelled", id=download.id)
            raise

    async def _fetch_market_data(self, download: Artifact) -> pd.DataFrame:
        """Fetch appropriate type of market data."""
        date_range = DateRange(start=download.start_date, end=download.end_date)

        try:
            if download.data_type == DataType.AGGREGATES:
                return await self.data_source.get_aggregates(
                    download.symbol,
                    download.frequency,
                    date_range,
                )
            elif download.data_type == DataType.QUOTES:
                return await self.data_source.get_quotes(
                    download.symbol,
                    date_range,
                )
            else:  # TRADES
                return await self.data_source.get_trades(
                    download.symbol,
                    date_range,
                )
        except asyncio.CancelledError:
            self.logger.info("Data fetch cancelled", id=download.id)
            raise
