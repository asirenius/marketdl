import asyncio
import time
from typing import Dict, List, Optional

import httpx
import pandas as pd

from marketdl.interfaces import DataSource, Logger
from marketdl.models import DataService, DateRange, Frequency


class PolygonMarketData(DataSource):
    """Polygon.io implementation of market data source"""

    def __init__(
        self,
        client: httpx.AsyncClient,
        api_key: str,
        timeout: int,
        max_retries: int,
        retry_delay: float,
        logger: Logger,
    ):
        self.client = client
        self.headers = {"Authorization": f"Bearer {api_key}"}
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.logger = logger

    async def get_aggregates(
        self, symbol: str, frequency: Frequency, date_range: DateRange
    ) -> pd.DataFrame:
        """Fetch aggregate market data from Polygon."""
        url = (
            f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/"
            f"{frequency.multiplier}/{frequency.unit.value}/"
            f"{date_range.start.strftime('%Y-%m-%d')}/"
            f"{date_range.end.strftime('%Y-%m-%d')}"
        )

        params = {
            "adjusted": "true",
            "sort": "asc",
            "limit": 50000,
        }

        try:
            self.logger.debug(
                "Fetching aggregates",
                symbol=symbol,
                frequency=str(frequency),
                start_date=date_range.start.isoformat(),
                end_date=date_range.end.isoformat(),
            )

            all_results = await self._fetch_all_pages(url, params)
            return self._process_response({"results": all_results})

        except Exception as e:
            self.logger.error(
                "Failed to fetch aggregates",
                symbol=symbol,
                error=str(e),
            )
            raise

    async def get_quotes(self, symbol: str, date_range: DateRange) -> pd.DataFrame:
        """Fetch quote data from Polygon"""
        date_str = date_range.start.strftime("%Y-%m-%d")
        url = f"https://api.polygon.io/v3/quotes/{symbol}"
        params = {
            "timestamp": date_str,
            "limit": 50000,
        }

        try:
            self.logger.debug(
                "Fetching quotes",
                symbol=symbol,
                date=date_str,
            )

            all_results = await self._fetch_all_pages(url, params)
            return self._process_response({"results": all_results})

        except Exception as e:
            self.logger.error(
                "Failed to fetch quotes",
                symbol=symbol,
                date=date_str,
                error=str(e),
            )
            raise

    async def get_trades(self, symbol: str, date_range: DateRange) -> pd.DataFrame:
        """Fetch trade data from Polygon"""
        date_str = date_range.start.strftime("%Y-%m-%d")
        url = f"https://api.polygon.io/v3/trades/{symbol}"
        params = {
            "timestamp": date_str,
            "limit": 50000,
        }

        try:
            self.logger.debug(
                "Fetching trades",
                symbol=symbol,
                date=date_str,
            )

            all_results = await self._fetch_all_pages(url, params)
            return self._process_response({"results": all_results})

        except Exception as e:
            self.logger.error(
                "Failed to fetch trades",
                symbol=symbol,
                date=date_str,
                error=str(e),
            )
            raise

    async def _fetch_all_pages(self, initial_url: str, initial_params: Dict) -> List:
        """Fetch all pages of data by following next_url links"""
        all_results = []
        next_url = initial_url
        params = initial_params

        while next_url:
            response_data = await self._make_request(next_url, params)

            if "results" in response_data:
                all_results.extend(response_data["results"])

            next_url = response_data.get("next_url")
            if next_url:
                params = None
                self.logger.debug("Fetching next page", next_url=next_url)

        return all_results

    def _process_response(self, data: Dict) -> pd.DataFrame:
        """Process API response into DataFrame."""
        if not data or "results" not in data:
            return pd.DataFrame()

        return pd.DataFrame(data["results"])

    async def _make_request(self, url: str, params: Optional[Dict] = None) -> Dict:
        """Make HTTP request with retry logic."""
        retries = 0

        while retries <= self.max_retries:
            try:
                response = await self.client.get(
                    url, params=params, headers=self.headers
                )

                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", "60"))
                    self.logger.warning(
                        "Rate limit hit, waiting",
                        attempt=retries + 1,
                        retry_after=retry_after,
                    )
                    await asyncio.sleep(retry_after)
                    retries += 1
                    continue

                response.raise_for_status()
                return response.json() or {}

            except Exception as e:
                if retries == self.max_retries:
                    raise RuntimeError("Max retries exceeded") from e

                retries += 1
                delay = self.retry_delay * (2**retries)

                self.logger.warning(
                    "Request failed, retrying",
                    error=str(e),
                    attempt=retries,
                    next_retry_in=delay,
                )

                await asyncio.sleep(delay)

        raise RuntimeError("Max retries exceeded")


class DataSourceFactory:
    """Factory for data sources"""

    @staticmethod
    def create(
        provider: DataService,
        client: httpx.AsyncClient,
        api_key: str,
        timeout: int,
        max_retries: int,
        retry_delay: float,
        logger: Logger,
    ) -> DataSource:
        """Create appropriate data source implementation"""
        providers = {
            DataService.POLYGON: PolygonMarketData,
        }

        if provider not in providers:
            raise ValueError(f"Unsupported provider: {provider}")

        return providers[provider](
            client=client,
            api_key=api_key,
            timeout=timeout,
            max_retries=max_retries,
            retry_delay=retry_delay,
            logger=logger,
        )
