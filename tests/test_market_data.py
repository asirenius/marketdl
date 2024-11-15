from datetime import datetime, timedelta
from unittest.mock import AsyncMock, Mock

import httpx
import pandas as pd
import pytest

from marketdl.logger import TextLogger
from marketdl.market_data import DataSourceFactory, PolygonMarketData
from marketdl.models import DataService, DateRange, Frequency, TimeUnit


@pytest.mark.asyncio
async def test_get_aggregates(polygon_client, mock_http_client):
    """Test fetching aggregate data."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "results": [
            {"t": 1625097600000, "o": 100, "h": 101, "l": 99, "c": 100.5, "v": 1000},
            {"t": 1625097660000, "o": 100.5, "h": 102, "l": 100, "c": 101, "v": 1200},
        ]
    }
    mock_http_client.get.return_value = mock_response

    # Test parameters
    symbol = "AAPL"
    frequency = Frequency(multiplier=1, unit=TimeUnit.MINUTE)
    date_range = DateRange(start=datetime(2024, 1, 1), end=datetime(2024, 1, 2))

    result = await polygon_client.get_aggregates(symbol, frequency, date_range)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert all(col in result.columns for col in ["o", "h", "l", "c", "v"])


@pytest.mark.asyncio
async def test_get_quotes(polygon_client, mock_http_client):
    """Test fetching quote data."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "results": [
            {"p": 100, "s": 100, "t": 1625097600000},
            {"p": 101, "s": 200, "t": 1625097601000},
        ]
    }
    mock_http_client.get.return_value = mock_response

    symbol = "AAPL"
    date_range = DateRange(start=datetime(2024, 1, 1), end=datetime(2024, 1, 2))

    result = await polygon_client.get_quotes(symbol, date_range)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert all(col in result.columns for col in ["p", "s", "t"])


@pytest.mark.asyncio
async def test_rate_limiting(polygon_client, mock_http_client):
    """Test rate limit handling."""
    rate_limit_response = Mock()
    rate_limit_response.status_code = 429
    rate_limit_response.headers = {"Retry-After": "1"}

    success_response = Mock()
    success_response.status_code = 200
    success_response.json.return_value = {"results": []}

    mock_http_client.get.side_effect = AsyncMock(
        side_effect=[rate_limit_response, success_response]
    )

    symbol = "AAPL"
    date_range = DateRange(start=datetime(2024, 1, 1), end=datetime(2024, 1, 2))

    result = await polygon_client.get_trades(symbol, date_range)
    assert mock_http_client.get.call_count == 2


@pytest.mark.asyncio
async def test_max_retries_exceeded(polygon_client, mock_http_client):
    """Test handling when max retries are exceeded."""
    error_response = Mock()
    error_response.status_code = 500
    error_response.json.return_value = {"error": "Server error"}
    error_response.raise_for_status = Mock(
        side_effect=httpx.HTTPStatusError(
            message="500 Server Error", request=Mock(), response=error_response
        )
    )

    # Always return error response
    mock_http_client.get.return_value = error_response

    with pytest.raises(RuntimeError, match="Max retries exceeded"):
        await polygon_client.get_trades(
            "AAPL", DateRange(start=datetime(2024, 1, 1), end=datetime(2024, 1, 2))
        )

    # Should have tried max_retries + 1 times
    assert mock_http_client.get.call_count == polygon_client.max_retries + 1


@pytest.mark.asyncio
async def test_empty_response(polygon_client, mock_http_client):
    """Test handling of empty response."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {}  # Empty response
    mock_http_client.get.return_value = mock_response

    result = await polygon_client.get_trades(
        "AAPL", DateRange(start=datetime(2024, 1, 1), end=datetime(2024, 1, 2))
    )

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0


def test_data_source_factory(mock_logger):
    """Test DataSourceFactory creation."""
    client = AsyncMock(spec=httpx.AsyncClient)

    source = DataSourceFactory.create(
        provider=DataService.POLYGON,
        client=client,
        api_key="test",
        timeout=30,
        max_retries=3,
        retry_delay=1.0,
        logger=mock_logger,
    )
    assert isinstance(source, PolygonMarketData)

    with pytest.raises(ValueError):
        DataSourceFactory.create(
            provider="invalid",
            client=client,
            api_key="test",
            timeout=30,
            max_retries=3,
            retry_delay=1.0,
            logger=mock_logger,
        )
