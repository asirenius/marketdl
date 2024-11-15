from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock

import pandas as pd
import pytest
from typer.testing import CliRunner

from marketdl.logger import TextLogger
from marketdl.market_data import PolygonMarketData
from marketdl.models import Artifact, DataType, DateRange, Frequency, TimeUnit
from marketdl.storage import ParquetStorage


@pytest.fixture
def test_data():
    return pd.DataFrame({"data": [1, 2, 3]})


@pytest.fixture
def test_dates():
    start = datetime(2024, 1, 1)
    return {
        "start": start,
        "end": start + timedelta(days=1),
        "invalid_end": start - timedelta(days=1),
    }


@pytest.fixture
def frequency():
    return Frequency(multiplier=1, unit=TimeUnit.MINUTE)


@pytest.fixture
def storage(tmp_path, logger):
    return ParquetStorage(compress=True, logger=logger)


@pytest.fixture
def mock_logger():
    return MagicMock(spec=TextLogger)


@pytest.fixture
def mock_storage():
    storage = MagicMock()
    storage.exists.return_value = False
    storage.save = AsyncMock()
    return storage


@pytest.fixture
def mock_data_source():
    source = AsyncMock()
    source.get_aggregates.return_value = pd.DataFrame({"data": [1, 2, 3]})
    source.get_quotes.return_value = pd.DataFrame({"data": [1, 2, 3]})
    source.get_trades.return_value = pd.DataFrame({"data": [1, 2, 3]})
    return source


@pytest.fixture
def mock_progress():
    progress = MagicMock()
    progress.mark_started = MagicMock()
    progress.mark_completed = MagicMock()
    progress.mark_failed = MagicMock()
    progress.set_total = MagicMock()
    return progress


@pytest.fixture
def date_range(test_dates):
    return DateRange(start=test_dates["start"], end=test_dates["end"])


@pytest.fixture
def logger():
    return TextLogger(level="DEBUG")


@pytest.fixture
def mock_coordinator():
    coordinator = AsyncMock()
    coordinator.start = AsyncMock()
    return coordinator


@pytest.fixture
def mock_config(tmp_path):
    return f"""
api:
  service: polygon
  timeout: 30
  max_retries: 3
  retry_delay: 1.0
storage:
  base_path: {tmp_path}
  format: parquet
  compress: true
logger:
  log_file: {tmp_path}/marketdl.log
downloads:
  - symbols: ["TEST"]
    data_types: ["aggregates"]
    frequencies: ["1minute"]
    start_date: "2024-01-01"
    end_date: "2024-01-02"
max_concurrent: 5
"""


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def test_artifact(test_dates, tmp_path, frequency):
    return Artifact(
        symbol="TEST",
        data_type=DataType.AGGREGATES,
        start_date=test_dates["start"],
        end_date=test_dates["end"],
        frequency=frequency,
        base_path=tmp_path,
    )


@pytest.fixture
def mock_http_client():
    client = AsyncMock()
    client.get = AsyncMock()
    client.get.return_value.status_code = 200
    client.get.return_value.json = AsyncMock(return_value={"results": [{"data": 1}]})
    return client


@pytest.fixture
def mock_response():
    """Fixture for mock HTTP response."""
    response = Mock()
    response.status_code = 200
    response.json = Mock(return_value={"results": []})
    return response


@pytest.fixture
def polygon_client(mock_http_client, mock_logger):
    """Fixture for Polygon client."""
    return PolygonMarketData(
        client=mock_http_client,
        api_key="test_key",
        timeout=30,
        max_retries=3,
        retry_delay=0.1,
        logger=mock_logger,
    )
