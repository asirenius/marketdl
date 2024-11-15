from datetime import datetime
from pathlib import Path

import pytest

from marketdl.config import APIConfig, Config, DownloadSpec, StorageConfig
from marketdl.models import DataService, DataType, Frequency, TimeUnit


def test_api_config():
    config = APIConfig(api_key="test", service=DataService.POLYGON)
    assert config.api_key == "test"
    assert config.service == DataService.POLYGON
    assert config.timeout == 30
    assert config.max_retries == 3


def test_storage_config(tmp_path):
    config = StorageConfig(base_path=tmp_path)
    assert config.base_path == tmp_path
    assert config.format == "parquet"
    assert config.compress is True


def test_download_spec(test_dates, frequency):
    spec = DownloadSpec(
        symbols={"TEST"},
        data_types={DataType.AGGREGATES},
        frequencies={frequency},
        start_date=test_dates["start"],
        end_date=test_dates["end"],
    )
    assert "TEST" in spec.symbols
    assert DataType.AGGREGATES in spec.data_types


def test_config_from_yaml(tmp_path):
    config_text = """
    api:
      service: polygon
    storage:
      base_path: data
      format: parquet
    downloads:
      - symbols: ["TEST"]
        data_types: ["aggregates"]
        frequencies: ["1minute"]
        start_date: "2024-01-01"
        end_date: "2024-01-02"
    """
    config_file = tmp_path / "config.yaml"
    config_file.write_text(config_text)
    config = Config.from_yaml(config_file, api_key="test_key")
    assert len(config.downloads) == 1
    assert config.api.api_key == "test_key"


def test_invalid_config_validation(tmp_path):
    config_text = """
    api:
      service: invalid
    """
    config_file = tmp_path / "config.yaml"
    config_file.write_text(config_text)
    with pytest.raises(ValueError):
        Config.from_yaml(config_file, api_key="test_key")
