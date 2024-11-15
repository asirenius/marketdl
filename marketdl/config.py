from datetime import datetime
from pathlib import Path
from typing import List, Optional, Set

from pydantic import BaseModel, Field, model_validator

from marketdl.models import DataService, DataType, Frequency


class APIConfig(BaseModel):
    """Configuration for API access"""

    service: DataService = Field(
        default=DataService.POLYGON, description="Market data service to use"
    )
    api_key: str = Field(..., description="API key for data provider")
    timeout: int = Field(
        default=30, description="Request timeout in seconds", ge=1, le=300
    )
    max_retries: int = Field(
        default=3, description="Maximum number of retry attempts", ge=0
    )
    retry_delay: float = Field(
        default=1.0, description="Initial delay between retries in seconds", ge=0.1
    )


class StorageConfig(BaseModel):
    """Configuration for data storage"""

    base_path: Path = Field(
        default=Path("data"), description="Base path for storing downloaded data"
    )
    format: str = Field(
        default="parquet",
        description="Storage format (parquet or csv)",
        pattern="^(parquet|csv)$",
    )
    compress: bool = Field(default=True, description="Whether to compress stored data")


class LoggerConfig(BaseModel):
    """Configuration for logging"""

    name: str = Field(default="marketdl", description="")
    level: str = Field(default="INFO", description="")
    log_file: Optional[Path] = Field(default=Path("marketdl.log"), description="")
    format: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


class DownloadSpec(BaseModel):
    """Specification for data download"""

    symbols: Set[str] = Field(..., description="Symbols to download", min_length=1)
    data_types: Set[DataType] = Field(
        default={DataType.AGGREGATES}, description="Types of data to download"
    )
    frequencies: Set[Frequency] = Field(
        description="Data frequencies to download (only applicable for aggregates)",
        default_factory=set,
    )
    start_date: datetime = Field(..., description="Start date for data download")
    end_date: datetime = Field(..., description="End date for data download")

    @model_validator(mode="after")
    def validate_spec(self) -> "DownloadSpec":
        if self.start_date >= self.end_date:
            raise ValueError("start_date must be before end_date")

        # Frequencies only needed for aggregates
        if DataType.AGGREGATES in self.data_types and not self.frequencies:
            raise ValueError("frequencies must be specified for aggregate data")

        # Frequencies should not be specified for quotes/trades
        if DataType.AGGREGATES not in self.data_types and self.frequencies:
            raise ValueError("frequencies should only be specified for aggregate data")

        return self


class Config(BaseModel):
    """Main configuration"""

    api: APIConfig = Field(default_factory=APIConfig, description="API configuration")
    storage: StorageConfig = Field(
        default_factory=StorageConfig, description="Storage configuration"
    )
    logger: LoggerConfig = Field(
        default_factory=LoggerConfig, description="Logger configuration"
    )
    downloads: List[DownloadSpec] = Field(
        default_factory=list, description="Download specifications", min_length=1
    )
    max_concurrent: Optional[int] = Field(
        default=None, description="Maximum concurrent downloads", ge=1
    )

    @classmethod
    def from_yaml(cls, path: Path, api_key: str) -> "Config":
        """Create Config instance from YAML file"""
        import yaml

        with open(path) as f:
            data = yaml.safe_load(f)

        # Add API key to the config data
        if "api" not in data:
            data["api"] = {}
        data["api"]["api_key"] = api_key

        # Convert frequency strings to Frequency objects
        if "downloads" in data:
            for spec in data["downloads"]:
                if "frequencies" in spec:
                    spec["frequencies"] = {
                        Frequency.from_string(f) for f in spec["frequencies"]
                    }

        return cls.model_validate(data)

    @classmethod
    def generate_default(cls) -> str:
        """Generate default configuration YAML"""
        default_config = {
            "api": {
                "service": "polygon",
                "timeout": 30,
                "max_retries": 3,
                "retry_delay": 1.0,
            },
            "storage": {"base_path": "data", "format": "parquet", "compress": True},
            "logger": {"log_file": "marketdl.log", "level": "INFO"},
            "downloads": [
                {
                    "symbols": ["C:EURUSD", "X:BTCUSD"],
                    "data_types": ["aggregates", "quotes"],
                    "frequencies": ["1minute"],
                    "start_date": "2023-12-26",
                    "end_date": "2023-12-31",
                },
                {
                    "symbols": ["AAPL", "AMZN"],
                    "data_types": ["aggregates"],
                    "frequencies": ["4hour", "1week"],
                    "start_date": "2020-01-01",
                    "end_date": "2023-12-31",
                },
            ],
            "max_concurrent": 5,
        }
        import yaml

        return yaml.dump(default_config, sort_keys=False, indent=2)
