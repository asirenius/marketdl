from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, model_validator


class DataService(str, Enum):
    """Supported market data service providers."""

    POLYGON = "polygon"


class DataType(str, Enum):
    """Types of market data available for download."""

    AGGREGATES = "aggregates"
    QUOTES = "quotes"
    TRADES = "trades"


class TimeUnit(str, Enum):
    """Time units for data frequencies."""

    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"


class Frequency(BaseModel):
    """Represents market data frequency (e.g., 1minute, 4hour)."""

    multiplier: int = Field(..., gt=0)
    unit: TimeUnit

    def __str__(self) -> str:
        """Convert frequency to string format."""
        return f"{self.multiplier}{self.unit.value}"

    @classmethod
    def from_string(cls, freq_str: str) -> "Frequency":
        """Parse frequency from string format."""
        import re

        pattern = r"(\d+)(minute|hour|day|week|month)"
        if match := re.match(pattern, freq_str):
            multiplier = int(match.group(1))
            unit = TimeUnit(match.group(2))
            return cls(multiplier=multiplier, unit=unit)
        raise ValueError(f"Invalid frequency format: {freq_str}")

    def __hash__(self):
        """Enable using Frequency as dict key or set member."""
        return hash((self.multiplier, self.unit))

    def should_split_by_day(self) -> bool:
        """Determine if data should be split by day based on frequency"""
        return self.unit in (TimeUnit.MINUTE, TimeUnit.SECOND)


class DateRange(BaseModel):
    """Represents a time period for data requests."""

    start: datetime
    end: datetime

    @model_validator(mode="after")
    def validate_dates(self) -> "DateRange":
        """Ensure start date precedes end date."""
        if self.start >= self.end:
            raise ValueError("start date must be before end date")
        return self


class Artifact:
    """Represents downloadable market data with metadata."""

    def __init__(
        self,
        symbol: str,
        data_type: DataType,
        start_date: datetime,
        end_date: datetime,
        frequency: Optional[Frequency] = None,
        base_path: Optional[Path] = None,
        storage_format: str = "parquet",
        compress: bool = True,
    ):
        """Initialize artifact with metadata and storage location."""
        self.symbol = symbol
        self.data_type = data_type
        self.start_date = start_date
        self.end_date = end_date
        self.frequency = frequency
        self.data = None
        self.storage_format = storage_format
        self.compress = compress
        self.output_path = self._set_output_path(base_path or Path("data"))

    @property
    def id(self) -> str:
        """Generate unique identifier for this artifact."""
        if self.data_type == DataType.AGGREGATES:
            return f"{self.symbol}{self.data_type.value}{self.frequency}_{self.start_date.date()}{self.end_date.date()}"
        return f"{self.symbol}{self.data_type.value}{self.start_date.date()}{self.end_date.date()}"

    @property
    def is_empty(self) -> bool:
        """Check if artifact contains data."""
        return self.data is None or len(self.data) == 0

    def _set_output_path(self, base_path: Path) -> Path:
        """Set output path using simplified directory structure."""
        parts = [base_path, self.symbol, self.data_type.value]

        if self.data_type == DataType.AGGREGATES and self.frequency:
            parts.append(str(self.frequency))

        path = Path(*parts)

        # Always split by day for minute data or when no frequency (quotes/trades)
        should_split = (
            self.frequency and self.frequency.should_split_by_day()
        ) or not self.frequency

        if should_split:
            # For minute-level data, quotes, and trades, always use start date only
            date_str = self.start_date.strftime("%Y-%m-%d")
        else:
            # For hour or higher frequencies, use range if dates differ
            if self.start_date.date() == self.end_date.date():
                date_str = self.start_date.strftime("%Y-%m-%d")
            else:
                date_str = f"{self.start_date.strftime('%Y-%m-%d')}_{self.end_date.strftime('%Y-%m-%d')}"

        if self.storage_format == "csv" and self.compress:
            extension = "csv.gz"
        else:
            extension = self.storage_format

        return path / f"{date_str}.{extension}"
