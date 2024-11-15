from abc import abstractmethod
from typing import Protocol

import pandas as pd

from marketdl.models import Artifact, DateRange, Frequency


class DataSource(Protocol):
    """Interface for data source providers"""

    @abstractmethod
    async def get_aggregates(
        self, symbol: str, frequency: Frequency, date_range: DateRange
    ) -> pd.DataFrame:
        """Fetch aggregate data"""
        pass

    @abstractmethod
    async def get_quotes(self, symbol: str, date_range: DateRange) -> pd.DataFrame:
        """Fetch quote data"""
        pass

    @abstractmethod
    async def get_trades(self, symbol: str, date_range: DateRange) -> pd.DataFrame:
        """Fetch trade data"""
        pass


class Storage(Protocol):
    """Interface for data storage"""

    @abstractmethod
    async def save(self, artifact: Artifact) -> None:
        """Save data to storage"""
        pass

    @abstractmethod
    def exists(self, artifact: Artifact) -> bool:
        """Check if data exists in storage"""
        pass


class Logger(Protocol):
    """Interface for logging"""

    @abstractmethod
    def debug(self, msg: str, **context) -> None:
        """Log debug message with context"""
        pass

    @abstractmethod
    def info(self, msg: str, **context) -> None:
        """Log info message with context"""
        pass

    @abstractmethod
    def warning(self, msg: str, **context) -> None:
        """Log warning message with context"""
        pass

    @abstractmethod
    def error(self, msg: str, **context) -> None:
        """Log error message with context"""
        pass


class ProgressTracker(Protocol):
    """Interface for tracking download progress"""

    @abstractmethod
    def mark_started(self, id: str) -> None:
        """Mark a download as started"""
        pass

    @abstractmethod
    def mark_completed(self, id: str) -> None:
        """Mark a download as completed"""
        pass

    @abstractmethod
    def mark_skipped(self, id: str) -> None:
        """Mark a download as skipped (already exists)"""
        pass

    @abstractmethod
    def mark_failed(self, id: str, error: str) -> None:
        """Mark a download as failed"""
        pass

    @abstractmethod
    def close(self) -> None:
        """Clean up progress tracking resources"""
        pass
