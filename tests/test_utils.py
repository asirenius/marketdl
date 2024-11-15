from datetime import datetime, timedelta

import pytest

from marketdl.models import DateRange, Frequency, TimeUnit
from marketdl.utils import split_date_range


def test_split_date_range_minute_data(date_range):
    """Test splitting minute-level data into daily chunks"""
    minute_freq = Frequency(multiplier=1, unit=TimeUnit.MINUTE)
    ranges = split_date_range(date_range, minute_freq)
    assert len(ranges) == 1
    assert ranges[0].start == date_range.start
    assert ranges[0].end == date_range.end


def test_split_date_range_hour_data(date_range):
    """Test not splitting hour-level data"""
    hour_freq = Frequency(multiplier=1, unit=TimeUnit.HOUR)
    ranges = split_date_range(date_range, hour_freq)
    assert len(ranges) == 1
    assert ranges[0].start == date_range.start
    assert ranges[0].end == date_range.end


def test_split_date_range_multiple_days():
    """Test splitting multiple days for minute data"""
    start = datetime(2024, 1, 1)
    end = start + timedelta(days=2)
    date_range = DateRange(start=start, end=end)
    minute_freq = Frequency(multiplier=1, unit=TimeUnit.MINUTE)
    ranges = split_date_range(date_range, minute_freq)
    assert len(ranges) == 2
    assert ranges[0].start.date() == start.date()
    assert ranges[1].end.date() == end.date()


def test_split_date_range_quotes_trades(date_range):
    """Test splitting quotes/trades data (no frequency)"""
    ranges = split_date_range(date_range)  # No frequency provided
    assert len(ranges) == 1
    assert ranges[0].start == date_range.start
    assert ranges[0].end == date_range.end
