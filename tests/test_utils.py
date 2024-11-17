from datetime import datetime, timedelta

import pytest

from marketdl.models import DateRange, Frequency, TimeUnit
from marketdl.utils import split_date_range


def test_split_date_range_minute_data(date_range):
    """Test splitting minute-level data into daily chunks"""
    minute_freq = Frequency(multiplier=1, unit=TimeUnit.MINUTE)
    ranges = split_date_range(date_range, minute_freq)

    assert len(ranges) == 2

    assert ranges[0].start == date_range.start.replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    assert ranges[0].end == date_range.start.replace(
        hour=23, minute=59, second=59, microsecond=999999
    )

    assert ranges[1].start == (date_range.start + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    assert ranges[1].end == (date_range.start + timedelta(days=1)).replace(
        hour=23, minute=59, second=59, microsecond=999999
    )


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

    assert len(ranges) == 3

    for i in range(3):
        expected_date = start + timedelta(days=i)
        assert ranges[i].start == expected_date.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        assert ranges[i].end == expected_date.replace(
            hour=23, minute=59, second=59, microsecond=999999
        )


def test_split_date_range_quotes_trades(date_range):
    """Test splitting quotes/trades data (no frequency)"""
    ranges = split_date_range(date_range)

    assert len(ranges) == 2

    assert ranges[0].start == date_range.start.replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    assert ranges[0].end == date_range.start.replace(
        hour=23, minute=59, second=59, microsecond=999999
    )

    assert ranges[1].start == (date_range.start + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    assert ranges[1].end == (date_range.start + timedelta(days=1)).replace(
        hour=23, minute=59, second=59, microsecond=999999
    )


def test_split_single_day():
    """Test range within single day"""
    start = datetime(2024, 1, 1, 14, 30)
    end = datetime(2024, 1, 1, 16, 45)
    date_range = DateRange(start=start, end=end)

    ranges = split_date_range(date_range)

    assert len(ranges) == 1
    assert ranges[0].start == start.replace(hour=0, minute=0, second=0, microsecond=0)
    assert ranges[0].end == start.replace(
        hour=23, minute=59, second=59, microsecond=999999
    )


def test_split_partial_days():
    """Test range spanning partial days"""
    start = datetime(2024, 1, 1, 14, 30)
    end = datetime(2024, 1, 3, 9, 15)
    date_range = DateRange(start=start, end=end)

    ranges = split_date_range(date_range)

    assert len(ranges) == 3

    for i in range(3):
        day = start.date() + timedelta(days=i)
        assert ranges[i].start == datetime.combine(day, datetime.min.time())
        assert ranges[i].end == datetime.combine(
            day, datetime.max.time().replace(microsecond=999999)
        )
