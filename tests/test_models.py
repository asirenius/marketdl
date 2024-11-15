from datetime import datetime, timedelta

import pytest

from marketdl.models import Artifact, DataType, DateRange, Frequency, TimeUnit


def test_frequency_from_string():
    freq = Frequency.from_string("1minute")
    assert freq.multiplier == 1
    assert freq.unit == TimeUnit.MINUTE


def test_frequency_invalid_string():
    with pytest.raises(ValueError):
        Frequency.from_string("invalid")


def test_daterange_validation(test_dates):
    with pytest.raises(ValueError):
        DateRange(start=test_dates["start"], end=test_dates["invalid_end"])


def test_artifact_id_generation(test_dates, tmp_path):
    artifact = Artifact(
        symbol="TEST",
        data_type=DataType.AGGREGATES,
        start_date=test_dates["start"],
        end_date=test_dates["end"],
        frequency=Frequency(multiplier=1, unit=TimeUnit.MINUTE),
        base_path=tmp_path,
    )
    assert "TEST" in artifact.id
    assert "aggregates" in artifact.id
    assert "1minute" in artifact.id


def test_artifact_empty(test_artifact):
    assert test_artifact.is_empty
    test_artifact.data = []
    assert test_artifact.is_empty


def test_frequency_should_split_by_day():
    """Test frequency split-by-day logic"""
    minute_freq = Frequency(multiplier=1, unit=TimeUnit.MINUTE)
    second_freq = Frequency(multiplier=1, unit=TimeUnit.SECOND)
    hour_freq = Frequency(multiplier=1, unit=TimeUnit.HOUR)
    day_freq = Frequency(multiplier=1, unit=TimeUnit.DAY)

    assert minute_freq.should_split_by_day() is True
    assert second_freq.should_split_by_day() is True
    assert hour_freq.should_split_by_day() is False
    assert day_freq.should_split_by_day() is False


def test_artifact_path_minute_data(test_dates, tmp_path):
    """Test artifact path generation for minute data"""
    artifact = Artifact(
        symbol="TEST",
        data_type=DataType.AGGREGATES,
        start_date=test_dates["start"],
        end_date=test_dates["end"],
        frequency=Frequency(multiplier=1, unit=TimeUnit.MINUTE),
        base_path=tmp_path,
        storage_format="csv",
        compress=True,
    )
    expected_date = test_dates["start"].strftime("%Y-%m-%d")
    assert str(artifact.output_path).endswith(f"{expected_date}.csv.gz")
    assert "TEST/aggregates/1minute" in str(artifact.output_path)


def test_artifact_path_hour_data(test_dates, tmp_path):
    """Test artifact path generation for hour data with date range"""
    start = test_dates["start"]
    end = start + timedelta(days=5)
    artifact = Artifact(
        symbol="TEST",
        data_type=DataType.AGGREGATES,
        start_date=start,
        end_date=end,
        frequency=Frequency(multiplier=1, unit=TimeUnit.HOUR),
        base_path=tmp_path,
        storage_format="csv",
        compress=True,
    )
    expected = f"{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}.csv.gz"
    assert str(artifact.output_path).endswith(expected)
    assert "TEST/aggregates/1hour" in str(artifact.output_path)


def test_artifact_path_quotes(test_dates, tmp_path):
    """Test artifact path generation for quotes data"""
    artifact = Artifact(
        symbol="TEST",
        data_type=DataType.QUOTES,
        start_date=test_dates["start"],
        end_date=test_dates["end"],
        base_path=tmp_path,
        storage_format="csv",
        compress=True,
    )
    expected_date = test_dates["start"].strftime("%Y-%m-%d")
    assert str(artifact.output_path).endswith(f"{expected_date}.csv.gz")
    assert "TEST/quotes" in str(artifact.output_path)
