from datetime import datetime, timedelta
from typing import List, Optional

from marketdl.models import DateRange, Frequency


def split_date_range(
    date_range: DateRange, frequency: Optional[Frequency] = None
) -> List[DateRange]:
    """Split date range into daily chunks based on frequency"""
    should_split = (frequency and frequency.should_split_by_day()) or not frequency

    if not should_split:
        return [date_range]

    daily_ranges = []

    current_date = date_range.start.replace(hour=0, minute=0, second=0, microsecond=0)

    end_date = date_range.end.replace(hour=23, minute=59, second=59, microsecond=999999)

    while current_date <= end_date:
        day_end = current_date.replace(
            hour=23, minute=59, second=59, microsecond=999999
        )

        daily_ranges.append(DateRange(start=current_date, end=day_end))

        current_date = (current_date + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

    return daily_ranges
