from datetime import timedelta
from typing import List, Optional

from marketdl.models import DateRange, Frequency


def split_date_range(
    date_range: DateRange, frequency: Optional[Frequency] = None
) -> List[DateRange]:
    """Split date range into appropriate chunks based on frequency"""
    should_split = (frequency and frequency.should_split_by_day()) or not frequency

    if not should_split:
        return [date_range]

    daily_ranges = []
    current_date = date_range.start

    while current_date < date_range.end:
        # Get end of current day
        next_date = min(
            (current_date + timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            ),
            date_range.end,
        )

        # Ensure we don't create zero-length ranges
        if next_date > current_date:
            daily_ranges.append(DateRange(start=current_date, end=next_date))

        current_date = next_date

    return daily_ranges
