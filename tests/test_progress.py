import pytest
from rich.table import Table

from marketdl.progress import ConsoleProgress, DownloadStats


def test_download_stats():
    stats = DownloadStats(total=10, completed=5, skipped=2, failed=1)
    table = stats.to_table()
    assert isinstance(table, Table)
    assert stats.in_progress == 0


def test_progress_initialization(mock_logger):
    progress = ConsoleProgress(logger=mock_logger)
    assert progress.stats.total == 0
    assert progress.task_id is not None


def test_progress_tracking(mock_logger):
    progress = ConsoleProgress(logger=mock_logger)
    progress.set_total(1)
    progress.mark_started("test_id")
    assert progress.stats.in_progress == 1
    progress.mark_completed("test_id")
    assert progress.stats.completed == 1


def test_progress_error_handling(mock_logger):
    progress = ConsoleProgress(logger=mock_logger)
    progress.set_total(1)
    progress.mark_started("test_id")
    progress.mark_failed("test_id", "error")
    assert progress.stats.failed == 1
