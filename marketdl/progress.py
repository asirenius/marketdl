from dataclasses import dataclass

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    TaskProgressColumn,
    TextColumn,
    TimeRemainingColumn,
)
from rich.table import Table

from .interfaces import Logger, ProgressTracker


@dataclass
class DownloadStats:
    """Statistics for download progress"""

    total: int = 0
    in_progress: int = 0
    completed: int = 0
    skipped: int = 0
    failed: int = 0

    def to_table(self) -> Table:
        """Generate rich table for display."""
        table = Table(title="Download Summary")
        table.add_column("Metric", style="cyan")
        table.add_column("Count", justify="right", style="green")

        table.add_row("Total", str(self.total))
        table.add_row("Completed", str(self.completed))
        table.add_row("Skipped", str(self.skipped))
        table.add_row("Failed", str(self.failed))
        if self.in_progress > 0:
            table.add_row("In Progress", str(self.in_progress))

        return table


class ConsoleProgress(ProgressTracker):
    """Console-based progress tracking"""

    def __init__(self, logger: Logger):
        """Initialize progress display."""
        self.stats = DownloadStats()
        self.logger = logger
        self.console = Console()

        self.progress = Progress(
            TaskProgressColumn(),
            BarColumn(),
            TextColumn("({task.completed}/{task.total})"),
            TimeRemainingColumn(compact=True, elapsed_when_finished=True),
            console=self.console,
            auto_refresh=False,  # Important for testing
        )
        # Add main progress task
        self.task_id = self.progress.add_task("Downloading", total=0, start=True)

        self.progress.start()

    def set_total(self, total: int) -> None:
        """Set total number of downloads"""
        self.stats.total = total
        self.progress.update(self.task_id, total=total)

    def mark_started(self, id: str) -> None:
        """Mark download as started"""
        self.stats.in_progress += 1
        self.logger.debug("Started processing", id=id)
        self.progress.refresh()

    def mark_completed(self, id: str) -> None:
        """Mark download as completed"""
        self.stats.completed += 1
        self.stats.in_progress -= 1
        self.progress.update(self.task_id, advance=1)
        self.logger.debug("Completed processing", id=id)
        self.progress.refresh()

    def mark_skipped(self, id: str) -> None:
        """Mark download as skipped"""
        self.stats.skipped += 1
        self.progress.update(self.task_id, advance=1)
        self.logger.debug("Skipped existing download", id=id)
        self.progress.refresh()

    def mark_failed(self, id: str, error: str) -> None:
        """Mark download as failed"""
        self.stats.failed += 1
        self.stats.in_progress -= 1
        self.progress.update(self.task_id, advance=1)
        self.logger.error("Failed processing", id=id, error=error)
        self.progress.refresh()

    def close(self) -> None:
        """Close progress display and show summary"""
        self.progress.stop()
        self.console.print()
        self.console.print(self.stats.to_table())
        self.logger.info(
            "Download process completed",
            completed=self.stats.completed,
            skipped=self.stats.skipped,
            failed=self.stats.failed,
        )
