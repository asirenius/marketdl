import logging
from pathlib import Path
from typing import Any, Dict, Optional

from marketdl.interfaces import Logger


class TextLogger(Logger):
    """Simple text-based logger implementation"""

    def __init__(
        self,
        name: str = "marketdl",
        level: str = "INFO",
        log_file: Optional[Path] = None,
        format: str = "%(asctime)s - %(levelname)s - %(message)s",
    ):
        self._logger = logging.getLogger(name)
        self._logger.setLevel(level)

        self._logger.handlers.clear()

        formatter = logging.Formatter(format)

        if log_file:
            log_file.parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            self._logger.addHandler(file_handler)
        else:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self._logger.addHandler(console_handler)

    def _format_context(self, context: Dict[str, Any]) -> str:
        """Format context data into readable string"""
        if not context:
            return ""
        return " | " + " | ".join(f"{k}={v}" for k, v in sorted(context.items()))

    def debug(self, msg: str, **context) -> None:
        """Log debug message with context"""
        self._logger.debug(f"{msg}{self._format_context(context)}")

    def info(self, msg: str, **context) -> None:
        """Log info message with context"""
        self._logger.info(f"{msg}{self._format_context(context)}")

    def warning(self, msg: str, **context) -> None:
        """Log warning message with context"""
        self._logger.warning(f"{msg}{self._format_context(context)}")

    def error(self, msg: str, **context) -> None:
        """Log error message with context"""
        self._logger.error(f"{msg}{self._format_context(context)}")
