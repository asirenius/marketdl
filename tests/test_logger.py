# tests/test_logger.py
import logging
from pathlib import Path

import pytest

from marketdl.logger import TextLogger


@pytest.fixture
def temp_log_file(tmp_path):
    return tmp_path / "test.log"


def test_logger_initialization():
    logger = TextLogger(name="test")
    assert isinstance(logger._logger, logging.Logger)
    assert logger._logger.level == logging.INFO


def test_logger_with_file(temp_log_file):
    logger = TextLogger(name="test", log_file=temp_log_file)
    logger.info("Test message")
    assert temp_log_file.exists()
    log_content = temp_log_file.read_text()
    assert "Test message" in log_content


def test_logger_with_context(temp_log_file):
    logger = TextLogger(name="test", log_file=temp_log_file)
    logger.info("Test message", key="value", number=42)
    log_content = temp_log_file.read_text()
    assert "Test message" in log_content
    assert "key=value" in log_content
    assert "number=42" in log_content


def test_logger_levels(temp_log_file):
    logger = TextLogger(name="test", level="DEBUG", log_file=temp_log_file)

    logger.debug("Debug message")
    logger.info("Info message")
    logger.warning("Warning message")
    logger.error("Error message")

    log_content = temp_log_file.read_text()
    assert "DEBUG" in log_content
    assert "INFO" in log_content
    assert "WARNING" in log_content
    assert "ERROR" in log_content


def test_logger_level_filtering(temp_log_file):
    logger = TextLogger(name="test", level="INFO", log_file=temp_log_file)
    logger.debug("Debug message")
    log_content = temp_log_file.read_text()
    assert "Debug message" not in log_content
