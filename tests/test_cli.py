from pathlib import Path
from unittest.mock import patch

import pytest
import typer
from typer.testing import CliRunner

from marketdl.cli import app
from marketdl.config import Config


@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    monkeypatch.delenv("POLYGON_API_KEY", raising=False)


def test_init_command(tmp_path, runner):
    result = runner.invoke(app, ["init", "-o", str(tmp_path / "config.yaml")])
    assert result.exit_code == 0
    assert (tmp_path / "config.yaml").exists()


def test_validate_command(tmp_path, mock_config, runner):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(mock_config)
    result = runner.invoke(app, ["validate", str(config_file)])
    assert result.exit_code == 0


def test_download_command(tmp_path, mock_config, runner):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(mock_config)
    result = runner.invoke(app, ["download", "-c", str(config_file), "-k", "test_key"])
    assert result.exit_code == 0
    assert "Download Summary" in result.output


def test_download_dry_run(tmp_path, mock_config, runner):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(mock_config)
    result = runner.invoke(
        app, ["download", "-c", str(config_file), "-k", "test_key", "--dry-run"]
    )
    assert result.exit_code == 0
    assert "No data was downloaded" in result.output


def test_download_with_missing_api_key(tmp_path, mock_config, runner, mock_env):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(mock_config)
    result = runner.invoke(app, ["download", "-c", str(config_path)])
    assert result.exit_code == 1
    assert "API key must be provided" in result.stdout


@patch("marketdl.cli._run_download_process")
def test_download_exception_handling(mock_run, tmp_path, mock_config, runner):
    mock_run.side_effect = Exception("Test error")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(mock_config)
    result = runner.invoke(app, ["download", "-c", str(config_path), "-k", "test_key"])
    assert result.exit_code == 1
    assert "Error: Test error" in result.stdout
