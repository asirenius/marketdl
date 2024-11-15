import asyncio
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional

import typer
from rich import print as rprint
from rich.console import Console

from marketdl.config import Config
from marketdl.containers import Container
from marketdl.coordinator import DownloadCoordinator
from marketdl.models import Artifact, DataType, DateRange, Frequency
from marketdl.utils import split_date_range

app = typer.Typer(
    name="marketdl",
    help="Reproducible market data downloader",
    rich_markup_mode="rich",
)

console = Console()


@app.command()
def init(
    output: Path = typer.Option(
        "config.yaml",
        "--output",
        "-o",
        help="Output path for configuration file",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="Overwrite existing file if present",
    ),
) -> None:
    """Generate a sample configuration file"""
    try:
        if output.exists() and not force:
            raise typer.BadParameter(
                f"File {output} already exists. Use --force to overwrite."
            )

        config_yaml = Config.generate_default()
        output.write_text(config_yaml)
        rprint(f"[green]Configuration file generated at: {output}[/green]")

    except Exception as e:
        rprint(f"[red]Error: {str(e)}[/red]")
        raise typer.Exit(1)


@app.command()
def validate(
    config_file: Path = typer.Argument(
        "config.yaml",
        help="Path to configuration file to validate",
        exists=True,
    ),
) -> None:
    """Validate configuration file"""
    try:
        _ = Config.from_yaml(config_file, api_key="dummy_key_for_validation")
        rprint("[green]Configuration file is valid![/green]")
    except Exception as e:
        rprint(f"[red]Configuration validation failed: {str(e)}[/red]")
        raise typer.Exit(code=1)


@app.command()
def download(
    config_file: Path = typer.Option(
        "config.yaml",
        "--config",
        "-c",
        help="Path to configuration file",
        exists=True,
    ),
    api_key: Optional[str] = typer.Option(
        None,
        "--api-key",
        "-k",
        help="API key for data provider",
        envvar="POLYGON_API_KEY",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Show what would be downloaded without downloading",
    ),
) -> None:
    """Download market data based on configuration"""
    try:
        _validate_api_key(api_key)
        config = Config.from_yaml(config_file, api_key=api_key)
        downloads = _generate_artifacts_list(config)

        if dry_run:
            _handle_dry_run(downloads, config)
            return

        container = _setup_container(config)
        try:
            _run_download_process(container, downloads)
        finally:
            container.unwire()

    except Exception as e:
        rprint(f"[red]Error: {str(e)}[/red]")
        raise typer.Exit(code=1)


def _validate_api_key(api_key: Optional[str]) -> None:
    """Validate the API key is provided"""
    if not api_key:
        raise typer.BadParameter(
            "API key must be provided either via --api-key option or "
            "POLYGON_API_KEY environment variable"
        )


def _generate_artifacts_list(config: Config) -> List[Artifact]:
    """Generate list of artifacts from configuration"""
    downloads = []
    for spec in config.downloads:
        for symbol in spec.symbols:
            for data_type in spec.data_types:
                date_range = DateRange(start=spec.start_date, end=spec.end_date)

                if data_type == DataType.AGGREGATES:
                    downloads.extend(
                        _generate_aggregate_artifacts(
                            symbol,
                            spec.frequencies,
                            date_range,
                            config.storage.base_path,
                            config.storage.format,
                            config.storage.compress,
                        )
                    )
                else:  # QUOTES or TRADES
                    downloads.extend(
                        _generate_market_data_artifacts(
                            symbol,
                            data_type,
                            date_range,
                            config.storage.base_path,
                            config.storage.format,
                            config.storage.compress,
                        )
                    )
    return downloads


def _generate_aggregate_artifacts(
    symbol: str,
    frequencies: List[Frequency],
    date_range: DateRange,
    base_path: Path,
    storage_format: str,
    compress: bool,
) -> List[Artifact]:
    """Generate artifacts for aggregate data"""
    artifacts = []
    for freq in frequencies:
        for range in split_date_range(date_range, freq):
            artifacts.append(
                Artifact(
                    symbol=symbol,
                    data_type=DataType.AGGREGATES,
                    frequency=freq,
                    start_date=range.start,
                    end_date=range.end,
                    base_path=base_path,
                    storage_format=storage_format,
                    compress=compress,
                )
            )
    return artifacts


def _generate_market_data_artifacts(
    symbol: str,
    data_type: DataType,
    date_range: DateRange,
    base_path: Path,
    storage_format: str,
    compress: str,
) -> List[Artifact]:
    """Generate artifacts for quotes/trades data"""
    return [
        Artifact(
            symbol=symbol,
            data_type=data_type,
            start_date=range.start,
            end_date=range.end,
            base_path=base_path,
            storage_format=storage_format,
            compress=compress,
        )
        for range in split_date_range(date_range)
    ]


def _handle_dry_run(downloads: List[Artifact], config: Config) -> None:
    """Handle dry run display of download information"""
    rprint("")
    grouped = _group_downloads_by_symbol(downloads)
    _display_grouped_downloads(grouped)
    _display_dry_run_summary(downloads, config)


def _group_downloads_by_symbol(downloads: List[Artifact]) -> Dict:
    """Group downloads by symbol and data type"""
    grouped = defaultdict(lambda: defaultdict(list))
    for d in downloads:
        grouped[d.symbol][d.data_type].append(d)
    return grouped


def _display_grouped_downloads(grouped: Dict) -> None:
    """Display grouped download information"""
    for symbol, data_types in grouped.items():
        rprint(f"[cyan]Symbol: {symbol}[/cyan]")
        for data_type, artifacts in data_types.items():
            rprint(f"  [blue]{data_type.value}:[/blue]")
            if data_type == DataType.AGGREGATES:
                _display_aggregate_info(artifacts)
            else:
                _display_market_data_info(artifacts)


def _display_aggregate_info(artifacts: List[Artifact]) -> None:
    """Display information for aggregate data"""
    by_freq = defaultdict(list)
    for a in artifacts:
        by_freq[a.frequency].append(a)
    for freq, freq_artifacts in by_freq.items():
        rprint(f"    Frequency: {freq}")
        rprint(
            f"    Date range: {freq_artifacts[0].start_date.date()} to {freq_artifacts[-1].end_date.date()}"
        )
        rprint(f"    Files: {len(freq_artifacts)}")
        rprint(f"    Output directory: {freq_artifacts[0].output_path.parent.parent}\n")


def _display_market_data_info(artifacts: List[Artifact]) -> None:
    """Display information for market data (quotes/trades)"""
    rprint(
        f"    Date range: {artifacts[0].start_date.date()} to {artifacts[-1].end_date.date()}"
    )
    rprint(f"    Files: {len(artifacts)}")
    rprint(f"    Output directory: {artifacts[0].output_path.parent.parent}\n")


def _display_dry_run_summary(downloads: List[Artifact], config: Config) -> None:
    """Display summary information for dry run"""
    rprint(f"\nTotal downloads planned: {len(downloads)}")
    rprint(f"Storage format: {config.storage.format}")
    rprint(f"Base path: {config.storage.base_path}")
    rprint("[yellow]No data was downloaded[/yellow]")


def _setup_container(config: Config) -> Container:
    """Set up and configure dependency injection container"""
    container = Container()
    config_dict = {
        "api": {
            **config.api.model_dump(),
            "service": config.api.service.value,
        },
        "storage": config.storage.model_dump(),
        "logger": config.logger.model_dump(),
        "total_downloads": len(config.downloads),
    }

    container.config.from_dict(config_dict)

    container.wire(modules=[sys.modules[__name__]])
    return container


def _run_download_process(container: Container, downloads: List[Artifact]) -> None:
    """Run the download process using coordinator"""
    coordinator = DownloadCoordinator(
        data_source=container.data_source(),
        storage=container.storage(),
        logger=container.logger(),
        progress=container.progress(),
        max_workers=container.config.max_concurrent.get() or 5,
    )
    asyncio.run(coordinator.start(downloads))
