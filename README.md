<div align="center">
<pre>
███╗   ███╗ █████╗ ██████╗ ██╗  ██╗███████╗████████╗██████╗ ██╗     
████╗ ████║██╔══██╗██╔══██╗██║ ██╔╝██╔════╝╚══██╔══╝██╔══██╗██║     
██╔████╔██║███████║██████╔╝█████╔╝ █████╗     ██║   ██║  ██║██║     
██║╚██╔╝██║██╔══██║██╔══██╗██╔═██╗ ██╔══╝     ██║   ██║  ██║██║     
██║ ╚═╝ ██║██║  ██║██║  ██║██║  ██╗███████╗   ██║   ██████╔╝███████╗
╚═╝     ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝╚══════╝   ╚═╝   ╚═════╝ ╚══════╝
-------------------------------------------------------------------
Reproducible market data downloader
</pre>

<a href="https://github.com/asirenius/marketdl/actions/workflows/tests.yaml"><img src="https://github.com/asirenius/marketdl/actions/workflows/tests.yaml/badge.svg" alt="Tests"/></a>
<a href="https://pypi.org/project/marketdl/"><img src="https://img.shields.io/pypi/v/marketdl" alt="PyPI Latest Release"/></a>
<a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT"/></a>

</div>

Want a simple and reproducible way to download market data? Try `marketdl` - a CLI program that downloads trading data based on a YAML configuration file.

This project evolved from a large collection of Python scripts I used for downloading and managing market data. While primarily developed for my own needs, the tool is designed to be useful for anyone. It is architecured to be expandable with more data providers and data formats in the future.

## Features

- ⚡ Asynchronous concurrent downloads
- 📦 Chunks large downloads automatically
- 📊 Multiple data types (aggregates, quotes, trades)
- 💾 Smart downloads - Only downloads missing files
- 📝 Flexible storage formats (Parquet, CSV)
- 📈 Progress tracking and detailed logging
- 🔄 Configurable retry logic and rate limiting
- ⚙️ YAML-based configuration

For now, the only supported data provider is [Polygon.io](https://polygon.io/).

## Installation
Use `pip` to install `marketdl`:

```bash
pip install marketdl
```

## Usage

`marketdl` supports the following commands:
- `init`: Generate sample configuration
- `validate`: Validate configuration file
- `download`: Download data based on configuration

### Quick Start
1. Get your API key from appropriate website
2. Generate a config file:
```bash
marketdl init
```
3. Edit the generated `config.yaml` with your symbols and date ranges
4. Run the downloader:
```bash
marketdl download --api-key YOUR_API_KEY
marketdl download # (Reads POLYGON_API_KEY)
```

### Examples

```bash
# Generate config
python -m marketdl init -o my_config.yaml

# Validate config
python -m marketdl validate my_config.yaml

# Download with specific config
python -m marketdl download -c my_config.yaml -k YOUR_API_KEY

# Dry run to see what would be downloaded
python -m marketdl download -c my_config.yaml -k YOUR_API_KEY --dry-run
```

## Configuration

Example `config.yaml`:
```yaml
api:
  service: polygon
  timeout: 30
  max_retries: 3
  retry_delay: 1.0

storage:
  base_path: data
  format: parquet
  compress: true

downloads:
  - symbols:
      - AAPL
      - MSFT
    data_types:
      - aggregates
      - quotes
      - trades
    frequencies:
      - 1minute
    start_date: '2024-01-01'
    end_date: '2024-01-31'
  - symbols:
      - C:EURUSD
      - X:BTCUSD
    data_types:
      - aggregates
    frequencies:
      - 1week
      - 1month
    start_date: '2018-01-01'
    end_date: '2023-12-31'

max_concurrent: 5
```

Names of symbols will match the data provider. For [Polygon.io](https://polygon.io/), see [Screener](https://polygon.io/quote/tickers).

## Data Storage

Data is stored in a hierarchical structure by symbol, data type, and frequency. Second-level and minute-level data is automatically split into daily files, while hourly and higher frequencies can span multiple days in a single file. File names contain the date or date range of the data.

```
data/
├── C:EURUSD/
│   └── aggregates/
│       ├── 4hour/
│       │   └── 2023-12-26_2023-12-31.csv.gz     # Multi-day data for lower frequencies
│       └── 5minute/
│           ├── 2023-12-26.csv.gz                # One file per day for minute data
│           ├── 2023-12-27.csv.gz
│           └── 2023-12-28.csv.gz
└── X:BTCUSD/
    └── aggregates/
        └── 5minute/
            └── 2023-12-26.csv.gz
```

## License

MIT License