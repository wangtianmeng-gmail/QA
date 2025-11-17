# Stock Data Downloader

A Python application for downloading high-frequency historical stock data from Interactive Brokers API with iterative chunk-based downloading and automatic market hours handling.

## Features

- **Smart Iterative Download**: Automatically adjusts chunk sizes based on actual data received
- **No Holiday Calendar Needed**: Handles market holidays and weekends automatically
- **Market Hours Aware**: Supports both regular trading hours (RTH) and extended hours
- **Configurable**: Easy stock/index management via CSV configuration
- **Extensible**: Support for stocks, ETFs, and indices (SPX, VIX, etc.)
- **Robust**: Retry logic, comprehensive logging, and thread-safe API connection
- **Type-Safe**: Dataclass-based architecture for better code quality
- **Modular Design**: Clean separation of concerns for easy maintenance

## Quick Start

### Prerequisites

- Python 3.12
- Conda environment: `test_dl`
- Interactive Brokers TWS or IB Gateway running
- `ibapi` Python package

### Installation

1. **Activate conda environment:**
   ```bash
   conda activate test_dl
   ```

2. **Install dependencies:**
   ```bash
   pip install pandas pytz ibapi
   ```

3. **Ensure IB TWS/Gateway is running:**
   - TWS: Port 7497 (default)
   - IB Gateway: Port 4001

### Usage

**Run the downloader:**
```bash
cd pull_data/src
python3.12 stock_data_downloader.py
```

**With custom paths:**
```bash
python3.12 stock_data_downloader.py ../config/custom_config.csv ../data/custom_output
```

### Output

Data files are saved to `data/` directory with detailed timing information:

```
data/
├── AAPL_saved_20250127_143000_start_20250126_093000_end_20250127_160000.csv
├── TSLA_saved_20250127_143500_start_20250126_093000_end_20250127_160000.csv
├── stock_download_v3_20250127_143000.csv    # Timing data for all downloads
└── stock_download_v3_20250127_143000.log    # Log file
```

## Configuration

Edit `config/stocks_config.csv` to manage stocks:

```csv
symbol,sec_type,currency,exchange,total_days,bar_size,what_to_show,initial_end_datetime_et,use_rth
AAPL,STK,USD,SMART,5,5 secs,TRADES,20250127 16:00:00,1
TSLA,STK,USD,SMART,3,5 secs,TRADES,,1
SPX,IND,USD,CBOE,1,1 min,MIDPOINT,,1
```

### Configuration Parameters

| Parameter | Description | Examples | Notes |
|-----------|-------------|----------|-------|
| `symbol` | Ticker symbol | AAPL, TSLA, SPX, VIX | |
| `sec_type` | Security type | STK (stocks), IND (indices) | |
| `currency` | Currency code | USD, EUR, GBP | |
| `exchange` | Exchange | SMART (stocks), CBOE (indices) | |
| `total_days` | Trading days to download | 1, 5, 10 | Not calendar days |
| `bar_size` | Bar interval | 5 secs, 1 min, 1 hour | Smaller = more data |
| `what_to_show` | Data type | TRADES, MIDPOINT, BID, ASK | |
| `initial_end_datetime_et` | End time (ET) | 20250127 16:00:00 | Leave empty for current time |
| `use_rth` | Regular trading hours | 1 (RTH), 0 (extended) | 1 = 9:30-16:00 ET only |

### Common Configurations

**High-frequency stock data (5-second bars):**
```csv
AAPL,STK,USD,SMART,5,5 secs,TRADES,,1
```

**Intraday minute data:**
```csv
AAPL,STK,USD,SMART,10,1 min,TRADES,,1
```

**Index data (use MIDPOINT for smoother data):**
```csv
SPX,IND,USD,CBOE,5,1 min,MIDPOINT,,1
VIX,IND,USD,CBOE,5,1 min,MIDPOINT,,1
```

**Extended hours trading:**
```csv
AAPL,STK,USD,SMART,1,1 min,TRADES,,0
```

### Understanding `what_to_show`

| Value | Description | Best For | Includes BarCount? |
|-------|-------------|----------|-------------------|
| `TRADES` | Actual trades | Stocks, active securities | Yes |
| `MIDPOINT` | Bid-ask midpoint | Indices, smoother data | No |
| `BID` | Bid prices | Buyer analysis | No |
| `ASK` | Ask prices | Seller analysis | No |

**Recommendation:**
- **Stocks/ETFs**: Use `TRADES`
- **Indices (SPX, VIX)**: Try `TRADES` first, use `MIDPOINT` if data is sparse

## How It Works

The downloader uses an **iterative chunk-based approach** with modern architecture:

1. **Request chunk** (e.g., 3 hours of data ending at specified time)
2. **Analyze actual data received** (e.g., got 2.5 hours of trading data)
3. **Calculate next chunk** using the earliest timestamp from received data
4. **Repeat** until total trading days downloaded

This automatically handles:
- Market holidays (no data = no progress)
- Weekends (no data = no progress)
- Early market closes
- Extended hours vs regular hours

### Architecture

The current version features a refactored, modular architecture:

- **Separation of concerns**: Focused classes with clear responsibilities
- **Type safety**: Dataclasses for configuration and results
- **Better error handling**: Comprehensive logging and retry logic (max 3 retries per chunk)
- **Testability**: Isolated components for easier unit testing
- **Efficient waiting**: Uses `threading.Event` for low-CPU blocking
- **Request ID uniqueness**: Auto-incrementing counter prevents collisions

**Core Components:**
- `IBDataDownloader` - IB API client wrapper
- `StockDataManager` - Main orchestrator
- `ChunkDownloader` - Chunk download with retry logic
- `ProgressTracker` - Incremental timing data saves
- `FileManager` - CSV file operations
- `RequestIDGenerator` - Unique ID generation

See `DEVELOPMENT.md` for detailed architecture information.

## Output Format

### Stock Data CSV

Each CSV contains OHLCV data with timestamps:

| Column | Description | Available For |
|--------|-------------|---------------|
| `Date` | Timestamp (index) | All |
| `Open` | Opening price | All |
| `High` | Highest price | All |
| `Low` | Lowest price | All |
| `Close` | Closing price | All |
| `Volume` | Trading volume | All |
| `WAP` | Weighted Average Price | All |
| `BarCount` | Number of trades in bar | `TRADES` only |
| `HasGaps` | Data completeness flag | All |

### Timing Data CSV

The aggregated timing file (`stock_download_v3_YYYYMMDD_HHMMSS.csv`) contains:

- Download performance metrics for each chunk
- Bars downloaded per chunk
- Trading hours downloaded
- Download duration in seconds
- Requested vs actual hours
- Retry attempt information

## Troubleshooting

### Connection Issues

**Error: "Failed to connect to Interactive Brokers"**

1. Check TWS/Gateway is running
2. Verify API connections enabled in TWS settings:
   - Configure → Settings → API → Settings
   - Enable "Enable ActiveX and Socket Clients"
3. Check port number:
   - TWS: 7497
   - IB Gateway: 4001

**Change port in code if needed:**
Edit `src/stock_data_downloader.py`:
```python
IB_PORT = 4001  # For IB Gateway
```

### Data Issues

**No Data Downloaded**

Possible causes:
1. Market is closed (only historical data within IB limits available)
2. Security symbol not found
3. Missing market data subscription
4. Wrong exchange for security type

**Sparse Data for Indices**

If SPX or VIX shows gaps with `TRADES`:

```csv
# Change from:
SPX,IND,USD,CBOE,1,1 min,TRADES,,1

# To:
SPX,IND,USD,CBOE,1,1 min,MIDPOINT,,1
```

**Timeout Issues**

If downloads timeout frequently:
1. Increase timeout in code (default: 30 seconds)
2. Reduce chunk size (default: 3 hours)
3. Check network connection to IB

Edit constants in `src/stock_data_downloader.py`:
```python
DOWNLOAD_TIMEOUT_SECONDS = 60  # Increase timeout
CHUNK_HOURS = 2.0              # Reduce chunk size
```

### Logging

Logs are saved to `data/stock_download_v3_YYYYMMDD_HHMMSS.log`

**Adjust log levels in code:**
```python
setup_logging(
    log_file,
    ib_downloader_level=logging.DEBUG,    # More detailed IB API logs
    stock_manager_level=logging.INFO,     # Stock download progress
    ibapi_level=logging.WARNING           # IB library logs (usually keep WARNING)
)
```

## Project Structure

```
pull_data/
├── README.md                    # This file (user guide)
├── DEVELOPMENT.md               # Developer reference
├── CLAUDE.md                    # Claude AI assistant notes
│
├── config/
│   ├── stocks_config.csv        # Main stock configuration
│   └── stocks_config_full.csv   # Full configuration example
│
├── src/
│   ├── stock_data_downloader.py # Current production version (v3 architecture)
│   ├── merge_csv_files.py       # Utility to merge CSV files
│   └── archive/                 # Legacy versions
│       ├── stock_data_downloader_v1.py
│       └── stock_data_downloader_v2.py
│
├── data/                        # Output directory (auto-created, gitignored)
│   ├── *.csv                    # Downloaded data files
│   └── *.log                    # Log files
│
└── reference/
    └── *.py                     # Reference IB API scripts
```

## Version History

### Current Version (v3 Architecture)
**File**: `src/stock_data_downloader.py`

**What's New:**
- Refactored architecture with separation of concerns
- Dataclasses for type safety and better code organization
- Improved error handling and retry logic (no more assertions)
- Better testability with focused classes
- Request ID collision bug fix (auto-incrementing counter)
- Efficient waiting with `threading.Event` (reduced CPU usage)
- Production-ready error handling
- Same performance and features as v2

### v2 (Archived)
**File**: `src/archive/stock_data_downloader_v2.py`

**Features:**
- Iterative download with real-time chunk adjustment
- Automatic handling of market holidays without calendar
- Market hours awareness (RTH vs extended)
- Progress tracking based on actual data received
- Configurable `use_rth` support
- CSV files include start and end timestamps from actual data
- Aggregated timing data in single CSV

### v1 (Archived)
**File**: `src/archive/stock_data_downloader_v1.py`

**Features:**
- Basic historical data download
- Single-request download per stock
- CSV configuration support
- Fixed duration and bar size

**Note**: Use Git tags to reference specific versions (`v1.0.0`, `v2.0.0`, `v3.0.0`). See `DEVELOPMENT.md` for version control details.

## Best Practices

### Recommended Settings

**For individual stocks (high-frequency):**
```csv
AAPL,STK,USD,SMART,5,5 secs,TRADES,,1
```

**For ETFs (minute bars):**
```csv
SPY,STK,USD,SMART,10,1 min,TRADES,,1
```

**For indices:**
```csv
SPX,IND,USD,CBOE,5,1 min,MIDPOINT,,1
```

### Avoiding Rate Limits

The script includes a 2-second wait between requests. For large configurations:

1. Limit to 10-20 securities per run
2. Monitor IB API rate limits (60 requests per 10 minutes for historical data)
3. Run during off-peak hours if possible

### Data Quality

1. **Check timing CSV** for successful downloads (look for errors like TIMEOUT, NO_DATA)
2. **Verify bar counts** match expectations (trading hours × bars per hour)
3. **Review logs** for warnings or errors
4. **Check retry attempts** - multiple retries may indicate connectivity issues

## Utilities

### Merge CSV Files

Use `merge_csv_files.py` to combine multiple downloaded CSV files:

```bash
cd pull_data/src
python3.12 merge_csv_files.py
```

This utility merges all CSV files in the data directory, removing duplicates and sorting by date.

## Development

For developers working on the codebase:

- See `DEVELOPMENT.md` for architecture details, testing strategies, and contribution guidelines
- Code follows modular design with dataclasses for type safety
- All classes are isolated and testable
- Use type hints and Google-style docstrings
- Follow conventional commit messages

## License

This project is for personal use with Interactive Brokers API.

## References

- [Interactive Brokers API Documentation](https://interactivebrokers.github.io/tws-api/)
- [Historical Data Limitations](https://interactivebrokers.github.io/tws-api/historical_limitations.html)
- [IB API Python Client](https://interactivebrokers.github.io/tws-api/introduction.html#gsc.tab=0)
- IB API Local Installation: `/Users/tianmengwang/Applications/twsapi_macunix/IBJts/source/pythonclient/ibapi`

## Support

For issues or questions:
1. Check logs in `data/` directory
2. Review `DEVELOPMENT.md` for common issues
3. Verify IB TWS/Gateway connection and settings
4. Check IB API rate limits and historical data availability
