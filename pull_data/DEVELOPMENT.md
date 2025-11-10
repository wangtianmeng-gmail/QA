# Development Notes

Developer reference for the Stock Data Downloader project.

## Development Environment

### Python Version
- **Required**: Python 3.12
- **Conda Environment**: `test_dl`

### Activation
```bash
conda activate test_dl
```

### IB API Local Installation
```
/Users/tianmengwang/Applications/twsapi_macunix/IBJts/source/pythonclient/ibapi
```

## Project Architecture

### Directory Structure

```
pull_data/
├── config/          # Configuration files (CSV)
├── src/             # Source code
├── data/            # Output data files (gitignored)
└── reference/       # Reference IB API scripts
```

### Design Principles

1. **Path Auto-Detection**: Script uses `Path(__file__).parent.parent` to find project root
2. **Separation of Concerns**: Configuration separate from code
3. **Extensibility**: Add securities via CSV without code changes

## Code Structure

### Class: `IBDataDownloader`

Inherits from both `EWrapper` (callbacks) and `EClient` (requests).

**Key Methods:**
- `error()` - Handle errors from IB API
- `historicalData()` - Receive data bars
- `historicalDataEnd()` - Data download complete signal

### Class: `StockDataManager`

Manages the download workflow with market hours awareness.

**Key Methods:**
- `load_config()` - Load CSV configuration
- `create_contract()` - Create IB Contract object
- `connect_to_ib()` - Establish connection to TWS/Gateway
- `request_historical_data()` - Request data from IB
- `download_stock_with_pagination()` - Iterative download with real-time adjustment
- `download_stocks()` - Main workflow for all stocks

### Threading Model

The IB API runs in a separate daemon thread:

```python
connection_thread = threading.Thread(
    target=self._run_connection,
    daemon=True
)
connection_thread.start()
```

This allows the main thread to send requests while the API thread handles callbacks.

## IB API Reference

### Connection Details

**TWS (Trader Workstation)**
- Default Port: `7497`
- Use for: Manual trading + API

**IB Gateway**
- Default Port: `4001`
- Use for: API-only (lighter weight)

### Historical Data Request

From IB API documentation:

```python
reqHistoricalData(
    reqId,              # Unique request identifier
    contract,           # Contract object
    endDateTime,        # End date/time ('' = now)
    durationStr,        # How far back (e.g., "10800 S" for 3 hours)
    barSizeSetting,     # Bar size (e.g., "5 secs", "1 min")
    whatToShow,         # Data type (e.g., "TRADES", "MIDPOINT")
    useRTH,             # 1=regular hours, 0=extended hours
    formatDate,         # 1=yyyyMMdd HH:mm:ss, 2=epoch
    keepUpToDate,       # 0=historical only, 1=streaming
    chartOptions        # Additional options (usually [])
)
```

### Duration String Formats

| Unit | Description | Example |
|------|-------------|---------|
| `S` | Seconds | `"10800 S"` = 3 hours |
| `D` | Days | `"1 D"` = 1 day |
| `W` | Weeks | `"1 W"` = 1 week |
| `M` | Months | `"1 M"` = 1 month |
| `Y` | Years | `"1 Y"` = 1 year |

### Bar Size Settings

Valid bar sizes:
- Seconds: `"1 secs"`, `"5 secs"`, `"10 secs"`, `"15 secs"`, `"30 secs"`
- Minutes: `"1 min"`, `"2 mins"`, `"3 mins"`, `"5 mins"`, `"10 mins"`, `"15 mins"`, `"30 mins"`
- Hours: `"1 hour"`, `"2 hours"`, `"3 hours"`, `"4 hours"`, `"8 hours"`
- Days: `"1 day"`, `"1 week"`, `"1 month"`

### What to Show Options

| Option | Description | Works For |
|--------|-------------|-----------|
| `TRADES` | Actual trades | All (best for stocks) |
| `MIDPOINT` | Bid-ask midpoint | All (smooth, no BarCount) |
| `BID` | Bid prices | All |
| `ASK` | Ask prices | All |
| `BID_ASK` | Bid-ask average | All |

## Configuration Constants

Located at the top of `stock_data_downloader_v2.py`:

```python
TRADING_HOURS_RTH = 6.5        # Regular Trading Hours (9:30-16:00 ET)
TRADING_HOURS_EXTENDED = 16.0  # RTH + extended sessions
DOWNLOAD_TIMEOUT_SECONDS = 2   # Timeout for download requests
REQUEST_ID_MULTIPLIER = 10000  # Multiplier for base request ID
IB_HOST = "127.0.0.1"          # IB host address
IB_PORT = 7497                 # IB port (7497=TWS, 4001=Gateway)
IB_CLIENT_ID = 1               # Client ID for IB connection
```

Adjust these as needed for your environment.

## Logging Configuration

The script uses Python's `logging` module with separate loggers:

```python
setup_logging(
    log_file,
    ib_downloader_level=logging.INFO,    # IBDataDownloader logs
    stock_manager_level=logging.INFO,    # StockDataManager logs
    ibapi_level=logging.WARNING          # IB API library logs
)
```

Available levels: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`

**Log output locations:**
- Console (stdout)
- File: `data/stock_download_v2_YYYYMMDD_HHMMSS.log`

## Testing

### Manual Testing Strategy

1. **Single stock test:**
   ```csv
   symbol,sec_type,currency,exchange,total_days,bar_size,what_to_show,initial_end_datetime_et,use_rth
   AAPL,STK,USD,SMART,1,5 secs,TRADES,,1
   ```

2. **Multiple stocks test:**
   ```csv
   AAPL,STK,USD,SMART,1,5 secs,TRADES,,1
   TSLA,STK,USD,SMART,1,5 secs,TRADES,,1
   ```

3. **Index test:**
   ```csv
   SPX,IND,USD,CBOE,1,1 min,MIDPOINT,,1
   ```

4. **RTH vs extended hours test:**
   ```csv
   AAPL,STK,USD,SMART,1,1 min,TRADES,,1  # RTH only
   AAPL,STK,USD,SMART,1,1 min,TRADES,,0  # Extended hours
   ```

### Verification

Check downloaded CSV files:
```bash
cd data/

# View file contents
head -20 AAPL_*.csv

# Count rows
wc -l *.csv

# Check timing data
cat stock_download_v2_*.csv
```

## Common Development Issues

### Issue: "No attribute 'hasGaps'"

**Cause**: Older IB API versions don't include `hasGaps` attribute

**Fix** (already implemented):
```python
"HasGaps": getattr(bar, 'hasGaps', False)  # Default to False
```

### Issue: Connection Timeout

**Debug approach:**
```python
self.logger.info(f"Attempting connection to {host}:{port}")
self.logger.info(f"Connection status: {self.app.isConnected()}")
```

### Issue: Empty Data

**Causes**:
1. Market closed
2. Security not found
3. Missing IB market data subscription
4. Wrong `whatToShow` for indices

**Debug**:
```python
self.logger.info(f"Received {len(self.app.data[req_id])} bars")
```

## Code Style

### Type Hints

Use type hints for clarity:

```python
def save_data_to_csv(
    self,
    req_id: int,
    symbol: str,
    timestamp: Optional[str] = None
) -> Optional[str]:
    pass
```

### Docstrings

Use Google-style docstrings:

```python
def request_historical_data(self, req_id: int, contract: Contract) -> bool:
    """
    Request historical data from IB.

    Args:
        req_id: Unique request identifier
        contract: Contract object for the security

    Returns:
        True if request sent successfully
    """
```

### Logging

Use appropriate log levels:

```python
self.logger.debug("Detailed debugging info")
self.logger.info("Normal operations")
self.logger.warning("Potential issues")
self.logger.error("Actual errors")
```

## Key Algorithm: Iterative Download (v2)

The v2 implementation uses an iterative approach:

1. **Calculate total trading hours** = `total_days × trading_hours_per_day`
   - RTH: 6.5 hours/day (9:30-16:00 ET)
   - Extended: 16.0 hours/day

2. **Download in chunks:**
   - Request chunk (default: 3 hours)
   - Parse actual data received
   - Calculate trading hours from bar count and bar size
   - Use earliest timestamp as next chunk's end time

3. **Progress tracking:**
   - `hours_downloaded += actual_hours_in_chunk`
   - Continue until `hours_downloaded >= total_trading_hours`

4. **Automatic holiday handling:**
   - If no data received (holiday/weekend), no progress made
   - Next chunk automatically adjusts to earlier date
   - No need for explicit holiday calendar

## Rate Limiting

**IB API Limits:**
- Historical data: 60 requests per 10 minutes
- Pacing violations result in temporary bans

**Current implementation:**
- 2-second wait between requests
- ~30 requests per minute (well below limit)

**For large configs:**
```python
# In download_stocks()
time.sleep(wait_time)  # Default: 2 seconds
```

## Git Workflow

### .gitignore

Important patterns:
```
data/              # Don't commit downloaded data
*.log              # Don't commit log files
__pycache__/       # Python cache
*.pyc              # Compiled Python
.DS_Store          # macOS files
```

## References

### Interactive Brokers Documentation
- [IB API Documentation](https://interactivebrokers.github.io/tws-api/)
- [Historical Data API](https://interactivebrokers.github.io/tws-api/historical_data.html)
- [Historical Data Limitations](https://interactivebrokers.github.io/tws-api/historical_limitations.html)

### Python Libraries
- `ibapi` - Interactive Brokers Python API
- `pandas` - Data manipulation and CSV handling
- `pathlib` - Modern path handling (cross-platform)
- `logging` - Structured logging
- `pytz` - Timezone handling (US/Eastern)
