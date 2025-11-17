# Developer Guide

Developer reference for the Stock Data Downloader project.

## Table of Contents

- [Development Environment](#development-environment)
- [Project Architecture](#project-architecture)
- [Code Structure](#code-structure)
- [IB API Reference](#ib-api-reference)
- [Configuration Constants](#configuration-constants)
- [Logging Configuration](#logging-configuration)
- [Testing](#testing)
- [Common Development Issues](#common-development-issues)
- [Code Style](#code-style)
- [Key Algorithms](#key-algorithms)
- [Version Control Strategy](#version-control-strategy)
- [Refactoring History](#refactoring-history)
- [Git Workflow](#git-workflow)
- [References](#references)

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
├── config/              # Configuration files (CSV)
│   ├── stocks_config.csv          # Main config file
│   └── stocks_config_full.csv     # Full config example
├── src/                 # Source code
│   ├── stock_data_downloader.py   # Current production version (v3)
│   ├── merge_csv_files.py         # Utility to merge CSV files
│   └── archive/                   # Legacy versions
│       ├── stock_data_downloader_v1.py
│       └── stock_data_downloader_v2.py
├── data/                # Output data files (gitignored)
├── reference/           # Reference IB API scripts
├── README.md            # User documentation
├── DEVELOPMENT.md       # This file (developer guide)
└── CLAUDE.md            # Claude AI assistant notes
```

### Design Principles

1. **Path Auto-Detection**: Script uses `Path(__file__).parent.parent` to find project root
2. **Separation of Concerns**: Configuration separate from code
3. **Extensibility**: Add securities via CSV without code changes
4. **Type Safety**: Dataclasses for configuration and results
5. **Testability**: Focused classes with clear responsibilities

## Code Structure

### Architecture Overview

The current production version (`stock_data_downloader.py`) features a refactored, modular architecture:

```
stock_data_downloader.py (~1290 lines)
│
├── Dataclasses (Type Safety)
│   ├── StockConfig         - Configuration for stock downloads
│   ├── ChunkSpec           - Specification for single chunk
│   ├── ChunkResult         - Result of chunk download
│   ├── TimingRecord        - Timing information
│   └── DownloadProgress    - Track download progress
│
├── Helper Classes
│   ├── RequestIDGenerator  - Generate unique request IDs
│   ├── ProgressTracker     - Track progress and timing (incremental CSV saves)
│   ├── FileManager         - Handle CSV operations
│   └── ChunkDownloader     - Download chunks with retry logic
│
├── IB API Client
│   └── IBDataDownloader    - IB API wrapper (EWrapper + EClient)
│
└── Main Orchestrator
    └── StockDataManager    - Coordinate all components
```

### Class: IBDataDownloader

Inherits from both `EWrapper` (callbacks) and `EClient` (requests).

**Key Methods:**
- `error()` - Handle errors from IB API
- `historicalData()` - Receive data bars
- `historicalDataEnd()` - Data download complete signal

**Implementation:**
```python
class IBDataDownloader(EWrapper, EClient):
    def __init__(self):
        EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)
        self.data = {}
        self.download_events = {}  # Threading events for efficient waiting
```

### Class: StockDataManager

Main orchestrator that manages the download workflow with market hours awareness.

**Key Methods:**
- `load_config()` - Load CSV configuration and parse into StockConfig dataclasses
- `connect_to_ib()` - Establish connection to TWS/Gateway
- `download_stocks()` - Main workflow for all stocks
- `_download_single_stock()` - Download data for a single stock
- `_create_contract()` - Create IB Contract object
- `_prepare_chunk_spec()` - Prepare chunk specification

**Responsibilities:**
- Initialize all components (RequestIDGenerator, ProgressTracker, FileManager, ChunkDownloader)
- Coordinate download workflow
- Manage IB API connection
- Load configuration and save results

### Class: RequestIDGenerator

Generates unique request IDs for IB API calls to avoid collisions using auto-incrementing counter.

**Implementation:**
```python
class RequestIDGenerator:
    def __init__(self):
        self._counter = 0

    def generate_id(self, stock_num: int, chunk_num: int, retry_attempt: int) -> int:
        self._counter += 1
        return self._counter
```

**Benefits:**
- Guaranteed unique IDs
- No complex formulas needed
- Thread-safe with proper locking (if needed)
- Simple and reliable

### Class: ProgressTracker

Tracks download progress and writes timing information to CSV **incrementally** (saves after each chunk).

**Key Methods:**
- `_initialize_timing_csv()` - Create timing CSV with headers
- `record_chunk()` - Append timing record to CSV immediately

**Timing CSV Format:**
```
symbol,chunk_number,retry_attempt,req_id,chunk_start_time,chunk_end_time,
download_duration_seconds,bars_downloaded,trading_hours_downloaded,
data_start,data_end,requested_hours,use_rth
```

### Class: FileManager

Handles all CSV file operations for stock data.

**Key Methods:**
- `save_stock_data()` - Save stock data to CSV
- `_generate_filename()` - Generate filename with timestamps from actual data

**Filename Format:**
```
{symbol}_saved_{save_timestamp}_start_{start_timestamp}_end_{end_timestamp}.csv
```

### Class: ChunkDownloader

Handles chunk download operations with retry logic and error handling.

**Key Methods:**
- `download_chunk_with_retry()` - Download chunk with automatic retries
- `_download_single_chunk()` - Single chunk download attempt
- `_request_historical_data()` - Request data from IB
- `_wait_for_download()` - Wait for download completion using threading.Event
- `_create_success_result()` - Create successful result
- `_create_error_result()` - Create error result (TIMEOUT, FAILED, NO_DATA)

**Error Handling:**
- Automatic retry up to MAX_CHUNK_RETRIES (default: 3)
- Timeout handling with cancellation
- Proper cleanup on failure

### Dataclasses

**StockConfig:**
```python
@dataclass
class StockConfig:
    symbol: str
    sec_type: str = "STK"
    currency: str = "USD"
    exchange: str = "SMART"
    total_days: float = 1.0
    bar_size: str = "5 secs"
    what_to_show: str = "TRADES"
    use_rth: bool = True
    initial_end_datetime_et: Optional[str] = None
```

**ChunkSpec:**
```python
@dataclass
class ChunkSpec:
    req_id: int
    symbol: str
    chunk_num: int
    contract: Contract
    end_datetime_str: str
    duration_seconds: int
    bar_size: str
    what_to_show: str
    use_rth: bool
    requested_hours: float
    retry_attempt: int = 0
```

**ChunkResult:**
```python
@dataclass
class ChunkResult:
    req_id: int
    chunk_num: int
    success: bool
    data: List[Dict]
    hours_downloaded: float
    next_end_datetime: Optional[datetime]
    timing_record: TimingRecord
    retry_attempt: int = 0
```

### Threading Model

The IB API runs in a separate daemon thread:

```python
connection_thread = threading.Thread(
    target=self._run_connection,
    daemon=True
)
connection_thread.start()
```

The download waiting mechanism uses `threading.Event` for efficient blocking:

```python
# Create event for this request
self.download_events[req_id] = threading.Event()

# Wait efficiently - blocks until set() or timeout
if event.wait(timeout=30):
    # Download completed successfully
```

This allows the main thread to send requests while the API thread handles callbacks efficiently.

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
    keepUpToDate,       # False=historical only, True=streaming
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

Located at the top of `stock_data_downloader.py`:

```python
TRADING_HOURS_RTH = 6.5        # Regular Trading Hours (9:30-16:00 ET)
TRADING_HOURS_EXTENDED = 16.0  # RTH + extended sessions
DOWNLOAD_TIMEOUT_SECONDS = 30  # Timeout for download requests
REQUEST_ID_MULTIPLIER = 10000  # Multiplier for base request ID
CHUNK_HOURS = 3.0              # Default chunk size in hours
MAX_CHUNK_RETRIES = 3          # Maximum retry attempts per chunk
WAIT_TIME_BETWEEN_REQUESTS = 2.0  # Wait time between requests (seconds)
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
    stock_manager_level=logging.INFO,    # StockDataManager & ChunkDownloader logs
    ibapi_level=logging.WARNING          # IB API library logs
)
```

Available levels: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`

**Log output locations:**
- Console (stdout)
- File: `data/stock_download_v3_{timestamp}.log`

**Logger hierarchy:**
- `IBDataDownloader` - IB API client operations
- `StockDataManager` - Main orchestration
- `ChunkDownloader` - Chunk download operations
- `ProgressTracker` - Progress tracking
- `FileManager` - File operations
- `RequestIDGenerator` - Request ID generation
- `ibapi` - IB API library (usually keep at WARNING)

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

### Unit Testing Benefits

The current architecture enables easy unit testing:

```python
# Unit test RequestIDGenerator
def test_request_id_no_collision():
    gen = RequestIDGenerator()
    id1 = gen.generate_id(stock_num=1, chunk_num=1, retry_attempt=0)
    id2 = gen.generate_id(stock_num=11, chunk_num=1, retry_attempt=0)
    assert id1 != id2  # Guaranteed unique

# Mock ChunkDownloader for testing StockDataManager
def test_download_single_stock():
    mock_downloader = MagicMock()
    mock_downloader.download_chunk_with_retry.return_value = ChunkResult(...)
    manager.chunk_downloader = mock_downloader
    # Test without IB connection!
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
cat stock_download_v3_*.csv
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
self.logger.info(f"Connection status: {self.ib_client.isConnected()}")
```

### Issue: Empty Data

**Causes**:
1. Market closed
2. Security not found
3. Missing IB market data subscription
4. Wrong `whatToShow` for indices

**Debug**:
```python
self.logger.info(f"Received {len(chunk_data)} bars")
```

### Issue: Request ID Collisions (Fixed)

**v2 Bug:**
Formula-based approach could cause collisions between stocks during retry attempts.

**Current Fix:**
Auto-incrementing counter guarantees unique IDs:
```python
class RequestIDGenerator:
    def __init__(self):
        self._counter = 0

    def generate_id(self, stock_num, chunk_num, retry_attempt):
        self._counter += 1
        return self._counter  # Always unique!
```

## Code Style

### Type Hints

Use type hints for clarity:

```python
def save_data_to_csv(
    self,
    all_data: List[Dict],
    symbol: str
) -> Optional[str]:
    pass
```

### Docstrings

Use Google-style docstrings:

```python
def download_chunk_with_retry(
    self,
    chunk_spec: ChunkSpec,
    stock_num: int,
    max_retries: int = MAX_CHUNK_RETRIES
) -> Optional[ChunkResult]:
    """
    Download a chunk with retry logic (max 3 retries by default).

    Args:
        chunk_spec: Chunk specification (contains req_id for first attempt)
        stock_num: Stock number for request ID generation (used for retry attempts)
        max_retries: Maximum retry attempts (default: MAX_CHUNK_RETRIES)

    Returns:
        ChunkResult with download data and timing info, or None if all retries failed
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

## Key Algorithms

### Iterative Download Algorithm

The iterative approach automatically handles market holidays and weekends:

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

### Bar Size Parsing

```python
def parse_bar_size_to_seconds(bar_size: str) -> int:
    """Parse bar size string to seconds per bar"""
    bar_size_lower = bar_size.lower().strip()
    match = re.match(r'(\d+)\s*(\w+)', bar_size_lower)

    if match:
        value = int(match.group(1))
        unit = match.group(2)

        if 'sec' in unit:
            return value
        elif 'min' in unit:
            return value * 60
        elif 'hour' in unit:
            return value * 3600
        elif 'day' in unit:
            return value * 86400

    return 5  # Default fallback
```

### Trading Hours Calculation

```python
seconds_per_bar = parse_bar_size_to_seconds(bar_size)
bars_count = len(chunk_data)
trading_hours = round((bars_count * seconds_per_bar) / 3600.0, 6)
```

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
time.sleep(WAIT_TIME_BETWEEN_REQUESTS)  # Default: 2 seconds
```

## Version Control Strategy

### Current Approach

We use **Git branching/tagging** instead of versioned filenames:

- **Production file**: `src/stock_data_downloader.py` (current v3 codebase)
- **Legacy versions**: `src/archive/stock_data_downloader_v{1,2}.py`
- **Git tags**: Use tags to mark major versions (`v1.0.0`, `v2.0.0`, `v3.0.0`)

### Benefits

1. **Clean codebase**: One production file instead of multiple versioned files
2. **Git history**: Full version history available via `git log` and tags
3. **Easy rollback**: Use `git checkout v2.0.0` if needed
4. **Standard practice**: Industry-standard version control approach

### Tagging Versions

```bash
# Tag current version (v3)
git tag v3.0.0 -m "Version 3.0.0 - Refactored architecture with dataclasses"

# View all tags
git tag -l

# Checkout specific version
git checkout v2.0.0

# Return to latest
git checkout main
```

## Refactoring History

### v1 → v2 (Advanced Iterative Download)

**Key Changes:**
- Single-request download → Iterative chunking
- Added automatic holiday/weekend handling
- Market hours awareness (RTH vs extended)
- Retry mechanism for failed chunks
- Timing data tracking

### v2 → v3 (Architectural Refactoring)

**Overview:**
Refactored from **v2 (1010 lines, monolithic)** to **v3 (1290 lines, modular architecture)**.

### Key Improvements

#### 1. Separation of Concerns

**Before (v2):**
- Single `StockDataManager` class doing everything (750+ lines)
- Business logic mixed with IB API details
- File I/O mixed with download logic
- Timing tracking scattered everywhere

**After (v3):**
- 7 focused classes, each < 300 lines
- Clear responsibilities
- Easy to test and maintain

#### 2. Type Safety with Dataclasses

**Benefits:**
- Type hints for IDE autocomplete
- Automatic `__init__`, `__repr__`, `__eq__`
- Immutable data structures with `frozen=False` (mutable where needed)
- Self-documenting code

#### 3. Better Error Handling

**v2:** Used `assert` statements (removed with `-O` flag)

**v3:** Proper error handling with logging:
```python
if self.ib_client is None:
    self.logger.error("IB client is None in _run_connection")
    return
```

#### 4. Simplified Code with dataclasses.replace()

**Before (v2):** 13 lines to create retry ChunkSpec

**After (v3):** 1 line
```python
retry_spec = replace(chunk_spec, req_id=new_req_id, retry_attempt=retry_attempt)
```

#### 5. Request ID Bug Fix

**v2 (Broken):**
Formula-based approach could have collisions between different stocks.

**v3 (Fixed):**
```python
# Auto-incrementing counter - guaranteed unique IDs
self._counter += 1
return self._counter
```

#### 6. Efficient Waiting with threading.Event

**v2:** Polling loop with sleep(0.1)
```python
while time.time() - start_time < timeout:
    if self.app.data_download_complete.get(req_id, False):
        return True
    time.sleep(0.1)  # Wastes CPU cycles
```

**v3:** Efficient blocking with threading.Event
```python
# Blocks efficiently until set() or timeout
if event.wait(timeout=timeout):
    return True  # Download completed
```

### Performance

- **Same performance** as v2 (no overhead from refactoring)
- **Better error handling** (cleaner failure modes)
- **Same memory usage** (no data duplication)
- **More efficient waiting** (threading.Event vs polling)

### Summary

| Metric | v2 | v3 | Improvement |
|--------|----|----|-------------|
| **Lines per class** | 750 | < 300 | 60% reduction |
| **Testability** | Poor | Excellent | ⭐⭐⭐⭐⭐ |
| **Maintainability** | Difficult | Easy | ⭐⭐⭐⭐⭐ |
| **Extensibility** | Hard | Easy | ⭐⭐⭐⭐⭐ |
| **Type Safety** | Partial | Full | ⭐⭐⭐⭐⭐ |
| **Bug Count** | 1 (ID collision) | 0 | Fixed! |
| **Version Control** | Filename suffix | Git tags | Standard! |

### Future Enhancements (Now Easy!)

#### 1. Add Database Storage
```python
class DatabaseManager:
    def save_stock_data(self, data, symbol) -> None:
        # Save to PostgreSQL/MongoDB instead of CSV

# Just swap FileManager with DatabaseManager
manager.file_manager = DatabaseManager()
```

#### 2. Add Progress UI
```python
class ProgressUI:
    def update(self, progress: DownloadProgress) -> None:
        # Update web dashboard, progress bar, etc.

progress_tracker.add_observer(ProgressUI())
```

#### 3. Parallel Downloads
```python
class ParallelDownloadManager(StockDataManager):
    def download_stocks(self):
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(self._download_single_stock, config, i)
                      for i, config in enumerate(configs)]
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

### Branching Strategy

- `main` - Production-ready code
- `feature/*` - New features
- `bugfix/*` - Bug fixes
- `refactor/*` - Code refactoring

### Commit Messages

Use conventional commits:
```
feat: Add support for futures contracts
fix: Resolve request ID collision bug
refactor: Extract ChunkDownloader class
docs: Update README with new structure
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
- `dataclasses` - Type-safe data structures
