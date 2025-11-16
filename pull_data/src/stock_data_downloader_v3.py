#!/usr/bin/env python3.12
"""
Stock Historical Data Downloader using Interactive Brokers API - Version 3

This script downloads high-frequency historical stock data from Interactive Brokers
over multiple trading days using an iterative approach that dynamically adjusts based on actual
data received. This eliminates the need for a holiday calendar, as the script automatically handles
market holidays, weekends, and other non-trading periods by checking what data IB actually returns.

Version History:
- v1: Initial implementation
  * Basic historical data download functionality
  * Simple single-request download per stock
  * CSV configuration file support
  * Configurable duration and bar size
  * Support for multiple stocks with wait time between requests

- v2: Advanced iterative download implementation
  * Iterative download with real-time chunk adjustment
  * Automatic handling of market holidays without needing a holiday calendar
  * Market hours awareness (9:30 AM - 4:00 PM ET)
  * Progress tracking based on actual data received
  * Configurable useRTH (regular trading hours) support
  * Dynamic trading hours calculation (6.5h for RTH, 16.0h for extended)
  * Improved bar size detection (not hardcoded to 5 seconds)
  * Better error handling and type hints
  * Constants for easy configuration
  * CSV files include start and end date/time from actual downloaded data
  * Format: {symbol}_saved_YYYYMMDD_HHMMSS_start_YYYYMMDD_HHMMSS_end_YYYYMMDD_HHMMSS.csv
  * Fixed IB API error handler compatibility with flexible arguments
  * Aggregated timing data saved to single CSV file: stock_download_v2_YYYYMMDD_HHMMSS.csv
  * Chunk download retry mechanism (max 3 retries per chunk)
  * Abort stock download if chunk fails after all retries (no incomplete CSV saved)
  * Type-safe implementation with proper type hints for Pylance compatibility

- v3: Refactored architecture with improved code organization
  * Separation of concerns using focused classes (RequestIDGenerator, ProgressTracker, FileManager, ChunkDownloader)
  * Dataclasses for type safety and better data modeling
  * Better testability and maintainability
  * Cleaner code structure with single responsibility principle
  * All v2 features maintained with improved architecture
  * Auto-incrementing request ID generator to eliminate all possible conflicts (replaces formula-based approach)

Author: Data Downloader Script
"""

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import threading
import time
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Optional, List
from dataclasses import dataclass, field, replace
import logging
import sys
import pytz
import re


# --- Configuration Constants ---
TRADING_HOURS_RTH = 6.5        # Regular Trading Hours (9:30 - 16:00 ET)
TRADING_HOURS_EXTENDED = 16.0  # RTH + typical extended sessions (tune as needed)
DOWNLOAD_TIMEOUT_SECONDS = 30  # Default timeout for download requests
REQUEST_ID_MULTIPLIER = 10000  # Multiplier for base request ID calculation
CHUNK_HOURS = 3.0              # Default hours per download chunk
MAX_CHUNK_RETRIES = 3          # Maximum retries for chunk download failures
WAIT_TIME_BETWEEN_REQUESTS = 2.0 # Wait time (seconds) between API requests to avoid rate limiting
                                 # Timing breakdown for multiple stocks:
                                 # - Download chunk 1 for Stock A → wait 2s → chunk 2 → wait 2s → chunk 3 → ... → wait 2s → Start Stock B
IB_HOST = "127.0.0.1"          # Interactive Brokers host address
IB_PORT = 7497                 # Interactive Brokers port (7497 for TWS, 4001 for IB Gateway)
IB_CLIENT_ID = 1               # Default client ID for IB connection


# --- Utility Functions ---

def parse_bar_size_to_seconds(bar_size: str) -> int:
    """
    Parse bar_size string to seconds per bar.

    Args:
        bar_size: Bar size string (e.g., "5 secs", "1 min", "1 hour")

    Returns:
        Number of seconds per bar (defaults to 5 if parsing fails)
    """
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

    # Fallback to 5 seconds if parsing fails
    logging.getLogger('BarSizeParser').warning(
        f"Failed to parse bar_size '{bar_size}', defaulting to 5 seconds"
    )
    return 5


# --- Dataclasses ---

@dataclass
class StockConfig:
    """
    Configuration for a single stock download.

    Attributes:
        symbol: Stock ticker symbol
        sec_type: Security type (STK for stocks, IND for indices)
        currency: Currency code (e.g., USD)
        exchange: Exchange name (e.g., SMART for IB smart routing)
        total_days: Total trading days to download
        bar_size: Bar size (e.g., "5 secs", "1 min", "1 hour")
        what_to_show: Data type to show (e.g., "TRADES", "MIDPOINT", "BID", "ASK")
        use_rth: Use regular trading hours only (True) or include extended hours (False)
        initial_end_datetime_et: Initial end datetime in ET timezone (format: "YYYYMMDD HH:MM:SS")
    """
    symbol: str
    sec_type: str = "STK"
    currency: str = "USD"
    exchange: str = "SMART"
    total_days: float = 1.0
    bar_size: str = "5 secs"
    what_to_show: str = "TRADES"
    use_rth: bool = True
    initial_end_datetime_et: Optional[str] = None


@dataclass
class ChunkSpec:
    """
    Specification for downloading a single chunk of data.

    Attributes:
        req_id: Unique request identifier for IB API
        symbol: Stock symbol being downloaded
        chunk_num: Sequential chunk number (1-indexed)
        contract: IB Contract object for the security
        end_datetime_str: End date/time for the request (format: "YYYYMMDD HH:MM:SS TZ")
        duration_seconds: Duration in seconds to request
        bar_size: Bar size setting (e.g., "5 secs", "1 min")
        what_to_show: Data type to show (e.g., "TRADES")
        use_rth: Use regular trading hours only
        requested_hours: Trading hours requested for this chunk
        retry_attempt: Retry attempt number (0 = first attempt, 1+ = retries)
    """
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


@dataclass
class TimingRecord:
    """
    Timing information for a chunk download.

    Attributes:
        symbol: Stock symbol
        chunk_number: Sequential chunk number
        retry_attempt: Retry attempt number (0 = first attempt, 1+ = retries)
        req_id: Request ID used for this chunk download
        chunk_start_time: When the chunk download started (format: "YYYY-MM-DD HH:MM:SS")
        chunk_end_time: When the chunk download ended (format: "YYYY-MM-DD HH:MM:SS")
        download_duration_seconds: Duration of download in seconds
        bars_downloaded: Number of bars downloaded
        trading_hours_downloaded: Trading hours downloaded based on bar count
        data_start: Start timestamp of data or status (e.g., 'TIMEOUT', 'FAILED', 'NO_DATA')
        data_end: End timestamp of data or status
        requested_hours: Hours requested for this chunk
        use_rth: Whether RTH was used for this download
    """
    symbol: str
    chunk_number: int
    retry_attempt: int
    req_id: int
    chunk_start_time: str
    chunk_end_time: str
    download_duration_seconds: float
    bars_downloaded: int
    trading_hours_downloaded: float
    data_start: str
    data_end: str
    requested_hours: float
    use_rth: bool


@dataclass
class ChunkResult:
    """
    Result of a chunk download operation.

    Attributes:
        req_id: Request identifier used for this chunk
        chunk_num: Sequential chunk number
        success: Whether the download was successful
        data: List of data bars downloaded
        hours_downloaded: Trading hours actually downloaded
        next_end_datetime: Datetime to use as end time for next chunk (earliest time from this chunk)
        timing_record: Timing information for this download
        retry_attempt: Retry attempt number (0 = first attempt, 1+ = retries)
    """
    req_id: int
    chunk_num: int
    success: bool
    data: List[Dict]
    hours_downloaded: float
    next_end_datetime: Optional[datetime]
    timing_record: TimingRecord
    retry_attempt: int = 0


@dataclass
class DownloadProgress:
    """
    Track download progress for a stock across multiple chunks.

    Attributes:
        symbol: Stock symbol being downloaded
        total_trading_hours: Total trading hours to download
        hours_downloaded: Trading hours downloaded so far
        chunks_completed: Number of chunks completed
        current_end_datetime: Current end datetime for next chunk request
        all_data: List of all data bars collected so far
        timing_records: List of timing records for each chunk
    """
    symbol: str
    total_trading_hours: float
    hours_downloaded: float = 0.0
    chunks_completed: int = 0
    current_end_datetime: Optional[datetime] = None
    all_data: List[Dict] = field(default_factory=list)
    timing_records: List[TimingRecord] = field(default_factory=list)


# --- Helper Classes ---

class RequestIDGenerator:
    """Generate unique request IDs for IB API calls to avoid conflicts"""

    def __init__(self):
        """Initialize the request ID generator with auto-incrementing counter."""
        self.logger = logging.getLogger('RequestIDGenerator')
        self._counter = 0

    def generate_id(self, stock_num: int, chunk_num: int, retry_attempt: int) -> int:
        """
        Generate unique request ID using auto-incrementing counter.
        This approach guarantees no conflicts regardless of stock count or retry attempts.

        Args:
            stock_num: Stock number (1-indexed)
            chunk_num: Chunk number (1-indexed)
            retry_attempt: Retry attempt (0-indexed)

        Returns:
            Unique request ID
        """
        self._counter += 1
        req_id = self._counter
        self.logger.debug(f"Generated req_id={req_id} for stock={stock_num}, chunk={chunk_num}, retry={retry_attempt}")
        return req_id


class ProgressTracker:
    """Track download progress and timing information, saving incrementally to CSV"""

    def __init__(self, output_dir: Path, et_tz: pytz.tzinfo.BaseTzInfo):
        """
        Initialize the progress tracker.

        Args:
            output_dir: Directory to save timing CSV file
            et_tz: Eastern timezone for timestamp formatting
        """
        self.output_dir = output_dir
        self.et_tz = et_tz
        self.logger = logging.getLogger('ProgressTracker')
        self.timing_csv_path: Path = self._initialize_timing_csv()

    def _initialize_timing_csv(self) -> Path:
        """
        Initialize timing CSV file with headers.

        Returns:
            Path to the timing CSV file
        """
        timestamp = datetime.now(self.et_tz).strftime("%Y%m%d_%H%M%S")
        timing_filename = f"stock_download_v3_{timestamp}.csv"
        timing_csv_path = self.output_dir / timing_filename

        header = "symbol,chunk_number,retry_attempt,req_id,chunk_start_time,chunk_end_time," \
                "download_duration_seconds,bars_downloaded,trading_hours_downloaded," \
                "data_start,data_end,requested_hours,use_rth\n"

        with open(timing_csv_path, 'w') as f:
            f.write(header)

        self.logger.info(f"Initialized timing data file: {timing_csv_path}")
        return timing_csv_path

    def record_chunk(self, timing_record: TimingRecord) -> None:
        """
        Save timing record to CSV file (incremental save).

        Args:
            timing_record: Timing record to save
        """
        try:
            with open(self.timing_csv_path, 'a') as f:
                line = f"{timing_record.symbol},{timing_record.chunk_number},{timing_record.retry_attempt}," \
                       f"{timing_record.req_id},{timing_record.chunk_start_time},{timing_record.chunk_end_time}," \
                       f"{timing_record.download_duration_seconds},{timing_record.bars_downloaded}," \
                       f"{timing_record.trading_hours_downloaded},{timing_record.data_start}," \
                       f"{timing_record.data_end},{timing_record.requested_hours},{timing_record.use_rth}\n"
                f.write(line)
        except Exception as e:
            self.logger.error(f"Error saving timing record: {e}")


class FileManager:
    """Handle CSV file operations for stock data, including filename generation with timestamps"""

    def __init__(self, output_dir: Path, et_tz: pytz.tzinfo.BaseTzInfo):
        """
        Initialize the file manager.

        Args:
            output_dir: Directory to save CSV files
            et_tz: Eastern timezone for timestamp formatting
        """
        self.output_dir = output_dir
        self.et_tz = et_tz
        self.logger = logging.getLogger('FileManager')
        self.output_dir.mkdir(exist_ok=True, parents=True)

    def save_stock_data(self, all_data: List[Dict], symbol: str) -> Optional[str]:
        """
        Save stock data to CSV file with timestamps in filename.

        Args:
            all_data: List of all data bars collected
            symbol: Stock symbol

        Returns:
            Path to saved CSV file, or None if no data
        """
        if not all_data:
            self.logger.warning(f"No data to save for {symbol}")
            return None

        # Create DataFrame and clean data (remove duplicates, sort by date)
        df = pd.DataFrame(all_data)
        df = df.drop_duplicates(subset=['Date'])
        df = df.sort_values('Date')

        # Generate filename with start and end timestamps from actual data
        save_timestamp = datetime.now(self.et_tz).strftime("%Y%m%d_%H%M%S")
        filename = self._generate_filename(df, symbol, save_timestamp)

        # Save to CSV
        df.set_index("Date", inplace=True)
        filepath = self.output_dir / filename
        df.to_csv(filepath, index=True)

        self.logger.info(f"Saved {len(df)} rows to {filepath}")
        return str(filepath)

    def _generate_filename(self, df: pd.DataFrame, symbol: str, save_timestamp: str) -> str:
        """
        Generate filename with start and end timestamps from actual data.

        Args:
            df: DataFrame containing the downloaded data
            symbol: Stock symbol
            save_timestamp: Timestamp when file is being saved

        Returns:
            Filename string in format: {symbol}_saved_{timestamp}_start_{start}_end_{end}.csv
        """
        if len(df) == 0:
            return f"{symbol}_saved_{save_timestamp}.csv"

        try:
            # Parse first and last dates from the data
            first_date_str = df.iloc[0]['Date']
            last_date_str = df.iloc[-1]['Date']

            # Extract datetime parts (remove timezone)
            first_dt_parts = first_date_str.rsplit(' ', 1)[0]
            last_dt_parts = last_date_str.rsplit(' ', 1)[0]

            # Parse to datetime and format for filename
            first_dt = datetime.strptime(first_dt_parts, "%Y%m%d %H:%M:%S")
            last_dt = datetime.strptime(last_dt_parts, "%Y%m%d %H:%M:%S")

            start_timestamp = first_dt.strftime("%Y%m%d_%H%M%S")
            end_timestamp = last_dt.strftime("%Y%m%d_%H%M%S")

            return f"{symbol}_saved_{save_timestamp}_start_{start_timestamp}_end_{end_timestamp}.csv"

        except Exception as e:
            self.logger.warning(f"Error generating filename: {e}")
            return f"{symbol}_saved_{save_timestamp}.csv"


def setup_logging(log_file: Path,
                  ib_downloader_level: int = logging.INFO,
                  stock_manager_level: int = logging.INFO,
                  ibapi_level: int = logging.WARNING) -> None:
    """
    Set up global logging configuration to write to both console and file.

    Args:
        log_file: Path to the log file
        ib_downloader_level: Logging level for IBDataDownloader (default: INFO)
        stock_manager_level: Logging level for StockDataManager and ChunkDownloader (default: INFO)
        ibapi_level: Logging level for ibapi library (default: WARNING)
    """
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)  # Set to DEBUG to allow all levels through
    root_logger.handlers.clear()

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    # Set specific logger levels
    logging.getLogger('IBDataDownloader').setLevel(ib_downloader_level)
    logging.getLogger('StockDataManager').setLevel(stock_manager_level)
    logging.getLogger('ChunkDownloader').setLevel(stock_manager_level)
    logging.getLogger('ibapi').setLevel(ibapi_level)

    logging.info(f"Logging to file: {log_file}")


class IBDataDownloader(EWrapper, EClient):
    """
    Interactive Brokers API client for downloading historical data.

    Inherits from both EWrapper (handles callbacks) and EClient (sends requests).
    """

    def __init__(self):
        """Initialize the IB API client."""
        EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)
        self.data = {}
        self.download_events = {}  # {req_id: threading.Event()}
        self.logger = logging.getLogger('IBDataDownloader')

    def error(self, reqId: int, errorCode: int, errorString: str, *_args, **_kwargs):
        """
        Handle error messages from IB API.

        Args:
            reqId: Request identifier
            errorCode: Error code from IB
            errorString: Error message description
            *_args: Additional positional arguments from IB API (unused)
            **_kwargs: Additional keyword arguments from IB API (unused)
        """
        self.logger.error(f"ReqId: {reqId}, ErrorCode: {errorCode}, ErrorString: {errorString}")

    def historicalData(self, reqId: int, bar):
        """
        Receive historical data bars from IB API.

        Args:
            reqId: Request identifier
            bar: Historical data bar containing OHLCV data
        """
        if reqId not in self.data:
            self.data[reqId] = []

        self.data[reqId].append({
            "Date": bar.date,
            "Open": bar.open,
            "High": bar.high,
            "Low": bar.low,
            "Close": bar.close,
            "Volume": bar.volume,
            "WAP": bar.wap,
            "BarCount": bar.barCount,
            "HasGaps": getattr(bar, 'hasGaps', False)
        })

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        """
        Called when all historical data has been received for a request.

        Args:
            reqId: Request identifier
            start: Start date of data
            end: End date of data
        """
        if reqId in self.download_events:
            self.download_events[reqId].set()
        self.logger.info(f"Data download complete for reqId: {reqId} (from {start} to {end})")


class ChunkDownloader:
    """Handle chunk download operations with retry logic and error handling"""

    def __init__(self, ib_client: IBDataDownloader, et_tz: pytz.tzinfo.BaseTzInfo,
                 progress_tracker: ProgressTracker, req_id_gen: RequestIDGenerator):
        """
        Initialize the chunk downloader.

        Args:
            ib_client: IB API client instance
            et_tz: Eastern timezone for timestamp parsing
            progress_tracker: Progress tracker for recording timing data
            req_id_gen: Shared request ID generator for unique request IDs
        """
        self.ib_client = ib_client
        self.et_tz = et_tz
        self.progress_tracker = progress_tracker
        self.req_id_gen = req_id_gen
        self.logger = logging.getLogger('ChunkDownloader')

    def download_chunk_with_retry(self, chunk_spec: ChunkSpec, stock_num: int,
                                   max_retries: int = MAX_CHUNK_RETRIES) -> Optional[ChunkResult]:
        """
        Download a chunk with retry logic (max 3 retries by default).

        Args:
            chunk_spec: Chunk specification (contains req_id for first attempt)
            stock_num: Stock number for request ID generation (used for retry attempts)
            max_retries: Maximum retry attempts (default: MAX_CHUNK_RETRIES)

        Returns:
            ChunkResult with download data and timing info, or None if all retries failed
        """
        for retry_attempt in range(max_retries):
            if retry_attempt > 0:
                self.logger.warning(
                    f"Retry {retry_attempt}/{max_retries - 1} for {chunk_spec.symbol} chunk {chunk_spec.chunk_num}"
                )
                time.sleep(WAIT_TIME_BETWEEN_REQUESTS)

                # Generate new request ID for retry attempt using shared generator
                new_req_id = self.req_id_gen.generate_id(stock_num, chunk_spec.chunk_num, retry_attempt)
                # Update chunk spec with new request ID and retry attempt number
                retry_spec = replace(chunk_spec, req_id=new_req_id, retry_attempt=retry_attempt)
            else:
                # First attempt - use the req_id already in chunk_spec
                retry_spec = chunk_spec

            result = self._download_single_chunk(retry_spec)

            if result and result.success and result.hours_downloaded > 0:
                if retry_attempt > 0:
                    self.logger.info(
                        f"Successfully downloaded {chunk_spec.symbol} chunk {chunk_spec.chunk_num} "
                        f"on retry {retry_attempt}"
                    )
                return result

        self.logger.error(
            f"Failed to download {chunk_spec.symbol} chunk {chunk_spec.chunk_num} after {max_retries} attempts"
        )
        return None

    def _download_single_chunk(self, chunk_spec: ChunkSpec) -> Optional[ChunkResult]:
        """
        Download a single chunk of data.

        Args:
            chunk_spec: Chunk specification with all request parameters

        Returns:
            ChunkResult with download data and timing info, or None on critical failure
        """
        chunk_start_time = time.time()
        chunk_start_datetime = datetime.now(self.et_tz)

        # Request data from IB API
        if not self._request_historical_data(chunk_spec):
            return self._create_error_result(chunk_spec, chunk_start_time, chunk_start_datetime, 'FAILED')

        # Wait for download to complete
        if not self._wait_for_download(chunk_spec.req_id):
            return self._create_error_result(chunk_spec, chunk_start_time, chunk_start_datetime, 'TIMEOUT')

        # Process downloaded data
        chunk_end_time = time.time()
        chunk_end_datetime = datetime.now(self.et_tz)
        chunk_duration = chunk_end_time - chunk_start_time

        chunk_data = self.ib_client.data.get(chunk_spec.req_id)
        if chunk_data is None:
            return self._create_error_result(chunk_spec, chunk_start_time, chunk_start_datetime,
                                            'FAILED', chunk_end_time, chunk_end_datetime)

        if not chunk_data:
            return self._create_error_result(chunk_spec, chunk_start_time, chunk_start_datetime,
                                            'NO_DATA', chunk_end_time, chunk_end_datetime)

        # Process successful download
        return self._create_success_result(chunk_spec, chunk_data, chunk_start_time,
                                           chunk_start_datetime, chunk_end_time,
                                           chunk_end_datetime, chunk_duration)

    def _request_historical_data(self, chunk_spec: ChunkSpec) -> bool:
        """
        Request historical data from IB API.

        Args:
            chunk_spec: Chunk specification with request parameters

        Returns:
            True if request sent successfully, False otherwise
        """
        try:
            # Create a new threading event for this request
            self.ib_client.download_events[chunk_spec.req_id] = threading.Event()

            self.ib_client.reqHistoricalData(
                reqId=chunk_spec.req_id,
                contract=chunk_spec.contract,
                endDateTime=chunk_spec.end_datetime_str,
                durationStr=f"{chunk_spec.duration_seconds} S",
                barSizeSetting=chunk_spec.bar_size,
                whatToShow=chunk_spec.what_to_show,
                useRTH=1 if chunk_spec.use_rth else 0,
                formatDate=1,
                keepUpToDate=False,
                chartOptions=[]
            )

            self.logger.info(
                f"Requested {chunk_spec.symbol} chunk {chunk_spec.chunk_num} "
                f"(reqId: {chunk_spec.req_id}, duration: {chunk_spec.duration_seconds}s, "
                f"end: {chunk_spec.end_datetime_str}, useRTH: {chunk_spec.use_rth})"
            )
            return True

        except Exception as e:
            self.logger.error(f"Error requesting historical data: {e}")
            return False

    def _wait_for_download(self, req_id: int, timeout: int = DOWNLOAD_TIMEOUT_SECONDS) -> bool:
        """
        Wait for data download to complete with timeout using threading.Event.

        Args:
            req_id: Request identifier to wait for
            timeout: Maximum time to wait in seconds (default: DOWNLOAD_TIMEOUT_SECONDS)

        Returns:
            True if download completed, False if timeout
        """
        # Get the event for this request
        event = self.ib_client.download_events.get(req_id)
        if not event:
            self.logger.warning(f"No event found for reqId: {req_id}")
            return False

        # Wait efficiently using threading.Event - blocks until set() or timeout
        if event.wait(timeout=timeout):
            # Event was set - download completed successfully
            return True

        # Timeout - cancel request and cleanup
        self.ib_client.cancelHistoricalData(req_id)
        self.logger.info(f"Cancelled request {req_id} due to timeout")

        # Clean up data dictionary entries to prevent stale data
        if req_id in self.ib_client.data:
            del self.ib_client.data[req_id]
        if req_id in self.ib_client.download_events:
            del self.ib_client.download_events[req_id]

        self.logger.warning(f"Timeout waiting for download (reqId: {req_id})")
        return False

    def _process_chunk_data(self, chunk_data: List[Dict], bar_size: str) -> tuple:
        """
        Process chunk data and extract timing information.

        Args:
            chunk_data: List of data bars from the chunk
            bar_size: Bar size setting

        Returns:
            Tuple of (trading_hours, earliest_datetime, earliest_date_str, latest_date_str)
            or (0.0, None, 'NO_DATA'/'ERROR', 'NO_DATA'/'ERROR') on error
        """
        if not chunk_data:
            return (0.0, None, 'NO_DATA', 'NO_DATA')

        earliest_date_str = chunk_data[0]['Date']   # First record (earliest/start)
        latest_date_str = chunk_data[-1]['Date']    # Last record (latest/end)

        try:
            # Extract just the datetime part (before timezone)
            dt_parts = earliest_date_str.rsplit(' ', 1)[0]  # Remove timezone
            earliest_dt = datetime.strptime(dt_parts, "%Y%m%d %H:%M:%S")
            earliest_dt = self.et_tz.localize(earliest_dt)

            seconds_per_bar = parse_bar_size_to_seconds(bar_size)
            bars_count = len(chunk_data)
            trading_hours = round((bars_count * seconds_per_bar) / 3600.0, 6)

            return (trading_hours, earliest_dt, earliest_date_str, latest_date_str)

        except Exception as e:
            self.logger.error(f"Error parsing date '{earliest_date_str}': {e}")
            return (0.0, None, 'ERROR', 'ERROR')

    def _create_timing_record(self, chunk_spec: ChunkSpec, chunk_start_datetime: datetime,
                             chunk_end_datetime: datetime, chunk_duration: float,
                             bars_downloaded: int, hours_downloaded: float,
                             data_start: str, data_end: str) -> TimingRecord:
        """
        Create a timing record for a chunk download.

        Args:
            chunk_spec: Chunk specification
            chunk_start_datetime: When the chunk download started
            chunk_end_datetime: When the chunk download ended
            chunk_duration: Duration of download in seconds
            bars_downloaded: Number of bars downloaded
            hours_downloaded: Trading hours downloaded
            data_start: Start timestamp of data or status
            data_end: End timestamp of data or status

        Returns:
            TimingRecord with all timing information
        """
        return TimingRecord(
            symbol=chunk_spec.symbol,
            chunk_number=chunk_spec.chunk_num,
            retry_attempt=chunk_spec.retry_attempt,
            req_id=chunk_spec.req_id,
            chunk_start_time=chunk_start_datetime.strftime("%Y-%m-%d %H:%M:%S"),
            chunk_end_time=chunk_end_datetime.strftime("%Y-%m-%d %H:%M:%S"),
            download_duration_seconds=chunk_duration,
            bars_downloaded=bars_downloaded,
            trading_hours_downloaded=hours_downloaded,
            data_start=data_start,
            data_end=data_end,
            requested_hours=chunk_spec.requested_hours,
            use_rth=chunk_spec.use_rth
        )

    def _create_success_result(self, chunk_spec: ChunkSpec, chunk_data: List[Dict],
                               chunk_start_time: float, chunk_start_datetime: datetime,
                               chunk_end_time: float, chunk_end_datetime: datetime,
                               chunk_duration: float) -> ChunkResult:
        """
        Create a successful chunk result.

        Args:
            chunk_spec: Chunk specification
            chunk_data: Downloaded data bars
            chunk_start_time: Start time in seconds
            chunk_start_datetime: Start datetime
            chunk_end_time: End time in seconds
            chunk_end_datetime: End datetime
            chunk_duration: Duration in seconds

        Returns:
            ChunkResult with success status and downloaded data
        """
        trading_hours, earliest_dt, data_start, data_end = self._process_chunk_data(
            chunk_data, chunk_spec.bar_size
        )

        timing_record = self._create_timing_record(
            chunk_spec, chunk_start_datetime, chunk_end_datetime, chunk_duration,
            len(chunk_data), trading_hours, data_start, data_end
        )

        # Save timing record immediately (incremental save)
        self.progress_tracker.record_chunk(timing_record)

        self.logger.info(
            f"Chunk {chunk_spec.chunk_num}: Downloaded {trading_hours:.2f} hours "
            f"({len(chunk_data)} bars from {data_start} to {data_end})"
        )

        return ChunkResult(
            req_id=chunk_spec.req_id,
            chunk_num=chunk_spec.chunk_num,
            success=True,
            data=chunk_data,
            hours_downloaded=trading_hours,
            next_end_datetime=earliest_dt,
            timing_record=timing_record,
            retry_attempt=chunk_spec.retry_attempt
        )

    def _create_error_result(self, chunk_spec: ChunkSpec, chunk_start_time: float,
                            chunk_start_datetime: datetime, status: str,
                            chunk_end_time: Optional[float] = None,
                            chunk_end_datetime: Optional[datetime] = None) -> ChunkResult:
        """
        Create an error result (timeout, failed, or no data)

        Args:
            chunk_spec: Chunk specification
            chunk_start_time: Start time in seconds
            chunk_start_datetime: Start datetime
            status: Status string ('TIMEOUT', 'FAILED', or 'NO_DATA')
            chunk_end_time: Optional end time in seconds
            chunk_end_datetime: Optional end datetime

        Returns:
            ChunkResult with error status
        """
        if chunk_end_time is None:
            chunk_end_time = time.time()
        if chunk_end_datetime is None:
            chunk_end_datetime = datetime.now(self.et_tz)

        chunk_duration = chunk_end_time - chunk_start_time

        timing_record = self._create_timing_record(
            chunk_spec, chunk_start_datetime, chunk_end_datetime, chunk_duration,
            0, 0.0, status, status
        )

        self.progress_tracker.record_chunk(timing_record)

        return ChunkResult(
            req_id=chunk_spec.req_id,
            chunk_num=chunk_spec.chunk_num,
            success=False,
            data=[],
            hours_downloaded=chunk_spec.requested_hours,
            next_end_datetime=None,
            timing_record=timing_record,
            retry_attempt=chunk_spec.retry_attempt
        )


class StockDataManager:
    """
    Main orchestrator for stock data downloading with iterative real-time adjustment.

    Manages the overall download process for multiple stocks, coordinating connections,
    chunk downloads, progress tracking, and file operations.
    """

    def __init__(self, config_file: Optional[str] = None, output_dir: Optional[str] = None):
        """
        Initialize the stock data manager.

        Args:
            config_file: Path to CSV configuration file (relative to project root or absolute)
            output_dir: Directory to save output CSV files (relative to project root or absolute)
        """
        # Get the project root directory (parent of src/)
        self.project_root = Path(__file__).parent.parent.resolve()

        # Set default paths relative to project root
        if config_file is None:
            self.config_file = self.project_root / "config" / "stocks_config.csv"
        else:
            self.config_file = Path(config_file)
            if not self.config_file.is_absolute():
                self.config_file = self.project_root / config_file

        if output_dir is None:
            self.output_dir = self.project_root / "data"
        else:
            self.output_dir = Path(output_dir)
            if not self.output_dir.is_absolute():
                self.output_dir = self.project_root / output_dir

        self.output_dir.mkdir(exist_ok=True, parents=True)

        # Initialize components
        self.et_tz = pytz.timezone('US/Eastern')  # Timezone for timestamp parsing
        self.ib_client: Optional[IBDataDownloader] = None
        self.connection_thread: Optional[threading.Thread] = None
        self.req_id_gen = RequestIDGenerator()
        self.progress_tracker = ProgressTracker(self.output_dir, self.et_tz)
        self.file_manager = FileManager(self.output_dir, self.et_tz)
        self.chunk_downloader: Optional[ChunkDownloader] = None

        self.logger = logging.getLogger('StockDataManager')
        self.logger.info(f"Project root: {self.project_root}")
        self.logger.info(f"Config file: {self.config_file}")
        self.logger.info(f"Output directory: {self.output_dir}")

    def load_config(self) -> List[StockConfig]:
        """
        Load stock configuration from CSV file.

        Returns:
            List of StockConfig objects parsed from the CSV file

        Raises:
            FileNotFoundError: If configuration file doesn't exist
            Exception: For other errors during file loading or parsing
        """
        try:
            config_df = pd.read_csv(self.config_file)
            self.logger.info(f"Loaded configuration for {len(config_df)} stocks from {self.config_file}")

            stock_configs = []
            for _, row in config_df.iterrows():
                # Parse use_rth column (supports 1/0, True/False, or text values)
                use_rth = True
                if 'use_rth' in row:
                    try:
                        use_rth = bool(int(row['use_rth']))
                    except Exception:
                        # Handle string values like 'true', 'false', 'yes', 'no'
                        use_rth = str(row['use_rth']).lower() not in ('0', 'false', 'no')

                config = StockConfig(
                    symbol=row['symbol'],
                    sec_type=row.get('sec_type', 'STK'),
                    currency=row.get('currency', 'USD'),
                    exchange=row.get('exchange', 'SMART'),
                    total_days=float(row.get('total_days', 1)),
                    bar_size=row.get('bar_size', '5 secs'),
                    what_to_show=row.get('what_to_show', 'TRADES'),
                    use_rth=use_rth,
                    initial_end_datetime_et=row.get('initial_end_datetime_et', None)
                )
                stock_configs.append(config)

            return stock_configs

        except FileNotFoundError:
            self.logger.error(f"Configuration file {self.config_file} not found")
            raise
        except Exception as e:
            self.logger.error(f"Error loading configuration: {e}")
            raise

    def connect_to_ib(self, host: str = IB_HOST, port: int = IB_PORT,
                     client_id: int = IB_CLIENT_ID) -> bool:
        """
        Connect to Interactive Brokers TWS or IB Gateway.

        Args:
            host: IB Gateway host (default: IB_HOST)
            port: IB Gateway port (7497 for TWS, 4001 for IB Gateway)
            client_id: Unique client identifier (default: IB_CLIENT_ID)

        Returns:
            True if connection successful, False otherwise
        """
        self.ib_client = IBDataDownloader()
        self.ib_client.connect(host, port, client_id)

        self.connection_thread = threading.Thread(
            target=self._run_connection,
            daemon=True
        )
        self.connection_thread.start()
        time.sleep(1)

        if self.ib_client.isConnected():
            self.logger.info(f"Successfully connected to IB at {host}:{port}")
            # Initialize chunk downloader after successful connection
            self.chunk_downloader = ChunkDownloader(
                self.ib_client, self.et_tz, self.progress_tracker, self.req_id_gen
            )
            return True
        else:
            self.logger.error("Failed to connect to IB")
            return False

    def _run_connection(self):
        """Run the IB API connection in a separate thread."""
        self.logger.info("Starting IB API connection thread")
        if self.ib_client is None:
            self.logger.error("IB client is None in _run_connection")
            return
        self.ib_client.run()
        self.logger.info("IB API connection thread ended")

    def disconnect(self):
        """Disconnect from IB."""
        if self.ib_client and self.ib_client.isConnected():
            self.ib_client.disconnect()
            self.logger.info("Disconnected from IB")
            time.sleep(0.5)

    def download_stocks(self, wait_time: float = WAIT_TIME_BETWEEN_REQUESTS) -> Dict[str, str]:
        """
        Download historical data for all configured stocks with iterative real-time adjustment.

        Args:
            wait_time: Time to wait between requests (seconds, default: WAIT_TIME_BETWEEN_REQUESTS)

        Returns:
            Dictionary mapping stock symbols to saved file paths
        """
        stock_configs = self.load_config()
        saved_files = {}

        total_stocks = len(stock_configs)

        for stock_num, stock_config in enumerate(stock_configs, start=1):
            self.logger.info(
                f"Processing {stock_config.symbol} ({stock_num}/{total_stocks}) - "
                f"{stock_config.total_days} trading days (useRTH={stock_config.use_rth})"
            )

            filepath = self._download_single_stock(stock_config, stock_num)

            if filepath:
                saved_files[stock_config.symbol] = filepath
            else:
                self.logger.warning(
                    f"Skipping {stock_config.symbol} - download failed after retries, no CSV saved"
                )

            # Wait between stocks, but not after the last one
            if stock_num < total_stocks:
                time.sleep(wait_time)

        self.logger.info(
            f"Completed downloading {len(saved_files)}/{total_stocks} stocks. "
            f"Timing data saved to {self.progress_tracker.timing_csv_path}"
        )

        return saved_files

    def _download_single_stock(self, config: StockConfig, stock_num: int) -> Optional[str]:
        """
        Download data for a single stock using iterative chunking approach.

        This method downloads data in chunks, dynamically adjusting based on actual data received.
        It automatically handles market holidays and weekends without needing a holiday calendar.

        Args:
            config: Stock configuration
            stock_num: Stock number (1-indexed) for request ID generation

        Returns:
            Path to saved CSV file, or None if download failed
        """
        # Initialize progress tracking
        trading_hours_per_day = TRADING_HOURS_RTH if config.use_rth else TRADING_HOURS_EXTENDED
        total_trading_hours = round(config.total_days * trading_hours_per_day, 6)

        progress = DownloadProgress(
            symbol=config.symbol,
            total_trading_hours=total_trading_hours,
            current_end_datetime=self._parse_initial_end_time(config.initial_end_datetime_et)
        )

        # Create IB contract
        contract = self._create_contract(config)

        # Download chunks iteratively
        chunk_num = 0
        while progress.hours_downloaded < progress.total_trading_hours:
            # Check if remaining time is too small (less than one bar)
            remaining_hours = progress.total_trading_hours - progress.hours_downloaded
            remaining_seconds = remaining_hours * 3600

            # Parse bar_size to get the minimum granularity
            bar_size_seconds = parse_bar_size_to_seconds(config.bar_size)

            if remaining_seconds < bar_size_seconds:
                self.logger.info(
                    f"Remaining time ({remaining_seconds:.2f}s) is less than bar size "
                    f"({bar_size_seconds}s). Marking download as complete."
                )
                break

            chunk_num += 1

            # Prepare chunk specification
            chunk_spec = self._prepare_chunk_spec(
                config, contract, progress, stock_num, chunk_num
            )

            # Download chunk with retry logic
            if self.chunk_downloader is None:
                self.logger.error("Chunk downloader not initialized")
                return None
            result = self.chunk_downloader.download_chunk_with_retry(chunk_spec, stock_num)

            if result is None:
                # Failed after all retries - abort this stock
                self.logger.error(
                    f"Aborting {config.symbol} download after chunk {chunk_num} failed all retry attempts"
                )
                return None

            # Update progress
            progress.all_data.extend(result.data)
            progress.hours_downloaded = progress.hours_downloaded + result.hours_downloaded
            progress.chunks_completed += 1

            if result.next_end_datetime:
                progress.current_end_datetime = result.next_end_datetime
            else:
                # Fallback: subtract requested hours if we couldn't parse the data
                if progress.current_end_datetime is None:
                    self.logger.error("current_end_datetime is None, cannot continue")
                    return None
                remaining_hours = progress.total_trading_hours - progress.hours_downloaded
                current_chunk_hours = min(CHUNK_HOURS, remaining_hours)
                progress.current_end_datetime -= timedelta(hours=current_chunk_hours)

            self.logger.info(
                f"Total progress: {progress.hours_downloaded:.2f}/"
                f"{progress.total_trading_hours:.2f} hours"
            )

            # Wait between chunks to avoid rate limiting
            time.sleep(WAIT_TIME_BETWEEN_REQUESTS)

        # Save all collected data
        return self.file_manager.save_stock_data(progress.all_data, config.symbol)

    def _create_contract(self, config: StockConfig) -> Contract:
        """
        Create an IB Contract object.

        Args:
            config: Stock configuration

        Returns:
            Contract object configured for the specified security
        """
        contract = Contract()
        contract.symbol = config.symbol
        contract.secType = config.sec_type
        contract.currency = config.currency
        contract.exchange = config.exchange
        return contract

    def _parse_initial_end_time(self, initial_end_datetime_et: Optional[str]) -> datetime:
        """
        Parse the initial end datetime from config or use current time.

        Args:
            initial_end_datetime_et: End datetime string from config (format: "YYYYMMDD HH:MM:SS")

        Returns:
            Parsed datetime in ET timezone
        """
        if initial_end_datetime_et and pd.notna(initial_end_datetime_et) and str(initial_end_datetime_et).strip():
            try:
                end_time = datetime.strptime(str(initial_end_datetime_et).strip(), "%Y%m%d %H:%M:%S")
                end_time = self.et_tz.localize(end_time)
                self.logger.info(f"Using initial_end_datetime_et from config: {initial_end_datetime_et}")
                return end_time
            except ValueError:
                self.logger.warning(
                    f"Invalid initial_end_datetime_et format: {initial_end_datetime_et}, "
                    f"using current time"
                )

        end_time = datetime.now(self.et_tz)
        self.logger.info("Using current time as initial end_datetime")
        return end_time

    def _prepare_chunk_spec(self, config: StockConfig, contract: Contract,
                           progress: DownloadProgress, stock_num: int,
                           chunk_num: int) -> ChunkSpec:
        """
        Prepare chunk specification for downloading.

        Args:
            config: Stock configuration
            contract: IB Contract object
            progress: Current download progress
            stock_num: Stock number (1-indexed)
            chunk_num: Chunk number (1-indexed)

        Returns:
            ChunkSpec with all parameters for chunk download
        """
        # Calculate chunk parameters
        remaining_hours = progress.total_trading_hours - progress.hours_downloaded
        current_chunk_hours = min(CHUNK_HOURS, remaining_hours)
        duration_seconds = int(current_chunk_hours * 3600)

        # Generate unique request ID for the first attempt (retry_attempt=0)
        req_id = self.req_id_gen.generate_id(stock_num, chunk_num, 0)

        # Format end datetime
        if progress.current_end_datetime is None:
            self.logger.error("current_end_datetime is None in _prepare_chunk_spec")
            raise ValueError("current_end_datetime cannot be None when preparing chunk spec")
        end_datetime_str = progress.current_end_datetime.strftime("%Y%m%d %H:%M:%S US/Eastern")

        return ChunkSpec(
            req_id=req_id,
            symbol=config.symbol,
            chunk_num=chunk_num,
            contract=contract,
            end_datetime_str=end_datetime_str,
            duration_seconds=duration_seconds,
            bar_size=config.bar_size,
            what_to_show=config.what_to_show,
            use_rth=config.use_rth,
            requested_hours=current_chunk_hours,
            retry_attempt=0
        )


def main():
    """
    Main execution function.

    Command-line usage:
        python stock_data_downloader_v3.py [config_file] [output_dir]

    Args (via sys.argv):
        config_file: Optional path to CSV configuration file
        output_dir: Optional path to output directory
    """
    config_file = sys.argv[1] if len(sys.argv) > 1 else None
    output_dir = sys.argv[2] if len(sys.argv) > 2 else None

    manager = StockDataManager(config_file=config_file, output_dir=output_dir)

    # Set up logging to both console and file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = manager.output_dir / f"stock_download_v3_{timestamp}.log"

    # Configure logging levels for each component
    # Available levels: logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL
    setup_logging(
        log_file,
        ib_downloader_level=logging.INFO,    # IBDataDownloader log level
        stock_manager_level=logging.INFO,    # StockDataManager and ChunkDownloader log level
        ibapi_level=logging.WARNING          # ibapi library log level
    )

    try:
        if not manager.connect_to_ib():
            print("Failed to connect to Interactive Brokers")
            return

        print("\nDownloading stock data (v3 - Refactored Architecture)...")
        saved_files = manager.download_stocks()

        print("\n" + "="*60)
        print("Download Summary:")
        print("="*60)
        for symbol, filepath in saved_files.items():
            print(f"{symbol:10s} -> {filepath}")
        print("="*60)

    except Exception as e:
        logging.error(f"Error in main execution: {e}")
        raise

    finally:
        manager.disconnect()


if __name__ == "__main__":
    main()
