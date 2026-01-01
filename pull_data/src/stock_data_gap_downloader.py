#!/usr/bin/env python3.12
"""
Stock Data Gap Downloader using Interactive Brokers API

This script identifies and fills data gaps in stock CSV files by:
1. Using AAPL as the master reference dataset for dates
2. Reading stock symbols from stocks_config.csv
3. Comparing each stock's dates against AAPL dates
4. Identifying missing date ranges (gaps)
5. Downloading missing data from IB API in 3-hour chunks (5-second bars)
6. Handling timeouts gracefully by moving to next chunk
7. Saving files:
   - {symbol}_saved_{timestamp}_gapfilled.csv (original data + filled gaps) for each stock
   - _consolidated_gapfil_log_{timestamp}.csv (single log of ALL chunk attempts across all stocks)
     Logs include: SUCCESS, TIMEOUT, OVERLAP_MISMATCH, FAILED status with details
     Written line-by-line as chunks are processed
8. NOT modifying the original CSV files

The script uses the same structure and subfunctions as stock_data_downloader.py for consistency.

Author: Data Gap Downloader Script
"""

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import threading
import time
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Tuple, Set
from dataclasses import dataclass, replace
import logging
import pytz
import re

# --- Configuration Constants ---
CHUNK_HOURS = 3.0              # Hours per download chunk (always use 3-hour chunks)
DOWNLOAD_TIMEOUT_SECONDS = 30  # Timeout for download requests
MAX_CHUNK_ATTEMPTS = 1         # Maximum attempts for chunk download
WAIT_TIME_BETWEEN_REQUESTS = 2.0  # Wait time between API requests
OVERLAP_TOLERANCE = 0.01       # Tolerance for comparing overlapping data values (1%)
IB_HOST = "127.0.0.1"          # IB host address
IB_PORT = 7497                 # IB port (7497 for TWS, 4001 for IB Gateway)
IB_CLIENT_ID = 2               # Client ID (different from stock_data_downloader.py)


# --- Utility Functions (shared with stock_data_downloader.py) ---

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
    """Configuration for a single stock."""
    symbol: str
    sec_type: str = "STK"
    currency: str = "USD"
    exchange: str = "SMART"
    bar_size: str = "5 secs"
    what_to_show: str = "TRADES"
    use_rth: bool = True


@dataclass
class GapRange:
    """Represents a continuous gap in data."""
    start_date: datetime
    end_date: datetime
    missing_count: int


@dataclass
class ChunkSpec:
    """Specification for downloading a chunk."""
    req_id: int
    symbol: str
    chunk_num: int
    contract: Contract
    start_datetime: datetime
    end_datetime: datetime
    bar_size: str
    what_to_show: str
    use_rth: bool


@dataclass
class ChunkResult:
    """Result of a chunk download."""
    req_id: int
    chunk_num: int
    success: bool
    timeout: bool
    data: List[Dict]
    start_datetime: datetime
    end_datetime: datetime


@dataclass
class ChunkLogRecord:
    """Record of a chunk download attempt (success or failure)."""
    symbol: str
    chunk_num: int
    start_datetime: str
    end_datetime: str
    status: str  # SUCCESS, TIMEOUT, OVERLAP_MISMATCH, FAILED
    bars_downloaded: int
    bars_filled: int
    details: str  # Additional details or error message


# --- Helper Classes ---

class RequestIDGenerator:
    """Generate unique request IDs for IB API calls."""

    def __init__(self):
        self.logger = logging.getLogger('RequestIDGenerator')
        self._counter = 0

    def generate_id(self) -> int:
        """Generate unique request ID."""
        self._counter += 1
        return self._counter


class IBDataDownloader(EWrapper, EClient):
    """IB API client for downloading historical data."""

    def __init__(self):
        EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)
        self.data = {}
        self.download_events = {}
        self.logger = logging.getLogger('IBDataDownloader')

    def error(self, reqId: int, errorCode: int, errorString: str, *_args, **_kwargs):
        """Handle error messages from IB API."""
        self.logger.error(f"ReqId: {reqId}, ErrorCode: {errorCode}, ErrorString: {errorString}")

    def historicalData(self, reqId: int, bar):
        """Receive historical data bars from IB API."""
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
        """Called when all historical data has been received."""
        if reqId in self.download_events:
            self.download_events[reqId].set()
        self.logger.info(f"Data download complete for reqId: {reqId} (from {start} to {end})")


class ChunkDownloader:
    """Handle chunk download operations with retry logic and timeout handling."""

    def __init__(self, ib_client: IBDataDownloader, et_tz: pytz.tzinfo.BaseTzInfo,
                 req_id_gen: RequestIDGenerator):
        self.ib_client = ib_client
        self.et_tz = et_tz
        self.req_id_gen = req_id_gen
        self.logger = logging.getLogger('ChunkDownloader')

    def download_chunk_with_retry(self, chunk_spec: ChunkSpec,
                                   max_attempts: int = MAX_CHUNK_ATTEMPTS) -> Optional[ChunkResult]:
        """
        Download a chunk with retry logic.
        On timeout, move to next chunk rather than fail completely.
        """
        for retry_attempt in range(max_attempts):
            if retry_attempt > 0:
                self.logger.warning(
                    f"Retry {retry_attempt}/{max_attempts - 1} for {chunk_spec.symbol} chunk {chunk_spec.chunk_num}"
                )
                time.sleep(WAIT_TIME_BETWEEN_REQUESTS)
                # Generate new request ID for retry
                new_req_id = self.req_id_gen.generate_id()
                retry_spec = replace(chunk_spec, req_id=new_req_id)
            else:
                retry_spec = chunk_spec

            result = self._download_single_chunk(retry_spec)

            if result and result.success:
                return result
            elif result and result.timeout:
                # On timeout, return the timeout result to move to next chunk
                self.logger.warning(
                    f"Timeout on {chunk_spec.symbol} chunk {chunk_spec.chunk_num}, moving to next chunk"
                )
                return result

        # If all retries failed (non-timeout errors), return failure result
        self.logger.error(
            f"Failed to download {chunk_spec.symbol} chunk {chunk_spec.chunk_num} after {max_attempts} attempts"
        )
        return ChunkResult(
            req_id=chunk_spec.req_id,
            chunk_num=chunk_spec.chunk_num,
            success=False,
            timeout=False,
            data=[],
            start_datetime=chunk_spec.start_datetime,
            end_datetime=chunk_spec.end_datetime
        )

    def _download_single_chunk(self, chunk_spec: ChunkSpec) -> Optional[ChunkResult]:
        """Download a single chunk of data."""
        # Request data from IB API
        if not self._request_historical_data(chunk_spec):
            return ChunkResult(
                req_id=chunk_spec.req_id,
                chunk_num=chunk_spec.chunk_num,
                success=False,
                timeout=False,
                data=[],
                start_datetime=chunk_spec.start_datetime,
                end_datetime=chunk_spec.end_datetime
            )

        # Wait for download to complete
        if not self._wait_for_download(chunk_spec.req_id):
            # Timeout occurred
            return ChunkResult(
                req_id=chunk_spec.req_id,
                chunk_num=chunk_spec.chunk_num,
                success=False,
                timeout=True,
                data=[],
                start_datetime=chunk_spec.start_datetime,
                end_datetime=chunk_spec.end_datetime
            )

        # Process downloaded data
        chunk_data = self.ib_client.data.get(chunk_spec.req_id, [])

        if not chunk_data:
            self.logger.warning(f"No data received for chunk {chunk_spec.chunk_num}")
            return ChunkResult(
                req_id=chunk_spec.req_id,
                chunk_num=chunk_spec.chunk_num,
                success=False,
                timeout=False,
                data=[],
                start_datetime=chunk_spec.start_datetime,
                end_datetime=chunk_spec.end_datetime
            )

        self.logger.info(
            f"Chunk {chunk_spec.chunk_num}: Downloaded {len(chunk_data)} bars "
            f"from {chunk_data[0]['Date']} to {chunk_data[-1]['Date']}"
        )

        return ChunkResult(
            req_id=chunk_spec.req_id,
            chunk_num=chunk_spec.chunk_num,
            success=True,
            timeout=False,
            data=chunk_data,
            start_datetime=chunk_spec.start_datetime,
            end_datetime=chunk_spec.end_datetime
        )

    def _request_historical_data(self, chunk_spec: ChunkSpec) -> bool:
        """Request historical data from IB API."""
        try:
            # Create event for this request
            self.ib_client.download_events[chunk_spec.req_id] = threading.Event()

            # ALWAYS use 3 hours (10800 seconds) as duration, no time calculations
            # This ensures IB API always requests exactly 2160 bars (at 5-sec intervals)
            duration_seconds = int(CHUNK_HOURS * 3600)  # Hardcoded: 10800 seconds

            # Format end datetime for IB API
            end_datetime_str = chunk_spec.end_datetime.strftime("%Y%m%d %H:%M:%S US/Eastern")

            self.ib_client.reqHistoricalData(
                reqId=chunk_spec.req_id,
                contract=chunk_spec.contract,
                endDateTime=end_datetime_str,
                durationStr=f"{duration_seconds} S",
                barSizeSetting=chunk_spec.bar_size,
                whatToShow=chunk_spec.what_to_show,
                useRTH=1 if chunk_spec.use_rth else 0,
                formatDate=1,
                keepUpToDate=False,
                chartOptions=[]
            )

            self.logger.info(
                f"Requested chunk {chunk_spec.chunk_num} (reqId: {chunk_spec.req_id}, "
                f"duration: {duration_seconds}s, start: {chunk_spec.start_datetime}, "
                f"end: {chunk_spec.end_datetime})"
            )
            return True

        except Exception as e:
            self.logger.error(f"Error requesting historical data: {e}")
            return False

    def _wait_for_download(self, req_id: int,
                           timeout: int = DOWNLOAD_TIMEOUT_SECONDS) -> bool:
        """Wait for data download to complete with timeout."""
        event = self.ib_client.download_events.get(req_id)
        if not event:
            self.logger.warning(f"No event found for reqId: {req_id}")
            return False

        # Wait for event
        if event.wait(timeout=timeout):
            return True

        # Timeout - cancel request and cleanup
        self.ib_client.cancelHistoricalData(req_id)
        self.logger.info(f"Cancelled request {req_id} due to timeout")

        # Cleanup
        if req_id in self.ib_client.data:
            del self.ib_client.data[req_id]
        if req_id in self.ib_client.download_events:
            del self.ib_client.download_events[req_id]

        self.logger.warning(f"Timeout waiting for download (reqId: {req_id})")
        return False


class GapAnalyzer:
    """Analyze and identify gaps in stock data."""

    def __init__(self, et_tz: pytz.tzinfo.BaseTzInfo):
        self.et_tz = et_tz
        self.logger = logging.getLogger('GapAnalyzer')

    def calculate_chunk_end_from_reference(self, start_datetime: datetime,
                                          aapl_dates_list: List[str],
                                          bar_size: str,
                                          max_end_datetime: Optional[datetime] = None) -> datetime:
        """
        Calculate chunk end time based on AAPL reference dates (actual trading timestamps).

        Instead of adding 3 hours of calendar time, this finds the Nth trading timestamp
        in the AAPL reference data, where N = number of bars in CHUNK_HOURS.

        Args:
            start_datetime: Start time of the chunk
            aapl_dates_list: Sorted list of AAPL reference dates
            bar_size: Bar size (e.g., "5 secs") to calculate number of bars
            max_end_datetime: Optional maximum end time (e.g., gap end) to constrain chunk

        Returns:
            End datetime based on actual trading timestamps
        """
        # Calculate number of bars in CHUNK_HOURS
        seconds_per_bar = parse_bar_size_to_seconds(bar_size)
        bars_per_chunk = int((CHUNK_HOURS * 3600) / seconds_per_bar)

        # Convert start_datetime to string format matching AAPL dates
        start_str = start_datetime.strftime("%Y%m%d %H:%M:%S US/Eastern")

        # Find the start position in AAPL dates
        try:
            start_idx = aapl_dates_list.index(start_str)
        except ValueError:
            # If exact start not found, find the next available timestamp
            self.logger.warning(f"Start time {start_str} not found in AAPL reference, finding next available")
            for idx, date_str in enumerate(aapl_dates_list):
                if date_str >= start_str:
                    start_idx = idx
                    break
            else:
                # If no future date found, use the last date + duration as fallback
                self.logger.error(f"No AAPL reference date >= {start_str}, using calendar time fallback")
                return start_datetime + timedelta(hours=CHUNK_HOURS)

        # Calculate nominal end index (start + number of bars for chunk duration)
        nominal_end_idx = start_idx + bars_per_chunk

        # If max_end_datetime is specified, find its index in AAPL dates and limit chunk to it
        if max_end_datetime is not None:
            max_end_str = max_end_datetime.strftime("%Y%m%d %H:%M:%S US/Eastern")

            # Find the index of max_end_datetime in AAPL dates
            max_end_idx = None
            try:
                max_end_idx = aapl_dates_list.index(max_end_str)
            except ValueError:
                # If exact max_end not found, find the closest one that doesn't exceed it
                for idx in range(len(aapl_dates_list) - 1, -1, -1):
                    if aapl_dates_list[idx] <= max_end_str:
                        max_end_idx = idx
                        break

            if max_end_idx is not None:
                # Use the smaller of nominal_end_idx and max_end_idx
                end_idx = min(nominal_end_idx, max_end_idx)
            else:
                end_idx = nominal_end_idx
        else:
            end_idx = nominal_end_idx

        # Check if end_idx exceeds AAPL reference data
        if end_idx >= len(aapl_dates_list):
            # Use the last available timestamp + one bar interval
            end_str = aapl_dates_list[-1]
            end_datetime = self._parse_date(end_str) + timedelta(seconds=seconds_per_bar)
            actual_bars = len(aapl_dates_list) - start_idx
            self.logger.warning(
                f"Chunk end exceeds AAPL reference (need idx {end_idx}, have {len(aapl_dates_list)}), "
                f"using last timestamp + 1 bar: {end_datetime}"
            )
        else:
            # Get the end timestamp from AAPL reference
            end_str = aapl_dates_list[end_idx]
            end_datetime = self._parse_date(end_str)
            actual_bars = end_idx - start_idx

        self.logger.info(
            f"Chunk: {start_str} -> {end_str} ({actual_bars} bars across trading days)"
        )

        return end_datetime

    def find_gaps(self, master_dates: Set[str], stock_dates: Set[str]) -> List[str]:
        """
        Find missing dates in stock data compared to master (AAPL).

        Returns:
            Sorted list of missing date strings
        """
        missing_dates = sorted(list(master_dates - stock_dates))
        self.logger.info(f"Found {len(missing_dates)} missing dates")
        return missing_dates

    def group_gaps_into_ranges(self, missing_dates: List[str], bar_size: str = "5 secs") -> List[GapRange]:
        """
        Group consecutive missing dates into gap ranges.

        Args:
            missing_dates: Sorted list of missing date strings
            bar_size: Bar size setting (e.g., "5 secs", "1 min") to determine expected interval

        Returns:
            List of GapRange objects representing continuous gaps
        """
        if not missing_dates:
            return []

        # Calculate expected interval between bars based on bar_size
        expected_interval_seconds = parse_bar_size_to_seconds(bar_size)

        gap_ranges = []
        current_start = self._parse_date(missing_dates[0])
        current_end = current_start
        count = 1

        for i in range(1, len(missing_dates)):
            date = self._parse_date(missing_dates[i])
            prev_date = self._parse_date(missing_dates[i-1])

            # Check if this date is consecutive (expected interval apart)
            if (date - prev_date).total_seconds() == expected_interval_seconds:
                current_end = date
                count += 1
            else:
                # Gap in sequence, save current range and start new one
                gap_ranges.append(GapRange(
                    start_date=current_start,
                    end_date=current_end,
                    missing_count=count
                ))
                current_start = date
                current_end = date
                count = 1

        # Add final range
        gap_ranges.append(GapRange(
            start_date=current_start,
            end_date=current_end,
            missing_count=count
        ))

        self.logger.info(f"Grouped {len(missing_dates)} missing dates into {len(gap_ranges)} gap ranges")
        return gap_ranges

    def _parse_date(self, date_str: str) -> datetime:
        """Parse date string from CSV format."""
        # Format: "20241203 13:00:00 US/Eastern"
        dt_parts = date_str.rsplit(' ', 1)[0]  # Remove timezone
        dt = datetime.strptime(dt_parts, "%Y%m%d %H:%M:%S")
        return self.et_tz.localize(dt)


class GapFillManager:
    """Main orchestrator for gap filling process."""

    def __init__(self, config_file: Optional[str] = None,
                 data_dir: Optional[str] = None):
        # Get project root directory
        self.project_root = Path(__file__).parent.parent.resolve()

        # Set default paths
        if config_file is None:
            self.config_file = self.project_root / "config" / "stocks_config.csv"
        else:
            self.config_file = Path(config_file)
            if not self.config_file.is_absolute():
                self.config_file = self.project_root / config_file

        if data_dir is None:
            self.data_dir = self.project_root / "data"
        else:
            self.data_dir = Path(data_dir)
            if not self.data_dir.is_absolute():
                self.data_dir = self.project_root / data_dir

        # Initialize components
        self.et_tz = pytz.timezone('US/Eastern')
        self.ib_client: Optional[IBDataDownloader] = None
        self.connection_thread: Optional[threading.Thread] = None
        self.req_id_gen = RequestIDGenerator()
        self.chunk_downloader: Optional[ChunkDownloader] = None
        self.gap_analyzer = GapAnalyzer(self.et_tz)

        # Consolidated chunk log - tracks ALL chunks (success and failure)
        self.consolidated_log_timestamp: str = datetime.now(pytz.timezone('US/Eastern')).strftime("%Y%m%d_%H%M%S")
        self.consolidated_log_path: Optional[Path] = None
        self.consolidated_log_file = None

        self.logger = logging.getLogger('GapFillManager')
        self.logger.info(f"Project root: {self.project_root}")
        self.logger.info(f"Config file: {self.config_file}")
        self.logger.info(f"Data directory: {self.data_dir}")

    def connect_to_ib(self, host: str = IB_HOST, port: int = IB_PORT,
                      client_id: int = IB_CLIENT_ID) -> bool:
        """Connect to IB TWS or Gateway."""
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
            self.chunk_downloader = ChunkDownloader(
                self.ib_client, self.et_tz, self.req_id_gen
            )
            return True
        else:
            self.logger.error("Failed to connect to IB")
            return False

    def initialize_consolidated_log(self) -> None:
        """Initialize the consolidated chunk log CSV file with header."""
        consolidated_filename = f"_consolidated_gapfil_log_{self.consolidated_log_timestamp}.csv"
        self.consolidated_log_path = self.data_dir / consolidated_filename

        # Open file in write mode and write header
        self.consolidated_log_file = open(self.consolidated_log_path, 'w')
        self.consolidated_log_file.write('symbol,chunk_num,start_datetime,end_datetime,status,bars_downloaded,bars_filled,details\n')
        self.consolidated_log_file.flush()

        self.logger.info(f"Initialized consolidated log: {consolidated_filename}")
        print(f"\nConsolidated log file: {consolidated_filename}")

    def write_chunk_log(self, record: ChunkLogRecord) -> None:
        """Write a chunk log record to the consolidated CSV file."""
        if self.consolidated_log_file is None:
            self.logger.error("Consolidated log file not initialized")
            return

        # Write CSV row
        self.consolidated_log_file.write(
            f'{record.symbol},{record.chunk_num},"{record.start_datetime}","{record.end_datetime}",'
            f'{record.status},{record.bars_downloaded},{record.bars_filled},"{record.details}"\n'
        )
        self.consolidated_log_file.flush()

    def close_consolidated_log(self) -> None:
        """Close the consolidated log file."""
        if self.consolidated_log_file:
            self.consolidated_log_file.close()
            if self.consolidated_log_path:
                self.logger.info(f"Closed consolidated log: {self.consolidated_log_path}")
                print(f"\nConsolidated log saved: {self.consolidated_log_path.name}")

    def _run_connection(self):
        """Run the IB API connection in a separate thread."""
        if self.ib_client is None:
            self.logger.error("IB client is None in _run_connection")
            return
        self.ib_client.run()

    def disconnect(self):
        """Disconnect from IB."""
        if self.ib_client and self.ib_client.isConnected():
            self.ib_client.disconnect()
            self.logger.info("Disconnected from IB")
            time.sleep(0.5)

    def load_config(self) -> List[StockConfig]:
        """Load stock configuration from CSV file."""
        df = pd.read_csv(self.config_file)
        self.logger.info(f"Loaded {len(df)} stocks from config")

        stock_configs = []
        for _, row in df.iterrows():
            config = StockConfig(
                symbol=row['symbol'],
                sec_type=row.get('sec_type', 'STK'),
                currency=row.get('currency', 'USD'),
                exchange=row.get('exchange', 'SMART'),
                bar_size=row.get('bar_size', '5 secs'),
                what_to_show=row.get('what_to_show', 'TRADES'),
                use_rth=True
            )
            stock_configs.append(config)

        return stock_configs

    def load_aapl_reference(self) -> Tuple[Optional[pd.DataFrame], Optional[Set[str]], Optional[List[str]]]:
        """Load AAPL data as reference.

        Returns:
            Tuple of (dataframe, dates_set, dates_list)
            - dates_list is sorted for chunk calculation based on trading timestamps
        """
        aapl_file = self._find_latest_csv('AAPL', self.data_dir)

        if aapl_file is None:
            self.logger.error("AAPL reference file not found")
            return None, None, None

        try:
            df = pd.read_csv(aapl_file)
            self.logger.info(f"Loaded AAPL reference: {len(df)} rows from {aapl_file.name}")
            dates_list = df['Date'].tolist()  # Already sorted if CSV is sorted
            dates_set = set(dates_list)
            return df, dates_set, dates_list
        except Exception as e:
            self.logger.error(f"Error loading AAPL reference: {e}")
            return None, None, None

    def load_stock_data(self, symbol: str) -> Tuple[Optional[pd.DataFrame], Optional[Set[str]]]:
        """Load stock data."""
        stock_file = self._find_latest_csv(symbol, self.data_dir)

        if stock_file is None:
            self.logger.warning(f"No data file found for {symbol}")
            return None, None

        try:
            df = pd.read_csv(stock_file)
            self.logger.info(f"Loaded {symbol}: {len(df)} rows from {stock_file.name}")
            dates = set(df['Date'].tolist())
            return df, dates
        except Exception as e:
            self.logger.error(f"Error loading {symbol}: {e}")
            return None, None

    def _find_latest_csv(self, symbol: str, directory: Path) -> Optional[Path]:
        """
        Find the latest CSV file for a symbol.

        If there's only one file, return it regardless of whether it has "_gapfil" in the name.
        This allows processing of gapfilled files when they're the only file available.
        """
        pattern = f"{symbol}_saved_*.csv"
        files = list(directory.glob(pattern))

        if not files:
            return None

        # Sort by modification time (newest first)
        files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
        return files[0]

    def _check_duplicate_csv_files(self, symbol: str) -> bool:
        """
        Check if there are multiple CSV files for a symbol (including gapfilled files).

        Args:
            symbol: Stock symbol to check

        Returns:
            True if duplicates found (error condition), False if only one file

        Raises:
            RuntimeError: If duplicate CSV files are found (2 or more files total)
        """
        pattern = f"{symbol}_saved_*.csv"
        files = list(self.data_dir.glob(pattern))

        if not files:
            return False

        # Check if there are 2 or more files (including gapfilled files)
        if len(files) >= 2:
            self.logger.error(
                f"ERROR: Found {len(files)} CSV files for {symbol}. "
                f"Only one file should exist in the data folder."
            )
            self.logger.error(f"Files found:")
            for idx, file in enumerate(files, 1):
                self.logger.error(f"  {idx}. {file.name}")

            raise RuntimeError(
                f"Duplicate CSV files detected for {symbol}. "
                f"Found {len(files)} files: {[f.name for f in files]}. "
                f"Please keep only one CSV file per stock in the data folder."
            )

        return False

    def process_stock(self, config: StockConfig, aapl_dates: Set[str],
                     aapl_dates_list: List[str]) -> bool:
        """
        Process a single stock to identify and fill gaps.

        Args:
            config: Stock configuration
            aapl_dates: Set of AAPL dates for gap detection
            aapl_dates_list: Sorted list of AAPL dates for chunk calculation

        Returns:
            True if processing completed successfully, False otherwise
        """
        self.logger.info("="*80)
        self.logger.info(f"Processing {config.symbol}")
        self.logger.info("="*80)

        # Check for duplicate CSV files
        try:
            self._check_duplicate_csv_files(config.symbol)
        except RuntimeError as e:
            self.logger.error(f"Stopping execution for {config.symbol}: {e}")
            raise  # Re-raise to stop execution

        # Load stock data
        stock_df, stock_dates = self.load_stock_data(config.symbol)

        # If no existing data, create empty DataFrame and download all AAPL dates
        if stock_df is None or stock_dates is None:
            self.logger.info(f"{config.symbol}: No existing data found, creating new file from AAPL reference")
            # Create empty DataFrame with correct columns
            stock_df = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'WAP', 'BarCount', 'HasGaps'])
            stock_dates = set()  # Empty set means all AAPL dates are missing

        # Find gaps (all AAPL dates if stock_dates is empty)
        missing_dates = self.gap_analyzer.find_gaps(aapl_dates, stock_dates)

        if not missing_dates:
            self.logger.info(f"{config.symbol}: No gaps found - data is complete")
            return True

        # Group gaps into ranges (pass bar_size for correct interval calculation)
        gap_ranges = self.gap_analyzer.group_gaps_into_ranges(missing_dates, config.bar_size)

        # Download data to fill gaps (pass stock_df, aapl_dates_list for overlap validation and chunk calculation)
        filled_data = self._download_gap_data(
            config, gap_ranges, stock_df, stock_dates, aapl_dates_list
        )

        # Only save results if we actually filled data
        if filled_data:
            self._save_results(config.symbol, stock_df, filled_data)
            self.logger.info(f"{config.symbol}: Saved gapfilled data with {len(filled_data)} new bars")
        else:
            self.logger.info(f"{config.symbol}: No data filled, skipping save (original file unchanged)")

        return True

    def _download_gap_data(self, config: StockConfig,
                           gap_ranges: List[GapRange],
                           original_df: pd.DataFrame,
                           original_dates: Set[str],
                           aapl_dates_list: List[str]) -> List[Dict]:
        """
        Download data to fill gaps using 3-hour chunks based on AAPL trading timestamps.
        Validates overlap and extracts only missing data.
        Logs all chunk attempts to consolidated log.

        Args:
            config: Stock configuration
            gap_ranges: List of gap ranges to fill
            original_df: Original stock dataframe for overlap validation
            original_dates: Set of existing dates
            aapl_dates_list: Sorted list of AAPL reference dates for chunk calculation

        Returns:
            List of filled data
        """
        filled_data = []

        # Create IB contract
        contract = self._create_contract(config)

        # Calculate bars per chunk (always 2160 for 3 hours at 5-second bars)
        seconds_per_bar = parse_bar_size_to_seconds(config.bar_size)
        bars_per_chunk = int((CHUNK_HOURS * 3600) / seconds_per_bar)

        # Convert aapl_dates_list to set for fast lookup in validation
        aapl_dates_set = set(aapl_dates_list)

        chunk_num = 0

        # Track global AAPL index across all gaps (don't reset between gaps)
        current_idx = None

        for gap_range in gap_ranges:
            gap_duration_hours = (gap_range.end_date - gap_range.start_date).total_seconds() / 3600

            self.logger.info(
                f"Processing gap: {gap_range.start_date} to {gap_range.end_date} "
                f"({gap_range.missing_count} missing bars, {gap_duration_hours:.2f} hours)"
            )

            # Find gap start and end indices in AAPL dates
            gap_start_str = gap_range.start_date.strftime("%Y%m%d %H:%M:%S US/Eastern")
            try:
                gap_start_idx = aapl_dates_list.index(gap_start_str)
            except ValueError:
                self.logger.error(f"Gap start {gap_start_str} not found in AAPL dates, skipping gap")
                continue

            gap_end_str = gap_range.end_date.strftime("%Y%m%d %H:%M:%S US/Eastern")
            try:
                gap_end_idx = aapl_dates_list.index(gap_end_str)
            except ValueError:
                self.logger.error(f"Gap end {gap_end_str} not found in AAPL dates, skipping gap")
                continue

            # Initialize current_idx if this is the first gap, otherwise continue from previous
            if current_idx is None:
                current_idx = gap_start_idx
            # If current_idx is behind this gap's start (e.g., after timeout/skip), jump to gap start
            elif current_idx < gap_start_idx:
                current_idx = gap_start_idx

            # Process this gap (and potentially beyond) in fixed 2160-bar chunks
            while current_idx <= gap_end_idx:
                chunk_num += 1

                # Always request exactly bars_per_chunk (2160) bars from AAPL reference
                chunk_end_idx = current_idx + bars_per_chunk - 1

                # Check if chunk_end_idx exceeds AAPL data
                if chunk_end_idx >= len(aapl_dates_list):
                    # Still download 2160 bars from API (hardcoded 3 hours)
                    # but use last AAPL index to determine which data is valid
                    chunk_end_idx = len(aapl_dates_list) - 1
                    self.logger.info(
                        f"Chunk {chunk_num} extends beyond AAPL boundary, "
                        f"will download full 2160 bars but only extract valid data"
                    )

                # Get start and end datetime from AAPL dates at these indices
                chunk_start_str = aapl_dates_list[current_idx]
                chunk_end_str = aapl_dates_list[chunk_end_idx]
                current_start = self.gap_analyzer._parse_date(chunk_start_str)
                chunk_end = self.gap_analyzer._parse_date(chunk_end_str)

                # Add 5 seconds buffer to chunk_end for IB API request to ensure it includes the exact end timestamp
                # IB sometimes doesn't include the exact end time when it's the boundary
                chunk_end_for_api = chunk_end + timedelta(seconds=5)

                self.logger.info(
                    f"Chunk {chunk_num}: AAPL idx [{current_idx}:{chunk_end_idx}] = "
                    f"{bars_per_chunk} bars from {chunk_start_str} to {chunk_end_str}"
                )

                # Create chunk spec (use buffered end time for API request)
                chunk_spec = ChunkSpec(
                    req_id=self.req_id_gen.generate_id(),
                    symbol=config.symbol,
                    chunk_num=chunk_num,
                    contract=contract,
                    start_datetime=current_start,
                    end_datetime=chunk_end_for_api,
                    bar_size=config.bar_size,
                    what_to_show=config.what_to_show,
                    use_rth=config.use_rth
                )

                # Download chunk
                if self.chunk_downloader is None:
                    self.logger.error("Chunk downloader not initialized")
                    return filled_data

                result = self.chunk_downloader.download_chunk_with_retry(chunk_spec)

                if result and result.success:
                    # Validate overlap and extract only missing data that exists in AAPL
                    validated_data, validation_error = self._validate_and_extract_gap_data(
                        result.data,
                        original_df,
                        original_dates,
                        aapl_dates_set
                    )

                    if validation_error:
                        # Overlap validation failed - log and continue
                        self.write_chunk_log(ChunkLogRecord(
                            symbol=config.symbol,
                            chunk_num=chunk_num,
                            start_datetime=current_start.strftime("%Y%m%d %H:%M:%S"),
                            end_datetime=chunk_end.strftime("%Y%m%d %H:%M:%S"),
                            status="OVERLAP_MISMATCH",
                            bars_downloaded=len(result.data),
                            bars_filled=0,
                            details=validation_error
                        ))
                        self.logger.error(
                            f"Overlap validation failed for chunk {chunk_num}: {validation_error}"
                        )
                    else:
                        # Success - add data and log
                        filled_data.extend(validated_data)
                        self.write_chunk_log(ChunkLogRecord(
                            symbol=config.symbol,
                            chunk_num=chunk_num,
                            start_datetime=current_start.strftime("%Y%m%d %H:%M:%S"),
                            end_datetime=chunk_end.strftime("%Y%m%d %H:%M:%S"),
                            status="SUCCESS",
                            bars_downloaded=len(result.data),
                            bars_filled=len(validated_data),
                            details=f"Validated {len(result.data) - len(validated_data)} overlapping bars"
                        ))
                        self.logger.info(
                            f"Successfully validated and extracted chunk {chunk_num}: "
                            f"{len(validated_data)} bars (from {len(result.data)} downloaded)"
                        )
                elif result and result.timeout:
                    # Timeout - log and move to next chunk
                    self.write_chunk_log(ChunkLogRecord(
                        symbol=config.symbol,
                        chunk_num=chunk_num,
                        start_datetime=current_start.strftime("%Y%m%d %H:%M:%S"),
                        end_datetime=chunk_end.strftime("%Y%m%d %H:%M:%S"),
                        status="TIMEOUT",
                        bars_downloaded=0,
                        bars_filled=0,
                        details="Download timed out"
                    ))
                    self.logger.warning(
                        f"Timeout on chunk {chunk_num}, moving to next chunk"
                    )
                else:
                    # Other failure - log and continue
                    self.write_chunk_log(ChunkLogRecord(
                        symbol=config.symbol,
                        chunk_num=chunk_num,
                        start_datetime=current_start.strftime("%Y%m%d %H:%M:%S"),
                        end_datetime=chunk_end.strftime("%Y%m%d %H:%M:%S"),
                        status="FAILED",
                        bars_downloaded=0,
                        bars_filled=0,
                        details="Download failed"
                    ))
                    self.logger.error(
                        f"Failed to download chunk {chunk_num}"
                    )

                # Move to next chunk by advancing exactly bars_per_chunk positions in AAPL list
                current_idx += bars_per_chunk

                # Wait between chunks
                time.sleep(WAIT_TIME_BETWEEN_REQUESTS)

        return filled_data

    def _validate_and_extract_gap_data(self, downloaded_data: List[Dict],
                                        original_df: pd.DataFrame,
                                        original_dates: Set[str],
                                        aapl_dates: Set[str]) -> Tuple[List[Dict], Optional[str]]:
        """
        Validate overlapping data and extract only the missing portion that exists in AAPL.

        CRITICAL: Only extract bars that exist in AAPL reference dates.
        This ensures the stock data exactly matches AAPL date range.

        Args:
            downloaded_data: Data downloaded from IB API
            original_df: Original stock dataframe
            original_dates: Set of existing stock dates
            aapl_dates: Set of AAPL reference dates (master date list)

        Returns:
            Tuple of (validated_data, error_message)
            - validated_data: List of only missing bars that exist in AAPL
            - error_message: None if success, error message if validation fails
        """
        if not downloaded_data:
            return [], "No data downloaded"

        # FIRST: Filter downloaded data to only include bars that exist in AAPL
        aapl_filtered_data = [bar for bar in downloaded_data if bar['Date'] in aapl_dates]
        non_aapl_bars = len(downloaded_data) - len(aapl_filtered_data)

        if non_aapl_bars > 0:
            self.logger.info(
                f"Filtered out {non_aapl_bars} bars not in AAPL reference "
                f"(keeping {len(aapl_filtered_data)} bars)"
            )

        # SECOND: Separate into overlapping (exist in stock) and missing (don't exist in stock)
        overlapping_data = []
        missing_data = []

        for bar in aapl_filtered_data:
            if bar['Date'] in original_dates:
                overlapping_data.append(bar)
            else:
                missing_data.append(bar)

        self.logger.info(
            f"Downloaded {len(downloaded_data)} bars: "
            f"{len(overlapping_data)} overlap, {len(missing_data)} new"
        )

        # If no new data (all overlap), skip validation and return empty
        if not missing_data:
            self.logger.info(
                "No new data found (all downloaded bars already exist), skipping validation"
            )
            return [], None

        # If no overlap, all data is new (large gap)
        if not overlapping_data:
            self.logger.info("No overlap detected, all data is new")
            return missing_data, None

        # Validate overlapping data (ALL bars, not just a sample)
        validation_errors = []
        overlap_sample_size = len(overlapping_data)

        for bar in overlapping_data:
            date_str = bar['Date']

            # Find matching row in original dataframe
            matching_rows = original_df[original_df['Date'] == date_str]

            if matching_rows.empty:
                continue

            original_bar = matching_rows.iloc[0]

            # Compare key fields with tolerance
            fields_to_compare = ['Open', 'High', 'Low', 'Close', 'Volume']

            for field in fields_to_compare:
                downloaded_val = bar[field]
                original_val = original_bar[field]

                # Skip comparison for zero values
                if downloaded_val == 0 and original_val == 0:
                    continue

                # Calculate relative difference
                if original_val != 0:
                    rel_diff = abs(downloaded_val - original_val) / abs(original_val)
                else:
                    rel_diff = abs(downloaded_val - original_val)

                if rel_diff > OVERLAP_TOLERANCE:
                    error_msg = (
                        f"Mismatch at {date_str} {field}: "
                        f"downloaded={downloaded_val}, original={original_val}, "
                        f"diff={rel_diff:.2%}"
                    )
                    validation_errors.append(error_msg)
                    self.logger.warning(error_msg)

        # If validation errors exist, return error
        if validation_errors:
            error_summary = f"Found {len(validation_errors)} mismatches in {overlap_sample_size} overlapping bars"
            self.logger.error(error_summary)
            return [], error_summary

        # Validation passed, return only missing data
        self.logger.info(
            f"Overlap validation passed (validated all {overlap_sample_size} overlapping bars), "
            f"extracting {len(missing_data)} missing bars"
        )

        return missing_data, None

    def _create_contract(self, config: StockConfig) -> Contract:
        """Create an IB Contract object."""
        contract = Contract()
        contract.symbol = config.symbol
        contract.secType = config.sec_type
        contract.currency = config.currency
        contract.exchange = config.exchange
        return contract

    def _generate_filename(self, df: pd.DataFrame, symbol: str, save_timestamp: str, suffix: str = "") -> str:
        """
        Generate filename with start and end timestamps from actual data.

        Args:
            df: DataFrame containing the data
            symbol: Stock symbol
            save_timestamp: Timestamp when file is being saved
            suffix: Optional suffix to append before .csv (e.g., "_gapfil")

        Returns:
            Filename string in format: {symbol}_saved_{timestamp}_start_{start}_end_{end}{suffix}.csv
        """
        if len(df) == 0:
            return f"{symbol}_saved_{save_timestamp}{suffix}.csv"

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

            return f"{symbol}_saved_{save_timestamp}_start_{start_timestamp}_end_{end_timestamp}{suffix}.csv"

        except Exception as e:
            self.logger.warning(f"Error generating filename: {e}")
            return f"{symbol}_saved_{save_timestamp}{suffix}.csv"

    def _save_results(self, symbol: str, original_df: pd.DataFrame,
                      filled_data: List[Dict]) -> None:
        """Save the complete data (original + filled gaps)."""
        timestamp = datetime.now(self.et_tz).strftime("%Y%m%d_%H%M%S")

        # Combine original and filled data
        if filled_data:
            filled_df = pd.DataFrame(filled_data)
            complete_df = pd.concat([original_df, filled_df], ignore_index=True)
            complete_df = complete_df.drop_duplicates(subset=['Date'], keep='first')
            complete_df = complete_df.sort_values('Date')
        else:
            complete_df = original_df

        # Generate filename with start and end timestamps from actual data
        complete_filename = self._generate_filename(complete_df, symbol, timestamp, "_gapfil")

        # Save complete data
        complete_path = self.data_dir / complete_filename
        complete_df.to_csv(complete_path, index=False)
        self.logger.info(
            f"Saved complete data: {complete_filename} "
            f"({len(complete_df)} total rows, {len(filled_data)} filled)"
        )


def setup_logging(log_file: Path,
                  ib_downloader_level: int = logging.INFO,
                  gap_manager_level: int = logging.INFO,
                  ibapi_level: int = logging.WARNING) -> None:
    """
    Set up global logging configuration to write to both console and file.

    Args:
        log_file: Path to the log file
        ib_downloader_level: Logging level for IBDataDownloader (default: INFO)
        gap_manager_level: Logging level for GapFillManager, ChunkDownloader, and GapAnalyzer (default: INFO)
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
    logging.getLogger('GapFillManager').setLevel(gap_manager_level)
    logging.getLogger('ChunkDownloader').setLevel(gap_manager_level)
    logging.getLogger('GapAnalyzer').setLevel(gap_manager_level)
    logging.getLogger('ibapi').setLevel(ibapi_level)

    logging.info(f"Logging to file: {log_file}")


def main():
    """Main execution function."""
    # Initialize manager
    manager = GapFillManager()

    # Set up logging
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = manager.data_dir / f"_log_gap_downloader_{timestamp}.log"

    # Configure logging levels for each component
    # Available levels: logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL
    setup_logging(
        log_file,
        ib_downloader_level=logging.INFO,    # IBDataDownloader log level
        gap_manager_level=logging.INFO,      # GapFillManager, ChunkDownloader, and GapAnalyzer log level
        ibapi_level=logging.WARNING          # ibapi library log level
    )

    logging.info("="*80)
    logging.info("STOCK DATA GAP DOWNLOADER - STARTED")
    logging.info("="*80)

    try:
        # Connect to IB
        if not manager.connect_to_ib():
            print("Failed to connect to Interactive Brokers")
            return

        # Initialize consolidated log file
        manager.initialize_consolidated_log()

        # Load AAPL reference
        print("\nLoading AAPL reference data...")
        aapl_df, aapl_dates, aapl_dates_list = manager.load_aapl_reference()

        if aapl_df is None or aapl_dates is None or aapl_dates_list is None:
            print("ERROR: Could not load AAPL reference data")
            return

        print(f"AAPL reference loaded: {len(aapl_dates)} unique dates")

        # Load stock configs
        stock_configs = manager.load_config()

        # Remove AAPL from processing list
        stock_configs = [c for c in stock_configs if c.symbol != 'AAPL']

        print(f"\nProcessing {len(stock_configs)} stocks...")

        # Process each stock
        for config in stock_configs:
            manager.process_stock(config, aapl_dates, aapl_dates_list)
            time.sleep(WAIT_TIME_BETWEEN_REQUESTS)

        print("\n" + "="*80)
        print("GAP FILLING COMPLETED")
        print("="*80)
        print(f"Log file: {log_file}")

    except Exception as e:
        logging.error(f"Error in main execution: {e}")
        raise

    finally:
        manager.close_consolidated_log()
        manager.disconnect()


if __name__ == "__main__":
    main()
