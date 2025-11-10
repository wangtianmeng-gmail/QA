#!/usr/bin/env python3.12
"""
Stock Historical Data Downloader using Interactive Brokers API - Version 2

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
from typing import Dict, Optional
import logging
import sys
import pytz


# --- Configuration Constants ---
TRADING_HOURS_RTH = 6.5        # Regular Trading Hours (9:30 - 16:00 ET)
TRADING_HOURS_EXTENDED = 16.0  # RTH + typical extended sessions (tune as needed)
DOWNLOAD_TIMEOUT_SECONDS = 60  # Default timeout for download requests
REQUEST_ID_MULTIPLIER = 10000  # Multiplier for base request ID calculation
IB_HOST = "127.0.0.1"          # Interactive Brokers host address
IB_PORT = 7497                 # Interactive Brokers port (7497 for TWS, 4001 for IB Gateway)
IB_CLIENT_ID = 1               # Default client ID for IB connection


def setup_logging(log_file: Path,
                  ib_downloader_level: int = logging.INFO,
                  stock_manager_level: int = logging.INFO,
                  ibapi_level: int = logging.WARNING) -> None:
    """
    Set up global logging configuration to write to both console and file.

    Args:
        log_file: Path to the log file
        ib_downloader_level: Logging level for IBDataDownloader (default: INFO)
        stock_manager_level: Logging level for StockDataManager (default: INFO)
        ibapi_level: Logging level for ibapi library (default: WARNING)
    """
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)  # Set to DEBUG to allow all levels through

    # Clear any existing handlers
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
    logging.getLogger('ibapi').setLevel(ibapi_level)

    logging.info(f"Logging to file: {log_file}")
    logging.info(f"Log levels - IBDataDownloader: {logging.getLevelName(ib_downloader_level)}, "
                f"StockDataManager: {logging.getLevelName(stock_manager_level)}, "
                f"ibapi: {logging.getLevelName(ibapi_level)}")


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
        self.data_download_complete = {}
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
        self.data_download_complete[reqId] = True
        self.logger.info(f"Data download complete for reqId: {reqId} (from {start} to {end})")


class StockDataManager:
    """
    Manages stock data downloading and saving operations with market hours awareness.
    Enhanced with configurable RTH and improved CSV file naming.
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
        self.app: Optional[IBDataDownloader] = None
        self.connection_thread: Optional[threading.Thread] = None
        self.logger = logging.getLogger('StockDataManager')

        # Timezone for timestamp parsing
        self.et_tz = pytz.timezone('US/Eastern')

        self.logger.info(f"Project root: {self.project_root}")
        self.logger.info(f"Config file: {self.config_file}")
        self.logger.info(f"Output directory: {self.output_dir}")

    def load_config(self) -> pd.DataFrame:
        """
        Load stock configuration from CSV file.

        Returns:
            DataFrame containing stock configuration
        """
        try:
            config_df = pd.read_csv(self.config_file)
            self.logger.info(f"Loaded configuration for {len(config_df)} stocks from {self.config_file}")
            return config_df
        except FileNotFoundError:
            self.logger.error(f"Configuration file {self.config_file} not found")
            raise
        except Exception as e:
            self.logger.error(f"Error loading configuration: {e}")
            raise

    def create_contract(self, symbol: str, sec_type: str = "STK",
                       currency: str = "USD", exchange: str = "SMART") -> Contract:
        """
        Create an IB Contract object.

        Args:
            symbol: Stock ticker symbol
            sec_type: Security type (STK for stocks, IND for indices)
            currency: Currency code
            exchange: Exchange name

        Returns:
            Contract object configured for the specified security
        """
        contract = Contract()
        contract.symbol = symbol
        contract.secType = sec_type
        contract.currency = currency
        contract.exchange = exchange
        return contract

    def connect_to_ib(self, host: str = IB_HOST, port: int = IB_PORT,
                     client_id: int = IB_CLIENT_ID) -> bool:
        """
        Connect to Interactive Brokers TWS or IB Gateway.

        Args:
            host: IB Gateway host
            port: IB Gateway port (7497 for TWS, 4001 for IB Gateway)
            client_id: Unique client identifier

        Returns:
            True if connection successful, False otherwise
        """
        self.app = IBDataDownloader()
        self.app.connect(host, port, client_id)

        self.connection_thread = threading.Thread(
            target=self._run_connection,
            daemon=True
        )
        self.connection_thread.start()

        time.sleep(1)

        if self.app.isConnected():
            self.logger.info(f"Successfully connected to IB at {host}:{port}")
            return True
        else:
            self.logger.error("Failed to connect to IB")
            return False

    def _run_connection(self):
        """Run the IB API connection in a separate thread."""
        self.logger.info("Starting IB API connection thread")
        assert self.app is not None
        self.app.run()
        self.logger.info("IB API connection thread ended")

    def compute_trading_hours_per_day(self, use_rth: bool) -> float:
        """
        Calculate trading hours per day based on RTH setting.

        Args:
            use_rth: True for regular trading hours only, False for extended hours

        Returns:
            Trading hours per day (6.5 for RTH, 16.0 for extended)
        """
        return TRADING_HOURS_RTH if use_rth else TRADING_HOURS_EXTENDED

    def _parse_bar_size_to_seconds(self, bar_size: str) -> int:
        """
        Parse bar_size string to seconds per bar.

        Args:
            bar_size: Bar size string (e.g., "5 secs", "1 min", "1 hour")

        Returns:
            Number of seconds per bar
        """
        import re

        bar_size_lower = bar_size.lower().strip()

        # Extract the numeric value from the string
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
        self.logger.warning(f"Failed to parse bar_size '{bar_size}', defaulting to 5 seconds")
        return 5

    def request_historical_data(self, req_id: int, contract: Contract,
                               end_datetime: str = '',
                               duration: str = "10800 S",
                               bar_size: str = "5 secs",
                               what_to_show: str = "TRADES",
                               use_rth: bool = True) -> bool:
        """
        Request historical data from IB.

        Args:
            req_id: Unique request identifier
            contract: Contract object for the security
            end_datetime: End date/time for the request (empty string for current time)
                         Format: "YYYYMMDD HH:MM:SS TZ"
            duration: Duration in seconds (e.g., "10800 S" for 3 hours)
            bar_size: Bar size (e.g., "5 secs", "1 min", "1 hour", "1 day")
            what_to_show: Data type to show (e.g., "TRADES", "MIDPOINT", "BID", "ASK")
            use_rth: Use regular trading hours only (True) or include extended hours (False)

        Returns:
            True if request sent successfully
        """
        try:
            assert self.app is not None
            self.app.data_download_complete[req_id] = False

            self.app.reqHistoricalData(
                reqId=req_id,
                contract=contract,
                endDateTime=end_datetime,
                durationStr=duration,
                barSizeSetting=bar_size,
                whatToShow=what_to_show,
                useRTH=1 if use_rth else 0,
                formatDate=1,
                keepUpToDate=0,
                chartOptions=[]
            )

            self.logger.info(f"Requested historical data for {contract.symbol} "
                           f"(reqId: {req_id}, duration: {duration}, end: {end_datetime or 'now'}, useRTH: {use_rth})")
            return True

        except Exception as e:
            self.logger.error(f"Error requesting historical data: {e}")
            return False

    def wait_for_download(self, req_id: int, timeout: int = DOWNLOAD_TIMEOUT_SECONDS) -> bool:
        """
        Wait for data download to complete.

        Args:
            req_id: Request identifier to wait for
            timeout: Maximum time to wait in seconds

        Returns:
            True if download completed, False if timeout
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.app and self.app.data_download_complete.get(req_id, False):
                return True
            time.sleep(0.1)

        self.logger.warning(f"Timeout waiting for data download (reqId: {req_id})")
        return False

    def _create_timing_record(self, symbol: str, chunk_num: int,
                            chunk_start_datetime: datetime, chunk_end_datetime: datetime,
                            chunk_duration: float, bars_in_chunk: int,
                            trading_hours_in_chunk: float, data_start: str, data_end: str,
                            requested_hours: float, use_rth: bool) -> Dict:
        """
        Create a timing record for a chunk download.

        Args:
            symbol: Stock symbol
            chunk_num: Chunk number
            chunk_start_datetime: When the chunk download started
            chunk_end_datetime: When the chunk download ended
            chunk_duration: Duration of download in seconds
            bars_in_chunk: Number of bars downloaded
            trading_hours_in_chunk: Trading hours downloaded
            data_start: Start timestamp of data
            data_end: End timestamp of data
            requested_hours: Hours requested for this chunk
            use_rth: Whether RTH was used

        Returns:
            Dictionary containing timing information
        """
        return {
            'symbol': symbol,
            'chunk_number': chunk_num,
            'chunk_start_time': chunk_start_datetime.strftime("%Y-%m-%d %H:%M:%S"),
            'chunk_end_time': chunk_end_datetime.strftime("%Y-%m-%d %H:%M:%S"),
            'download_duration_seconds': chunk_duration,
            'bars_downloaded': bars_in_chunk,
            'trading_hours_downloaded': trading_hours_in_chunk,
            'data_start': data_start,
            'data_end': data_end,
            'requested_hours': requested_hours,
            'use_rth': use_rth
        }

    def _process_chunk_data(self, chunk_data: list, chunk_num: int,
                          bar_size: str, symbol: str) -> tuple:
        """
        Process downloaded chunk data and extract timing information.

        Args:
            chunk_data: List of data bars from the chunk
            chunk_num: Chunk number
            bar_size: Bar size setting
            symbol: Stock symbol

        Returns:
            Tuple of (trading_hours_in_chunk, earliest_dt, earliest_date_str, latest_date_str)
            or (0.0, None, 'ERROR', 'ERROR') on error
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

            seconds_per_bar = self._parse_bar_size_to_seconds(bar_size)
            bars_in_chunk = len(chunk_data)
            trading_hours_in_chunk = (bars_in_chunk * seconds_per_bar) / 3600.0

            self.logger.info(f"Chunk {chunk_num}: Downloaded {trading_hours_in_chunk:.2f} trading hours "
                           f"({bars_in_chunk} bars from {earliest_date_str} to {latest_date_str})")

            return (trading_hours_in_chunk, earliest_dt, earliest_date_str, latest_date_str)

        except Exception as e:
            self.logger.error(f"Error parsing date '{earliest_date_str}': {e}")
            return (0.0, None, 'ERROR', 'ERROR')

    def _save_all_data(self, all_data: list, symbol: str, chunk_timing_data: list) -> Optional[str]:
        """
        Save all collected data to CSV file. Timing data will be aggregated separately.

        Args:
            all_data: List of all data bars collected
            symbol: Stock symbol
            chunk_timing_data: List of timing records for each chunk (not saved here)

        Returns:
            Path to saved CSV file, or None if no data
        """
        save_timestamp = datetime.now(self.et_tz).strftime("%Y%m%d_%H%M%S")

        # Note: Timing data is no longer saved here - it will be aggregated in download_stocks()

        if not all_data:
            self.logger.warning(f"No data collected for {symbol}")
            return None

        # Create DataFrame and remove duplicates
        df = pd.DataFrame(all_data)
        df = df.drop_duplicates(subset=['Date'])
        df = df.sort_values('Date')

        # Generate filename with start and end timestamps
        filename = self._generate_filename(df, symbol, save_timestamp)

        df.set_index("Date", inplace=True)
        filepath = self.output_dir / filename

        df.to_csv(filepath, index=True)
        self.logger.info(f"Saved total of {len(df)} rows to {filepath}")

        return str(filepath)

    def _generate_filename(self, df: pd.DataFrame, symbol: str, save_timestamp: str) -> str:
        """
        Generate filename with start and end timestamps from actual data.

        Args:
            df: DataFrame containing the downloaded data
            symbol: Stock symbol
            save_timestamp: Timestamp when file is being saved

        Returns:
            Filename string
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
            self.logger.warning(f"Error parsing dates for filename, using default: {e}")
            return f"{symbol}_saved_{save_timestamp}.csv"

    def download_stock_with_pagination(self, symbol: str, sec_type: str, currency: str,
                                      exchange: str, total_days: int, bar_size: str,
                                      what_to_show: str, initial_end_datetime_et: Optional[str],
                                      base_req_id: int,
                                      use_rth: bool = True, chunk_hours: float = 3.0,
                                      wait_time: int = 2) -> tuple[Optional[str], list]:
        """
        Download historical data for a single stock with iterative real-time adjustment.

        This method uses an iterative approach that checks the actual data downloaded from each
        chunk and uses that information to determine the next chunk's parameters. This eliminates
        the need for a holiday calendar or market hours pre-calculation.

        Args:
            symbol: Stock symbol
            sec_type: Security type
            currency: Currency code
            exchange: Exchange name
            total_days: Total trading days to download (used to calculate total hours)
            bar_size: Bar size setting
            what_to_show: What data to show
            initial_end_datetime_et: Initial end datetime in ET timezone from config
                                    (format: "YYYYMMDD HH:MM:SS")
            base_req_id: Base request ID (will be incremented for each chunk)
            use_rth: Use regular trading hours only (True) or include extended hours (False)
            chunk_hours: Maximum hours per chunk (default 3.0)
            wait_time: Wait time between requests

        Returns:
            Tuple of (Path to saved CSV file or None, list of timing records)
        """
        contract = self.create_contract(symbol, sec_type, currency, exchange)

        all_data = []
        chunk_timing_data = []
        trading_hours_per_day = self.compute_trading_hours_per_day(use_rth)
        total_trading_hours = total_days * trading_hours_per_day
        hours_downloaded = 0.0
        chunk_num = 0

        # Parse initial end datetime from config
        current_end = self._parse_initial_end_time(initial_end_datetime_et)

        # Download chunks iteratively
        while hours_downloaded < total_trading_hours:
            chunk_num += 1
            req_id = base_req_id + chunk_num

            # Calculate duration for this chunk
            remaining_hours = total_trading_hours - hours_downloaded
            current_chunk_hours = min(chunk_hours, remaining_hours)
            duration_seconds = int(current_chunk_hours * 3600)

            end_datetime_str = current_end.strftime("%Y%m%d %H:%M:%S US/Eastern")

            self.logger.info(f"Downloading {symbol} chunk {chunk_num} "
                           f"(requesting {current_chunk_hours:.2f} hours, ending at {end_datetime_str}, useRTH={use_rth})")

            # Download chunk and record timing
            chunk_result = self._download_single_chunk(
                req_id, contract, end_datetime_str, duration_seconds,
                bar_size, what_to_show, use_rth, symbol, chunk_num,
                current_chunk_hours, all_data, chunk_timing_data
            )

            if chunk_result is None:
                # Failed to request data
                self.logger.error(f"Failed to request data for {symbol} chunk {chunk_num}")
                break

            hours_downloaded_in_chunk, next_end = chunk_result

            # Update progress
            hours_downloaded += hours_downloaded_in_chunk
            if next_end:
                current_end = next_end
            else:
                # Fallback: subtract requested hours if we couldn't parse the data
                current_end -= timedelta(hours=current_chunk_hours)

            self.logger.info(f"Total progress: {hours_downloaded:.2f}/{total_trading_hours:.2f} hours")

            # Wait between chunks to avoid rate limiting
            time.sleep(wait_time)

        # Save all collected data and return with timing data
        filepath = self._save_all_data(all_data, symbol, chunk_timing_data)
        return (filepath, chunk_timing_data)

    def _parse_initial_end_time(self, initial_end_datetime_et: Optional[str]) -> datetime:
        """
        Parse the initial end datetime from config or use current time.

        Args:
            initial_end_datetime_et: End datetime string from config

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
                self.logger.warning(f"Invalid initial_end_datetime_et format: {initial_end_datetime_et}, using current time")

        end_time = datetime.now(self.et_tz)
        self.logger.info("Using current time as initial end_datetime")
        return end_time

    def _download_single_chunk(self, req_id: int, contract: Contract,
                              end_datetime_str: str, duration_seconds: int,
                              bar_size: str, what_to_show: str, use_rth: bool,
                              symbol: str, chunk_num: int, requested_hours: float,
                              all_data: list, chunk_timing_data: list) -> Optional[tuple]:
        """
        Download a single chunk of data and update tracking lists.

        Args:
            req_id: Request ID
            contract: IB Contract object
            end_datetime_str: End datetime string
            duration_seconds: Duration in seconds
            bar_size: Bar size setting
            what_to_show: What data to show
            use_rth: Use regular trading hours
            symbol: Stock symbol
            chunk_num: Current chunk number
            requested_hours: Hours requested for this chunk
            all_data: List to append downloaded data to
            chunk_timing_data: List to append timing records to

        Returns:
            Tuple of (hours_downloaded, next_end_datetime) or None if request failed
        """
        # Record chunk start time
        chunk_start_time = time.time()
        chunk_start_datetime = datetime.now(self.et_tz)

        # Request data
        if not self.request_historical_data(
            req_id, contract,
            end_datetime=end_datetime_str,
            duration=f"{duration_seconds} S",
            bar_size=bar_size,
            what_to_show=what_to_show,
            use_rth=use_rth
        ):
            return None

        # Wait for download to complete
        if not self.wait_for_download(req_id, timeout=DOWNLOAD_TIMEOUT_SECONDS):
            # Timeout - cancel the outstanding request and cleanup
            if self.app:
                self.app.cancelHistoricalData(req_id)
                self.logger.info(f"Cancelled historical data request {req_id} due to timeout")

                # Clean up data dictionary entries to prevent stale data
                if req_id in self.app.data:
                    del self.app.data[req_id]
                if req_id in self.app.data_download_complete:
                    del self.app.data_download_complete[req_id]

            chunk_end_time = time.time()
            chunk_end_datetime = datetime.now(self.et_tz)
            chunk_duration = chunk_end_time - chunk_start_time

            self.logger.error(f"Timeout downloading {symbol} chunk {chunk_num}")

            timing_record = self._create_timing_record(
                symbol, chunk_num, chunk_start_datetime, chunk_end_datetime,
                chunk_duration, 0, 0.0, 'TIMEOUT', 'TIMEOUT', requested_hours, use_rth
            )
            chunk_timing_data.append(timing_record)

            return (requested_hours, None)  # Assume requested hours and signal fallback

        # Download completed successfully
        chunk_end_time = time.time()
        chunk_end_datetime = datetime.now(self.et_tz)
        chunk_duration = chunk_end_time - chunk_start_time

        # Process the downloaded data
        if not (self.app and req_id in self.app.data):
            # No data received
            timing_record = self._create_timing_record(
                symbol, chunk_num, chunk_start_datetime, chunk_end_datetime,
                chunk_duration, 0, 0.0, 'FAILED', 'FAILED', requested_hours, use_rth
            )
            chunk_timing_data.append(timing_record)
            return (requested_hours, None)

        chunk_data = self.app.data[req_id]

        if not chunk_data:
            # Empty data
            self.logger.warning(f"No data for {symbol} chunk {chunk_num}")
            timing_record = self._create_timing_record(
                symbol, chunk_num, chunk_start_datetime, chunk_end_datetime,
                chunk_duration, 0, 0.0, 'NO_DATA', 'NO_DATA', requested_hours, use_rth
            )
            chunk_timing_data.append(timing_record)
            return (requested_hours, None)

        # Process chunk data
        all_data.extend(chunk_data)
        self.logger.info(f"Downloaded {len(chunk_data)} bars for chunk {chunk_num}")

        trading_hours, earliest_dt, data_start, data_end = self._process_chunk_data(
            chunk_data, chunk_num, bar_size, symbol
        )

        # Log progress
        if trading_hours > 0:
            self.logger.info(f"Download time: {chunk_duration:.2f} seconds")

        # Create and save timing record
        timing_record = self._create_timing_record(
            symbol, chunk_num, chunk_start_datetime, chunk_end_datetime,
            chunk_duration, len(chunk_data), trading_hours, data_start, data_end,
            requested_hours, use_rth
        )
        chunk_timing_data.append(timing_record)

        # Return hours downloaded and next end time
        if trading_hours > 0 and earliest_dt:
            return (trading_hours, earliest_dt)
        else:
            # Fallback: use requested hours
            return (requested_hours, None)

    def download_stocks(self, wait_time: int = 2) -> Dict[str, str]:
        """
        Download historical data for all stocks in configuration with iterative real-time adjustment.

        Args:
            wait_time: Time to wait between requests (seconds)

        Returns:
            Dictionary mapping stock symbols to saved file paths
        """
        config_df = self.load_config()
        saved_files = {}
        all_timing_data = []  # Collect timing data from all stocks
        total_stocks = len(config_df)

        for idx, row in config_df.iterrows():
            symbol = row['symbol']
            sec_type = row.get('sec_type', 'STK')
            currency = row.get('currency', 'USD')
            exchange = row.get('exchange', 'SMART')
            total_days = float(row.get('total_days', 1))
            bar_size = row.get('bar_size', '5 secs')
            what_to_show = row.get('what_to_show', 'TRADES')
            initial_end_datetime_et = row.get('initial_end_datetime_et', None)

            # Support optional use_rth column (1/0 or True/False)
            use_rth = True  # Default to regular trading hours
            if 'use_rth' in row:
                try:
                    use_rth = bool(int(row['use_rth']))
                except Exception:
                    # Handle string values like 'true', 'false', 'yes', 'no'
                    use_rth = str(row['use_rth']).lower() not in ('0', 'false', 'no')

            self.logger.info(f"Processing {symbol} ({idx + 1}/{total_stocks}) - "
                           f"{total_days} trading days (useRTH={use_rth})...")

            # Use idx * REQUEST_ID_MULTIPLIER as base req_id to avoid conflicts
            # (supports up to REQUEST_ID_MULTIPLIER chunks per stock)
            base_req_id = idx * REQUEST_ID_MULTIPLIER

            filepath, chunk_timing_data = self.download_stock_with_pagination(
                symbol, sec_type, currency, exchange, total_days,
                bar_size, what_to_show, initial_end_datetime_et,
                base_req_id, use_rth=use_rth, wait_time=wait_time
            )

            if filepath:
                saved_files[symbol] = filepath

            # Collect timing data from this stock
            if chunk_timing_data:
                all_timing_data.extend(chunk_timing_data)

            # Wait between stocks, but not after the last one
            if idx + 1 < total_stocks:
                time.sleep(wait_time)

        # Save aggregated timing data for all stocks
        if all_timing_data:
            timing_df = pd.DataFrame(all_timing_data)
            timestamp = datetime.now(self.et_tz).strftime("%Y%m%d_%H%M%S")
            timing_filename = f"stock_download_v2_{timestamp}.csv"
            timing_filepath = self.output_dir / timing_filename
            timing_df.to_csv(timing_filepath, index=False)
            self.logger.info(f"Saved aggregated timing data ({len(timing_df)} chunks from {total_stocks} stocks) to {timing_filepath}")

        return saved_files

    def disconnect(self):
        """Disconnect from IB."""
        if self.app and self.app.isConnected():
            self.app.disconnect()
            self.logger.info("Disconnected from IB")
            time.sleep(0.5)


def main():
    """Main execution function."""
    # You can optionally pass custom paths as command-line arguments
    # Usage: python stock_data_downloader_v2.py [config_file] [output_dir]
    config_file = sys.argv[1] if len(sys.argv) > 1 else None
    output_dir = sys.argv[2] if len(sys.argv) > 2 else None

    manager = StockDataManager(
        config_file=config_file,
        output_dir=output_dir
    )

    # Set up logging to both console and file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = manager.output_dir / f"stock_download_v2_{timestamp}.log"

    # Configure logging levels for each component
    # Available levels: logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL
    setup_logging(
        log_file,
        ib_downloader_level=logging.INFO,    # IBDataDownloader log level
        stock_manager_level=logging.INFO,    # StockDataManager log level
        ibapi_level=logging.WARNING          # ibapi library log level
    )

    try:
        if not manager.connect_to_ib():
            print("Failed to connect to Interactive Brokers")
            return

        print("\nDownloading stock data with configurable RTH (v2 with enhanced features)...")
        saved_files = manager.download_stocks(wait_time=2)

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
