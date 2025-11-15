#!/usr/bin/env python3.12
"""
Stock Historical Data Downloader using Interactive Brokers API - Version 3

This version refactors v2 with improved architecture:
- Separation of concerns using focused classes
- Dataclasses for type safety
- Better testability
- Cleaner code structure

Version History:
- v1: Initial implementation
- v2: Iterative download with retry mechanism
- v3: Refactored architecture with better code organization

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
TRADING_HOURS_RTH = 6.5
TRADING_HOURS_EXTENDED = 16.0
DOWNLOAD_TIMEOUT_SECONDS = 30
REQUEST_ID_MULTIPLIER = 10000
CHUNK_HOURS = 3.0
MAX_CHUNK_RETRIES = 3
WAIT_TIME_BETWEEN_REQUESTS = 2.0
IB_HOST = "127.0.0.1"
IB_PORT = 7497
IB_CLIENT_ID = 1


# --- Dataclasses ---

@dataclass
class StockConfig:
    """Configuration for a single stock download"""
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
    """Specification for downloading a single chunk"""
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
class ChunkResult:
    """Result of a chunk download"""
    req_id: int
    chunk_num: int
    success: bool
    data: List[Dict]
    hours_downloaded: float
    next_end_datetime: Optional[datetime]
    timing_record: 'TimingRecord'
    retry_attempt: int = 0


@dataclass
class TimingRecord:
    """Timing information for a chunk download"""
    symbol: str
    chunk_number: int
    retry_attempt: int
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
class DownloadProgress:
    """Track download progress for a stock"""
    symbol: str
    total_trading_hours: float
    hours_downloaded: float = 0.0
    chunks_completed: int = 0
    current_end_datetime: Optional[datetime] = None
    all_data: List[Dict] = field(default_factory=list)
    timing_records: List[TimingRecord] = field(default_factory=list)


# --- Helper Classes ---

class RequestIDGenerator:
    """Generate unique request IDs for IB API calls"""

    def __init__(self):
        self.logger = logging.getLogger('RequestIDGenerator')

    def generate_id(self, stock_num: int, chunk_num: int, retry_attempt: int) -> int:
        """
        Generate unique request ID using formula:
        (base_req_id * 100000) + (chunk_num * 10) + retry_attempt

        Args:
            stock_num: Stock number (1-indexed)
            chunk_num: Chunk number (1-indexed)
            retry_attempt: Retry attempt (0-indexed)

        Returns:
            Unique request ID
        """
        base_req_id = (stock_num - 1) * REQUEST_ID_MULTIPLIER
        req_id = (base_req_id * 100000) + (chunk_num * 10) + retry_attempt
        self.logger.debug(f"Generated req_id={req_id} for stock={stock_num}, chunk={chunk_num}, retry={retry_attempt}")
        return req_id


class ProgressTracker:
    """Track download progress and timing information"""

    def __init__(self, output_dir: Path, et_tz: pytz.tzinfo.BaseTzInfo):
        self.output_dir = output_dir
        self.et_tz = et_tz
        self.logger = logging.getLogger('ProgressTracker')
        self.timing_csv_path: Path = self._initialize_timing_csv()

    def _initialize_timing_csv(self) -> Path:
        """Initialize timing CSV file with headers"""
        timestamp = datetime.now(self.et_tz).strftime("%Y%m%d_%H%M%S")
        timing_filename = f"stock_download_v3_{timestamp}.csv"
        timing_csv_path = self.output_dir / timing_filename

        header = "symbol,chunk_number,retry_attempt,chunk_start_time,chunk_end_time," \
                "download_duration_seconds,bars_downloaded,trading_hours_downloaded," \
                "data_start,data_end,requested_hours,use_rth\n"

        with open(timing_csv_path, 'w') as f:
            f.write(header)

        self.logger.info(f"Initialized timing data file: {timing_csv_path}")
        return timing_csv_path

    def record_chunk(self, timing_record: TimingRecord) -> None:
        """Save timing record to CSV file"""
        try:
            with open(self.timing_csv_path, 'a') as f:
                line = f"{timing_record.symbol},{timing_record.chunk_number},{timing_record.retry_attempt}," \
                       f"{timing_record.chunk_start_time},{timing_record.chunk_end_time}," \
                       f"{timing_record.download_duration_seconds},{timing_record.bars_downloaded}," \
                       f"{timing_record.trading_hours_downloaded},{timing_record.data_start}," \
                       f"{timing_record.data_end},{timing_record.requested_hours},{timing_record.use_rth}\n"
                f.write(line)
        except Exception as e:
            self.logger.error(f"Error saving timing record: {e}")


class FileManager:
    """Handle CSV file operations for stock data"""

    def __init__(self, output_dir: Path, et_tz: pytz.tzinfo.BaseTzInfo):
        self.output_dir = output_dir
        self.et_tz = et_tz
        self.logger = logging.getLogger('FileManager')
        self.output_dir.mkdir(exist_ok=True, parents=True)

    def save_stock_data(self, all_data: List[Dict], symbol: str) -> Optional[str]:
        """
        Save stock data to CSV file

        Args:
            all_data: List of all data bars
            symbol: Stock symbol

        Returns:
            Path to saved file, or None if no data
        """
        if not all_data:
            self.logger.warning(f"No data to save for {symbol}")
            return None

        # Create DataFrame and clean data
        df = pd.DataFrame(all_data)
        df = df.drop_duplicates(subset=['Date'])
        df = df.sort_values('Date')

        # Generate filename
        save_timestamp = datetime.now(self.et_tz).strftime("%Y%m%d_%H%M%S")
        filename = self._generate_filename(df, symbol, save_timestamp)

        # Save to CSV
        df.set_index("Date", inplace=True)
        filepath = self.output_dir / filename
        df.to_csv(filepath, index=True)

        self.logger.info(f"Saved {len(df)} rows to {filepath}")
        return str(filepath)

    def _generate_filename(self, df: pd.DataFrame, symbol: str, save_timestamp: str) -> str:
        """Generate filename with start and end timestamps"""
        if len(df) == 0:
            return f"{symbol}_saved_{save_timestamp}.csv"

        try:
            first_date_str = df.iloc[0]['Date']
            last_date_str = df.iloc[-1]['Date']

            # Parse timestamps
            first_dt_parts = first_date_str.rsplit(' ', 1)[0]
            last_dt_parts = last_date_str.rsplit(' ', 1)[0]

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
    """Set up global logging configuration"""
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
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

    # Set logger levels
    logging.getLogger('IBDataDownloader').setLevel(ib_downloader_level)
    logging.getLogger('StockDataManager').setLevel(stock_manager_level)
    logging.getLogger('ChunkDownloader').setLevel(stock_manager_level)
    logging.getLogger('ibapi').setLevel(ibapi_level)

    logging.info(f"Logging to file: {log_file}")


class IBDataDownloader(EWrapper, EClient):
    """Interactive Brokers API client for downloading historical data"""

    def __init__(self):
        EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)
        self.data = {}
        self.data_download_complete = {}
        self.logger = logging.getLogger('IBDataDownloader')

    def error(self, reqId: int, errorCode: int, errorString: str, *_args, **_kwargs):
        """Handle error messages from IB API"""
        self.logger.error(f"ReqId: {reqId}, ErrorCode: {errorCode}, ErrorString: {errorString}")

    def historicalData(self, reqId: int, bar):
        """Receive historical data bars from IB API"""
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
        """Called when all historical data has been received"""
        self.data_download_complete[reqId] = True
        self.logger.info(f"Data download complete for reqId: {reqId} (from {start} to {end})")


class ChunkDownloader:
    """Handle chunk download operations with retry logic"""

    def __init__(self, ib_client: IBDataDownloader, et_tz: pytz.tzinfo.BaseTzInfo,
                 progress_tracker: ProgressTracker):
        self.ib_client = ib_client
        self.et_tz = et_tz
        self.progress_tracker = progress_tracker
        self.logger = logging.getLogger('ChunkDownloader')

    def download_chunk_with_retry(self, chunk_spec: ChunkSpec,
                                   max_retries: int = MAX_CHUNK_RETRIES) -> Optional[ChunkResult]:
        """
        Download a chunk with retry logic

        Args:
            chunk_spec: Chunk specification
            max_retries: Maximum retry attempts

        Returns:
            ChunkResult or None if all retries failed
        """
        for retry_attempt in range(max_retries):
            if retry_attempt > 0:
                self.logger.warning(
                    f"Retry {retry_attempt}/{max_retries - 1} for {chunk_spec.symbol} chunk {chunk_spec.chunk_num}"
                )
                time.sleep(WAIT_TIME_BETWEEN_REQUESTS)

            # Update chunk spec with retry attempt
            retry_spec = replace(chunk_spec, retry_attempt=retry_attempt)

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
        """Download a single chunk"""
        chunk_start_time = time.time()
        chunk_start_datetime = datetime.now(self.et_tz)

        # Request data
        if not self._request_historical_data(chunk_spec):
            return self._create_error_result(chunk_spec, chunk_start_time, chunk_start_datetime, 'FAILED')

        # Wait for download
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
        """Request historical data from IB"""
        try:
            self.ib_client.data_download_complete[chunk_spec.req_id] = False

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
        """Wait for data download to complete"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.ib_client.data_download_complete.get(req_id, False):
                return True
            time.sleep(0.1)

        # Timeout - cancel request and cleanup
        self.ib_client.cancelHistoricalData(req_id)
        self.logger.info(f"Cancelled request {req_id} due to timeout")

        if req_id in self.ib_client.data:
            del self.ib_client.data[req_id]
        if req_id in self.ib_client.data_download_complete:
            del self.ib_client.data_download_complete[req_id]

        self.logger.warning(f"Timeout waiting for download (reqId: {req_id})")
        return False

    def _parse_bar_size_to_seconds(self, bar_size: str) -> int:
        """Parse bar size string to seconds"""
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

        self.logger.warning(f"Failed to parse bar_size '{bar_size}', defaulting to 5 seconds")
        return 5

    def _process_chunk_data(self, chunk_data: List[Dict], bar_size: str) -> tuple:
        """Process chunk data and extract timing information"""
        if not chunk_data:
            return (0.0, None, 'NO_DATA', 'NO_DATA')

        earliest_date_str = chunk_data[0]['Date']
        latest_date_str = chunk_data[-1]['Date']

        try:
            dt_parts = earliest_date_str.rsplit(' ', 1)[0]
            earliest_dt = datetime.strptime(dt_parts, "%Y%m%d %H:%M:%S")
            earliest_dt = self.et_tz.localize(earliest_dt)

            seconds_per_bar = self._parse_bar_size_to_seconds(bar_size)
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
        """Create a timing record"""
        return TimingRecord(
            symbol=chunk_spec.symbol,
            chunk_number=chunk_spec.chunk_num,
            retry_attempt=chunk_spec.retry_attempt,
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
        """Create a successful chunk result"""
        trading_hours, earliest_dt, data_start, data_end = self._process_chunk_data(
            chunk_data, chunk_spec.bar_size
        )

        timing_record = self._create_timing_record(
            chunk_spec, chunk_start_datetime, chunk_end_datetime, chunk_duration,
            len(chunk_data), trading_hours, data_start, data_end
        )

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
    """Main orchestrator for stock data downloading"""

    def __init__(self, config_file: Optional[str] = None, output_dir: Optional[str] = None):
        """Initialize the stock data manager"""
        # Set up paths
        self.project_root = Path(__file__).parent.parent.resolve()

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
        self.et_tz = pytz.timezone('US/Eastern')
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
        """Load stock configuration from CSV file"""
        try:
            config_df = pd.read_csv(self.config_file)
            self.logger.info(f"Loaded configuration for {len(config_df)} stocks")

            stock_configs = []
            for _, row in config_df.iterrows():
                # Parse use_rth
                use_rth = True
                if 'use_rth' in row:
                    try:
                        use_rth = bool(int(row['use_rth']))
                    except Exception:
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
        """Connect to Interactive Brokers"""
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
            # Initialize chunk downloader after connection
            self.chunk_downloader = ChunkDownloader(
                self.ib_client, self.et_tz, self.progress_tracker
            )
            return True
        else:
            self.logger.error("Failed to connect to IB")
            return False

    def _run_connection(self):
        """Run IB API connection in separate thread"""
        self.logger.info("Starting IB API connection thread")
        if self.ib_client is None:
            self.logger.error("IB client is None in _run_connection")
            return
        self.ib_client.run()
        self.logger.info("IB API connection thread ended")

    def disconnect(self):
        """Disconnect from IB"""
        if self.ib_client and self.ib_client.isConnected():
            self.ib_client.disconnect()
            self.logger.info("Disconnected from IB")
            time.sleep(0.5)

    def download_stocks(self, wait_time: float = WAIT_TIME_BETWEEN_REQUESTS) -> Dict[str, str]:
        """Download historical data for all configured stocks"""
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
                    f"Skipping {stock_config.symbol} - download failed after retries"
                )

            # Wait between stocks
            if stock_num < total_stocks:
                time.sleep(wait_time)

        self.logger.info(
            f"Completed downloading {len(saved_files)}/{total_stocks} stocks. "
            f"Timing data saved to {self.progress_tracker.timing_csv_path}"
        )

        return saved_files

    def _download_single_stock(self, config: StockConfig, stock_num: int) -> Optional[str]:
        """Download data for a single stock"""
        # Initialize progress
        trading_hours_per_day = TRADING_HOURS_RTH if config.use_rth else TRADING_HOURS_EXTENDED
        total_trading_hours = round(config.total_days * trading_hours_per_day, 6)

        progress = DownloadProgress(
            symbol=config.symbol,
            total_trading_hours=total_trading_hours,
            current_end_datetime=self._parse_initial_end_time(config.initial_end_datetime_et)
        )

        # Create contract
        contract = self._create_contract(config)

        # Download chunks iteratively
        chunk_num = 0
        while progress.hours_downloaded < progress.total_trading_hours:
            # Check if remaining time is too small (less than one bar)
            remaining_hours = progress.total_trading_hours - progress.hours_downloaded
            remaining_seconds = remaining_hours * 3600

            # Parse bar_size to get the minimum granularity
            bar_size_seconds = self._parse_bar_size_simple(config.bar_size)

            if remaining_seconds < bar_size_seconds:
                self.logger.info(
                    f"Remaining time ({remaining_seconds:.2f}s) is less than bar size "
                    f"({bar_size_seconds}s). Marking download as complete."
                )
                break

            chunk_num += 1

            # Prepare chunk spec
            chunk_spec = self._prepare_chunk_spec(
                config, contract, progress, stock_num, chunk_num
            )

            # Download chunk
            if self.chunk_downloader is None:
                self.logger.error("Chunk downloader not initialized")
                return None
            result = self.chunk_downloader.download_chunk_with_retry(chunk_spec)

            if result is None:
                # Failed after all retries
                self.logger.error(
                    f"Aborting {config.symbol} download after chunk {chunk_num} failed"
                )
                return None

            # Update progress
            progress.all_data.extend(result.data)
            progress.hours_downloaded = progress.hours_downloaded + result.hours_downloaded
            progress.chunks_completed += 1

            if result.next_end_datetime:
                progress.current_end_datetime = result.next_end_datetime
            else:
                # Fallback: subtract requested hours
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

            # Wait between chunks
            time.sleep(WAIT_TIME_BETWEEN_REQUESTS)

        # Save data
        return self.file_manager.save_stock_data(progress.all_data, config.symbol)

    def _create_contract(self, config: StockConfig) -> Contract:
        """Create IB Contract object"""
        contract = Contract()
        contract.symbol = config.symbol
        contract.secType = config.sec_type
        contract.currency = config.currency
        contract.exchange = config.exchange
        return contract

    def _parse_initial_end_time(self, initial_end_datetime_et: Optional[str]) -> datetime:
        """Parse initial end datetime from config"""
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

    def _parse_bar_size_simple(self, bar_size: str) -> int:
        """Parse bar size string to seconds (simplified version)"""
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

        self.logger.warning(f"Failed to parse bar_size '{bar_size}', defaulting to 5 seconds")
        return 5

    def _prepare_chunk_spec(self, config: StockConfig, contract: Contract,
                           progress: DownloadProgress, stock_num: int,
                           chunk_num: int) -> ChunkSpec:
        """Prepare chunk specification"""
        # Calculate chunk parameters
        remaining_hours = progress.total_trading_hours - progress.hours_downloaded
        current_chunk_hours = min(CHUNK_HOURS, remaining_hours)
        duration_seconds = int(current_chunk_hours * 3600)

        # Generate request ID
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
    """Main execution function"""
    config_file = sys.argv[1] if len(sys.argv) > 1 else None
    output_dir = sys.argv[2] if len(sys.argv) > 2 else None

    manager = StockDataManager(config_file=config_file, output_dir=output_dir)

    # Set up logging
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = manager.output_dir / f"stock_download_v3_{timestamp}.log"

    setup_logging(
        log_file,
        ib_downloader_level=logging.INFO,
        stock_manager_level=logging.INFO,
        ibapi_level=logging.WARNING
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
