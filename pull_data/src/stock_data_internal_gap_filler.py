#!/usr/bin/env python3.12
"""
Stock Data Internal Gap Filler using Interactive Brokers API

This script processes existing stock CSV files to identify and fill internal gaps by:
1. Reading all CSV files in the data folder
2. Checking the HasGaps column for True values
3. For stocks with HasGaps = True:
   - Finding consecutive gap sequences larger than 100 bars
   - Attempting to re-download those gap sequences using IB API in 3-hour chunks
   - If new download succeeds (non-empty), replacing the gap and setting HasGaps = False
   - If new download is still empty, leaving HasGaps = True
4. Always generating a new final CSV file with updated data

The script uses the same structure and conventions as stock_data_gap_downloader.py.

Author: Internal Gap Filler Script
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
from dataclasses import dataclass, replace
import logging
import pytz
import re

# --- Configuration Constants ---
CHUNK_HOURS = 3.0              # Hours per download chunk (always use 3-hour chunks)
DOWNLOAD_TIMEOUT_SECONDS = 30  # Timeout for download requests
MAX_CHUNK_ATTEMPTS = 1         # Maximum attempts for chunk download
WAIT_TIME_BETWEEN_REQUESTS = 2.0  # Wait time between API requests
MIN_CONSECUTIVE_GAP_SIZE = 100  # Minimum consecutive gap size to trigger re-download
IB_HOST = "127.0.0.1"          # IB host address
IB_PORT = 7497                 # IB port (7497 for TWS, 4001 for IB Gateway)
IB_CLIENT_ID = 3               # Client ID (different from other scripts)


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
class StockFileInfo:
    """Information about a stock CSV file."""
    file_path: Path
    symbol: str
    has_gaps: bool
    gap_count: int


@dataclass
class ConsecutiveGapRange:
    """Represents a consecutive gap range with HasGaps = True."""
    start_index: int
    end_index: int
    gap_count: int
    start_datetime: datetime
    end_datetime: datetime


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
    status: str  # SUCCESS, TIMEOUT, FAILED
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

            # Calculate duration in seconds
            duration = chunk_spec.end_datetime - chunk_spec.start_datetime
            duration_seconds = int(duration.total_seconds())

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


class InternalGapFiller:
    """Main orchestrator for internal gap filling process."""

    def __init__(self, data_dir: Optional[str] = None):
        # Get project root directory
        self.project_root = Path(__file__).parent.parent.resolve()

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

        # Consolidated chunk log - tracks ALL chunks (success and failure)
        self.consolidated_log_timestamp: str = datetime.now(pytz.timezone('US/Eastern')).strftime("%Y%m%d_%H%M%S")
        self.consolidated_log_path: Optional[Path] = None
        self.consolidated_log_file = None

        self.logger = logging.getLogger('InternalGapFiller')
        self.logger.info(f"Project root: {self.project_root}")
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

    def initialize_consolidated_log(self) -> None:
        """Initialize the consolidated chunk log CSV file with header."""
        consolidated_filename = f"_consolidated_internal_gapfil_log_{self.consolidated_log_timestamp}.csv"
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

    def scan_data_folder(self) -> List[StockFileInfo]:
        """
        Scan data folder for CSV files and check for HasGaps.

        Returns:
            List of StockFileInfo objects for files with HasGaps = True
        """
        csv_files = list(self.data_dir.glob("*_saved_*.csv"))
        self.logger.info(f"Found {len(csv_files)} CSV files in data folder")

        files_with_gaps = []

        for csv_file in csv_files:
            try:
                # Extract symbol from filename
                symbol = csv_file.name.split('_')[0]

                # Read CSV and check for HasGaps
                df = pd.read_csv(csv_file)

                if 'HasGaps' not in df.columns:
                    self.logger.warning(f"{symbol}: No HasGaps column found, skipping")
                    continue

                # Count rows with HasGaps = True
                gap_count = (df['HasGaps'] == True).sum()

                if gap_count > 0:
                    self.logger.info(f"{symbol}: Found {gap_count} rows with HasGaps = True")
                    files_with_gaps.append(StockFileInfo(
                        file_path=csv_file,
                        symbol=symbol,
                        has_gaps=True,
                        gap_count=gap_count
                    ))
                else:
                    self.logger.info(f"{symbol}: No gaps found (HasGaps = False for all rows)")

            except Exception as e:
                self.logger.error(f"Error processing {csv_file.name}: {e}")
                continue

        return files_with_gaps

    def find_consecutive_gaps(self, df: pd.DataFrame) -> List[ConsecutiveGapRange]:
        """
        Find consecutive gap ranges with HasGaps = True that exceed MIN_CONSECUTIVE_GAP_SIZE.

        Args:
            df: DataFrame with HasGaps column

        Returns:
            List of ConsecutiveGapRange objects
        """
        consecutive_ranges = []
        current_start_idx: Optional[int] = None
        current_count = 0

        for idx in range(len(df)):
            has_gap = df.iloc[idx]['HasGaps']

            if has_gap == True:
                if current_start_idx is None:
                    # Start new consecutive range
                    current_start_idx = idx
                    current_count = 1
                else:
                    # Continue consecutive range
                    current_count += 1
            else:
                # End of consecutive range
                if current_start_idx is not None and current_count >= MIN_CONSECUTIVE_GAP_SIZE:
                    # Save this range
                    start_date = self._parse_date(df.iloc[current_start_idx]['Date'])
                    end_date = self._parse_date(df.iloc[idx - 1]['Date'])

                    consecutive_ranges.append(ConsecutiveGapRange(
                        start_index=current_start_idx,
                        end_index=idx - 1,
                        gap_count=current_count,
                        start_datetime=start_date,
                        end_datetime=end_date
                    ))

                    self.logger.info(
                        f"Found consecutive gap: idx {current_start_idx} to {idx - 1} "
                        f"({current_count} bars, {start_date} to {end_date})"
                    )

                # Reset
                current_start_idx = None
                current_count = 0

        # Handle case where gaps extend to end of file
        if current_start_idx is not None and current_count >= MIN_CONSECUTIVE_GAP_SIZE:
            start_date = self._parse_date(df.iloc[current_start_idx]['Date'])
            end_date = self._parse_date(df.iloc[len(df) - 1]['Date'])

            consecutive_ranges.append(ConsecutiveGapRange(
                start_index=current_start_idx,
                end_index=len(df) - 1,
                gap_count=current_count,
                start_datetime=start_date,
                end_datetime=end_date
            ))

            self.logger.info(
                f"Found consecutive gap at end: idx {current_start_idx} to {len(df) - 1} "
                f"({current_count} bars, {start_date} to {end_date})"
            )

        return consecutive_ranges

    def _parse_date(self, date_str: str) -> datetime:
        """Parse date string from CSV format."""
        # Format: "20241203 13:00:00 US/Eastern"
        dt_parts = date_str.rsplit(' ', 1)[0]  # Remove timezone
        dt = datetime.strptime(dt_parts, "%Y%m%d %H:%M:%S")
        return self.et_tz.localize(dt)

    def process_stock_file(self, file_info: StockFileInfo) -> bool:
        """
        Process a single stock file to fill internal gaps.

        Args:
            file_info: Stock file information

        Returns:
            True if processing completed successfully, False otherwise
        """
        self.logger.info("="*80)
        self.logger.info(f"Processing {file_info.symbol}")
        self.logger.info("="*80)

        try:
            # Read CSV file
            df = pd.read_csv(file_info.file_path)

            # Find consecutive gaps > MIN_CONSECUTIVE_GAP_SIZE
            consecutive_gaps = self.find_consecutive_gaps(df)

            if not consecutive_gaps:
                self.logger.info(
                    f"{file_info.symbol}: No consecutive gaps >= {MIN_CONSECUTIVE_GAP_SIZE} bars found, "
                    f"skipping re-download"
                )
                return True

            # Try to re-download each consecutive gap
            filled_any_gaps = False

            for gap_range in consecutive_gaps:
                self.logger.info(
                    f"Attempting to re-download gap: {gap_range.start_datetime} to {gap_range.end_datetime} "
                    f"({gap_range.gap_count} bars)"
                )

                # Download gap data
                filled_data = self._download_gap_range(
                    file_info.symbol, gap_range
                )

                if filled_data:
                    # Replace gap data and set HasGaps = False
                    self._replace_gap_data(df, gap_range, filled_data)
                    filled_any_gaps = True
                    self.logger.info(
                        f"Successfully filled gap with {len(filled_data)} bars, set HasGaps = False"
                    )
                else:
                    self.logger.info(
                        f"Failed to fill gap (empty download), leaving HasGaps = True"
                    )

            # Always save updated CSV file
            self._save_updated_file(file_info.symbol, df, filled_any_gaps)

            return True

        except Exception as e:
            self.logger.error(f"Error processing {file_info.symbol}: {e}")
            return False

    def _download_gap_range(self, symbol: str, gap_range: ConsecutiveGapRange) -> List[Dict]:
        """
        Download data for a gap range using 3-hour chunks.
        Always downloads full 3-hour chunks for reliability (IB API may have minimum duration requirements).
        Filters downloaded data to only include bars within the gap range.

        Args:
            symbol: Stock symbol
            gap_range: Consecutive gap range to fill

        Returns:
            List of filled data (empty if download failed)
        """
        filled_data = []

        # Create IB contract (using defaults from original script)
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.currency = "USD"
        contract.exchange = "SMART"

        # Default bar size (can be customized if needed)
        bar_size = "5 secs"
        what_to_show = "TRADES"
        use_rth = True

        chunk_num = 0
        current_start = gap_range.start_datetime
        gap_end = gap_range.end_datetime + timedelta(seconds=5)  # Include end

        while current_start < gap_end:
            chunk_num += 1

            # Calculate chunk end (always 3 hours for reliability)
            chunk_end = current_start + timedelta(hours=CHUNK_HOURS)

            # Create chunk spec
            chunk_spec = ChunkSpec(
                req_id=self.req_id_gen.generate_id(),
                symbol=symbol,
                chunk_num=chunk_num,
                contract=contract,
                start_datetime=current_start,
                end_datetime=chunk_end,
                bar_size=bar_size,
                what_to_show=what_to_show,
                use_rth=use_rth
            )

            # Download chunk
            if self.chunk_downloader is None:
                self.logger.error("Chunk downloader not initialized")
                return []

            result = self.chunk_downloader.download_chunk_with_retry(chunk_spec)

            if result and result.success:
                # Filter downloaded data to only include bars within the gap range
                gap_start_str = gap_range.start_datetime.strftime("%Y%m%d %H:%M:%S US/Eastern")
                gap_end_str = gap_range.end_datetime.strftime("%Y%m%d %H:%M:%S US/Eastern")

                filtered_data = [
                    bar for bar in result.data
                    if gap_start_str <= bar['Date'] <= gap_end_str
                ]

                bars_filtered_out = len(result.data) - len(filtered_data)
                filled_data.extend(filtered_data)

                # Log successful chunk download
                self.write_chunk_log(ChunkLogRecord(
                    symbol=symbol,
                    chunk_num=chunk_num,
                    start_datetime=current_start.strftime("%Y%m%d %H:%M:%S"),
                    end_datetime=chunk_end.strftime("%Y%m%d %H:%M:%S"),
                    status="SUCCESS",
                    bars_downloaded=len(result.data),
                    bars_filled=len(filtered_data),
                    details=f"Downloaded {len(result.data)} bars, filtered to {len(filtered_data)} bars within gap range (excluded {bars_filtered_out} bars outside gap)"
                ))
                self.logger.info(
                    f"Successfully downloaded chunk {chunk_num}: {len(result.data)} bars, "
                    f"kept {len(filtered_data)} bars within gap range"
                )
            else:
                if result and result.timeout:
                    # Log timeout
                    self.write_chunk_log(ChunkLogRecord(
                        symbol=symbol,
                        chunk_num=chunk_num,
                        start_datetime=current_start.strftime("%Y%m%d %H:%M:%S"),
                        end_datetime=chunk_end.strftime("%Y%m%d %H:%M:%S"),
                        status="TIMEOUT",
                        bars_downloaded=0,
                        bars_filled=0,
                        details="Download timed out"
                    ))
                    self.logger.warning(f"Timeout on chunk {chunk_num}, continuing...")
                else:
                    # Log other failures
                    self.write_chunk_log(ChunkLogRecord(
                        symbol=symbol,
                        chunk_num=chunk_num,
                        start_datetime=current_start.strftime("%Y%m%d %H:%M:%S"),
                        end_datetime=chunk_end.strftime("%Y%m%d %H:%M:%S"),
                        status="FAILED",
                        bars_downloaded=0,
                        bars_filled=0,
                        details="Download failed"
                    ))
                    self.logger.warning(f"Failed to download chunk {chunk_num}, continuing...")

            # Move to next chunk
            current_start = chunk_end

            # Wait between chunks
            time.sleep(WAIT_TIME_BETWEEN_REQUESTS)

        return filled_data

    def _replace_gap_data(self, df: pd.DataFrame, gap_range: ConsecutiveGapRange,
                          filled_data: List[Dict]) -> None:
        """
        Replace gap data in dataframe and set HasGaps = False.

        Args:
            df: DataFrame to update (modified in place)
            gap_range: Gap range being filled
            filled_data: New data to replace the gap
        """
        if not filled_data:
            return

        # Convert filled data to DataFrame
        filled_df = pd.DataFrame(filled_data)

        # Update the gap range in the original dataframe
        for idx in range(gap_range.start_index, gap_range.end_index + 1):
            date_str = df.iloc[idx]['Date']

            # Find matching row in filled data
            matching_rows = filled_df[filled_df['Date'] == date_str]

            if not matching_rows.empty:
                # Replace with new data
                new_row = matching_rows.iloc[0]
                df.at[idx, 'Open'] = new_row['Open']
                df.at[idx, 'High'] = new_row['High']
                df.at[idx, 'Low'] = new_row['Low']
                df.at[idx, 'Close'] = new_row['Close']
                df.at[idx, 'Volume'] = new_row['Volume']
                df.at[idx, 'WAP'] = new_row['WAP']
                df.at[idx, 'BarCount'] = new_row['BarCount']
                df.at[idx, 'HasGaps'] = False  # Set to False

    def _generate_filename(self, df: pd.DataFrame, symbol: str, save_timestamp: str, suffix: str = "") -> str:
        """
        Generate filename with start and end timestamps from actual data.

        Args:
            df: DataFrame containing the data
            symbol: Stock symbol
            save_timestamp: Timestamp when file is being saved
            suffix: Optional suffix to append before .csv (e.g., "_gapfilled")

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

    def _save_updated_file(self, symbol: str, df: pd.DataFrame,
                           filled_gaps: bool) -> None:
        """
        Save updated CSV file.

        Args:
            symbol: Stock symbol
            df: Updated dataframe
            filled_gaps: Whether any gaps were filled
        """
        timestamp = datetime.now(self.et_tz).strftime("%Y%m%d_%H%M%S")

        # Generate new filename with start and end timestamps from actual data
        suffix = "_gapfilled" if filled_gaps else "_processed"
        new_filename = self._generate_filename(df, symbol, timestamp, suffix)
        new_path = self.data_dir / new_filename

        # Save updated data
        df.to_csv(new_path, index=False)

        self.logger.info(f"Saved updated file: {new_filename}")
        print(f"Saved: {new_filename}")


def setup_logging(log_file: Path) -> None:
    """
    Set up global logging configuration.

    Args:
        log_file: Path to the log file
    """
    # Configure root logger
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

    # Set specific logger levels
    logging.getLogger('IBDataDownloader').setLevel(logging.INFO)
    logging.getLogger('InternalGapFiller').setLevel(logging.INFO)
    logging.getLogger('ChunkDownloader').setLevel(logging.INFO)
    logging.getLogger('ibapi').setLevel(logging.WARNING)

    logging.info(f"Logging to file: {log_file}")


def main():
    """Main execution function."""
    # Initialize manager
    manager = InternalGapFiller()

    # Set up logging
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = manager.data_dir / f"_log_internal_gap_filler_{timestamp}.log"

    setup_logging(log_file)

    logging.info("="*80)
    logging.info("STOCK DATA INTERNAL GAP FILLER - STARTED")
    logging.info("="*80)

    try:
        # Scan data folder for files with HasGaps = True
        print("\nScanning data folder for files with HasGaps = True...")
        files_with_gaps = manager.scan_data_folder()

        if not files_with_gaps:
            print("No files with HasGaps = True found. Nothing to do.")
            logging.info("No files with HasGaps = True found")
            return

        print(f"\nFound {len(files_with_gaps)} files with gaps:")
        for file_info in files_with_gaps:
            print(f"  {file_info.symbol}: {file_info.gap_count} gaps")

        # Connect to IB
        print("\nConnecting to Interactive Brokers...")
        if not manager.connect_to_ib():
            print("Failed to connect to Interactive Brokers")
            return

        # Initialize consolidated log file
        manager.initialize_consolidated_log()

        # Process each file
        print(f"\nProcessing {len(files_with_gaps)} files...")
        for file_info in files_with_gaps:
            manager.process_stock_file(file_info)
            time.sleep(WAIT_TIME_BETWEEN_REQUESTS)

        print("\n" + "="*80)
        print("INTERNAL GAP FILLING COMPLETED")
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
