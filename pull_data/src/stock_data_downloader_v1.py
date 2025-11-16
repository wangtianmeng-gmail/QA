#!/usr/bin/env python3.12
"""
Stock Historical Data Downloader using Interactive Brokers API - Version 1

This script downloads historical stock data from Interactive Brokers
and saves it to CSV files. Configuration is managed through stocks_config.csv.

Version History:
- v1: Initial implementation
  * Basic historical data download functionality
  * Simple single-request download per stock
  * CSV configuration file support
  * Configurable duration and bar size
  * Support for multiple stocks with wait time between requests

Author: Data Downloader Script
"""

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import threading
import time
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import logging
import sys


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
        self.nextValidOrderId = None
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Set up logging configuration."""
        logger = logging.getLogger('IBDataDownloader')
        logger.setLevel(logging.INFO)

        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        return logger

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

    def nextValidId(self, orderId: int):
        """
        Receive next valid order ID.

        Args:
            orderId: Next valid order ID
        """
        self.nextValidOrderId = orderId


class StockDataManager:
    """
    Manages stock data downloading and saving operations.
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
        self.app = None
        self.connection_thread = None
        self.logger = self._setup_logger()

        self.logger.info(f"Project root: {self.project_root}")
        self.logger.info(f"Config file: {self.config_file}")
        self.logger.info(f"Output directory: {self.output_dir}")

    def _setup_logger(self) -> logging.Logger:
        """Set up logging configuration."""
        logger = logging.getLogger('StockDataManager')
        logger.setLevel(logging.INFO)

        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        return logger

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

    def connect_to_ib(self, host: str = "127.0.0.1", port: int = 7497,
                     client_id: int = 1) -> bool:
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
        self.app.run()
        self.logger.info("IB API connection thread ended")

    def request_historical_data(self, req_id: int, contract: Contract,
                               duration: str = "1 D", bar_size: str = "1 min",
                               what_to_show: str = "TRADES") -> bool:
        """
        Request historical data from IB.

        Args:
            req_id: Unique request identifier
            contract: Contract object for the security
            duration: Duration of historical data (e.g., "1 D", "1 W", "1 M")
            bar_size: Bar size (e.g., "1 min", "5 mins", "1 hour", "1 day")
            what_to_show: Data type to show (e.g., "TRADES", "MIDPOINT", "BID", "ASK")

        Returns:
            True if request sent successfully
        """
        try:
            self.app.data_download_complete[req_id] = False

            self.app.reqHistoricalData(
                reqId=req_id,
                contract=contract,
                endDateTime='',
                durationStr=duration,
                barSizeSetting=bar_size,
                whatToShow=what_to_show,
                useRTH=1,
                formatDate=1,
                keepUpToDate=0,
                chartOptions=[]
            )

            self.logger.info(f"Requested historical data for {contract.symbol} (reqId: {req_id}, whatToShow: {what_to_show})")
            return True

        except Exception as e:
            self.logger.error(f"Error requesting historical data: {e}")
            return False

    def wait_for_download(self, req_id: int, timeout: int = 30) -> bool:
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
            if self.app.data_download_complete.get(req_id, False):
                return True
            time.sleep(0.1)

        self.logger.warning(f"Timeout waiting for data download (reqId: {req_id})")
        return False

    def save_data_to_csv(self, req_id: int, symbol: str,
                        timestamp: Optional[str] = None) -> Optional[str]:
        """
        Save downloaded data to CSV file.

        Args:
            req_id: Request identifier
            symbol: Stock symbol
            timestamp: Optional timestamp for filename

        Returns:
            Path to saved CSV file, or None if no data
        """
        if req_id not in self.app.data or not self.app.data[req_id]:
            self.logger.warning(f"No data available for {symbol} (reqId: {req_id})")
            return None

        df = pd.DataFrame(self.app.data[req_id])
        df.set_index("Date", inplace=True)

        if timestamp is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        filename = f"{symbol}_{timestamp}.csv"
        filepath = self.output_dir / filename

        df.to_csv(filepath, index=True)
        self.logger.info(f"Saved {len(df)} rows to {filepath}")

        return str(filepath)

    def download_stocks(self, wait_time: int = 2) -> Dict[str, str]:
        """
        Download historical data for all stocks in configuration.

        Args:
            wait_time: Time to wait between requests (seconds)

        Returns:
            Dictionary mapping stock symbols to saved file paths
        """
        config_df = self.load_config()
        saved_files = {}
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        for idx, row in config_df.iterrows():
            symbol = row['symbol']
            sec_type = row.get('sec_type', 'STK')
            currency = row.get('currency', 'USD')
            exchange = row.get('exchange', 'SMART')

            # Convert total_days to duration format for IB API
            # IB API requires integer days or use seconds for fractional days
            total_days = float(row.get('total_days', 1))
            if total_days >= 1 and total_days == int(total_days):
                # Use days format for whole days
                duration = f"{int(total_days)} D"
            else:
                # Use seconds format for fractional days
                # Assuming RTH: 6.5 hours per day = 23400 seconds per day
                seconds_per_day = 6.5 * 3600  # 23400 seconds
                total_seconds = int(total_days * seconds_per_day)
                duration = f"{total_seconds} S"

            bar_size = row.get('bar_size', '1 min')
            what_to_show = row.get('what_to_show', 'TRADES')

            self.logger.info(f"Processing {symbol}...")

            contract = self.create_contract(symbol, sec_type, currency, exchange)

            req_id = idx
            if self.request_historical_data(req_id, contract, duration, bar_size, what_to_show):
                if self.wait_for_download(req_id, timeout=30):
                    filepath = self.save_data_to_csv(req_id, symbol, timestamp)
                    if filepath:
                        saved_files[symbol] = filepath
                else:
                    self.logger.error(f"Failed to download data for {symbol}")

            if idx < len(config_df) - 1:
                time.sleep(wait_time)

        return saved_files

    def disconnect(self):
        """Disconnect from IB."""
        if self.app and self.app.isConnected():
            self.app.disconnect()
            self.logger.info("Disconnected from IB")
            time.sleep(0.5)


def main():
    """Main execution function."""
    # logging.basicConfig(level=logging.INFO)

    # You can optionally pass custom paths as command-line arguments
    # Usage: python stock_data_downloader.py [config_file] [output_dir]
    config_file = sys.argv[1] if len(sys.argv) > 1 else None
    output_dir = sys.argv[2] if len(sys.argv) > 2 else None

    manager = StockDataManager(
        config_file=config_file,
        output_dir=output_dir
    )

    try:
        if not manager.connect_to_ib():
            print("Failed to connect to Interactive Brokers")
            return

        print("\nDownloading stock data...")
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
