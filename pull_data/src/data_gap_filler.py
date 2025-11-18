#!/usr/bin/env python3.12
"""
Data Gap Filler for Stock Data

This script fills missing data gaps in stock CSV files by:
1. Using AAPL as the master reference dataset
2. Reading integrity check report to identify stocks with Overall_Status = FAIL
3. Finding missing dates (dates in AAPL but not in the stock)
4. Filling gaps with HasGaps = True, copying previous close price, and setting Volume/BarCount to 0
5. Creating two files for each modified stock:
   - {symbol}_saved_{timestamp}_gapfiller_origin.csv (backup)
   - {symbol}_saved_{timestamp}_gapfiller_filled.csv (gap-filled)
6. Limiting changes to 100 lines max - skipping stocks with more gaps
7. Skipping stocks with gaps at the very beginning (no historical data to fill from)
8. Generating detailed logs and summary reports

Author: Data Gap Filler Script
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import logging
from typing import List, Dict, Optional, Tuple, Any
import shutil


def setup_logging(log_file: Path) -> None:
    """
    Set up logging configuration.

    Args:
        log_file: Path to the log file
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )


def load_symbols(config_file: Path) -> List[str]:
    """
    Load stock symbols from the configuration CSV file.

    Args:
        config_file: Path to the stocks_config.csv file

    Returns:
        List of stock symbols
    """
    df = pd.read_csv(config_file)
    symbols = df['symbol'].tolist()
    logging.info(f"Loaded {len(symbols)} symbols from {config_file}")
    return symbols


def find_latest_csv_file(symbol: str, data_dir: Path) -> Tuple[Optional[Path], int]:
    """
    Find the latest CSV file for a given symbol.

    Args:
        symbol: Stock symbol (e.g., 'AAPL', 'MSFT')
        data_dir: Directory containing the CSV files

    Returns:
        Tuple of (Path to the latest CSV file or None if not found, number of files found)
    """
    pattern = f"{symbol}_saved_*.csv"
    files = list(data_dir.glob(pattern))

    if not files:
        logging.warning(f"No CSV files found for {symbol}")
        return None, 0

    # Sort by modification time (newest first)
    files.sort(key=lambda f: f.stat().st_mtime, reverse=True)

    latest_file = files[0]
    file_count = len(files)

    if file_count > 1:
        logging.warning(f"DUPLICATE FILES WARNING: Found {file_count} files for {symbol}, using latest: {latest_file.name}")
        for idx, file in enumerate(files):
            logging.warning(f"  File {idx+1}: {file.name}")
    else:
        logging.info(f"Found latest file for {symbol}: {latest_file.name}")

    return latest_file, file_count


def load_stock_data(symbol: str, data_dir: Path) -> Tuple[Optional[pd.DataFrame], int]:
    """
    Load stock data for a given symbol.

    Args:
        symbol: Stock symbol
        data_dir: Directory containing the CSV files

    Returns:
        Tuple of (DataFrame with stock data or None if file not found, number of files found)
    """
    file_path, file_count = find_latest_csv_file(symbol, data_dir)

    if file_path is None:
        return None, file_count

    try:
        df = pd.read_csv(file_path)
        logging.info(f"Loaded {len(df)} rows for {symbol}")
        return df, file_count
    except Exception as e:
        logging.error(f"Error loading data for {symbol}: {e}")
        return None, file_count


def find_latest_integrity_report(data_dir: Path) -> Optional[Path]:
    """
    Find the latest integrity check report.

    Args:
        data_dir: Directory containing the report files

    Returns:
        Path to the latest integrity report or None if not found
    """
    pattern = "_report_integrity_check_*.csv"
    files = list(data_dir.glob(pattern))

    if not files:
        logging.warning("No integrity check report found")
        return None

    # Sort by modification time (newest first)
    files.sort(key=lambda f: f.stat().st_mtime, reverse=True)

    latest_report = files[0]
    logging.info(f"Found latest integrity report: {latest_report.name}")
    return latest_report


def load_integrity_report(data_dir: Path) -> Optional[pd.DataFrame]:
    """
    Load the latest integrity check report.

    Args:
        data_dir: Directory containing the report files

    Returns:
        DataFrame with integrity report or None if not found
    """
    report_path = find_latest_integrity_report(data_dir)

    if report_path is None:
        return None

    try:
        df = pd.read_csv(report_path)
        logging.info(f"Loaded integrity report with {len(df)} stocks")
        return df
    except Exception as e:
        logging.error(f"Error loading integrity report: {e}")
        return None


def get_failed_stocks(integrity_df: pd.DataFrame) -> List[str]:
    """
    Get list of stocks with Overall_Status = FAIL.

    Args:
        integrity_df: DataFrame with integrity report

    Returns:
        List of stock symbols with FAIL status
    """
    failed_stocks = integrity_df[integrity_df['Overall_Status'] == 'FAIL']['Symbol'].tolist()
    logging.info(f"Found {len(failed_stocks)} stocks with FAIL status")
    return failed_stocks


def get_previous_close_price(df: pd.DataFrame, missing_date: str) -> Optional[float]:
    """
    Get the previous close price for a missing date.

    Args:
        df: DataFrame with stock data
        missing_date: Date string for the missing date

    Returns:
        Previous close price or None if no earlier data exists
    """
    # Find the last available close price before the missing date
    df_sorted = df.sort_values('Date')
    earlier_data = df_sorted[df_sorted['Date'] < missing_date]

    if len(earlier_data) > 0:
        return float(earlier_data.iloc[-1]['Close'])
    else:
        # If no earlier data exists, return None (gap is at the beginning)
        logging.warning(f"Gap at beginning: no data before {missing_date}")
        return None


def fill_gaps_for_stock(symbol: str, stock_df: pd.DataFrame, master_dates: List[str],
                        data_dir: Path, file_count: int = 1, max_gaps: int = 100) -> Dict[str, Any]:
    """
    Fill data gaps for a single stock.

    Args:
        symbol: Stock symbol
        stock_df: DataFrame with stock data
        master_dates: List of dates from master (AAPL) dataset
        data_dir: Directory to save output files
        file_count: Number of CSV files found for this symbol
        max_gaps: Maximum number of gaps allowed (default: 100)

    Returns:
        Dictionary with gap filling results
    """
    result = {
        'symbol': symbol,
        'success': False,
        'gaps_found': 0,
        'gaps_filled': 0,
        'beginning_gaps_skipped': 0,
        'missing_dates': [],
        'origin_file': None,
        'fillgap_file': None,
        'skipped_reason': None,
        'duplicate_files': file_count if file_count > 1 else None,
        'duplicate_warning': f'WARNING: {file_count} files found' if file_count > 1 else None
    }

    # Find missing dates
    stock_dates = set(stock_df['Date'].tolist())
    master_dates_set = set(master_dates)
    missing_dates = sorted(list(master_dates_set - stock_dates))

    result['gaps_found'] = len(missing_dates)
    result['missing_dates'] = missing_dates

    if len(missing_dates) == 0:
        logging.info(f"{symbol}: No gaps found")
        result['success'] = True
        return result

    logging.info(f"{symbol}: Found {len(missing_dates)} missing dates")

    # Check if gaps exceed limit
    if len(missing_dates) > max_gaps:
        logging.warning(
            f"{symbol}: Gap count ({len(missing_dates)}) exceeds limit ({max_gaps}), skipping"
        )
        result['skipped_reason'] = f"Gap count ({len(missing_dates)}) exceeds limit ({max_gaps})"
        return result

    # Create backup (_gapfiller_origin file)
    try:
        original_file, _ = find_latest_csv_file(symbol, data_dir)
        if original_file is None:
            logging.error(f"{symbol}: Original file not found")
            result['skipped_reason'] = "Original file not found"
            return result

        # Extract timestamp from original filename (e.g., "AAPL_saved_20240101_120000.csv")
        original_filename = original_file.stem  # filename without extension
        # The pattern is: {symbol}_saved_{timestamp}
        # Extract the timestamp part after "saved_"
        parts = original_filename.split('_saved_')
        if len(parts) == 2:
            original_timestamp = parts[1]
        else:
            # Fallback to current timestamp if pattern doesn't match
            original_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        origin_filename = f"{symbol}_saved_{original_timestamp}_gapfiller_origin.csv"
        origin_path = data_dir / origin_filename
        shutil.copy2(original_file, origin_path)
        result['origin_file'] = origin_path.name
        logging.info(f"{symbol}: Created backup file {origin_filename}")

    except Exception as e:
        logging.error(f"{symbol}: Error creating backup file: {e}")
        result['skipped_reason'] = f"Error creating backup: {e}"
        return result

    # Fill gaps
    filled_rows = []
    beginning_gaps = []
    middle_end_gaps = []

    for missing_date in missing_dates:
        prev_close = get_previous_close_price(stock_df, missing_date)

        # Check if gap is at the beginning (no previous data)
        if prev_close is None:
            beginning_gaps.append(missing_date)
            logging.info(f"{symbol}: GAP SKIPPED - Date: {missing_date} (beginning gap, no historical data)")
            continue  # Skip this gap but continue processing other gaps

        # Gap is in the middle or end, fill it
        middle_end_gaps.append(missing_date)
        filled_row = {
            'Date': missing_date,
            'Open': prev_close,
            'High': prev_close,
            'Low': prev_close,
            'Close': prev_close,
            'Volume': 0,
            'WAP': prev_close,
            'BarCount': 0,
            'HasGaps': True
        }
        filled_rows.append(filled_row)
        logging.info(f"{symbol}: GAP FILLED - Date: {missing_date}, Price: {prev_close}")

    # If ALL gaps are at the beginning (no gaps to fill), skip this stock
    if len(filled_rows) == 0:
        logging.warning(
            f"{symbol}: Skipping - all {len(beginning_gaps)} gaps are at beginning (no historical data to fill from)"
        )
        result['skipped_reason'] = f"All {len(beginning_gaps)} gaps at beginning (no historical data to fill from)"
        result['beginning_gaps_skipped'] = len(beginning_gaps)
        # Delete the origin file we just created
        try:
            origin_path.unlink()
            logging.info(f"{symbol}: Deleted origin file due to skipping")
        except Exception as e:
            logging.error(f"{symbol}: Error deleting origin file: {e}")
        return result

    # If we have some fillable gaps, log the details
    if beginning_gaps:
        logging.info(
            f"{symbol}: Summary - GAPS SKIPPED: {len(beginning_gaps)} (beginning), GAPS FILLED: {len(middle_end_gaps)} (middle/end)"
        )

    # Create new DataFrame with filled gaps
    filled_df = pd.DataFrame(filled_rows)
    combined_df = pd.concat([stock_df, filled_df], ignore_index=True)
    combined_df = combined_df.sort_values('Date')
    combined_df = combined_df.drop_duplicates(subset=['Date'], keep='first')

    # Save filled gap file
    fillgap_filename = f"{symbol}_saved_{original_timestamp}_gapfiller_filled.csv"
    fillgap_path = data_dir / fillgap_filename
    combined_df.to_csv(fillgap_path, index=False)
    result['fillgap_file'] = fillgap_path.name
    result['gaps_filled'] = len(filled_rows)  # Only count gaps actually filled
    result['beginning_gaps_skipped'] = len(beginning_gaps)
    result['success'] = True

    logging.info(f"{symbol}: Filled {len(filled_rows)} gaps (skipped {len(beginning_gaps)} beginning gaps), saved to {fillgap_filename}")

    # Delete the original CSV file after successfully creating _origin and _fillgap
    try:
        original_file.unlink()
        logging.info(f"{symbol}: Deleted original file {original_file.name}")
    except Exception as e:
        logging.error(f"{symbol}: Error deleting original file: {e}")

    return result


def generate_summary_report(results: List[Dict], output_dir: Path, timestamp: str) -> Path:
    """
    Generate a CSV summary report of gap filling results.

    Args:
        results: List of gap filling results
        output_dir: Directory to save the report
        timestamp: Timestamp string for filename

    Returns:
        Path to the saved report
    """
    report_data = []

    for result in results:
        row = {
            'Symbol': result['symbol'],
            'Gaps_Found': result['gaps_found'],
            'Beginning_Gaps_Skipped': result.get('beginning_gaps_skipped', 0),
            'Gaps_Filled': result['gaps_filled'],
            'Status': 'SUCCESS' if result['success'] else 'SKIPPED',
            'Duplicate_Files_Warning': result.get('duplicate_warning', 'N/A'),
            'Origin_File': result.get('origin_file', 'N/A'),
            'Fillgap_File': result.get('fillgap_file', 'N/A'),
            'Skipped_Reason': result.get('skipped_reason', 'N/A'),
            'Sample_Missing_Dates': ', '.join(result['missing_dates'][:5]) if result['missing_dates'] else 'N/A'
        }
        report_data.append(row)

    # Create DataFrame and save
    report_df = pd.DataFrame(report_data)
    report_file = output_dir / f"_report_gap_filling_{timestamp}.csv"
    report_df.to_csv(report_file, index=False)

    logging.info(f"Summary report saved to {report_file}")
    return report_file


def print_summary(results: List[Dict]) -> None:
    """
    Print a summary of gap filling results.

    Args:
        results: List of gap filling results
    """
    print("\n" + "="*80)
    print("DATA GAP FILLING SUMMARY")
    print("="*80)

    success_count = sum(1 for r in results if r['success'])
    skipped_count = len(results) - success_count
    total_gaps_filled = sum(r['gaps_filled'] for r in results)
    total_beginning_gaps_skipped = sum(r.get('beginning_gaps_skipped', 0) for r in results)
    duplicate_count = sum(1 for r in results if (r.get('duplicate_files') or 0) > 1)

    print(f"\nTotal stocks processed: {len(results)}")
    print(f"Successfully filled: {success_count}")
    print(f"Skipped: {skipped_count}")
    print(f"Total gaps filled: {total_gaps_filled}")
    print(f"Total beginning gaps skipped: {total_beginning_gaps_skipped}")
    if duplicate_count > 0:
        print(f"Stocks with duplicate files: {duplicate_count}")

    if duplicate_count > 0:
        print("\n" + "-"*80)
        print("DUPLICATE FILES WARNING:")
        print("-"*80)
        for result in results:
            if (result.get('duplicate_files') or 0) > 1:
                print(f"  {result['symbol']:10s}: {result['duplicate_warning']}")

    if success_count > 0:
        print("\n" + "-"*80)
        print("SUCCESSFULLY FILLED STOCKS:")
        print("-"*80)
        for result in results:
            if result['success'] and result['gaps_filled'] > 0:
                beginning_gaps = result.get('beginning_gaps_skipped', 0)
                if beginning_gaps > 0:
                    print(f"  {result['symbol']:10s}: Filled {result['gaps_filled']} gaps, Skipped {beginning_gaps} beginning gaps")
                else:
                    print(f"  {result['symbol']:10s}: Filled {result['gaps_filled']} gaps")
                if result.get('duplicate_warning'):
                    print(f"               {result['duplicate_warning']}")
                print(f"               Origin: {result['origin_file']}")
                print(f"               Fillgap: {result['fillgap_file']}")
                if result['missing_dates']:
                    sample_dates = result['missing_dates'][:3]
                    print(f"               Sample missing dates: {', '.join(sample_dates)}")

    if skipped_count > 0:
        print("\n" + "-"*80)
        print("SKIPPED STOCKS:")
        print("-"*80)
        for result in results:
            if not result['success']:
                print(f"  {result['symbol']:10s}: {result['skipped_reason']}")
                if result.get('duplicate_warning'):
                    print(f"               {result['duplicate_warning']}")

    print("="*80 + "\n")


def main():
    """Main execution function."""
    # Configuration constants
    MAX_GAPS = 100  # Maximum number of gaps allowed per stock

    # Set up paths
    project_root = Path(__file__).parent.parent.resolve()
    config_file = project_root / "config" / "stocks_config.csv"
    data_dir = project_root / "data"

    # Set up logging
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = data_dir / f"_log_gap_filling_{timestamp}.log"
    setup_logging(log_file)

    logging.info("="*80)
    logging.info("DATA GAP FILLING - STARTED")
    logging.info("="*80)
    logging.info(f"Project root: {project_root}")
    logging.info(f"Config file: {config_file}")
    logging.info(f"Data directory: {data_dir}")
    logging.info(f"Log file: {log_file}")

    # Load symbols
    symbols = load_symbols(config_file)

    # Load master data (AAPL)
    MASTER_SYMBOL = 'AAPL'
    logging.info("="*80)
    logging.info(f"Loading master data: {MASTER_SYMBOL}")
    logging.info("="*80)

    master_df, master_file_count = load_stock_data(MASTER_SYMBOL, data_dir)

    if master_df is None:
        logging.error(f"Failed to load master data for {MASTER_SYMBOL}")
        print(f"ERROR: Could not load master data for {MASTER_SYMBOL}")
        return

    master_dates = master_df['Date'].tolist()
    logging.info(f"Master data loaded: {len(master_df)} rows, {len(master_dates)} dates")

    # Load integrity report
    logging.info("="*80)
    logging.info("Loading integrity check report")
    logging.info("="*80)

    integrity_df = load_integrity_report(data_dir)

    if integrity_df is None:
        logging.error("Failed to load integrity report")
        print("ERROR: Could not load integrity report")
        return

    # Get failed stocks
    failed_stocks = get_failed_stocks(integrity_df)

    if not failed_stocks:
        logging.info("No stocks with FAIL status found")
        print("No stocks need gap filling")
        return

    # Remove AAPL from failed stocks if present (master shouldn't be processed)
    if MASTER_SYMBOL in failed_stocks:
        failed_stocks.remove(MASTER_SYMBOL)
        logging.info(f"Removed {MASTER_SYMBOL} from processing list (master stock)")

    logging.info(f"Will process {len(failed_stocks)} stocks: {failed_stocks}")

    # Process each failed stock
    results = []

    for symbol in failed_stocks:
        logging.info("="*80)
        logging.info(f"Processing symbol: {symbol}")
        logging.info("="*80)

        # Load stock data
        stock_df, file_count = load_stock_data(symbol, data_dir)

        if stock_df is None:
            logging.warning(f"Skipping {symbol} - no data found")
            results.append({
                'symbol': symbol,
                'success': False,
                'gaps_found': 0,
                'gaps_filled': 0,
                'beginning_gaps_skipped': 0,
                'missing_dates': [],
                'origin_file': None,
                'fillgap_file': None,
                'skipped_reason': 'No data file found',
                'duplicate_files': file_count if file_count > 1 else None,
                'duplicate_warning': f'WARNING: {file_count} files found' if file_count > 1 else None
            })
            continue

        # Fill gaps
        result = fill_gaps_for_stock(symbol, stock_df, master_dates, data_dir, file_count=file_count, max_gaps=MAX_GAPS)
        results.append(result)

    # Generate summary report
    logging.info("="*80)
    logging.info("Generating gap filling summary report")
    logging.info("="*80)

    report_file = generate_summary_report(results, data_dir, timestamp)

    # Print summary
    print_summary(results)

    logging.info("="*80)
    logging.info("DATA GAP FILLING - COMPLETED")
    logging.info("="*80)
    logging.info(f"Log file saved: {log_file}")
    logging.info(f"Summary report saved: {report_file}")

    print(f"Log file: {log_file.name}")
    print(f"Summary report: {report_file.name}")


if __name__ == "__main__":
    main()
