#!/usr/bin/env python3.12
"""
Data Integrity Checker for Stock Data

This script checks the integrity of stock data files by:
1. Checking for missing stocks (stocks in config but no CSV files)
2. Checking for duplicate CSV files (stocks with multiple CSV files - abnormal)
3. Setting AAPL as the master reference
4. Comparing Date columns across all stocks to ensure consistency
5. Checking for zero values in Open, High, Low, Close columns
6. Generating detailed logs and reports

Author: Data Integrity Checker Script
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import logging
from typing import List, Dict, Tuple, Optional


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


def find_all_csv_files(symbol: str, data_dir: Path) -> List[Path]:
    """
    Find all CSV files for a given symbol.

    Args:
        symbol: Stock symbol (e.g., 'AAPL', 'MSFT')
        data_dir: Directory containing the CSV files

    Returns:
        List of all CSV files for the symbol
    """
    pattern = f"{symbol}_saved_*.csv"
    files = list(data_dir.glob(pattern))

    if not files:
        logging.warning(f"No CSV files found for {symbol}")
        return []

    # Sort by modification time (newest first)
    files.sort(key=lambda f: f.stat().st_mtime, reverse=True)

    return files


def find_latest_csv_file(symbol: str, data_dir: Path) -> Optional[Path]:
    """
    Find the latest CSV file for a given symbol.

    Args:
        symbol: Stock symbol (e.g., 'AAPL', 'MSFT')
        data_dir: Directory containing the CSV files

    Returns:
        Path to the latest CSV file or None if not found
    """
    files = find_all_csv_files(symbol, data_dir)

    if not files:
        return None

    latest_file = files[0]
    logging.info(f"Found latest file for {symbol}: {latest_file.name}")
    return latest_file


def load_stock_data(symbol: str, data_dir: Path) -> Optional[pd.DataFrame]:
    """
    Load stock data for a given symbol.

    Args:
        symbol: Stock symbol
        data_dir: Directory containing the CSV files

    Returns:
        DataFrame with stock data or None if file not found
    """
    file_path = find_latest_csv_file(symbol, data_dir)

    if file_path is None:
        return None

    try:
        df = pd.read_csv(file_path)
        logging.info(f"Loaded {len(df)} rows for {symbol}")
        return df
    except Exception as e:
        logging.error(f"Error loading data for {symbol}: {e}")
        return None


def check_date_consistency(master_df: pd.DataFrame, test_df: pd.DataFrame,
                          master_symbol: str, test_symbol: str) -> Dict[str, any]:
    """
    Check if the Date column of test_df matches the master_df.

    Args:
        master_df: Master DataFrame (AAPL)
        test_df: DataFrame to test
        master_symbol: Master symbol name
        test_symbol: Test symbol name

    Returns:
        Dictionary with check results
    """
    result = {
        'symbol': test_symbol,
        'date_match': False,
        'master_rows': len(master_df),
        'test_rows': len(test_df),
        'missing_dates': [],
        'extra_dates': [],
        'row_count_match': False,
        'date_sequence_match': False
    }

    master_dates = set(master_df['Date'].tolist())
    test_dates = set(test_df['Date'].tolist())

    # Check row count
    result['row_count_match'] = len(master_df) == len(test_df)

    # Find missing and extra dates
    missing = master_dates - test_dates
    extra = test_dates - master_dates

    result['missing_dates'] = sorted(list(missing))[:10]  # Limit to first 10
    result['extra_dates'] = sorted(list(extra))[:10]  # Limit to first 10
    result['missing_dates_count'] = len(missing)
    result['extra_dates_count'] = len(extra)

    # Check if dates match exactly
    if missing or extra:
        result['date_match'] = False
        logging.warning(f"{test_symbol}: Date mismatch detected")
        logging.warning(f"  Missing {len(missing)} dates from master")
        logging.warning(f"  Has {len(extra)} extra dates not in master")
    else:
        # Dates match, now check sequence
        master_dates_list = master_df['Date'].tolist()
        test_dates_list = test_df['Date'].tolist()

        result['date_sequence_match'] = master_dates_list == test_dates_list
        result['date_match'] = result['date_sequence_match']

        if result['date_sequence_match']:
            logging.info(f"{test_symbol}: Date column matches master perfectly")
        else:
            logging.warning(f"{test_symbol}: Dates exist but sequence differs")

    return result


def check_zero_values(df: pd.DataFrame, symbol: str) -> Dict[str, any]:
    """
    Check for zero values in Open, High, Low, Close columns.

    Args:
        df: DataFrame to check
        symbol: Stock symbol

    Returns:
        Dictionary with check results
    """
    result = {
        'symbol': symbol,
        'has_zero_values': False,
        'zero_counts': {},
        'zero_dates': {}
    }

    price_columns = ['Open', 'High', 'Low', 'Close']

    for col in price_columns:
        if col in df.columns:
            zero_mask = df[col] == 0
            zero_count = zero_mask.sum()

            if zero_count > 0:
                result['has_zero_values'] = True
                result['zero_counts'][col] = zero_count

                # Get dates with zero values (limit to first 10)
                zero_dates = df[zero_mask]['Date'].tolist()[:10]
                result['zero_dates'][col] = zero_dates

                logging.warning(f"{symbol}: Found {zero_count} zero values in {col} column")
                if zero_dates:
                    logging.warning(f"  Example dates: {zero_dates[:3]}")

    if not result['has_zero_values']:
        logging.info(f"{symbol}: No zero values found in price columns")

    return result


def check_missing_stocks(symbols: List[str], data_dir: Path) -> List[str]:
    """
    Check which stocks from the config file have no CSV files.

    Args:
        symbols: List of stock symbols from config
        data_dir: Directory containing the CSV files

    Returns:
        List of missing stock symbols
    """
    missing_stocks = []

    for symbol in symbols:
        files = find_all_csv_files(symbol, data_dir)
        if not files:
            missing_stocks.append(symbol)
            logging.error(f"MISSING STOCK: {symbol} - No CSV files found")

    if not missing_stocks:
        logging.info("All stocks have CSV files - No missing stocks")
    else:
        logging.warning(f"Found {len(missing_stocks)} missing stocks: {missing_stocks}")

    return missing_stocks


def check_duplicate_csv_files(symbols: List[str], data_dir: Path) -> Dict[str, List[str]]:
    """
    Check which stocks have multiple CSV files (abnormal situation).

    Args:
        symbols: List of stock symbols from config
        data_dir: Directory containing the CSV files

    Returns:
        Dictionary mapping symbols to list of duplicate file names
    """
    duplicate_files = {}

    for symbol in symbols:
        files = find_all_csv_files(symbol, data_dir)
        if len(files) > 1:
            duplicate_files[symbol] = [f.name for f in files]
            logging.warning(f"ABNORMAL: {symbol} has {len(files)} CSV files (should have only 1)")
            for i, file in enumerate(files, 1):
                logging.warning(f"  {i}. {file.name}")

    if not duplicate_files:
        logging.info("All stocks have single CSV files - No duplicates")
    else:
        logging.error(f"Found {len(duplicate_files)} stocks with multiple CSV files")

    return duplicate_files


def generate_integrity_report(date_results: List[Dict],
                             zero_results: List[Dict],
                             missing_stocks: List[str],
                             duplicate_files: Dict[str, List[str]],
                             master_symbol: str,
                             master_row_count: int,
                             output_dir: Path,
                             timestamp: str) -> Path:
    """
    Generate a CSV report with integrity check results.

    Args:
        date_results: List of date check results
        zero_results: List of zero value check results
        missing_stocks: List of missing stock symbols
        duplicate_files: Dictionary of stocks with duplicate CSV files
        master_symbol: The master stock symbol (e.g., 'AAPL')
        master_row_count: Number of rows in master stock
        output_dir: Directory to save the report
        timestamp: Timestamp string for filename

    Returns:
        Path to the saved report
    """
    # Combine results
    report_data = []

    # Create a lookup for zero results
    zero_lookup = {r['symbol']: r for r in zero_results}

    # Add master stock first (if not already in date_results)
    master_in_date_results = any(dr['symbol'] == master_symbol for dr in date_results)
    if not master_in_date_results:
        master_zero_result = zero_lookup.get(master_symbol, {})
        has_duplicates = master_symbol in duplicate_files
        duplicate_count = len(duplicate_files[master_symbol]) if has_duplicates else 1

        master_row = {
            'Symbol': master_symbol,
            'File_Status': 'ABNORMAL' if has_duplicates else 'NORMAL (MASTER)',
            'CSV_File_Count': duplicate_count,
            'Date_Match': 'MASTER',
            'Row_Count_Match': 'MASTER',
            'Master_Rows': master_row_count,
            'Test_Rows': master_row_count,
            'Missing_Dates_Count': 0,
            'Extra_Dates_Count': 0,
            'Zero_Values': 'FAIL' if master_zero_result.get('has_zero_values', False) else 'PASS',
            'Zero_Open': master_zero_result.get('zero_counts', {}).get('Open', 0),
            'Zero_High': master_zero_result.get('zero_counts', {}).get('High', 0),
            'Zero_Low': master_zero_result.get('zero_counts', {}).get('Low', 0),
            'Zero_Close': master_zero_result.get('zero_counts', {}).get('Close', 0),
            'Overall_Status': 'FAIL' if (master_zero_result.get('has_zero_values', False) or has_duplicates) else 'PASS'
        }
        report_data.append(master_row)

    for date_result in date_results:
        symbol = date_result['symbol']
        zero_result = zero_lookup.get(symbol, {})

        # Check if symbol has duplicate files
        has_duplicates = symbol in duplicate_files
        duplicate_count = len(duplicate_files[symbol]) if has_duplicates else 1

        row = {
            'Symbol': symbol,
            'File_Status': 'ABNORMAL' if has_duplicates else 'NORMAL',
            'CSV_File_Count': duplicate_count,
            'Date_Match': 'PASS' if date_result['date_match'] else 'FAIL',
            'Row_Count_Match': 'PASS' if date_result['row_count_match'] else 'FAIL',
            'Master_Rows': date_result['master_rows'],
            'Test_Rows': date_result['test_rows'],
            'Missing_Dates_Count': date_result['missing_dates_count'],
            'Extra_Dates_Count': date_result['extra_dates_count'],
            'Zero_Values': 'FAIL' if zero_result.get('has_zero_values', False) else 'PASS',
            'Zero_Open': zero_result.get('zero_counts', {}).get('Open', 0),
            'Zero_High': zero_result.get('zero_counts', {}).get('High', 0),
            'Zero_Low': zero_result.get('zero_counts', {}).get('Low', 0),
            'Zero_Close': zero_result.get('zero_counts', {}).get('Close', 0),
            'Overall_Status': 'PASS' if (date_result['date_match'] and
                                        not zero_result.get('has_zero_values', False) and
                                        not has_duplicates) else 'FAIL'
        }

        report_data.append(row)

    # Add rows for missing stocks
    for symbol in missing_stocks:
        has_duplicates = symbol in duplicate_files
        duplicate_count = len(duplicate_files[symbol]) if has_duplicates else 0

        row = {
            'Symbol': symbol,
            'File_Status': 'MISSING',
            'CSV_File_Count': duplicate_count,
            'Date_Match': 'N/A',
            'Row_Count_Match': 'N/A',
            'Master_Rows': 'N/A',
            'Test_Rows': 'N/A',
            'Missing_Dates_Count': 'N/A',
            'Extra_Dates_Count': 'N/A',
            'Zero_Values': 'N/A',
            'Zero_Open': 'N/A',
            'Zero_High': 'N/A',
            'Zero_Low': 'N/A',
            'Zero_Close': 'N/A',
            'Overall_Status': 'FAIL'
        }

        report_data.append(row)

    # Create DataFrame and save
    report_df = pd.DataFrame(report_data)
    report_file = output_dir / f"integrity_check_report_{timestamp}.csv"
    report_df.to_csv(report_file, index=False)

    logging.info(f"Report saved to {report_file}")
    return report_file


def print_summary(date_results: List[Dict], zero_results: List[Dict],
                 missing_stocks: List[str], duplicate_files: Dict[str, List[str]]) -> None:
    """
    Print a summary of the integrity check results.

    Args:
        date_results: List of date check results
        zero_results: List of zero value check results
        missing_stocks: List of missing stock symbols
        duplicate_files: Dictionary of stocks with duplicate CSV files
    """
    print("\n" + "="*80)
    print("DATA INTEGRITY CHECK SUMMARY")
    print("="*80)

    # Missing stocks summary
    print("\n1. MISSING STOCKS CHECK:")
    print("-" * 80)
    if missing_stocks:
        print(f"   FAIL: {len(missing_stocks)} stocks have no CSV files")
        print("\n   Missing stocks:")
        for symbol in missing_stocks:
            print(f"   - {symbol}")
    else:
        print(f"   PASS: All stocks have CSV files")

    # Duplicate files summary
    print("\n2. DUPLICATE CSV FILES CHECK:")
    print("-" * 80)
    if duplicate_files:
        print(f"   ABNORMAL: {len(duplicate_files)} stocks have multiple CSV files")
        print("\n   Stocks with duplicates:")
        for symbol, files in duplicate_files.items():
            print(f"   - {symbol}: {len(files)} files")
            for i, file in enumerate(files, 1):
                print(f"      {i}. {file}")
    else:
        print(f"   PASS: All stocks have single CSV files")

    # Date consistency summary
    print("\n3. DATE CONSISTENCY CHECK (vs AAPL master):")
    print("-" * 80)
    date_pass = sum(1 for r in date_results if r['date_match'])
    date_fail = len(date_results) - date_pass
    print(f"   PASS: {date_pass} stocks")
    print(f"   FAIL: {date_fail} stocks")

    if date_fail > 0:
        print("\n   Failed stocks:")
        for result in date_results:
            if not result['date_match']:
                print(f"   - {result['symbol']:6s}: "
                      f"Missing {result['missing_dates_count']} dates, "
                      f"Extra {result['extra_dates_count']} dates")

    # Zero values summary
    print("\n4. ZERO VALUES CHECK:")
    print("-" * 80)
    zero_pass = sum(1 for r in zero_results if not r['has_zero_values'])
    zero_fail = len(zero_results) - zero_pass
    print(f"   PASS: {zero_pass} stocks")
    print(f"   FAIL: {zero_fail} stocks")

    if zero_fail > 0:
        print("\n   Failed stocks:")
        for result in zero_results:
            if result['has_zero_values']:
                zero_cols = [f"{col}:{count}" for col, count in result['zero_counts'].items()]
                print(f"   - {result['symbol']:6s}: {', '.join(zero_cols)}")

    # Overall summary
    print("\n5. OVERALL STATUS:")
    print("-" * 80)
    overall_pass = sum(1 for i, dr in enumerate(date_results)
                      if dr['date_match'] and not zero_results[i]['has_zero_values'])
    # Exclude stocks with duplicates from pass count
    for symbol in duplicate_files.keys():
        if any(dr['symbol'] == symbol and dr['date_match'] and
               not zero_results[i]['has_zero_values']
               for i, dr in enumerate(date_results)):
            overall_pass -= 1
    overall_fail = len(date_results) - overall_pass + len(missing_stocks)
    print(f"   PASS: {overall_pass} stocks")
    print(f"   FAIL: {overall_fail} stocks")

    print("="*80 + "\n")


def main():
    """Main execution function."""
    # Set up paths
    project_root = Path(__file__).parent.parent.resolve()
    config_file = project_root / "config" / "stocks_config.csv"
    data_dir = project_root / "data"

    # Set up logging
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = data_dir / f"integrity_check_{timestamp}.log"
    setup_logging(log_file)

    logging.info("="*80)
    logging.info("DATA INTEGRITY CHECK - STARTED")
    logging.info("="*80)
    logging.info(f"Project root: {project_root}")
    logging.info(f"Config file: {config_file}")
    logging.info(f"Data directory: {data_dir}")
    logging.info(f"Log file: {log_file}")

    # Load symbols
    symbols = load_symbols(config_file)

    # Check for missing stocks and duplicate CSV files
    logging.info("="*80)
    logging.info("Checking for missing stocks and duplicate CSV files")
    logging.info("="*80)

    missing_stocks = check_missing_stocks(symbols, data_dir)
    duplicate_files = check_duplicate_csv_files(symbols, data_dir)

    # Load AAPL as master
    MASTER_SYMBOL = 'AAPL'
    logging.info("="*80)
    logging.info(f"Loading master data: {MASTER_SYMBOL}")
    logging.info("="*80)

    master_df = load_stock_data(MASTER_SYMBOL, data_dir)

    if master_df is None:
        logging.error(f"Failed to load master data for {MASTER_SYMBOL}")
        print(f"ERROR: Could not load master data for {MASTER_SYMBOL}")
        return

    logging.info(f"Master data loaded: {len(master_df)} rows")

    # Check all other symbols
    date_results = []
    zero_results = []

    # First check AAPL itself for zero values
    logging.info("="*80)
    logging.info(f"Checking master stock: {MASTER_SYMBOL}")
    logging.info("="*80)
    master_zero_result = check_zero_values(master_df, MASTER_SYMBOL)
    zero_results.append(master_zero_result)

    # Process other symbols
    for symbol in symbols:
        if symbol == MASTER_SYMBOL:
            continue

        logging.info("="*80)
        logging.info(f"Checking symbol: {symbol}")
        logging.info("="*80)

        # Load data
        test_df = load_stock_data(symbol, data_dir)

        if test_df is None:
            logging.warning(f"Skipping {symbol} - no data found")
            continue

        # Check date consistency
        date_result = check_date_consistency(master_df, test_df, MASTER_SYMBOL, symbol)
        date_results.append(date_result)

        # Check zero values
        zero_result = check_zero_values(test_df, symbol)
        zero_results.append(zero_result)

    # Generate report
    logging.info("="*80)
    logging.info("Generating integrity check report")
    logging.info("="*80)

    report_file = generate_integrity_report(date_results, zero_results,
                                           missing_stocks, duplicate_files,
                                           MASTER_SYMBOL, len(master_df),
                                           data_dir, timestamp)

    # Print summary
    print_summary(date_results, zero_results, missing_stocks, duplicate_files)

    logging.info("="*80)
    logging.info("DATA INTEGRITY CHECK - COMPLETED")
    logging.info("="*80)
    logging.info(f"Log file saved: {log_file}")
    logging.info(f"Report file saved: {report_file}")

    print(f"Log file: {log_file.name}")
    print(f"Report file: {report_file.name}")


if __name__ == "__main__":
    main()
