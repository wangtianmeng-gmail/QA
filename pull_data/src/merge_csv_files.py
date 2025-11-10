#!/usr/bin/env python3.12
"""
CSV File Merger for Stock Data

This script merges multiple CSV files with overlapping time periods for each stock symbol.
It reads stock symbols from stocks_config.csv, finds matching CSV files for each symbol,
merges them while removing duplicates, and saves the merged result with a new naming convention.

Author: Data Merger Script
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import logging
import re
from typing import List, Tuple, Optional


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


def get_date_range_from_file(file_path: Path) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Get the actual date range from a CSV file by reading its first and last rows.

    Args:
        file_path: Path to the CSV file

    Returns:
        Tuple of (start_datetime, end_datetime) or (None, None) if parsing fails
    """
    try:
        df = pd.read_csv(file_path)
        if len(df) == 0:
            return (None, None)

        # Get first and last dates
        first_date_str = df.iloc[0]['Date']
        last_date_str = df.iloc[-1]['Date']

        # Extract datetime parts (remove timezone)
        first_dt_parts = first_date_str.rsplit(' ', 1)[0]
        last_dt_parts = last_date_str.rsplit(' ', 1)[0]

        # Parse to datetime
        first_dt = datetime.strptime(first_dt_parts, "%Y%m%d %H:%M:%S")
        last_dt = datetime.strptime(last_dt_parts, "%Y%m%d %H:%M:%S")

        return (first_dt, last_dt)
    except Exception as e:
        logging.warning(f"Error reading date range from {file_path.name}: {e}")
        return (None, None)


def find_csv_files_for_symbol(symbol: str, data_dir: Path) -> List[Path]:
    """
    Find all CSV files matching the pattern for a given symbol.

    Args:
        symbol: Stock symbol (e.g., 'FI', 'SNPS')
        data_dir: Directory containing the CSV files

    Returns:
        List of matching CSV file paths, sorted by actual date range (earliest start date first)
    """
    pattern = f"{symbol}_saved_*.csv"
    files = list(data_dir.glob(pattern))

    # Sort by actual date range instead of filename
    files_with_dates = []
    for file_path in files:
        start_dt, end_dt = get_date_range_from_file(file_path)
        if start_dt is not None:
            files_with_dates.append((file_path, start_dt, end_dt))
        else:
            # If we can't parse the date, put it at the end
            files_with_dates.append((file_path, datetime.max, datetime.max))

    # Sort by start date, then by end date
    files_with_dates.sort(key=lambda x: (x[1], x[2]))
    files = [f[0] for f in files_with_dates]

    logging.info(f"Found {len(files)} CSV files for {symbol}: {[f.name for f in files]}")
    for file_path, start_dt, end_dt in files_with_dates:
        if start_dt != datetime.max:
            logging.info(f"  {file_path.name}: {start_dt} to {end_dt}")

    return files


def parse_timestamps_from_filename(filename: str) -> Optional[Tuple[str, str]]:
    """
    Parse start and end timestamps from filename.

    Args:
        filename: CSV filename

    Returns:
        Tuple of (start_timestamp, end_timestamp) or None if parsing fails
    """
    # Pattern: SYMBOL_saved_YYYYMMDD_HHMMSS_start_YYYYMMDD_HHMMSS_end_YYYYMMDD_HHMMSS.csv
    pattern = r'_start_(\d{8}_\d{6})_end_(\d{8}_\d{6})\.csv'
    match = re.search(pattern, filename)

    if match:
        return (match.group(1), match.group(2))
    return None


def validate_file_overlap(df1: pd.DataFrame, df2: pd.DataFrame, file1_name: str, file2_name: str) -> bool:
    """
    Validate that two consecutive CSV files have proper overlapping data.

    The overlap must be:
    1. At the END of file1 and BEGINNING of file2
    2. Continuous (no missing lines in the overlap)
    3. Exact matches (all columns must be identical)

    Args:
        df1: First DataFrame
        df2: Second DataFrame
        file1_name: Name of first file (for logging)
        file2_name: Name of second file (for logging)

    Returns:
        True if overlap is valid, False otherwise

    Raises:
        ValueError: If overlap validation fails
    """
    logging.info(f"Validating overlap between {file1_name} and {file2_name}")

    # Find overlapping dates
    dates1 = set(df1['Date'].tolist())
    dates2 = set(df2['Date'].tolist())
    common_dates = dates1.intersection(dates2)

    if not common_dates:
        raise ValueError(f"No overlap found between {file1_name} and {file2_name}. Files cannot be merged.")

    logging.info(f"  Found {len(common_dates)} overlapping dates")

    # Sort the DataFrames by Date to ensure proper ordering
    df1_sorted = df1.sort_values('Date').reset_index(drop=True)
    df2_sorted = df2.sort_values('Date').reset_index(drop=True)

    # Get the overlapping rows from each file
    overlap1 = df1_sorted[df1_sorted['Date'].isin(common_dates)].reset_index(drop=True)
    overlap2 = df2_sorted[df2_sorted['Date'].isin(common_dates)].reset_index(drop=True)

    # Check 1: Overlap must be at the END of file1
    last_n_rows_df1 = df1_sorted.tail(len(overlap1)).reset_index(drop=True)
    if not last_n_rows_df1['Date'].equals(overlap1['Date']):
        raise ValueError(
            f"Overlap validation failed: Overlapping dates in {file1_name} are not at the END of the file. "
            f"Overlap must be continuous and at the end of the file."
        )

    # Check 2: Overlap must be at the BEGINNING of file2
    first_n_rows_df2 = df2_sorted.head(len(overlap2)).reset_index(drop=True)
    if not first_n_rows_df2['Date'].equals(overlap2['Date']):
        raise ValueError(
            f"Overlap validation failed: Overlapping dates in {file2_name} are not at the BEGINNING of the file. "
            f"Overlap must be continuous and at the beginning of the file."
        )

    # Check 3: Dates must be continuous (no gaps in the overlap sequence)
    overlap_dates_sorted = sorted(common_dates)
    overlap1_dates_sorted = overlap1['Date'].sort_values().tolist()
    overlap2_dates_sorted = overlap2['Date'].sort_values().tolist()

    if overlap1_dates_sorted != overlap_dates_sorted or overlap2_dates_sorted != overlap_dates_sorted:
        raise ValueError(
            f"Overlap validation failed: Overlapping dates are not continuous. "
            f"There may be missing lines in the overlap region."
        )

    # Check 4: All rows in the overlap must be EXACTLY the same
    # Compare all columns for each overlapping row
    for idx in range(len(overlap1)):
        date = overlap1.iloc[idx]['Date']
        row1 = overlap1.iloc[idx]
        row2 = overlap2.iloc[idx]

        # Compare all values
        if not row1.equals(row2):
            # Find which columns differ
            diff_cols = []
            for col in overlap1.columns:
                if row1[col] != row2[col]:
                    diff_cols.append(f"{col}: {row1[col]} vs {row2[col]}")

            raise ValueError(
                f"Overlap validation failed: Row mismatch at date {date}.\n"
                f"  Position in {file1_name}: {df1_sorted[df1_sorted['Date'] == date].index[0]}\n"
                f"  Position in {file2_name}: {df2_sorted[df2_sorted['Date'] == date].index[0]}\n"
                f"  Differences: {', '.join(diff_cols)}"
            )

    logging.info(f"  Overlap validation PASSED: {len(common_dates)} rows match exactly")
    return True


def merge_csv_files(files: List[Path], symbol: str) -> Optional[pd.DataFrame]:
    """
    Merge multiple CSV files for a symbol, removing duplicates.

    Validates that files have proper overlapping data before merging:
    - Files must have overlapping dates
    - Overlap must be at the END of one file and BEGINNING of the next
    - Overlap must be continuous with no missing lines
    - Overlapping rows must match exactly

    Args:
        files: List of CSV file paths to merge
        symbol: Stock symbol

    Returns:
        Merged DataFrame or None if no data

    Raises:
        ValueError: If overlap validation fails
    """
    if not files:
        logging.warning(f"No files to merge for {symbol}")
        return None

    if len(files) == 1:
        logging.info(f"Only one file for {symbol}, loading it directly")
        df = pd.read_csv(files[0])
        return df

    # Read all files
    dfs = []
    for file_path in files:
        logging.info(f"Reading {file_path.name}...")
        df = pd.read_csv(file_path)
        logging.info(f"  Loaded {len(df)} rows")
        dfs.append(df)

    # Validate overlap between consecutive files
    logging.info(f"Validating overlaps between {len(files)} files...")
    for i in range(len(dfs) - 1):
        try:
            validate_file_overlap(dfs[i], dfs[i + 1], files[i].name, files[i + 1].name)
        except ValueError as e:
            logging.error(f"Overlap validation failed for {symbol}: {e}")
            raise

    # Concatenate all dataframes
    merged_df = pd.concat(dfs, ignore_index=True)
    logging.info(f"Total rows before deduplication: {len(merged_df)}")

    # Remove duplicates based on Date column
    merged_df = merged_df.drop_duplicates(subset=['Date'], keep='first')
    logging.info(f"Total rows after deduplication: {len(merged_df)}")

    # Sort by Date
    merged_df = merged_df.sort_values('Date')

    return merged_df


def generate_merged_filename(symbol: str, df: pd.DataFrame) -> str:
    """
    Generate filename for the merged CSV file.

    Args:
        symbol: Stock symbol
        df: Merged DataFrame

    Returns:
        Filename string
    """
    save_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

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
        logging.warning(f"Error parsing dates for filename: {e}")
        return f"{symbol}_saved_{save_timestamp}.csv"


def save_merged_file(df: pd.DataFrame, output_path: Path) -> None:
    """
    Save merged DataFrame to CSV file.

    Args:
        df: DataFrame to save
        output_path: Path to save the file
    """
    df.to_csv(output_path, index=False)
    logging.info(f"Saved merged file to {output_path} ({len(df)} rows)")


def remove_old_files(files: List[Path]) -> None:
    """
    Remove old CSV files after successful merge.

    Args:
        files: List of file paths to remove
    """
    for file_path in files:
        try:
            file_path.unlink()
            logging.info(f"Removed old file: {file_path.name}")
        except Exception as e:
            logging.error(f"Error removing {file_path.name}: {e}")


def main():
    """Main execution function."""
    # Set up paths
    project_root = Path(__file__).parent.parent.resolve()
    config_file = project_root / "config" / "stocks_config.csv"
    data_dir = project_root / "data"

    # Set up logging
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = data_dir / f"merge_csv_files_{timestamp}.log"
    setup_logging(log_file)

    logging.info("="*60)
    logging.info("Starting CSV Merge Process")
    logging.info("="*60)
    logging.info(f"Project root: {project_root}")
    logging.info(f"Config file: {config_file}")
    logging.info(f"Data directory: {data_dir}")

    # Load symbols
    symbols = load_symbols(config_file)

    # Process each symbol
    merged_files = {}

    for symbol in symbols:
        logging.info("="*60)
        logging.info(f"Processing symbol: {symbol}")
        logging.info("="*60)

        # Find CSV files for this symbol
        csv_files = find_csv_files_for_symbol(symbol, data_dir)

        if not csv_files:
            logging.warning(f"No CSV files found for {symbol}, skipping")
            continue

        if len(csv_files) == 1:
            logging.info(f"Only one file for {symbol}, no merge needed")
            continue

        # Merge the files
        try:
            merged_df = merge_csv_files(csv_files, symbol)
        except ValueError as e:
            logging.error(f"Failed to merge files for {symbol}: {e}")
            logging.error(f"Skipping {symbol} and continuing with next symbol")
            continue

        if merged_df is None or len(merged_df) == 0:
            logging.warning(f"No data after merge for {symbol}, skipping")
            continue

        # Generate filename and save
        merged_filename = generate_merged_filename(symbol, merged_df)
        output_path = data_dir / merged_filename
        save_merged_file(merged_df, output_path)

        merged_files[symbol] = output_path

        # Remove old files
        logging.info(f"Removing {len(csv_files)} old files for {symbol}")
        remove_old_files(csv_files)

    # Summary
    logging.info("="*60)
    logging.info("Merge Process Complete")
    logging.info("="*60)
    logging.info(f"Successfully merged files for {len(merged_files)} symbols:")
    for symbol, filepath in merged_files.items():
        logging.info(f"  {symbol}: {filepath.name}")

    print("\n" + "="*60)
    print("CSV Merge Summary:")
    print("="*60)
    for symbol, filepath in merged_files.items():
        print(f"{symbol:10s} -> {filepath.name}")
    print("="*60)


if __name__ == "__main__":
    main()
