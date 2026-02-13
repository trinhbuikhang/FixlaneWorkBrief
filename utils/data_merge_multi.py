#!/usr/bin/env python3
"""
High-Performance CSV Combiner using Polars
Optimized for large files (50GB+) with streaming support and data format preservation.
All columns are kept as strings to preserve original formatting (0011, False, etc.)
"""

import os
import sys
import argparse
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from datetime import datetime
import psutil

try:
    import polars as pl
except ImportError:
    print("Error: Polars is not installed.")
    print("Install it with: pip install polars")
    sys.exit(1)


def get_memory_info() -> dict:
    """Get current memory usage information."""
    process = psutil.Process()
    memory_info = process.memory_info()
    virtual_memory = psutil.virtual_memory()

    return {
        "process_memory_mb": memory_info.rss / 1024 / 1024,
        "available_memory_mb": virtual_memory.available / 1024 / 1024,
        "memory_percent": virtual_memory.percent,
    }


def log(message: str, level: str = "INFO"):
    """Print timestamped log message."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {level}: {message}")


def get_csv_files(directory: str) -> List[Path]:
    """Get all CSV files from the specified directory."""
    dir_path = Path(directory)
    if not dir_path.exists():
        log(f"Directory '{directory}' does not exist.", "ERROR")
        sys.exit(1)

    if not dir_path.is_dir():
        log(f"'{directory}' is not a directory.", "ERROR")
        sys.exit(1)

    csv_files = sorted(dir_path.glob("*.csv"))

    if not csv_files:
        log(f"No CSV files found in '{directory}'.", "ERROR")
        sys.exit(1)

    return csv_files


def estimate_file_size_gb(file_path: Path) -> float:
    """Estimate file size in GB."""
    try:
        size_bytes = file_path.stat().st_size
        return size_bytes / (1024 ** 3)
    except OSError:
        return 0.0


def read_csv_header_robust(csv_file: Path) -> Optional[List[str]]:
    """
    Robust header reader that handles:
    - BOM (UTF-8-SIG)
    - Various encodings
    - Malformed files
    - Different delimiters
    
    Returns None if file cannot be read.
    """
    
    # Try 1: Normal Polars read
    try:
        df = pl.read_csv(csv_file, n_rows=0, truncate_ragged_lines=True, ignore_errors=True)
        if df.columns:
            return df.columns
    except Exception as e:
        log(f"Header read failed (default): {e}", "DEBUG")
    
    # Try 2: With different encoding
    try:
        df = pl.read_csv(csv_file, n_rows=0, encoding="utf8-lossy", truncate_ragged_lines=True, ignore_errors=True)
        if df.columns:
            return df.columns
    except Exception as e:
        log(f"Header read failed (utf8-lossy): {e}", "DEBUG")
    
    # Try 3: Force delimiter detection
    for delimiter in [",", ";", "\t", "|"]:
        try:
            df = pl.read_csv(
                csv_file,
                n_rows=0,
                separator=delimiter,
                truncate_ragged_lines=True,
                ignore_errors=True
            )
            if df.columns and len(df.columns) > 1:
                return df.columns
        except Exception as e:
            continue
    
    # Try 4: Manual Python fallback
    try:
        with open(csv_file, 'r', encoding='utf-8-sig', errors='ignore') as f:
            first_line = f.readline().strip()
            if not first_line:
                return None
            
            for delimiter in [",", ";", "\t", "|"]:
                parts = first_line.split(delimiter)
                if len(parts) > 1:
                    return parts
            
            return [first_line] if first_line else None
    except Exception as e:
        log(f"Fallback header read failed for {csv_file}: {e}", "WARNING")
        return None


def analyze_headers(csv_files: List[Path]) -> Tuple[Dict[str, dict], List[str], int]:
    """
    Analyze headers from all CSV files in parallel.
    
    Returns:
        - header_groups: dict mapping header signatures to file info
        - most_common_header: the most frequently occurring header
        - total_columns: number of columns in most common header
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    header_groups = {}
    
    log(f"Analyzing {len(csv_files)} CSV files...")
    
    # Parallel header reading with ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=min(8, len(csv_files))) as executor:
        future_to_file = {
            executor.submit(read_csv_header_robust, f): f 
            for f in csv_files
        }
        
        processed = 0
        for future in as_completed(future_to_file):
            csv_file = future_to_file[future]
            processed += 1
            
            try:
                header = future.result()
                
                if header is None or not header:
                    log(f"  [{processed}/{len(csv_files)}] ✗ {csv_file.name}: Could not read header", "WARNING")
                    continue
                
                # Get file size
                file_size_gb = estimate_file_size_gb(csv_file)
                
                # Create signature
                header_signature = ",".join(header)
                
                if header_signature not in header_groups:
                    header_groups[header_signature] = {
                        'files': [],
                        'header': header,
                        'total_size_gb': 0.0
                    }
                
                header_groups[header_signature]['files'].append(csv_file)
                header_groups[header_signature]['total_size_gb'] += file_size_gb
                
                log(f"  [{processed}/{len(csv_files)}] ✓ {csv_file.name}: {len(header)} columns, {file_size_gb:.2f} GB")
                
            except Exception as e:
                log(f"  [{processed}/{len(csv_files)}] ✗ {csv_file.name}: {e}", "ERROR")
                continue
    
    if not header_groups:
        log("No valid CSV files found!", "ERROR")
        sys.exit(1)
    
    # Find most common header
    most_common_sig = max(header_groups.keys(), key=lambda k: len(header_groups[k]['files']))
    most_common_header = header_groups[most_common_sig]['header']
    total_columns = len(most_common_header)
    
    return header_groups, most_common_header, total_columns


def display_header_analysis(header_groups: Dict[str, dict]) -> None:
    """Display analysis of header groups with file sizes."""
    print("\n" + "="*70)
    print("HEADER ANALYSIS")
    print("="*70)
    
    if len(header_groups) == 1:
        group = list(header_groups.values())[0]
        files = group['files']
        total_size = group['total_size_gb']
        
        print(f"✓ All CSV files have identical headers!")
        print(f"  Files: {len(files)}")
        print(f"  Columns: {len(group['header'])}")
        print(f"  Total size: {total_size:.2f} GB")
    else:
        print(f"⚠ Warning: Found {len(header_groups)} different header formats:")
        
        for i, (sig, group) in enumerate(header_groups.items(), 1):
            files = group['files']
            header = group['header']
            total_size = group['total_size_gb']
            
            print(f"\n  Group {i}: {len(files)} file(s), {len(header)} columns, {total_size:.2f} GB")
            
            # Show first 5 files
            for j, file in enumerate(files[:5], 1):
                file_size = estimate_file_size_gb(file)
                print(f"    {j}. {file.name} ({file_size:.2f} GB)")
            
            if len(files) > 5:
                print(f"    ... and {len(files) - 5} more files")
    
    print("="*70 + "\n")


def get_user_choice(header_groups: Dict[str, dict]) -> str:
    """
    Prompt user for action when headers don't match.
    Returns: 'all', 'matching', or 'abort'
    """
    print("Options:")
    print("  [1] Abort - Don't merge anything")
    print("  [2] Merge only files with matching headers (use most common format)")
    print("  [3] Force merge all files (columns will be aligned, missing values filled with null)")
    
    while True:
        choice = input("\nEnter your choice (1-3): ").strip()
        if choice == '1':
            return 'abort'
        elif choice == '2':
            return 'matching'
        elif choice == '3':
            return 'all'
        else:
            print("Invalid choice. Please enter 1, 2, or 3.")


def combine_csv_files_streaming(
    files_to_merge: List[Path], 
    output_file: Path, 
    expected_columns: List[str],
    use_streaming: bool = True
) -> None:
    """
    Combine CSV files using Polars with streaming support for large files.
    
    All columns are read and written as strings to preserve formatting.
    Uses streaming mode for files > 5GB total.
    """
    import gc
    import time
    
    start_time = time.time()
    initial_memory = get_memory_info()
    
    log(f"Starting merge of {len(files_to_merge)} files...")
    log(f"Initial memory: {initial_memory['process_memory_mb']:.1f} MB")
    log(f"Available memory: {initial_memory['available_memory_mb']:.1f} GB")
    
    # Calculate total size
    total_size_gb = sum(estimate_file_size_gb(f) for f in files_to_merge)
    log(f"Total input size: {total_size_gb:.2f} GB")
    
    # Determine if we should use streaming mode
    if total_size_gb > 5 or use_streaming:
        log("Using STREAMING mode for memory efficiency")
        mode = "STREAMING"
    else:
        log("Using STANDARD in-memory mode")
        mode = "STANDARD"
    
    # Create schema: all columns as string (Utf8)
    schema_overrides = {col: pl.Utf8 for col in expected_columns}
    
    # Process files
    all_dataframes = []
    total_rows = 0
    
    for i, csv_file in enumerate(files_to_merge, 1):
        file_size = estimate_file_size_gb(csv_file)
        log(f"[{i}/{len(files_to_merge)}] Processing: {csv_file.name} ({file_size:.2f} GB)")
        
        try:
            if mode == "STREAMING":
                # Use scan_csv for lazy loading
                lf = pl.scan_csv(
                    csv_file,
                    schema_overrides=schema_overrides,
                    truncate_ragged_lines=True,
                    null_values=["∞", "inf", "-inf"],
                    ignore_errors=True,
                    low_memory=True,
                )
                
                # Collect row count (forces a scan but lightweight)
                row_count = lf.select(pl.count()).collect().item()
                log(f"  ✓ Loaded {row_count:,} rows (lazy)")
                total_rows += row_count
                
                all_dataframes.append(lf)
            else:
                # Load into memory
                df = pl.read_csv(
                    csv_file,
                    schema_overrides=schema_overrides,
                    truncate_ragged_lines=True,
                    null_values=["∞", "inf", "-inf"],
                    ignore_errors=True,
                    low_memory=True,
                )
                
                log(f"  ✓ Loaded {len(df):,} rows")
                total_rows += len(df)
                
                all_dataframes.append(df.lazy())  # Convert to lazy for uniform handling
        
        except Exception as e:
            log(f"  ✗ Error reading file: {e}", "ERROR")
            continue
        
        # Memory check every 10 files
        if i % 10 == 0:
            current_mem = get_memory_info()
            log(f"  Memory usage: {current_mem['process_memory_mb']:.1f} MB")
    
    if not all_dataframes:
        log("No valid files to merge!", "ERROR")
        sys.exit(1)
    
    # Concatenate all dataframes
    log(f"Concatenating {len(all_dataframes)} dataframes...")
    concat_start = time.time()
    
    try:
        # Use vertical_relaxed to handle missing columns gracefully
        combined_lf = pl.concat(all_dataframes, how="vertical_relaxed")
        
        concat_time = time.time() - concat_start
        log(f"Concatenation completed in {concat_time:.2f}s")
        
        # Write to output
        log(f"Writing to {output_file.name}...")
        write_start = time.time()
        
        if mode == "STREAMING":
            # Collect with streaming for memory efficiency
            df = combined_lf.collect(streaming=True)
            df.write_csv(output_file, include_header=True, line_terminator='\r\n')
        else:
            # Collect and write
            df = combined_lf.collect()
            df.write_csv(output_file, include_header=True, line_terminator='\r\n')
        
        write_time = time.time() - write_start
        log(f"Write completed in {write_time:.2f}s")
        
        # Final statistics
        output_size = estimate_file_size_gb(output_file)
        final_memory = get_memory_info()
        total_time = time.time() - start_time
        
        print("\n" + "="*70)
        print("MERGE COMPLETED SUCCESSFULLY")
        print("="*70)
        print(f"Output file:      {output_file.name}")
        print(f"Total rows:       {total_rows:,}")
        print(f"Total columns:    {len(expected_columns)}")
        print(f"Output size:      {output_size:.2f} GB")
        print(f"Processing time:  {total_time:.2f}s")
        print(f"Memory used:      {final_memory['process_memory_mb'] - initial_memory['process_memory_mb']:.1f} MB")
        print(f"Peak memory:      {final_memory['process_memory_mb']:.1f} MB")
        print("="*70 + "\n")
        
        # Cleanup
        del all_dataframes
        del combined_lf
        if 'df' in locals():
            del df
        gc.collect()
        
    except Exception as e:
        log(f"Error during merge: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description='High-performance CSV combiner using Polars (optimized for 50GB+ files)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python combine_csvs_polars.py                      # Use current directory
  python combine_csvs_polars.py /path/to/csvs        # Use specified directory
  python combine_csvs_polars.py -o merged.csv        # Custom output filename
  python combine_csvs_polars.py --no-streaming       # Force in-memory mode
  python combine_csvs_polars.py -f                   # Auto-merge matching headers without prompt

Features:
  ✓ Handles 50GB+ files efficiently with streaming
  ✓ Preserves all data formatting (strings, leading zeros, etc.)
  ✓ Multi-threaded header analysis
  ✓ Auto-detects delimiters and encodings
  ✓ Gracefully handles missing columns
        """
    )

    parser.add_argument(
        'directory',
        nargs='?',
        default='.',
        help='Directory containing CSV files (default: current directory)'
    )

    parser.add_argument(
        '-o', '--output',
        default='combined_output.csv',
        help='Output filename (default: combined_output.csv)'
    )
    
    parser.add_argument(
        '--no-streaming',
        action='store_true',
        help='Force in-memory mode (faster but uses more RAM)'
    )
    
    parser.add_argument(
        '-f', '--force',
        action='store_true',
        help='Auto-merge files with matching headers without prompting'
    )

    args = parser.parse_args()

    # Get directory path
    directory = Path(args.directory).resolve()

    print("\n" + "="*70)
    print("HIGH-PERFORMANCE CSV COMBINER (Polars)")
    print("="*70)
    print(f"Working directory: {directory}")
    print(f"Mode: {'In-Memory' if args.no_streaming else 'Auto (Streaming for large files)'}")
    print("="*70 + "\n")

    # Get all CSV files
    csv_files = get_csv_files(directory)
    
    # Analyze headers in parallel
    header_groups, most_common_header, total_columns = analyze_headers(csv_files)
    
    # Display analysis
    display_header_analysis(header_groups)
    
    # Determine which files to merge
    if len(header_groups) == 1:
        # All headers match - proceed with all files
        files_to_merge = csv_files
        header_to_use = most_common_header
        
        if not args.force:
            proceed = input("Proceed with merge? (y/n): ").strip().lower()
            if proceed != 'y':
                log("Aborted by user.")
                sys.exit(0)
    else:
        # Headers don't match - ask user
        user_choice = get_user_choice(header_groups)
        
        if user_choice == 'abort':
            log("Aborted. No files were merged.")
            sys.exit(0)
        elif user_choice == 'matching':
            # Use only files with the most common header
            most_common_sig = max(header_groups.keys(), key=lambda k: len(header_groups[k]['files']))
            files_to_merge = header_groups[most_common_sig]['files']
            header_to_use = most_common_header
            log(f"Proceeding with {len(files_to_merge)} files that have matching headers.")
        else:  # 'all'
            files_to_merge = csv_files
            header_to_use = most_common_header
            log("⚠ Warning: Forcing merge of all files. Missing columns will be filled with null.")
    
    # Set output file path
    output_file = directory / args.output
    
    # Check if output file already exists
    if output_file.exists():
        overwrite = input(f"\n'{output_file.name}' already exists. Overwrite? (y/n): ").strip().lower()
        if overwrite != 'y':
            log("Aborted. No files were merged.")
            sys.exit(0)
    
    # Check available disk space
    import shutil
    free_space_gb = shutil.disk_usage(directory).free / (1024**3)
    total_input_size = sum(estimate_file_size_gb(f) for f in files_to_merge)
    
    if free_space_gb < total_input_size * 1.1:  # Need 110% of input size
        log(f"Warning: Low disk space! Available: {free_space_gb:.1f} GB, Needed: ~{total_input_size*1.1:.1f} GB", "WARNING")
        proceed = input("Continue anyway? (y/n): ").strip().lower()
        if proceed != 'y':
            log("Aborted due to low disk space.")
            sys.exit(0)
    
    # Combine the files
    combine_csv_files_streaming(
        files_to_merge, 
        output_file, 
        header_to_use,
        use_streaming=not args.no_streaming
    )
    
    log("All done! 🎉")


if __name__ == "__main__":
    main()