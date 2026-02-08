import gc
import logging
import os
import time
from datetime import datetime

# ⚡ Lazy import for Polars (heavy library)
from utils.lazy_imports import polars as pl
import psutil

from utils.file_lock import FileLock, FileLockTimeout


def process_data(input_file: str, output_file: str, progress_callback=None) -> None:
    """
    High-performance CSV processor for very large files using Polars.

    - Automatically chooses in-memory or streaming mode based on file size and RAM.
    - Applies the same business rules as the original implementation:
        * Remove empty rows (by first column)
        * Optionally remove duplicate TestDateUTC rows (standard mode only)
        * Remove rows where both RawSlope170 and RawSlope270 are empty
        * Remove rows where TrailingFactor < 0.15
        * Remove rows where abs(tsdSlopeMinY) / tsdSlopeMaxY < 0.15
        * Remove rows where Lane contains "SK"
        * Remove rows where Ignore == True/"true"
    - Keeps all original columns.
    - Casts all columns to string before writing to preserve formatting.
    """
    if progress_callback:
        def log_func(message, level="INFO"):
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
            progress_callback(f"{timestamp} - {level} - {message}")
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )
        log_func = logging.info

    file_size_gb = _estimate_file_size_gb(input_file)
    memory_info = _get_memory_info()
    available_gb = memory_info["available_memory_mb"] / 1024

    log_func(
        f"File analysis: {file_size_gb:.2f} GB file, "
        f"{available_gb:.2f} GB available memory "
        f"(process using {memory_info['process_memory_mb']:.1f} MB)"
    )

    # Heuristic: use streaming mode for very large files or low available RAM
    use_streaming = file_size_gb > 5 or file_size_gb > available_gb * 0.5

    if use_streaming:
        log_func("Using HIGH-PERFORMANCE STREAMING mode (optimized for 5GB+ files)")
        _process_streaming(input_file, output_file, log_func)
    else:
        log_func("Using STANDARD in-memory mode (optimized for smaller files)")
        _process_standard(input_file, output_file, log_func, file_size_gb)


def _estimate_file_size_gb(file_path: str) -> float:
    """Estimate file size in GB."""
    try:
        size_bytes = os.path.getsize(file_path)
        return size_bytes / (1024 ** 3)
    except OSError:
        return 0.0


def _get_memory_info() -> dict:
    """Get current memory usage information."""
    process = psutil.Process()
    memory_info = process.memory_info()
    virtual_memory = psutil.virtual_memory()

    return {
        "process_memory_mb": memory_info.rss / 1024 / 1024,
        "available_memory_mb": virtual_memory.available / 1024 / 1024,
        "memory_percent": virtual_memory.percent,
    }


def _read_header(input_file: str) -> list[str]:
    """
    Robust header reader that handles:
    - BOM (UTF-8-SIG)
    - malformed first lines
    - non-comma delimiters
    - ragged lines
    - extremely wide CSVs
    """

    # 1) Try normal read
    try:
        df = pl.read_csv(input_file, n_rows=0)
        return df.columns
    except:
        pass

    # 2) Try with UTF-8-SIG (BOM)
    try:
        df = pl.read_csv(input_file, n_rows=0, encoding="utf8-lossy")
        return df.columns
    except:
        pass

    # 3) Try forcing delimiter inference
    for delimiter in [",", ";", "\t", "|"]:
        try:
            df = pl.read_csv(
                input_file,
                n_rows=0,
                separator=delimiter,
                truncate_ragged_lines=True,
                ignore_errors=True,
            )
            return df.columns
        except:
            continue

    # 4) Manual fallback: read first line via Python, split heuristically
    try:
        with open(input_file, "r", encoding="utf-8", errors="ignore") as f:
            first_line = f.readline().strip()

        # Try common delimiters
        for delimiter in [",", ";", "\t", "|"]:
            parts = first_line.split(delimiter)
            if len(parts) > 1:
                return parts

        # If still can't split → treat entire line as one column
        return [first_line] if first_line else []
    except:
        return []


def _process_standard(
    input_file: str,
    output_file: str,
    log_func,
    file_size_gb: float,
) -> None:
    """
    Standard in-memory processing for smaller files.

    Still uses Polars vectorised ops everywhere, but keeps TestDateUTC
    de-duplication (like the original code).
    """
    start_time = time.time()
    process = psutil.Process(os.getpid())
    initial_memory = process.memory_info().rss / 1024 / 1024  # MB
    log_func(f"Starting STANDARD processing with {initial_memory:.1f} MB memory")

    # Discover columns and first column name
    columns = _read_header(input_file)
    if not columns:
        raise RuntimeError("Failed to read header from CSV file.")
    first_col = columns[0]

    # === Fast CSV read - READ ALL COLUMNS AS STRING to preserve formatting ===
    # This ensures 0011 stays as "0011", False stays as "False", etc.
    log_func(f"Reading CSV file into memory (all columns as string to preserve formatting): {input_file}")
    read_start = time.time()
    # Create schema_overrides dict to force all columns to be read as strings
    schema_overrides = {col: pl.Utf8 for col in columns}
    df = pl.read_csv(
        input_file,
        schema_overrides=schema_overrides,
        truncate_ragged_lines=True,
        null_values=["∞", "inf", "-inf"],
        ignore_errors=True,
        low_memory=True,
    )
    read_time = time.time() - read_start
    after_read_memory = process.memory_info().rss / 1024 / 1024
    log_func(
        f"CSV loaded in {read_time:.2f}s, {len(df)} rows, "
        f"memory: {after_read_memory:.1f} MB"
    )

    start_rows = len(df)

    # === Criterion 0: Remove empty rows by first column ===
    df = df.filter(df[first_col].is_not_null() & (df[first_col] != ""))
    log_func(f"After removing empty rows -> {len(df)} rows remaining")

    # === Criterion 0.5: Remove TestDateUTC duplicates (keep first) ===
    if "TestDateUTC" in df.columns:
        before = len(df)
        log_func(f"Removing duplicate TestDateUTC rows... ({before} rows)")
        dup_start = time.time()
        df = df.unique(subset=["TestDateUTC"], keep="first", maintain_order=True)
        dup_time = time.time() - dup_start
        removed = before - len(df)
        if removed > 0:
            log_func(
                f"Removed {removed} duplicate TestDateUTC rows in {dup_time:.2f}s "
                f"-> {len(df)} rows remaining"
            )
        else:
            log_func(f"No duplicate TestDateUTC rows found ({dup_time:.2f}s)")
    else:
        log_func("Column 'TestDateUTC' not found, skipping duplicate removal")

    # === Criterion 1: Remove rows where both RawSlope170 and RawSlope270 are empty ===
    if "RawSlope170" in df.columns and "RawSlope270" in df.columns:
        before = len(df)
        slope170 = df["RawSlope170"].cast(pl.Utf8, strict=False)
        slope270 = df["RawSlope270"].cast(pl.Utf8, strict=False)
        mask_keep = ~(
            (slope170.is_null() | (slope170 == ""))
            & (slope270.is_null() | (slope270 == ""))
        )
        df = df.filter(mask_keep)
        removed = before - len(df)
        log_func(
            f"Removed {removed} rows where both RawSlope170 and RawSlope270 are empty "
            f"-> {len(df)} rows remaining"
        )
    else:
        log_func(
            "Columns 'RawSlope170' and/or 'RawSlope270' not found, "
            "skipping slope emptiness filtering",
        )

    # === Criterion 2: Remove rows where TrailingFactor < 0.15 ===
    if "TrailingFactor" in df.columns:
        before = len(df)
        tf = df["TrailingFactor"].cast(pl.Float64, strict=False)
        # Keep rows where tf is null OR >= 0.15
        mask_keep = tf.is_null() | (tf >= 0.15)
        df = df.filter(mask_keep)
        removed = before - len(df)
        log_func(
            f"Removed {removed} rows where TrailingFactor < 0.15 "
            f"-> {len(df)} rows remaining"
        )
    else:
        log_func("Column 'TrailingFactor' not found, skipping trailing factor filtering")

    # === Criterion 3: Remove rows where abs(tsdSlopeMinY) / tsdSlopeMaxY < 0.15 ===
    if "tsdSlopeMinY" in df.columns and "tsdSlopeMaxY" in df.columns:
        before = len(df)
        min_y = df["tsdSlopeMinY"].cast(pl.Float64, strict=False)
        max_y = df["tsdSlopeMaxY"].cast(pl.Float64, strict=False)
        ratio = (min_y.abs() / max_y)
        mask_keep = ratio.is_null() | (ratio >= 0.15)
        df = df.filter(mask_keep)
        removed = before - len(df)
        log_func(
            f"Removed {removed} rows where abs(tsdSlopeMinY)/tsdSlopeMaxY < 0.15 "
            f"-> {len(df)} rows remaining"
        )
    else:
        log_func(
            "Columns 'tsdSlopeMinY' and/or 'tsdSlopeMaxY' not found, "
            "skipping slope ratio filtering",
        )

    # === Criterion 4: Remove rows where Lane contains 'SK' ===
    if "Lane" in df.columns:
        before = len(df)
        lane_utf8 = df["Lane"].cast(pl.Utf8, strict=False)
        mask_keep = ~(lane_utf8.str.contains("SK").fill_null(False))
        df = df.filter(mask_keep)
        removed = before - len(df)
        log_func(
            f"Removed {removed} rows where Lane contains 'SK' "
            f"-> {len(df)} rows remaining"
        )
    else:
        log_func("Column 'Lane' not found, skipping Lane filtering")

    # === Criterion 5: Remove rows where Ignore is true (case-insensitive) ===
    if "Ignore" in df.columns:
        before = len(df)
        # Already string, but use str.to_lowercase() for case-insensitive comparison
        # This handles: True, true, TRUE, False, false, FALSE
        ignore_str = df["Ignore"].str.to_lowercase()
        mask_keep = (ignore_str != "true") & (ignore_str.is_not_null())
        df = df.filter(mask_keep)
        removed = before - len(df)
        log_func(
            f"Removed {removed} rows where Ignore is true "
            f"-> {len(df)} rows remaining"
        )
    else:
        log_func("Column 'Ignore' not found, skipping Ignore filtering")

    final_count = len(df)
    removed_total = start_rows - final_count
    log_func(
        f"Final dataset has {final_count} rows "
        f"(removed {removed_total} of {start_rows})"
    )

    # === All columns are already strings (read as string from the start) ===
    # No need to cast - original formatting preserved (0011, False, etc.)

    # === Write CSV ===
    write_start = time.time()
    try:
        with FileLock(output_file, timeout=60):
            df.write_csv(output_file, include_header=True)
    except FileLockTimeout:
        log_func("ERROR: Output file is locked by another process", "ERROR")
        raise RuntimeError(f"Cannot write to {output_file} - file is locked")
    
    write_time = time.time() - write_start
    log_func(f"Written CSV to {output_file} in {write_time:.2f}s")

    # Optional trailing newline cleanup ONLY for small files (avoid 10GB Python read)
    if file_size_gb < 0.5:
        _clean_trailing_newlines(output_file, log_func)
    else:
        log_func(
            "Skipping trailing newline cleanup for large file "
            "(would be too slow in Python)."
        )

    end_time = time.time()
    total_time = end_time - start_time
    final_memory = process.memory_info().rss / 1024 / 1024
    memory_used = final_memory - initial_memory
    log_func(
        f"STANDARD processing completed in {total_time:.2f}s, "
        f"memory used: {memory_used:.1f} MB (final: {final_memory:.1f} MB)"
    )


def _process_streaming(input_file: str, output_file: str, log_func) -> None:
    """
    High-performance streaming pipeline using Polars LazyFrame.

    NOTE:
    - To keep memory bounded and speed high, this mode intentionally
      SKIPS TestDateUTC de-duplication (same as your old streaming path).
    - All other criteria are still applied.
    """
    start_time = time.time()
    process = psutil.Process(os.getpid())
    initial_memory = process.memory_info().rss / 1024 / 1024
    log_func(f"Initial memory (STREAMING): {initial_memory:.1f} MB")

    columns = _read_header(input_file)
    if not columns:
        raise RuntimeError("Failed to read header from CSV file.")
    first_col = columns[0]

    # === Build lazy scan - READ ALL COLUMNS AS STRING to preserve formatting ===
    # This ensures 0011 stays as "0011", False stays as "False", etc.
    log_func("Building lazy scan pipeline (all columns as string to preserve formatting)...")
    # Create schema_overrides dict to force all columns to be read as strings
    schema_overrides = {col: pl.Utf8 for col in columns}
    lf = pl.scan_csv(
        input_file,
        schema_overrides=schema_overrides,
        truncate_ragged_lines=True,
        null_values=["∞", "inf", "-inf"],
        ignore_errors=True,
        low_memory=True,
    )

    # === Criterion 0: Remove empty rows by first column ===
    lf = lf.filter(pl.col(first_col).is_not_null() & (pl.col(first_col) != ""))

    # === Criterion 1: Remove rows where both RawSlope170 and RawSlope270 are empty ===
    if "RawSlope170" in columns and "RawSlope270" in columns:
        slope170 = pl.col("RawSlope170").cast(pl.Utf8, strict=False)
        slope270 = pl.col("RawSlope270").cast(pl.Utf8, strict=False)
        lf = lf.filter(
            ~(
                (slope170.is_null() | (slope170 == ""))
                & (slope270.is_null() | (slope270 == ""))
            )
        )
    else:
        log_func(
            "Columns 'RawSlope170' and/or 'RawSlope270' not found, "
            "skipping slope emptiness filtering (STREAMING)",
        )

    # === Criterion 2: Remove rows where TrailingFactor < 0.15 ===
    if "TrailingFactor" in columns:
        tf = pl.col("TrailingFactor").cast(pl.Float64, strict=False)
        # Keep rows where tf is null OR >= 0.15
        lf = lf.filter(tf.is_null() | (tf >= 0.15))
    else:
        log_func("Column 'TrailingFactor' not found, skipping TrailingFactor filter")

    # === Criterion 3: Remove rows where abs(tsdSlopeMinY) / tsdSlopeMaxY < 0.15 ===
    if "tsdSlopeMinY" in columns and "tsdSlopeMaxY" in columns:
        min_y = pl.col("tsdSlopeMinY").cast(pl.Float64, strict=False)
        max_y = pl.col("tsdSlopeMaxY").cast(pl.Float64, strict=False)
        ratio = (min_y.abs() / max_y)
        lf = lf.filter(ratio.is_null() | (ratio >= 0.15))
    else:
        log_func(
            "Columns 'tsdSlopeMinY' and/or 'tsdSlopeMaxY' not found, "
            "skipping slope ratio filtering (STREAMING)",
        )

    # === Criterion 4: Remove rows where Lane contains 'SK' ===
    if "Lane" in columns:
        lane_utf8 = pl.col("Lane").cast(pl.Utf8, strict=False)
        lf = lf.filter(~lane_utf8.str.contains("SK").fill_null(False))
    else:
        log_func("Column 'Lane' not found, skipping Lane filter (STREAMING)")

    # === Criterion 5: Remove rows where Ignore is true (case-insensitive) ===
    if "Ignore" in columns:
        # Already string, but use str.to_lowercase() for case-insensitive comparison
        # This handles: True, true, TRUE, False, false, FALSE
        ignore_str = pl.col("Ignore").str.to_lowercase()
        lf = lf.filter((ignore_str != "true") & (ignore_str.is_not_null()))
    else:
        log_func("Column 'Ignore' not found, skipping Ignore filter (STREAMING)")

    # === All columns are already strings (read as string from the start) ===
    # No need to cast - original formatting preserved (0011, False, etc.)
    # Just select the original columns in case any were filtered out
    lf = lf.select([pl.col(c) for c in columns if c in lf.columns])

    # === Execute pipeline with streaming enabled ===
    log_func("Collecting filtered data with streaming=True ...")
    collect_start = time.time()
    df = lf.collect(streaming=True)
    collect_time = time.time() - collect_start
    current_memory = process.memory_info().rss / 1024 / 1024
    log_func(
        f"Streaming collect finished in {collect_time:.2f}s, "
        f"{len(df)} rows in result, memory: {current_memory:.1f} MB"
    )

    # === Write CSV ===
    write_start = time.time()
    try:
        with FileLock(output_file, timeout=60):
            df.write_csv(output_file, include_header=True)
    except FileLockTimeout:
        log_func("ERROR: Output file is locked by another process", "ERROR")
        raise RuntimeError(f"Cannot write to {output_file} - file is locked")
    
    write_time = time.time() - write_start
    log_func(f"Written CSV to {output_file} in {write_time:.2f}s")
    # === Explicit memory cleanup ===
    del df
    gc.collect()
    log_func("Memory cleanup completed (DataFrame released)")
    # === Explicit memory cleanup ===
    del df
    del lf
    gc.collect()
    log_func("Memory cleanup completed (DataFrame and LazyFrame released)")

    end_time = time.time()
    total_time = end_time - start_time
    final_memory = process.memory_info().rss / 1024 / 1024
    memory_used = final_memory - initial_memory
    log_func(
        f"STREAMING processing completed in {total_time:.2f}s, "
        f"memory used: {memory_used:.1f} MB (final: {final_memory:.1f} MB)"
    )


def _clean_trailing_newlines(output_file: str, log_func) -> None:
    """
    Remove trailing empty lines / extra newlines from a (small) CSV file.

    IMPORTANT:
    - This runs only for small files (e.g. <0.5 GB).
    - For huge files it would be too slow to load in Python, so we skip it.
    """
    try:
        with open(output_file, "r", encoding="utf-8") as f:
            content = f.read()

        lines = content.rstrip("\n\r").split("\n")
        while lines and lines[-1].strip() == "":
            lines.pop()

        cleaned_content = "\n".join(lines)
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(cleaned_content)

        log_func(f"Trailing newline cleanup done for {output_file}")
    except Exception as e:
        log_func(f"Warning: failed to clean trailing newlines: {e}")
