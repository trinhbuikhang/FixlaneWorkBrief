import gc
import logging
import os
import sqlite3
import time
from datetime import datetime
from pathlib import Path
import tempfile
import shutil
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from typing import Optional, Set, List, Tuple
import multiprocessing as mp
import threading

# ⚡ Lazy import for Polars (heavy library)
from utils.lazy_imports import polars as pl
import psutil

from utils.file_lock import FileLock, FileLockTimeout

logger = logging.getLogger(__name__)


# ============================================================================
# MEMORY-SAFE CONFIGURATION (overridable via config/env)
# ============================================================================
def _get_memory_limits():
    """Respect system RAM and config so we never use more than a safe fraction."""
    try:
        from config.app_config import AppConfig
        max_ram_gb = AppConfig.LMD_MAX_RAM_GB
        max_workers = AppConfig.LMD_MAX_WORKERS
        dedup_cap = AppConfig.LMD_DEDUP_SET_MAX_SIZE
        standard_max_gb = AppConfig.LMD_STANDARD_MODE_MAX_FILE_GB
    except Exception:
        max_ram_gb = 0.0
        max_workers = 2
        dedup_cap = 5_000_000
        standard_max_gb = 1.5
    vm = psutil.virtual_memory()
    total_gb = vm.total / (1024 ** 3)
    available_gb = vm.available / (1024 ** 3)
    # Use at most 50% of available RAM, or 40% of total, so OS and child processes don't OOM
    auto_ram_gb = min(available_gb * 0.5, total_gb * 0.4)
    if max_ram_gb <= 0:
        safe_ram_gb = max(4.0, min(48.0, auto_ram_gb))  # clamp 4–48 GB when auto
    else:
        safe_ram_gb = min(max_ram_gb, auto_ram_gb)
    return safe_ram_gb, max_workers, dedup_cap, standard_max_gb

# Fallbacks when not using config (e.g. tests)
MAX_RAM_USAGE_GB = 48  # Default cap; _get_memory_limits() overrides at runtime
MEMORY_MONITOR_INTERVAL = 2  # Check memory every 2 seconds
EMERGENCY_GC_THRESHOLD_RATIO = 0.92  # Emergency when process uses this fraction of safe_ram
SAFE_MEMORY_THRESHOLD_RATIO = 0.85  # Start proactive GC at this fraction


class MemoryMonitor:
    """
    Real-time memory monitor to prevent OOM crashes.
    Uses a configurable RAM cap (from system + config) and ratio-based thresholds.
    """
    
    def __init__(self, max_memory_gb: float = MAX_RAM_USAGE_GB):
        self.max_memory_gb = max_memory_gb
        self.max_memory_bytes = max_memory_gb * 1024 * 1024 * 1024
        self.emergency_gb = max_memory_gb * EMERGENCY_GC_THRESHOLD_RATIO
        self.safe_gb = max_memory_gb * SAFE_MEMORY_THRESHOLD_RATIO
        self.running = False
        self.thread = None
        self.emergency_triggered = False
        self.cleanup_callbacks = []
        
    def set_limit_gb(self, max_memory_gb: float):
        """Update RAM cap and derived thresholds (call before start for current run)."""
        self.max_memory_gb = max_memory_gb
        self.max_memory_bytes = max_memory_gb * 1024 * 1024 * 1024
        self.emergency_gb = max_memory_gb * EMERGENCY_GC_THRESHOLD_RATIO
        self.safe_gb = max_memory_gb * SAFE_MEMORY_THRESHOLD_RATIO
        
    def register_cleanup_callback(self, callback):
        """Register a function to call during emergency cleanup."""
        self.cleanup_callbacks.append(callback)
    
    def start(self):
        """Start monitoring in background thread."""
        self.running = True
        self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.thread.start()
    
    def stop(self):
        """Stop monitoring."""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
    
    def _monitor_loop(self):
        """Main monitoring loop."""
        while self.running:
            try:
                process = psutil.Process()
                current_memory_mb = process.memory_info().rss / 1024 / 1024
                current_memory_gb = current_memory_mb / 1024
                
                if current_memory_gb > self.emergency_gb:
                    if not self.emergency_triggered:
                        logger.critical(
                            "EMERGENCY: Memory at %.1f GB (limit %.1f GB)! "
                            "Triggering cleanup...",
                            current_memory_gb, self.max_memory_gb,
                        )
                        self.emergency_triggered = True
                        self._emergency_cleanup()
                
                elif current_memory_gb > self.safe_gb:
                    logger.warning(
                        "Memory pressure: %.1f GB / %.1f GB – running GC",
                        current_memory_gb, self.max_memory_gb,
                    )
                    gc.collect()
                
                else:
                    self.emergency_triggered = False
                
            except Exception as e:
                logger.error("Memory monitor error: %s", e)
            
            time.sleep(MEMORY_MONITOR_INTERVAL)
    
    def _emergency_cleanup(self):
        """Perform emergency cleanup."""
        # Force aggressive garbage collection
        for _ in range(3):
            gc.collect()
        
        # Call registered cleanup callbacks
        for callback in self.cleanup_callbacks:
            try:
                callback()
            except Exception as e:
                logger.warning("Emergency cleanup callback failed: %s", e)
    
    def get_current_usage_gb(self) -> float:
        """Get current memory usage in GB."""
        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024 / 1024


# Global memory monitor instance
memory_monitor = MemoryMonitor()


def _cleanup_stale_temp_dirs(near_path: str, prefix: str, max_age_seconds: float = 3600) -> None:
    """
    Remove stale temp dirs (lmd_merge_*, csv_safe_*) in the same directory as *near_path*
    to clean up after crash or abnormal exit.
    """
    out_dir = os.path.dirname(os.path.abspath(near_path))
    if not os.path.isdir(out_dir):
        return
    try:
        now = time.time()
        for name in os.listdir(out_dir):
            if name.startswith(prefix):
                path = os.path.join(out_dir, name)
                if os.path.isdir(path):
                    try:
                        mtime = os.path.getmtime(path)
                        if now - mtime > max_age_seconds:
                            shutil.rmtree(path, ignore_errors=True)
                            logger.info("Cleaned stale temp dir: %s", path)
                    except OSError:
                        pass
    except OSError:
        pass


def _temp_dir_near(output_file: str, prefix: str = "csv_safe_") -> str:
    """
    Create a temp directory on the **same drive/volume** as *output_file*
    so we never fill the system temp (usually C:) with large intermediate data.

    Falls back to the system temp dir if the output directory is not writable.
    """
    out_dir = os.path.dirname(os.path.abspath(output_file))
    try:
        os.makedirs(out_dir, exist_ok=True)
        return tempfile.mkdtemp(prefix=prefix, dir=out_dir)
    except OSError:
        # Fall back to system temp
        return tempfile.mkdtemp(prefix=prefix)


def _check_disk_space(path: str, needed_gb: float, log_func=None) -> None:
    """
    Raise RuntimeError if the volume that contains *path* has less than
    *needed_gb* GB free.  *log_func* receives a warning before the raise.
    """
    try:
        usage = shutil.disk_usage(os.path.abspath(path))
        free_gb = usage.free / (1024 ** 3)
        if log_func:
            log_func(f"Disk space check: {free_gb:.1f} GB free on {os.path.splitdrive(os.path.abspath(path))[0] or '/'}")
        if free_gb < needed_gb:
            msg = (
                f"Not enough disk space: {free_gb:.1f} GB free, "
                f"but ~{needed_gb:.1f} GB required.  "
                f"Free up space on the drive containing: {path}"
            )
            if log_func:
                log_func(msg, "ERROR")
            raise RuntimeError(msg)
    except OSError:
        pass  # can't check – proceed and let the OS error surface naturally


def _get_progress_file_logger():
    """Return a dedicated file logger for process_data progress (lazy-init)."""
    fl = logging.getLogger("lmd_data_processor_progress")
    if not fl.handlers:
        fl.setLevel(logging.DEBUG)
        fl.propagate = False
        try:
            from config.app_config import AppConfig
            log_dir = AppConfig.LOG_DIR
        except Exception:
            log_dir = Path(__file__).resolve().parent.parent / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(str(log_dir / "lmd_processing.log"),
                                       mode="a", encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s  %(message)s"))
        fl.addHandler(handler)
    return fl


def process_data(input_file: str, output_file: str, progress_callback=None) -> None:
    """
    MEMORY-SAFE ultra-fast CSV processor for large LMD files (Polars only).
    
    Uses Polars in standard or chunked mode depending on file size and RAM.
    Features:
    - Real-time memory monitoring
    - Automatic memory pressure relief
    - Adaptive batch sizing based on current memory
    """
    _flog = _get_progress_file_logger()

    if progress_callback:
        def log_func(message, level="INFO", percent=None):
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
            line = f"{timestamp} - {level} - {message}"
            try:
                if percent is not None:
                    progress_callback(line, percent)
                else:
                    progress_callback(line)
            except TypeError:
                progress_callback(line)
            _flog.info(message)
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )
        def log_func(message, level="INFO", percent=None):
            logging.info(message)
            _flog.info(message)

    file_size_gb = _estimate_file_size_gb(input_file)

    log_func(f"Input file: {os.path.basename(input_file)} ({file_size_gb:.2f} GB)", percent=0)
    log_func(f"Output file: {os.path.abspath(output_file)}")

    # Polars path (standard or chunked mode)
    memory_info = _get_memory_info()
    available_gb = memory_info["available_memory_mb"] / 1024
    cpu_count = mp.cpu_count()
    try:
        from config.app_config import AppConfig
        cpu_fraction = getattr(AppConfig, 'LMD_CPU_FRACTION', 0.5)
    except Exception:
        cpu_fraction = 0.5
    effective_cpu = max(1, int(cpu_count * cpu_fraction))

    safe_ram_gb, max_workers_cap, dedup_cap, standard_max_gb = _get_memory_limits()
    log_func(
        f"System resources: {file_size_gb:.2f} GB file, "
        f"{available_gb:.2f} GB available memory, "
        f"{cpu_count} CPU cores (using {effective_cpu}); RAM cap: {safe_ram_gb:.1f} GB"
    )

    _check_disk_space(output_file, needed_gb=file_size_gb * 1.2, log_func=log_func)
    memory_monitor.set_limit_gb(safe_ram_gb)
    memory_monitor.start()
    log_func(f"Memory monitor started (max: {safe_ram_gb:.1f} GB, emergency at {safe_ram_gb * EMERGENCY_GC_THRESHOLD_RATIO:.1f} GB)")

    try:
        estimated_load_gb = file_size_gb * 4
        use_chunked = file_size_gb > standard_max_gb or estimated_load_gb > (safe_ram_gb * 0.5)
        if use_chunked:
            log_func(f"Using MEMORY-SAFE CHUNKED mode (file {file_size_gb:.1f} GB, RAM cap {safe_ram_gb:.1f} GB)", percent=5)
            _process_memory_safe_ultra_fast(
                input_file, output_file, log_func, file_size_gb, effective_cpu,
                safe_ram_gb=safe_ram_gb, max_workers_cap=max_workers_cap, dedup_cap=dedup_cap
            )
        else:
            log_func(f"Using MEMORY-SAFE STANDARD mode (file <= {standard_max_gb} GB)", percent=5)
            _process_memory_safe_standard(input_file, output_file, log_func, file_size_gb)
    finally:
        memory_monitor.stop()
        log_func("Memory monitor stopped", percent=100)


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
        "total_memory_gb": virtual_memory.total / 1024 / 1024 / 1024,
    }


def _read_header(input_file: str) -> list[str]:
    """Fast header reader."""
    try:
        df = pl.read_csv(input_file, n_rows=0)
        return df.columns
    except Exception as e:
        logger.debug("read_csv header (default) failed for %s: %s", input_file, e)

    try:
        df = pl.read_csv(input_file, n_rows=0, encoding="utf8-lossy")
        return df.columns
    except Exception as e:
        logger.debug("read_csv header (utf8-lossy) failed for %s: %s", input_file, e)

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
        except Exception as e:
            logger.debug("read_csv header delimiter %r failed: %s", delimiter, e)
            continue

    try:
        with open(input_file, "r", encoding="utf-8", errors="ignore") as f:
            first_line = f.readline().strip()
        for delimiter in [",", ";", "\t", "|"]:
            parts = first_line.split(delimiter)
            if len(parts) > 1:
                return parts
        return [first_line] if first_line else []
    except Exception as e:
        logger.warning("Fallback header read failed for %s: %s", input_file, e)
        return []


def _estimate_total_rows(input_file: str, file_size_gb: float) -> int:
    """Estimate total rows: heuristic for large files (avoid extra read), else small sample."""
    # For large files use heuristic to avoid a full 10k-row read (saves I/O and time)
    if file_size_gb > 1.0:
        return int(file_size_gb * 2_500_000)  # ~2.5M rows/GB typical for wide CSV
    try:
        sample_df = pl.read_csv(
            input_file,
            n_rows=5000,
            ignore_errors=True,
            truncate_ragged_lines=True,
            low_memory=False,
        )
        sample_rows = len(sample_df)
        if sample_rows > 0:
            sample_size_bytes = sample_df.estimated_size()
            total_bytes = file_size_gb * 1024 * 1024 * 1024
            estimated_rows = int((total_bytes / sample_size_bytes) * sample_rows)
            del sample_df
            gc.collect()
            return estimated_rows
    except Exception as e:
        logger.debug("Could not estimate total rows for %s: %s", input_file, e)
    return int(file_size_gb * 2_500_000)


def _calculate_safe_partitions(
    file_size_gb: float,
    cpu_count: int,
    current_memory_gb: float,
    safe_ram_gb: float,
    max_workers_cap: int,
) -> Tuple[int, int]:
    """
    Calculate partitions so total RAM stays under safe_ram_gb.
    Each worker process can use ~3x partition size (read + process + temp).
    So: max_workers * partition_size_gb * 3 <= safe_ram_gb * 0.6 (leave 40% for main + OS).
    """
    max_workers = max(1, min(cpu_count, max_workers_cap, 6))
    # Budget for workers: 60% of cap; each worker holds one chunk ~3x partition size
    budget_gb = safe_ram_gb * 0.6
    safe_partition_size_gb = budget_gb / (max_workers * 3)
    safe_partition_size_gb = max(0.2, min(safe_partition_size_gb, 2.0))  # 0.2–2 GB per partition
    num_partitions = max(8, int(file_size_gb / safe_partition_size_gb))
    num_partitions = min(num_partitions, 60)  # Avoid too many small chunks
    return num_partitions, max_workers


def _process_chunk_worker_safe(args) -> Tuple[int, Optional[pl.DataFrame], Set[str], dict]:
    """
    MEMORY-SAFE chunk worker with aggressive cleanup.

    Key changes:
    1. Use low_memory=True (trades speed for safety)
    2. Immediate cleanup after processing
    3. No intermediate copies
    4. Return only essential data
    """
    chunk_id, input_file, start_row, end_row, columns, first_col, schema_overrides = args

    chunk_df = None
    try:
        # Read with MEMORY-SAFE settings
        chunk_df = pl.read_csv(
            input_file,
            skip_rows=start_row,
            n_rows=end_row - start_row,
            schema_overrides=schema_overrides,
            truncate_ragged_lines=True,
            null_values=["∞", "inf", "-inf"],
            ignore_errors=True,
            low_memory=True,  # ← KEY: Memory-safe mode
            rechunk=False,  # ← Don't rechunk to save memory
            has_header=False,
            new_columns=columns,
        )

        initial_count = len(chunk_df)

        # === Apply filters (see Help tab / LMD Data Cleaner for full rules) ===
        # 0. First column empty -> remove
        # 1. Both RawSlope170 and RawSlope270 empty -> remove
        # 2. TrailingFactor < 0.15 -> remove (null kept)
        # 3. abs(tsdSlopeMinY)/tsdSlopeMaxY < 0.15 -> remove; tsdSlopeMaxY null/0 -> remove
        # 4. Lane contains "SK" -> remove
        # 5. Ignore == "true" (case-insensitive) or Ignore null -> remove
        # 6. Dedup by TestDateUTC (keep first)

        # Criterion 0: Remove empty rows
        chunk_df = chunk_df.filter(
            chunk_df[first_col].is_not_null() & (chunk_df[first_col] != "")
        )

        # Criterion 1: Both slopes empty
        if "RawSlope170" in columns and "RawSlope270" in columns:
            chunk_df = chunk_df.filter(
                ~(
                    (chunk_df["RawSlope170"].is_null() | (chunk_df["RawSlope170"] == ""))
                    & (chunk_df["RawSlope270"].is_null() | (chunk_df["RawSlope270"] == ""))
                )
            )

        # Criterion 2: TrailingFactor
        if "TrailingFactor" in columns:
            tf = chunk_df["TrailingFactor"].cast(pl.Float64, strict=False)
            chunk_df = chunk_df.filter(tf.is_null() | (tf >= 0.15))
            del tf  # Immediate cleanup

        # Criterion 3: Slope ratio abs(tsdSlopeMinY)/tsdSlopeMaxY >= 0.15 (remove when < 0.15 or when max_y is 0/null)
        if "tsdSlopeMinY" in columns and "tsdSlopeMaxY" in columns:
            min_y = chunk_df["tsdSlopeMinY"].cast(pl.Float64, strict=False)
            max_y = chunk_df["tsdSlopeMaxY"].cast(pl.Float64, strict=False)
            # Avoid division by zero: drop rows where max_y is null or 0 (invalid ratio)
            valid_denom = max_y.is_not_null() & (max_y != 0)
            ratio = pl.when(valid_denom).then(min_y.abs() / max_y).otherwise(pl.lit(None))
            chunk_df = chunk_df.filter(valid_denom & ((ratio.is_null()) | (ratio >= 0.15)))
            del min_y, max_y, ratio, valid_denom  # Cleanup

        # Criterion 4: Lane filter
        if "Lane" in columns:
            chunk_df = chunk_df.filter(
                ~chunk_df["Lane"].str.contains("SK").fill_null(False)
            )

        # Criterion 5: Ignore filter
        if "Ignore" in columns:
            chunk_df = chunk_df.filter(
                (chunk_df["Ignore"].str.to_lowercase() != "true")
                & (chunk_df["Ignore"].is_not_null())
            )

        # Extract TestDateUTC values (lightweight)
        seen_dates = set()
        if "TestDateUTC" in columns:
            # Convert to list and create set in one go (more memory efficient)
            seen_dates = set(chunk_df["TestDateUTC"].to_list())

        filtered_count = len(chunk_df)

        stats = {
            'chunk_id': chunk_id,
            'initial_rows': initial_count,
            'filtered_rows': filtered_count,
            'removed_rows': initial_count - filtered_count,
            'unique_dates': len(seen_dates)
        }

        return (chunk_id, chunk_df, seen_dates, stats)

    except Exception as e:
        # Cleanup on error
        if chunk_df is not None:
            del chunk_df
        gc.collect()
        return (chunk_id, None, set(), {'error': str(e), 'chunk_id': chunk_id})


def _process_chunk_worker_safe_to_file(args) -> Tuple[int, Optional[str], dict]:
    """
    Worker: process chunk, write to temp file, return (chunk_id, file path, stats)
    """
    # unpack args including temp_dir
    chunk_id, input_file, start_row, end_row, columns, first_col, schema_overrides, temp_dir = args
    chunk_df = None
    temp_file = None
    try:
        chunk_df = pl.read_csv(
            input_file,
            skip_rows=start_row,
            n_rows=end_row - start_row,
            schema_overrides=schema_overrides,
            truncate_ragged_lines=True,
            null_values=["∞", "inf", "-inf"],
            ignore_errors=True,
            low_memory=False,  # Chunk size bounded by partition; faster read
            rechunk=False,
            has_header=False,
            new_columns=columns,
        )
        initial_count = len(chunk_df)
        # Apply same filters as _process_chunk_worker_safe
        chunk_df = chunk_df.filter(chunk_df[first_col].is_not_null() & (chunk_df[first_col] != ""))
        if "RawSlope170" in columns and "RawSlope270" in columns:
            chunk_df = chunk_df.filter(
                ~(
                    (chunk_df["RawSlope170"].is_null() | (chunk_df["RawSlope170"] == ""))
                    & (chunk_df["RawSlope270"].is_null() | (chunk_df["RawSlope270"] == ""))
                )
            )
        if "TrailingFactor" in columns:
            tf = chunk_df["TrailingFactor"].cast(pl.Float64, strict=False)
            chunk_df = chunk_df.filter(tf.is_null() | (tf >= 0.15))
            del tf
        if "tsdSlopeMinY" in columns and "tsdSlopeMaxY" in columns:
            min_y = chunk_df["tsdSlopeMinY"].cast(pl.Float64, strict=False)
            max_y = chunk_df["tsdSlopeMaxY"].cast(pl.Float64, strict=False)
            valid_denom = max_y.is_not_null() & (max_y != 0)
            ratio = pl.when(valid_denom).then(min_y.abs() / max_y).otherwise(pl.lit(None))
            chunk_df = chunk_df.filter(valid_denom & ((ratio.is_null()) | (ratio >= 0.15)))
            del min_y, max_y, ratio, valid_denom
        if "Lane" in columns:
            chunk_df = chunk_df.filter(~chunk_df["Lane"].str.contains("SK").fill_null(False))
        if "Ignore" in columns:
            chunk_df = chunk_df.filter((chunk_df["Ignore"].str.to_lowercase() != "true") & (chunk_df["Ignore"].is_not_null()))
        filtered_count = len(chunk_df)
        stats = {
            'chunk_id': chunk_id,
            'initial_rows': initial_count,
            'filtered_rows': filtered_count,
            'removed_rows': initial_count - filtered_count
        }
        if filtered_count > 0:
            temp_file = os.path.join(temp_dir, f"chunk_{chunk_id}.csv")
            chunk_df.write_csv(temp_file, include_header=False)
            del chunk_df
            gc.collect()
            return (chunk_id, temp_file, stats)
        else:
            del chunk_df
            gc.collect()
            return (chunk_id, None, stats)
    except Exception as e:
        if chunk_df is not None:
            del chunk_df
        gc.collect()
        return (chunk_id, None, {'error': str(e), 'chunk_id': chunk_id})


def _process_memory_safe_ultra_fast(
    input_file: str,
    output_file: str,
    log_func,
    file_size_gb: float,
    cpu_count: int,
    safe_ram_gb: float = 48.0,
    max_workers_cap: int = 2,
    dedup_cap: int = 5_000_000,
) -> None:
    """
    MEMORY-SAFE chunked processing with strict RAM cap.
    Uses safe_ram_gb and max_workers_cap so total usage stays below system limit.
    """
    start_time = time.time()
    process = psutil.Process(os.getpid())
    initial_memory = process.memory_info().rss / 1024 / 1024
    
    log_func(f"Initial memory: {initial_memory:.1f} MB (RAM cap: {safe_ram_gb:.1f} GB)")
    
    columns = _read_header(input_file)
    if not columns:
        raise RuntimeError("Failed to read header from CSV file.")
    first_col = columns[0]
    schema_overrides = {col: pl.Utf8 for col in columns}
    
    log_func(f"Analyzing file structure: {os.path.basename(input_file)}")
    estimated_rows = _estimate_total_rows(input_file, file_size_gb)
    current_memory_gb = memory_monitor.get_current_usage_gb()
    num_partitions, max_workers = _calculate_safe_partitions(
        file_size_gb, cpu_count, current_memory_gb,
        safe_ram_gb=safe_ram_gb, max_workers_cap=max_workers_cap
    )
    
    log_func(
        f"  Estimated ~{estimated_rows:,} rows, "
        f"{num_partitions} partitions, {max_workers} workers"
    )
    
    # Calculate row ranges
    rows_per_partition = estimated_rows // num_partitions
    partition_ranges = []
    for i in range(num_partitions):
        start_row = 1 + (i * rows_per_partition)
        end_row = start_row + rows_per_partition if i < num_partitions - 1 else estimated_rows
        partition_ranges.append((i, start_row, end_row))
    
    # === PHASE 1: Parallel filtering with STREAMING WRITE ===
    log_func(f"PHASE 1: File {os.path.basename(input_file)} – processing {num_partitions} chunks (dedup streaming)...", percent=10)
    phase1_start = time.time()
    
    # Clean up stale temp dirs from previous crashed runs
    _cleanup_stale_temp_dirs(output_file, "csv_safe_", max_age_seconds=3600)

    # Temp dir on the SAME drive as the output file to avoid filling system temp
    temp_dir = _temp_dir_near(output_file, prefix="csv_safe_")
    log_func(f"Temp directory: {temp_dir}")
    temp_output = os.path.join(temp_dir, "temp_output.csv")

    try:
        # Track seen dates globally (memory efficient)
        global_seen_dates = set()
        total_written_rows = 0
        total_duplicate_rows = 0
        total_initial_rows = 0
        temp_chunk_files = []

        # Process chunks in batches to control memory
        batch_size = max_workers
        for batch_start in range(0, num_partitions, batch_size):
            batch_end = min(batch_start + batch_size, num_partitions)
            batch_ranges = partition_ranges[batch_start:batch_end]

            current_mem = memory_monitor.get_current_usage_gb()
            pct = 10 + int(75 * batch_end / num_partitions) if num_partitions else 85
            log_func(
                f"  Batch {batch_start//batch_size + 1}/{(num_partitions + batch_size - 1)//batch_size} "
                f"(chunk {batch_start+1}-{batch_end}), RAM: {current_mem:.1f} GB",
                percent=min(pct, 85),
            )

            # Process this batch in parallel
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                work_items = [
                    (chunk_id, input_file, start_row, end_row, columns, first_col, schema_overrides, temp_dir)
                    for chunk_id, start_row, end_row in batch_ranges
                ]

                futures = {executor.submit(_process_chunk_worker_safe_to_file, item): item[0] for item in work_items}

                for future in as_completed(futures):
                    chunk_id, chunk_file, stats = future.result()
                    if chunk_file:
                        temp_chunk_files.append((chunk_id, chunk_file, stats))
                        total_initial_rows += stats.get('initial_rows', 0)

            # Aggressive cleanup after batch
            gc.collect()
            batch_mem = memory_monitor.get_current_usage_gb()
            log_func(f"  Batch complete, memory: {batch_mem:.1f}GB")

        # === PHASE: Deduplicate and write to output ===
        # Strategy: use in-memory set until cap, then switch to SQLite.
        # SQLite uses batch temp-table + LEFT JOIN for 10-50x faster dedup.
        dedup_use_sqlite = False
        dedup_conn = None
        testdate_col = "TestDateUTC" if "TestDateUTC" in columns else None

        def _switch_to_sqlite():
            """Migrate in-memory set to SQLite for unbounded dedup."""
            nonlocal dedup_use_sqlite, dedup_conn, global_seen_dates
            if dedup_use_sqlite:
                return
            log_func(f"  Dedup set cap reached ({dedup_cap:,}); switching to SQLite batch mode")
            db_path = os.path.join(temp_dir, "dedup_seen.db")
            dedup_conn = sqlite3.connect(db_path)
            dedup_conn.execute("PRAGMA journal_mode=WAL")
            dedup_conn.execute("PRAGMA synchronous=OFF")
            dedup_conn.execute("PRAGMA cache_size=-64000")  # 64 MB cache
            dedup_conn.execute("PRAGMA temp_store=MEMORY")
            dedup_conn.execute("PRAGMA mmap_size=268435456")  # 256 MB mmap for faster reads
            dedup_conn.execute("CREATE TABLE seen (val TEXT PRIMARY KEY)")
            dedup_conn.executemany(
                "INSERT OR IGNORE INTO seen VALUES (?)",
                ((d,) for d in global_seen_dates),
            )
            dedup_conn.execute(
                "CREATE TEMP TABLE batch (pos INTEGER PRIMARY KEY, val TEXT)"
            )
            dedup_conn.commit()
            global_seen_dates = None  # free RAM
            dedup_use_sqlite = True
            gc.collect()

        def _dedup_batch_sqlite(dates: list) -> list[bool]:
            """
            Batch dedup: load dates into a temp table, LEFT JOIN against seen,
            then INSERT the new ones.  ~10-50x faster than row-by-row.
            """
            n = len(dates)
            # 1. Fill temp table
            dedup_conn.execute("DELETE FROM batch")
            dedup_conn.executemany(
                "INSERT INTO batch VALUES (?,?)",
                ((i, str(d) if d is not None else "") for i, d in enumerate(dates)),
            )
            # 2. Find positions that are NOT yet in 'seen'
            cur = dedup_conn.execute(
                "SELECT b.pos FROM batch b "
                "LEFT JOIN seen s ON b.val = s.val "
                "WHERE s.val IS NULL"
            )
            new_positions = set(row[0] for row in cur)
            # 3. Insert the new values into 'seen'
            dedup_conn.execute(
                "INSERT OR IGNORE INTO seen (val) "
                "SELECT DISTINCT val FROM batch b "
                "WHERE NOT EXISTS (SELECT 1 FROM seen s WHERE s.val = b.val)"
            )
            dedup_conn.commit()
            # 4. Handle duplicates WITHIN this batch (keep first occurrence)
            mask = [False] * n
            seen_in_batch: set = set()
            for i in range(n):
                if i in new_positions:
                    key = str(dates[i]) if dates[i] is not None else ""
                    if key not in seen_in_batch:
                        seen_in_batch.add(key)
                        mask[i] = True
            return mask

        try:
            with open(temp_output, 'w', encoding='utf-8') as f_out:
                f_out.write(','.join(columns) + '\n')
                for chunk_id, chunk_file, stats in sorted(temp_chunk_files):
                    chunk_df = pl.read_csv(
                        chunk_file,
                        has_header=False,
                        new_columns=columns,
                        schema_overrides=schema_overrides,
                        truncate_ragged_lines=True,
                        ignore_errors=True,
                    )

                    if testdate_col:
                        test_dates = chunk_df[testdate_col].to_list()

                        if dedup_use_sqlite:
                            mask = _dedup_batch_sqlite(test_dates)
                        elif len(global_seen_dates) + len(test_dates) > dedup_cap:
                            _switch_to_sqlite()
                            mask = _dedup_batch_sqlite(test_dates)
                        else:
                            # In-memory set (fast for < dedup_cap)
                            mask = []
                            for dv in test_dates:
                                if dv not in global_seen_dates:
                                    global_seen_dates.add(dv)
                                    mask.append(True)
                                else:
                                    mask.append(False)

                        n_new = sum(mask)
                        total_written_rows += n_new
                        total_duplicate_rows += len(mask) - n_new

                        if n_new > 0:
                            filtered_chunk = chunk_df.filter(pl.Series(mask))
                            f_out.write(filtered_chunk.write_csv(include_header=False))
                            del filtered_chunk
                        del test_dates, mask
                    else:
                        f_out.write(chunk_df.write_csv(include_header=False))
                        total_written_rows += len(chunk_df)

                    del chunk_df
                    try:
                        os.remove(chunk_file)
                    except OSError:
                        pass
                    gc.collect()

                if dedup_conn:
                    dedup_conn.close()
                    dedup_conn = None
        finally:
            # Ensure SQLite connection is always closed (e.g. on exception during chunk loop)
            if dedup_conn:
                try:
                    dedup_conn.close()
                except Exception:
                    pass
                dedup_conn = None

        phase1_time = time.time() - phase1_start
        log_func(
            f"Phase 1 complete in {phase1_time:.2f}s: "
            f"{total_written_rows:,} unique rows, "
            f"{total_duplicate_rows:,} duplicates removed"
        )

        # === PHASE 2: Move temp file to final output ===
        log_func(f"PHASE 2: Writing final file: {os.path.basename(output_file)}", percent=90)
        phase2_start = time.time()

        try:
            with FileLock(output_file, timeout=60):
                shutil.move(temp_output, output_file)
        except FileLockTimeout:
            log_func("ERROR: Output file is locked", "ERROR")
            raise RuntimeError(f"Cannot write to {output_file} - file is locked")

        phase2_time = time.time() - phase2_start
        log_func(f"Phase 2 complete in {phase2_time:.2f}s")

    finally:
        # Cleanup temp directory
        try:
            shutil.rmtree(temp_dir)
        except OSError as e:
            logger.warning("Failed to remove temp dir %s: %s", temp_dir, e)

    # === Final summary ===
    end_time = time.time()
    total_time = end_time - start_time
    final_memory = process.memory_info().rss / 1024 / 1024
    peak_memory = memory_monitor.get_current_usage_gb()

    log_func("=" * 70)
    log_func(f"✓ File complete: {os.path.basename(input_file)}", percent=100)
    log_func(f"  Total time: {total_time:.2f}s ({total_time/60:.2f} min)")
    log_func(f"  Phase 1 (Filter+Dedup): {phase1_time:.2f}s ({phase1_time/total_time*100:.1f}%)")
    log_func(f"  Phase 2 (Write): {phase2_time:.2f}s ({phase2_time/total_time*100:.1f}%)")
    log_func(f"  Input rows:  {total_initial_rows:,}")
    log_func(f"  Output rows: {total_written_rows:,}")
    log_func(f"  Removed:     {total_initial_rows - total_written_rows:,} ({(total_initial_rows - total_written_rows)/max(total_initial_rows, 1)*100:.1f}%)")
    log_func(f"  Peak RAM: {peak_memory:.1f} GB | Throughput: {file_size_gb/total_time*60:.2f} GB/min")
    log_func("=" * 70)

    # === Aggressive RAM cleanup ===
    # Force Python GC + tell Windows to release working set pages
    gc.collect()
    try:
        import ctypes
        ctypes.windll.kernel32.SetProcessWorkingSetSize(
            ctypes.windll.kernel32.GetCurrentProcess(),
            ctypes.c_size_t(-1),
            ctypes.c_size_t(-1),
        )
    except Exception:
        pass


def _process_memory_safe_parallel(
    input_file: str,
    output_file: str,
    log_func,
    file_size_gb: float,
    cpu_count: int,
) -> None:
    """Falls back to ultra-fast mode."""
    _process_memory_safe_ultra_fast(input_file, output_file, log_func, file_size_gb, cpu_count)


def _process_memory_safe_standard(
    input_file: str,
    output_file: str,
    log_func,
    file_size_gb: float,
) -> None:
    """Standard mode for small files: single read, filter, dedup, write. Uses fast read when file < 0.4 GB."""
    start_time = time.time()
    process = psutil.Process(os.getpid())
    initial_memory = process.memory_info().rss / 1024 / 1024
    log_func(f"Starting STANDARD mode with {initial_memory:.1f} MB")

    columns = _read_header(input_file)
    if not columns:
        raise RuntimeError("Failed to read header from CSV file.")
    first_col = columns[0]

    log_func(f"Reading file: {os.path.basename(input_file)}", percent=10)
    schema_overrides = {col: pl.Utf8 for col in columns}
    # Use low_memory=False for small files for faster read/write; safe when file <= standard_max_gb
    use_fast_read = file_size_gb < 0.4

    df = pl.read_csv(
        input_file,
        schema_overrides=schema_overrides,
        truncate_ragged_lines=True,
        null_values=["∞", "inf", "-inf"],
        ignore_errors=True,
        low_memory=not use_fast_read,
    )

    log_func(f"  Loaded: {len(df):,} rows", percent=30)
    start_rows = len(df)

    log_func("  Applying filters (required column, TrailingFactor, Lane, Ignore, then dedup)...", percent=40)
    # Apply all filters first so dedup runs on fewer rows (faster)
    df = df.filter(df[first_col].is_not_null() & (df[first_col] != ""))

    if "RawSlope170" in df.columns and "RawSlope270" in df.columns:
        df = df.filter(
            ~((df["RawSlope170"].is_null() | (df["RawSlope170"] == ""))
              & (df["RawSlope270"].is_null() | (df["RawSlope270"] == "")))
        )
    if "TrailingFactor" in df.columns:
        tf = df["TrailingFactor"].cast(pl.Float64, strict=False)
        df = df.filter(tf.is_null() | (tf >= 0.15))
        del tf
    if "tsdSlopeMinY" in df.columns and "tsdSlopeMaxY" in df.columns:
        min_y = df["tsdSlopeMinY"].cast(pl.Float64, strict=False)
        max_y = df["tsdSlopeMaxY"].cast(pl.Float64, strict=False)
        valid_denom = max_y.is_not_null() & (max_y != 0)
        ratio = pl.when(valid_denom).then(min_y.abs() / max_y).otherwise(pl.lit(None))
        df = df.filter(valid_denom & ((ratio.is_null()) | (ratio >= 0.15)))
        del min_y, max_y, ratio, valid_denom
    if "Lane" in df.columns:
        df = df.filter(~df["Lane"].str.contains("SK").fill_null(False))
    if "Ignore" in df.columns:
        df = df.filter(
            (df["Ignore"].str.to_lowercase() != "true")
            & (df["Ignore"].is_not_null())
        )
    # Dedup last so it runs on fewer rows
    if "TestDateUTC" in df.columns:
        df = df.unique(subset=["TestDateUTC"], keep="first", maintain_order=True)

    log_func(f"  After filter: {len(df):,} rows (removed {start_rows - len(df):,})", percent=60)

    log_func(f"  Writing output: {os.path.basename(output_file)}", percent=80)
    try:
        with FileLock(output_file, timeout=60):
            df.write_csv(output_file, include_header=True)
    except FileLockTimeout:
        log_func("ERROR: Output file is locked", "ERROR")
        raise RuntimeError(f"Cannot write to {output_file} - file is locked")

    del df
    gc.collect()

    end_time = time.time()
    log_func(f"File completed in {end_time - start_time:.2f}s", percent=100)

    # Aggressive RAM cleanup – return working-set pages to the OS (Windows)
    try:
        import ctypes
        ctypes.windll.kernel32.SetProcessWorkingSetSize(
            ctypes.windll.kernel32.GetCurrentProcess(),
            ctypes.c_size_t(-1),
            ctypes.c_size_t(-1),
        )
    except Exception:
        pass


# ============================================================================
# PUBLIC helpers (re-usable by GUI / CLI)
# ============================================================================

def read_header(input_file: str) -> list[str]:
    """Public wrapper around the internal header reader."""
    return _read_header(input_file)


# ============================================================================
# FOLDER MODE – merge & clean all CSVs in a directory
# ============================================================================


def merge_then_clean_folder(
    folder_path: str,
    output_file: str,
    progress_callback=None,
    cancel_check=None,
) -> None:
    """
    Fast folder workflow: merge all CSVs with byte copy (no parse), then clean once.
    Typically faster than merge_and_clean_folder when many files (fewer reads, merge is I/O only).

    Steps:
      1. Fast merge: first file full, rest append from line 2 (header from first only).
      2. Single process_data(combined, output_file) for filter + dedup.
    """
    _flog = _get_progress_file_logger()

    def log_func(message, level="INFO", percent=None):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
        line = f"{timestamp} - {level} - {message}"
        if progress_callback:
            try:
                if percent is not None:
                    progress_callback(line, percent)
                else:
                    progress_callback(line)
            except TypeError:
                progress_callback(line)
        _flog.info(message)

    def _cancelled() -> bool:
        return cancel_check() if cancel_check else False

    folder = Path(folder_path).resolve()
    output_path_resolved = Path(output_file).resolve()
    # Exclude output file from merge list (output is often in same folder, would be merged in otherwise)
    csv_files = [f for f in sorted(folder.glob("*.csv")) if f.resolve() != output_path_resolved]
    if not csv_files:
        raise ValueError(
            f"No CSV files to merge in {folder_path}. "
            f"Output file is excluded; ensure folder contains input CSVs only or use another output path."
        )

    total_input_gb = sum(f.stat().st_size for f in csv_files) / (1024 ** 3)
    log_func(f"Found {len(csv_files)} CSV files ({total_input_gb:.2f} GB total)")
    log_func("Using fast merge then clean (merge = byte copy, then single clean pass)")

    # Disk: need space for combined file + output (process_data temp is inside same dir as output)
    needed_gb = total_input_gb * 1.1 + total_input_gb * 0.5  # combined + margin for clean temp
    _check_disk_space(output_file, needed_gb=needed_gb, log_func=log_func)

    temp_dir = _temp_dir_near(output_file, prefix="lmd_merge_fast_")
    combined_path = os.path.join(temp_dir, "combined.csv")

    try:
        log_func("Step 1: Fast merge (byte copy, no parse)...", percent=0)
        with open(combined_path, "wb") as out:
            for i, csv_file in enumerate(csv_files):
                if _cancelled():
                    raise InterruptedError("Processing cancelled by user")
                pct = int((i / len(csv_files)) * 15)  # Reserve 0–15% for merge
                log_func(f"  Merging {i+1}/{len(csv_files)}: {csv_file.name}", percent=pct)
                with open(csv_file, "rb") as inf:
                    if i == 0:
                        out.write(inf.read())
                    else:
                        inf.readline()  # skip header
                        out.write(inf.read())

        combined_gb = os.path.getsize(combined_path) / (1024 ** 3)
        log_func(f"  Merged -> {combined_path} ({combined_gb:.2f} GB)", percent=15)

        log_func("Step 2: Cleaning (filter + dedup, single pass)...", percent=18)
        process_data(combined_path, output_file, progress_callback=_progress_with_offset(progress_callback, 20, 80))
        log_func("Folder processing complete.", percent=100)
    finally:
        try:
            if os.path.isfile(combined_path):
                os.remove(combined_path)
        except OSError as e:
            logger.warning("Could not remove combined temp file %s: %s", combined_path, e)
        try:
            shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception:
            pass


def _progress_with_offset(progress_callback, start_pct: int, end_pct: int):
    """Wrap progress_callback so percent is scaled from start_pct to end_pct."""
    if not progress_callback:
        return None

    def wrapped(msg, percent=None):
        if percent is not None:
            p = start_pct + (end_pct - start_pct) * (percent / 100.0)
            try:
                progress_callback(msg, p)
            except TypeError:
                progress_callback(msg)
        else:
            try:
                progress_callback(msg)
            except TypeError:
                progress_callback(msg)

    return wrapped


def merge_and_clean_folder(
    folder_path: str,
    output_file: str,
    progress_callback=None,
    cancel_check=None,
) -> None:
    """
    High-level entry-point for *folder mode*:
      1. Clean each individual CSV (uses chunked mode for large files).
      2. Merge all cleaned files into *output_file*, deduplicating across
         files using an on-disk SQLite set so RAM stays bounded.

    Parameters
    ----------
    folder_path : str
        Directory containing CSV files.
    output_file : str
        Final merged output path.
    progress_callback : callable, optional
        ``fn(message: str)`` – receives timestamped progress lines.
    cancel_check : callable, optional
        ``fn() -> bool`` – returns True when the caller wants to abort.
    """
    import sqlite3 as _sqlite3

    _flog = _get_progress_file_logger()

    def log_func(message, level="INFO", percent=None):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
        line = f"{timestamp} - {level} - {message}"
        if progress_callback:
            try:
                if percent is not None:
                    progress_callback(line, percent)
                else:
                    progress_callback(line)
            except TypeError:
                progress_callback(line)
        _flog.info(message)

    def _cancelled() -> bool:
        return cancel_check() if cancel_check else False

    # --- memory limits ---
    safe_ram_gb, _mw, dedup_cap, standard_max_gb = _get_memory_limits()
    memory_monitor.set_limit_gb(safe_ram_gb)
    memory_monitor.start()
    log_func(f"Memory monitor started (RAM cap: {safe_ram_gb:.1f} GB)")

    try:
        folder = Path(folder_path)
        csv_files = sorted(folder.glob("*.csv"))
        if not csv_files:
            raise ValueError(f"No CSV files found in {folder_path}")

        total_input_gb = sum(f.stat().st_size for f in csv_files) / (1024 ** 3)
        log_func(f"Found {len(csv_files)} CSV files ({total_input_gb:.1f} GB total)")

        # Disk space: we only need room for the LARGEST single file's temp data
        # + the final output, because we pipeline: clean → merge → delete one at a time.
        largest_file_gb = max(f.stat().st_size for f in csv_files) / (1024 ** 3)
        # Need: ~2x largest file (for chunk temps) + total_input (for final output) + margin
        needed_gb = largest_file_gb * 2.5 + total_input_gb * 0.8
        _check_disk_space(output_file, needed_gb=needed_gb, log_func=log_func)

        # Clean up stale temp dirs from previous crashed runs
        _cleanup_stale_temp_dirs(output_file, "lmd_merge_", max_age_seconds=3600)

        log_func(f"Output: {os.path.abspath(output_file)}")
        # Temp dir on the SAME drive as output to avoid filling system temp
        temp_dir = _temp_dir_near(output_file, prefix="lmd_merge_")
        log_func(f"Temp directory: {temp_dir}")

        try:
            # ── Step 1: Read headers from ORIGINAL files (no cleaning, just first row) ──
            log_func("Step 1: Reading headers from source files...")
            header_groups: dict = {}
            for csv_file in csv_files:
                try:
                    hdr = _read_header(str(csv_file))
                    sig = ",".join(hdr)
                    if sig not in header_groups:
                        header_groups[sig] = {"files": [], "header": hdr}
                    header_groups[sig]["files"].append(csv_file)
                except Exception as e:
                    log_func(f"  Warning: could not read header of {csv_file.name}: {e}", "WARNING")

            if not header_groups:
                raise ValueError("No valid CSV files found")

            most_common_sig = max(header_groups, key=lambda k: len(header_groups[k]["files"]))
            files_to_process = header_groups[most_common_sig]["files"]
            expected_columns = header_groups[most_common_sig]["header"]
            schema_overrides = {col: pl.Utf8 for col in expected_columns}
            has_testdate = "TestDateUTC" in expected_columns

            log_func(
                f"Will process {len(files_to_process)} files  "
                f"({len(expected_columns)} columns, dedup TestDateUTC={has_testdate})"
            )

            # ── Step 2: Pipeline – clean ONE file → merge into output → delete temp ──
            # This keeps disk usage bounded: only 1 cleaned temp file at a time.
            log_func("Step 2: Clean each file then merge into output...", percent=0)

            dedup_db_path = os.path.join(temp_dir, "dedup_cross.db")
            dedup_conn = None
            if has_testdate:
                dedup_conn = _sqlite3.connect(dedup_db_path)
                dedup_conn.execute("PRAGMA journal_mode=WAL")
                dedup_conn.execute("PRAGMA synchronous=OFF")
                dedup_conn.execute("PRAGMA cache_size=-64000")  # 64 MB cache
                dedup_conn.execute("PRAGMA temp_store=MEMORY")
                dedup_conn.execute("PRAGMA mmap_size=268435456")  # 256 MB mmap
                dedup_conn.execute(
                    "CREATE TABLE IF NOT EXISTS seen (val TEXT PRIMARY KEY)"
                )
                dedup_conn.execute(
                    "CREATE TEMP TABLE batch (pos INTEGER PRIMARY KEY, val TEXT)"
                )
                dedup_conn.commit()

            def _cross_dedup_batch(dates: list) -> list[bool]:
                """Batch cross-file dedup via temp table + LEFT JOIN (fast)."""
                n = len(dates)
                dedup_conn.execute("DELETE FROM batch")
                dedup_conn.executemany(
                    "INSERT INTO batch VALUES (?,?)",
                    ((i, str(d) if d is not None else "") for i, d in enumerate(dates)),
                )
                cur = dedup_conn.execute(
                    "SELECT b.pos FROM batch b "
                    "LEFT JOIN seen s ON b.val = s.val "
                    "WHERE s.val IS NULL"
                )
                new_positions = set(row[0] for row in cur)
                dedup_conn.execute(
                    "INSERT OR IGNORE INTO seen (val) "
                    "SELECT DISTINCT val FROM batch b "
                    "WHERE NOT EXISTS (SELECT 1 FROM seen s WHERE s.val = b.val)"
                )
                dedup_conn.commit()
                # Keep first occurrence within batch
                mask = [False] * n
                seen_in_batch: set = set()
                for i in range(n):
                    if i in new_positions:
                        key = str(dates[i]) if dates[i] is not None else ""
                        if key not in seen_in_batch:
                            seen_in_batch.add(key)
                            mask[i] = True
                return mask

            temp_merged = os.path.join(temp_dir, "merged_output.csv")
            total_written = 0
            total_dupes = 0

            with open(temp_merged, "w", encoding="utf-8", newline="") as f_out:
                f_out.write(",".join(expected_columns) + "\n")

                for i, csv_file in enumerate(files_to_process, 1):
                    if _cancelled():
                        raise InterruptedError("Processing cancelled by user")

                    file_size_gb = csv_file.stat().st_size / (1024 ** 3)
                    mem_gb = memory_monitor.get_current_usage_gb()
                    disk_free_gb = shutil.disk_usage(temp_dir).free / (1024 ** 3)
                    n_files = len(files_to_process)
                    pct_start = int((i - 1) / n_files * 100) if n_files else 0
                    log_func(
                        f"[{i}/{len(files_to_process)}] File: {csv_file.name} "
                        f"({file_size_gb:.2f} GB)  [RAM: {mem_gb:.1f} GB | Disk: {disk_free_gb:.1f} GB free]",
                        percent=pct_start,
                    )
                    log_func(f"  Cleaning file {csv_file.name}...")

                    # --- 2a. Clean this single file into a temp file ---
                    cleaned_file = os.path.join(temp_dir, f"cleaned_{i}.csv")
                    clean_start = time.time()
                    try:
                        # Only forward messages, not percent (folder progress is per-file)
                        def _folder_file_log(msg, _percent=None):
                            try:
                                progress_callback(msg)
                            except TypeError:
                                progress_callback(msg)
                        process_data(str(csv_file), cleaned_file,
                                     progress_callback=_folder_file_log)
                    except Exception as e:
                        log_func(f"  Error cleaning {csv_file.name}: {e} – skipping file", "ERROR")
                        try:
                            os.remove(cleaned_file)
                        except OSError:
                            pass
                        continue

                    clean_elapsed = time.time() - clean_start
                    try:
                        rows_cleaned = pl.scan_csv(
                            cleaned_file, ignore_errors=True,
                            truncate_ragged_lines=True,
                        ).select(pl.len()).collect().item()
                        log_func(f"  Clean done: {rows_cleaned:,} rows (time: {clean_elapsed:.1f}s)")
                    except Exception:
                        log_func(f"  Clean done (time: {clean_elapsed:.1f}s)")

                    log_func(f"  Merging file {csv_file.name} into output...")
                    # --- 2b. Merge cleaned file into output ---
                    # Single-pass: one read of the file. No pre-scan (CSV is row-based,
                    # so "scan one column" still parses the whole file = 250s on 9M rows).
                    merge_start = time.time()
                    MERGE_CHUNK_ROWS = 2_000_000

                    if not has_testdate or dedup_conn is None:
                        # No dedup – raw file append (fast)
                        try:
                            with open(cleaned_file, 'r', encoding='utf-8') as f_in:
                                f_in.readline()  # skip header
                                while True:
                                    block = f_in.read(8 * 1024 * 1024)
                                    if not block:
                                        break
                                    f_out.write(block)
                            row_count = pl.scan_csv(
                                cleaned_file, ignore_errors=True,
                                truncate_ragged_lines=True,
                            ).select(pl.len()).collect().item()
                            total_written += row_count
                        except Exception as e:
                            log_func(f"  ERROR appending cleaned file: {e}", "ERROR")
                    else:
                        # Dedup: single-pass batched read → dedup per batch → write
                        # Optimized: if batch is all unique, write without filtering (fast-path)
                        try:
                            reader = pl.read_csv_batched(
                                cleaned_file,
                                schema_overrides=schema_overrides,
                                truncate_ragged_lines=True,
                                ignore_errors=True,
                                batch_size=MERGE_CHUNK_ROWS,
                            )
                            while True:
                                batches = reader.next_batches(1)
                                if not batches:
                                    break
                                chunk_df = batches[0]
                                dates = chunk_df["TestDateUTC"].to_list()
                                mask = _cross_dedup_batch(dates)
                                n_new = sum(mask)
                                total_dupes += len(mask) - n_new
                                del dates

                                if n_new == 0:
                                    pass  # skip batch (all dupes)
                                elif n_new == len(mask):
                                    # Fast-path: all unique, write whole batch without filter
                                    f_out.write(chunk_df.write_csv(include_header=False))
                                    total_written += n_new
                                else:
                                    # Some dupes: filter and write
                                    filtered = chunk_df.filter(pl.Series(mask))
                                    f_out.write(filtered.write_csv(include_header=False))
                                    total_written += len(filtered)
                                    del filtered
                                del chunk_df, mask
                            del reader
                        except StopIteration:
                            pass
                        except Exception as e:
                            log_func(f"  ERROR during merge: {e}", "ERROR")

                    merge_time = time.time() - merge_start
                    pct_done = int(i / n_files * 100) if n_files else 100
                    log_func(
                        f"  Merge done {csv_file.name}: "
                        f"total {total_written:,} rows, {total_dupes:,} dupes removed "
                        f"(merge: {merge_time:.1f}s)",
                        percent=pct_done,
                    )

                    # --- 2c. Aggressive RAM cleanup ---
                    gc.collect()
                    try:
                        import ctypes
                        ctypes.windll.kernel32.SetProcessWorkingSetSize(
                            ctypes.windll.kernel32.GetCurrentProcess(),
                            ctypes.c_size_t(-1),
                            ctypes.c_size_t(-1),
                        )
                    except Exception:
                        pass

                    # --- 2d. DELETE cleaned temp file IMMEDIATELY to free disk ---
                    try:
                        os.remove(cleaned_file)
                    except OSError:
                        pass

                    mem_after = memory_monitor.get_current_usage_gb()
                    log_func(f"  RAM after cleanup: {mem_after:.1f} GB")

            # Cleanup dedup DB
            if dedup_conn is not None:
                dedup_conn.close()
                dedup_conn = None
                try:
                    os.remove(dedup_db_path)
                except OSError:
                    pass

            # Move temp to final output
            try:
                with FileLock(output_file, timeout=60):
                    shutil.move(temp_merged, output_file)
            except FileLockTimeout:
                raise RuntimeError(f"Cannot write to {output_file} – file is locked")

            log_func(
                f"Merge complete: {total_written:,} rows, "
                f"{total_dupes:,} cross-file duplicates removed",
                percent=100,
            )

        finally:
            # Ensure SQLite connection is always closed (e.g. on exception in file loop)
            if dedup_conn is not None:
                try:
                    dedup_conn.close()
                except Exception:
                    pass
                dedup_conn = None
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception:
                pass

    finally:
        memory_monitor.stop()
        log_func("Memory monitor stopped")