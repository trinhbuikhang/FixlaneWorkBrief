"""
Cross-Platform File Locking Module
Provides file locking mechanism to prevent concurrent writes
"""

import os
import platform
import time
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class FileLock:
    """
    Cross-platform file locking context manager.
    Prevents multiple processes from writing to the same file simultaneously.
    
    Usage:
        try:
            with FileLock(output_file, timeout=30):
                df.write_csv(output_file, include_header=True)
        except FileLockTimeout:
            print("File is locked by another process")
    """
    
    def __init__(self, file_path: str, timeout: float = 30.0):
        """
        Initialize file lock.
        
        Args:
            file_path: Path to the file to lock
            timeout: Maximum time to wait for lock (seconds)
        """
        self.file_path = Path(file_path).resolve()
        self.lock_file = Path(str(self.file_path) + ".lock")
        self.timeout = timeout
        self.handle: Optional[object] = None
        self._lock_acquired = False
    
    def __enter__(self):
        """Acquire the lock"""
        start_time = time.time()
        
        while True:
            try:
                # Try to create lock file exclusively
                # On Windows and Unix, 'x' mode fails if file exists
                self.handle = open(self.lock_file, 'x')
                self._lock_acquired = True
                
                # Write process info to lock file
                pid = os.getpid()
                self.handle.write(f"PID: {pid}\nTime: {time.ctime()}\n")
                self.handle.flush()
                
                logger.debug(f"Lock acquired for {self.file_path}")
                return self
                
            except FileExistsError:
                # Lock file exists - another process has the lock
                elapsed = time.time() - start_time
                
                if elapsed >= self.timeout:
                    # Check if lock is stale (older than timeout)
                    if self._is_stale_lock():
                        logger.warning(f"Removing stale lock file: {self.lock_file}")
                        try:
                            os.remove(self.lock_file)
                            continue  # Try again
                        except:
                            pass
                    
                    raise FileLockTimeout(
                        f"Could not acquire lock for {self.file_path} "
                        f"after {self.timeout:.1f} seconds"
                    )
                
                # Wait a bit before retrying
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Unexpected error acquiring lock: {e}")
                raise
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Release the lock"""
        if self.handle and self._lock_acquired:
            try:
                self.handle.close()
            except:
                pass
            
            try:
                if self.lock_file.exists():
                    os.remove(self.lock_file)
                    logger.debug(f"Lock released for {self.file_path}")
            except Exception as e:
                logger.warning(f"Failed to remove lock file: {e}")
            
            self._lock_acquired = False
    
    def _is_stale_lock(self) -> bool:
        """
        Check if lock file is stale (older than timeout period).
        A lock is considered stale if it's older than 2x the timeout.
        """
        try:
            if not self.lock_file.exists():
                return False
            
            lock_age = time.time() - self.lock_file.stat().st_mtime
            stale_threshold = self.timeout * 2
            
            return lock_age > stale_threshold
            
        except Exception as e:
            logger.warning(f"Error checking lock staleness: {e}")
            return False


class FileLockTimeout(Exception):
    """Raised when file lock cannot be acquired within timeout period"""
    pass


def safe_write_csv(df, output_file: str, timeout: float = 30.0, **write_options):
    """
    Safely write DataFrame to CSV with file locking.
    
    Args:
        df: Polars DataFrame to write
        output_file: Path to output file
        timeout: Lock timeout in seconds
        **write_options: Additional options passed to write_csv()
    
    Raises:
        FileLockTimeout: If lock cannot be acquired
    
    Example:
        safe_write_csv(df, "output.csv", include_header=True, null_value="")
    """
    try:
        with FileLock(output_file, timeout=timeout):
            df.write_csv(output_file, **write_options)
        logger.info(f"Successfully wrote to {output_file}")
        
    except FileLockTimeout as e:
        logger.error(str(e))
        raise RuntimeError(
            f"Output file {output_file} is locked by another process. "
            "Please wait for other operations to complete."
        ) from e


def is_file_locked(file_path: str) -> bool:
    """
    Check if a file is currently locked.
    
    Args:
        file_path: Path to check
        
    Returns:
        True if file is locked, False otherwise
    """
    lock_file = Path(str(file_path) + ".lock")
    return lock_file.exists()
