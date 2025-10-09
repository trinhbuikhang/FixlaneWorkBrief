"""
Utility functions and helpers for the Fixlane WorkBrief application.
"""

import os
import sys
import logging
from pathlib import Path
from typing import Optional, Union


logger = logging.getLogger(__name__)


def sanitize_path(path: Union[str, Path]) -> str:
    """
    Sanitize file path by removing quotes and extra whitespace.
    
    Args:
        path: File path to sanitize
        
    Returns:
        Cleaned path string
    """
    if isinstance(path, Path):
        path = str(path)
    
    return path.strip().strip('"').strip("'")


def ensure_directory_exists(directory: Union[str, Path]) -> bool:
    """
    Ensure a directory exists, create if necessary.
    
    Args:
        directory: Directory path to check/create
        
    Returns:
        True if directory exists or was created successfully
    """
    try:
        Path(directory).mkdir(parents=True, exist_ok=True)
        return True
    except Exception as e:
        logger.error(f"Failed to create directory {directory}: {e}")
        return False


def get_file_size_mb(file_path: Union[str, Path]) -> float:
    """
    Get file size in megabytes.
    
    Args:
        file_path: Path to file
        
    Returns:
        File size in MB, or 0 if file doesn't exist
    """
    try:
        size_bytes = Path(file_path).stat().st_size
        return size_bytes / (1024 * 1024)
    except Exception:
        return 0.0


def validate_csv_file(file_path: Union[str, Path]) -> bool:
    """
    Basic validation that file exists and appears to be CSV.
    
    Args:
        file_path: Path to CSV file
        
    Returns:
        True if file appears valid
    """
    path = Path(file_path)
    
    # Check if file exists
    if not path.exists():
        logger.error(f"File does not exist: {file_path}")
        return False
    
    # Check if it's a file (not directory)
    if not path.is_file():
        logger.error(f"Path is not a file: {file_path}")
        return False
    
    # Check file extension
    if path.suffix.lower() not in ['.csv', '.txt']:
        logger.warning(f"File may not be CSV format: {file_path}")
        # Don't return False, as some CSV files might have different extensions
    
    # Check if file is readable
    try:
        with open(path, 'r', encoding='utf-8') as f:
            f.read(1)  # Try to read first character
        return True
    except Exception as e:
        logger.error(f"Cannot read file {file_path}: {e}")
        return False


def format_file_size(size_bytes: int) -> str:
    """
    Format file size in human-readable format.
    
    Args:
        size_bytes: Size in bytes
        
    Returns:
        Formatted size string
    """
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"


def get_available_memory_mb() -> Optional[float]:
    """
    Get available system memory in MB.
    
    Returns:
        Available memory in MB, or None if cannot determine
    """
    try:
        import psutil
        memory = psutil.virtual_memory()
        return memory.available / (1024 * 1024)
    except ImportError:
        logger.warning("psutil not available, cannot check memory usage")
        return None
    except Exception as e:
        logger.error(f"Error checking memory usage: {e}")
        return None


def estimate_processing_time(file_size_mb: float, records_count: int) -> str:
    """
    Estimate processing time based on file size and record count.
    
    Args:
        file_size_mb: File size in megabytes
        records_count: Number of records to process
        
    Returns:
        Estimated time string
    """
    # Very rough estimates based on typical processing speeds
    # These would need to be calibrated based on actual performance
    
    if records_count < 1000:
        return "< 1 minute"
    elif records_count < 10000:
        return "1-5 minutes"
    elif records_count < 100000:
        return "5-15 minutes"
    elif records_count < 1000000:
        return "15-60 minutes"
    else:
        return "> 1 hour"


def backup_file(file_path: Union[str, Path], backup_suffix: str = "_backup") -> Optional[str]:
    """
    Create a backup copy of a file.
    
    Args:
        file_path: Original file path
        backup_suffix: Suffix to add to backup file name
        
    Returns:
        Backup file path if successful, None otherwise
    """
    try:
        original_path = Path(file_path)
        backup_path = original_path.parent / f"{original_path.stem}{backup_suffix}{original_path.suffix}"
        
        import shutil
        shutil.copy2(original_path, backup_path)
        
        logger.info(f"Created backup: {backup_path}")
        return str(backup_path)
        
    except Exception as e:
        logger.error(f"Failed to create backup for {file_path}: {e}")
        return None


def clean_temp_files(directory: Union[str, Path], pattern: str = "*.tmp") -> int:
    """
    Clean temporary files from a directory.
    
    Args:
        directory: Directory to clean
        pattern: File pattern to match (default: *.tmp)
        
    Returns:
        Number of files cleaned
    """
    try:
        import glob
        
        search_pattern = str(Path(directory) / pattern)
        temp_files = glob.glob(search_pattern)
        
        cleaned = 0
        for temp_file in temp_files:
            try:
                os.remove(temp_file)
                cleaned += 1
                logger.debug(f"Removed temp file: {temp_file}")
            except Exception as e:
                logger.warning(f"Could not remove temp file {temp_file}: {e}")
        
        if cleaned > 0:
            logger.info(f"Cleaned {cleaned} temporary files")
        
        return cleaned
        
    except Exception as e:
        logger.error(f"Error cleaning temp files: {e}")
        return 0


class ProgressTracker:
    """Simple progress tracking utility."""
    
    def __init__(self, total_items: int, callback=None):
        self.total_items = total_items
        self.current_item = 0
        self.callback = callback
        self.last_reported_percent = -1
    
    def update(self, increment: int = 1) -> float:
        """
        Update progress and return percentage complete.
        
        Args:
            increment: Number of items to increment
            
        Returns:
            Percentage complete (0-100)
        """
        self.current_item = min(self.current_item + increment, self.total_items)
        percentage = (self.current_item / self.total_items) * 100 if self.total_items > 0 else 0
        
        # Only call callback if percentage changed significantly
        if self.callback and int(percentage) != self.last_reported_percent:
            self.callback(f"Processing: {self.current_item}/{self.total_items}", percentage)
            self.last_reported_percent = int(percentage)
        
        return percentage
    
    def finish(self):
        """Mark progress as complete."""
        self.current_item = self.total_items
        if self.callback:
            self.callback("Processing complete", 100.0)


def setup_error_handling():
    """Setup global error handling for uncaught exceptions."""
    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        
        logger.critical("Uncaught exception:", exc_info=(exc_type, exc_value, exc_traceback))
    
    sys.excepthook = handle_exception


# Module initialization
setup_error_handling()