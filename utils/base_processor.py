"""
Base Processor Module
Provides common functionality for all data processors
"""

import os
import logging
from abc import ABC, abstractmethod
from typing import Optional, Callable, Tuple
import polars as pl

logger = logging.getLogger(__name__)


class BaseProcessor(ABC):
    """
    Abstract base class for all data processors.
    
    Provides common functionality:
    - Progress reporting
    - File validation
    - Duplicate removal
    - Error handling patterns
    
    All processor classes should inherit from this base class
    to ensure consistent behavior and reduce code duplication.
    """
    
    def __init__(self, progress_callback: Optional[Callable] = None):
        """
        Initialize base processor.
        
        Args:
            progress_callback: Optional callback function for progress updates.
                              Should accept (message: str, progress: float = None)
        """
        self.progress_callback = progress_callback
        self.logger = logger
    
    # ═══════════════════════════════════════════════════════════════
    # PROGRESS REPORTING
    # ═══════════════════════════════════════════════════════════════
    
    def _emit_progress(self, message: str, progress: Optional[float] = None):
        """
        Emit progress update to callback and logger.
        
        Args:
            message: Progress message
            progress: Optional progress percentage (0.0 to 1.0)
        """
        if self.progress_callback:
            if progress is not None:
                self.progress_callback(message, progress)
            else:
                self.progress_callback(message)
        
        self.logger.info(message)
    
    # ═══════════════════════════════════════════════════════════════
    # FILE VALIDATION
    # ═══════════════════════════════════════════════════════════════
    
    def _validate_file_exists(self, file_path: str, file_description: str = "File") -> bool:
        """
        Validate that a file exists.
        
        Args:
            file_path: Path to file to check
            file_description: Description for error messages (e.g., "Input file", "LMD file")
            
        Returns:
            True if file exists, False otherwise
        """
        if not os.path.exists(file_path):
            error_msg = f"{file_description} not found: {file_path}"
            self.logger.error(error_msg)
            self._emit_progress(f"❌ ERROR: {error_msg}")
            return False
        
        self.logger.debug(f"{file_description} validated: {file_path}")
        return True
    
    def _validate_file_readable(self, file_path: str, file_description: str = "File") -> bool:
        """
        Validate that a file is readable.
        
        Args:
            file_path: Path to file to check
            file_description: Description for error messages
            
        Returns:
            True if file is readable, False otherwise
        """
        if not self._validate_file_exists(file_path, file_description):
            return False
        
        if not os.access(file_path, os.R_OK):
            error_msg = f"{file_description} is not readable: {file_path}"
            self.logger.error(error_msg)
            self._emit_progress(f"❌ ERROR: {error_msg}")
            return False
        
        return True
    
    def _get_file_size_gb(self, file_path: str) -> float:
        """
        Get file size in gigabytes.
        
        Args:
            file_path: Path to file
            
        Returns:
            File size in GB
        """
        try:
            size_bytes = os.path.getsize(file_path)
            size_gb = size_bytes / (1024 ** 3)
            return size_gb
        except Exception as e:
            self.logger.warning(f"Could not get file size: {e}")
            return 0.0
    
    # ═══════════════════════════════════════════════════════════════
    # DATAFRAME OPERATIONS
    # ═══════════════════════════════════════════════════════════════
    
    def _remove_duplicate_testdateutc(
        self, 
        df: pl.DataFrame,
        column_name: str = 'TestDateUTC'
    ) -> Tuple[pl.DataFrame, int]:
        """
        Remove duplicate rows based on TestDateUTC column.
        
        Args:
            df: DataFrame to process
            column_name: Name of column to check for duplicates (default: 'TestDateUTC')
            
        Returns:
            Tuple of (processed DataFrame, number of duplicates removed)
        """
        if column_name not in df.columns:
            self.logger.debug(f"Column '{column_name}' not found, skipping duplicate removal")
            return df, 0
        
        before_count = len(df)
        df_unique = df.unique(
            subset=[column_name], 
            keep='first', 
            maintain_order=True
        )
        removed_count = before_count - len(df_unique)
        
        if removed_count > 0:
            self.logger.info(f"Removed {removed_count:,} duplicate {column_name} rows")
        
        return df_unique, removed_count
    
    def _validate_columns_exist(
        self, 
        df: pl.DataFrame, 
        required_columns: list[str],
        file_description: str = "DataFrame"
    ) -> bool:
        """
        Validate that required columns exist in DataFrame.
        
        Args:
            df: DataFrame to check
            required_columns: List of required column names
            file_description: Description for error messages
            
        Returns:
            True if all columns exist, False otherwise
        """
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            error_msg = (
                f"{file_description} is missing required columns: "
                f"{', '.join(missing_columns)}"
            )
            self.logger.error(error_msg)
            self._emit_progress(f"❌ ERROR: {error_msg}")
            
            # Log available columns for debugging
            self.logger.debug(f"Available columns: {', '.join(df.columns)}")
            
            return False
        
        self.logger.debug(f"All required columns present in {file_description}")
        return True
    
    def _log_dataframe_info(self, df: pl.DataFrame, name: str = "DataFrame"):
        """
        Log information about a DataFrame.
        
        Args:
            df: DataFrame to inspect
            name: Name/description of the DataFrame
        """
        self.logger.info(
            f"{name}: {len(df):,} rows, {len(df.columns)} columns"
        )
        self.logger.debug(
            f"{name} columns: {', '.join(df.columns[:10])}"
            f"{'...' if len(df.columns) > 10 else ''}"
        )
    
    # ═══════════════════════════════════════════════════════════════
    # STANDARDIZATION METHODS
    # ═══════════════════════════════════════════════════════════════
    
    def _standardize_boolean_columns(
        self, 
        df: pl.DataFrame, 
        boolean_columns: list[str]
    ) -> pl.DataFrame:
        """
        Standardize boolean columns to True/False values.
        
        Converts various representations:
        - '1', 'True', 'true', 'T', 't' -> True
        - '0', 'False', 'false', 'F', 'f', '' -> False
        
        Args:
            df: DataFrame to process
            boolean_columns: List of column names to standardize
            
        Returns:
            DataFrame with standardized boolean columns
        """
        for col in boolean_columns:
            if col not in df.columns:
                continue
            
            df = df.with_columns([
                pl.when(
                    pl.col(col).cast(pl.Utf8).str.strip_chars().is_in(
                        ['1', 'True', 'true', 'TRUE', 'T', 't']
                    )
                )
                .then(True)
                .when(
                    pl.col(col).cast(pl.Utf8).str.strip_chars().is_in(
                        ['0', 'False', 'false', 'FALSE', 'F', 'f', '']
                    )
                )
                .then(False)
                .otherwise(None)
                .alias(col)
            ])
        
        self.logger.debug(f"Standardized {len(boolean_columns)} boolean columns")
        return df
    
    # ═══════════════════════════════════════════════════════════════
    # ERROR HANDLING
    # ═══════════════════════════════════════════════════════════════
    
    def _handle_error(self, error: Exception, context: str = "Processing"):
        """
        Handle errors with consistent logging and user feedback.
        
        Args:
            error: Exception that occurred
            context: Context description for error message
        """
        from utils.security import UserFriendlyError
        
        error_msg = UserFriendlyError.format_error(error, context=context)
        
        self.logger.error(f"{context} failed: {error}", exc_info=True)
        self._emit_progress(f"❌ ERROR: {context} failed")
        
        return error_msg
    
    # ═══════════════════════════════════════════════════════════════
    # ABSTRACT METHODS
    # ═══════════════════════════════════════════════════════════════
    
    @abstractmethod
    def process(self, *args, **kwargs):
        """
        Process data - must be implemented by subclasses.
        
        This is the main entry point for the processor.
        Subclasses should implement their specific processing logic here.
        
        Returns:
            Processing result (type varies by processor)
        """
        raise NotImplementedError("Subclasses must implement process() method")
    
    # ═══════════════════════════════════════════════════════════════
    # UTILITY METHODS
    # ═══════════════════════════════════════════════════════════════
    
    def _format_number(self, number: int) -> str:
        """Format number with thousand separators"""
        return f"{number:,}"
    
    def _format_percentage(self, value: float, decimals: int = 2) -> str:
        """Format value as percentage"""
        return f"{value * 100:.{decimals}f}%"
    
    def _format_file_size(self, size_bytes: int) -> str:
        """Format file size in human-readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} PB"


class CancellableProcessor(BaseProcessor):
    """
    Base processor with cancellation support.
    
    Extends BaseProcessor with ability to cancel long-running operations.
    """
    
    def __init__(self, progress_callback: Optional[Callable] = None):
        """Initialize cancellable processor"""
        super().__init__(progress_callback)
        self._is_cancelled = False
    
    def cancel(self):
        """Request cancellation of current operation"""
        self._is_cancelled = True
        self.logger.info("Cancellation requested")
        self._emit_progress("⚠️ Cancellation requested...")
    
    def is_cancelled(self) -> bool:
        """Check if cancellation has been requested"""
        return self._is_cancelled
    
    def reset_cancellation(self):
        """Reset cancellation flag"""
        self._is_cancelled = False
    
    def _check_cancellation(self):
        """
        Check for cancellation and raise exception if requested.
        
        Call this method periodically in long-running operations.
        
        Raises:
            ProcessingCancelled: If cancellation was requested
        """
        if self._is_cancelled:
            raise ProcessingCancelled("Processing cancelled by user")


class ProcessingCancelled(Exception):
    """Exception raised when processing is cancelled by user"""
    pass
