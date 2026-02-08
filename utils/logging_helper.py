"""
Logging Helper Module
Provides utilities for proper logging level usage across the application.
"""

import logging
import time
from functools import wraps
from typing import Any, Optional


class LoggingHelper:
    """Helper class for consistent logging across the application"""
    
    @staticmethod
    def log_function_call(logger: logging.Logger):
        """
        Decorator to log function entry and exit.
        
        Args:
            logger: Logger instance to use
        
        Example:
            @LoggingHelper.log_function_call(logger)
            def process_data(file_path):
                ...
        """
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                func_name = func.__name__
                logger.debug(f"Entering {func_name}() with args={args}, kwargs={kwargs}")
                
                start_time = time.time()
                try:
                    result = func(*args, **kwargs)
                    elapsed = time.time() - start_time
                    logger.debug(f"Exiting {func_name}() successfully in {elapsed:.2f}s")
                    return result
                except Exception as e:
                    elapsed = time.time() - start_time
                    logger.error(f"Exception in {func_name}() after {elapsed:.2f}s: {e}", exc_info=True)
                    raise
            
            return wrapper
        return decorator
    
    @staticmethod
    def log_performance(logger: logging.Logger, operation: str):
        """
        Context manager to log operation performance.
        
        Args:
            logger: Logger instance
            operation: Description of operation
        
        Example:
            with LoggingHelper.log_performance(logger, "Processing large file"):
                process_data(file_path)
        """
        class PerformanceLogger:
            def __init__(self, logger, operation):
                self.logger = logger
                self.operation = operation
                self.start_time = None
            
            def __enter__(self):
                self.start_time = time.time()
                self.logger.info(f"Starting: {self.operation}")
                return self
            
            def __exit__(self, exc_type, exc_val, exc_tb):
                elapsed = time.time() - self.start_time
                
                if exc_type is None:
                    self.logger.info(f"Completed: {self.operation} in {elapsed:.2f}s")
                else:
                    self.logger.error(
                        f"Failed: {self.operation} after {elapsed:.2f}s - {exc_type.__name__}: {exc_val}",
                        exc_info=True
                    )
        
        return PerformanceLogger(logger, operation)
    
    @staticmethod
    def log_data_info(logger: logging.Logger, data_description: str, row_count: int, 
                      column_count: Optional[int] = None, memory_mb: Optional[float] = None):
        """
        Log data processing information.
        
        Args:
            logger: Logger instance
            data_description: Description of the data
            row_count: Number of rows
            column_count: Number of columns (optional)
            memory_mb: Memory usage in MB (optional)
        """
        parts = [f"{data_description}: {row_count:,} rows"]
        
        if column_count:
            parts.append(f"{column_count} columns")
        
        if memory_mb:
            parts.append(f"~{memory_mb:.1f} MB")
        
        logger.info(", ".join(parts))
    
    @staticmethod
    def log_warning_with_threshold(logger: logging.Logger, message: str, 
                                   current_value: float, threshold: float,
                                   unit: str = ""):
        """
        Log warning when value exceeds threshold.
        
        Args:
            logger: Logger instance
            message: Warning message
            current_value: Current value
            threshold: Threshold value
            unit: Unit of measurement
        """
        if current_value > threshold:
            percentage = (current_value / threshold) * 100
            logger.warning(
                f"{message}: {current_value:.1f}{unit} "
                f"exceeds threshold {threshold:.1f}{unit} ({percentage:.0f}%)"
            )
        else:
            logger.debug(
                f"{message}: {current_value:.1f}{unit} "
                f"within threshold {threshold:.1f}{unit}"
            )
    
    @staticmethod
    def log_progress_milestone(logger: logging.Logger, current: int, total: int, 
                               operation: str, interval: int = 10):
        """
        Log progress at specific intervals.
        
        Args:
            logger: Logger instance
            current: Current progress
            total: Total items
            operation: Operation description
            interval: Log every N percent (default 10%)
        """
        if total == 0:
            return
        
        percentage = (current / total) * 100
        
        # Log at intervals (0%, 10%, 20%, ..., 100%)
        if percentage % interval == 0 or current == total:
            logger.info(f"{operation}: {current:,}/{total:,} ({percentage:.0f}%)")
    
    @staticmethod
    def log_memory_usage(logger: logging.Logger, context: str = ""):
        """
        Log current memory usage.
        
        Args:
            logger: Logger instance
            context: Context description
        """
        try:
            import os

            import psutil
            
            process = psutil.Process(os.getpid())
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / 1024 / 1024
            
            prefix = f"{context}: " if context else ""
            logger.debug(f"{prefix}Memory usage: {memory_mb:.1f} MB")
            
        except ImportError:
            logger.debug("psutil not available, cannot log memory usage")
        except Exception as e:
            logger.debug(f"Failed to get memory usage: {e}")
    
    @staticmethod
    def get_logger_with_context(name: str, context: dict = None) -> logging.Logger:
        """
        Get logger with context information.
        
        Args:
            name: Logger name
            context: Context dictionary to add to log messages
        
        Returns:
            Logger instance
        """
        logger = logging.getLogger(name)
        
        if context:
            # Add context as logger attribute for use in formatters
            logger.context = context
        
        return logger


class LogLevel:
    """Constants for logging levels with usage guidelines"""
    
    # DEBUG: Detailed diagnostic information for troubleshooting
    # Use for: Variable values, function parameters, loop iterations
    DEBUG = logging.DEBUG
    
    # INFO: General informational messages about program flow
    # Use for: Operation start/completion, file processing, user actions
    INFO = logging.INFO
    
    # WARNING: Something unexpected but recoverable happened
    # Use for: Missing optional data, values near limits, deprecated features
    WARNING = logging.WARNING
    
    # ERROR: Error occurred but application can continue
    # Use for: Failed operations, validation errors, recoverable exceptions
    ERROR = logging.ERROR
    
    # CRITICAL: Serious error that may cause program to abort
    # Use for: Unrecoverable errors, system failures, data corruption
    CRITICAL = logging.CRITICAL
    
    @staticmethod
    def get_level_name(level: int) -> str:
        """Get level name from level number"""
        return logging.getLevelName(level)


# Example usage functions
def example_logging_usage():
    """Example of proper logging level usage"""
    logger = logging.getLogger(__name__)
    
    # DEBUG: Detailed diagnostic information
    logger.debug("Processing chunk 5/10, rows 50000-60000")
    logger.debug(f"Memory usage: 1024.5 MB")
    logger.debug(f"Configuration loaded: {{'chunk_size': 10000}}")
    
    # INFO: General operational messages
    logger.info("Starting data processing for file: data.csv")
    logger.info("Completed successfully: output.csv (100,000 rows)")
    logger.info("User selected 5 columns for export")
    
    # WARNING: Unexpected but not critical
    logger.warning("File size 15GB exceeds recommended limit 10GB, processing may be slow")
    logger.warning("Column 'OptionalField' not found, using default value")
    logger.warning("Memory usage at 85%, approaching 90% threshold")
    
    # ERROR: Error occurred but can continue
    logger.error("Failed to create backup file: Permission denied")
    logger.error("Validation failed for row 1234: Invalid date format", exc_info=True)
    logger.error("Unable to write to cache, continuing without cache")
    
    # CRITICAL: Severe error, may need to abort
    logger.critical("Out of memory, aborting processing")
    logger.critical("Corrupted data detected in input file, cannot continue")
    logger.critical("Database connection lost and cannot reconnect")


if __name__ == '__main__':
    # Setup logging for testing
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run examples
    example_logging_usage()
