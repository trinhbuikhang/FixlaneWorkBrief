"""
Enhanced Logging with Context and Correlation IDs
Provides contextual logging for better debugging and tracking
"""

import json
import logging
import traceback
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import psutil


class ContextLogger:
    """
    Enhanced logger with context tracking.
    
    Features:
    - Correlation IDs for tracking operations across logs
    - Automatic context enrichment (memory, CPU, disk usage)
    - Structured logging support
    - Error tracking with unique IDs
    """
    
    def __init__(self, logger: logging.Logger, operation: str = "unknown"):
        """
        Initialize context logger.
        
        Args:
            logger: Base Python logger
            operation: Name of the operation being performed
        """
        self.logger = logger
        self.context = {
            'correlation_id': str(uuid.uuid4()),
            'operation': operation,
            'start_time': datetime.now().isoformat()
        }
        self._error_count = 0
        self._warning_count = 0
    
    def set_context(self, **kwargs):
        """
        Set additional context for this logger.
        
        Args:
            **kwargs: Context key-value pairs
        
        Example:
            logger.set_context(input_file="data.csv", user="admin")
        """
        self.context.update(kwargs)
        return self  # Allow chaining
    
    def _get_system_info(self) -> Dict[str, Any]:
        """Get current system information"""
        try:
            process = psutil.Process()
            return {
                'memory_mb': round(process.memory_info().rss / 1024 / 1024, 1),
                'cpu_percent': psutil.cpu_percent(interval=0.1),
                'disk_usage_percent': psutil.disk_usage('/').percent if hasattr(psutil, 'disk_usage') else None
            }
        except:
            return {}
    
    def _build_log_entry(self, level: str, message: str, **extra) -> Dict[str, Any]:
        """Build structured log entry"""
        entry = {
            'timestamp': datetime.now().isoformat(),
            'level': level,
            'correlation_id': self.context.get('correlation_id'),
            'operation': self.context.get('operation'),
            'message': message,
        }
        
        # Add context
        entry.update({k: v for k, v in self.context.items() 
                     if k not in ['correlation_id', 'operation']})
        
        # Add extra data
        if extra:
            entry['extra'] = extra
        
        # Add system info for errors
        if level in ['ERROR', 'CRITICAL']:
            entry['system_info'] = self._get_system_info()
        
        return entry
    
    def debug(self, message: str, **extra):
        """Log debug message with context"""
        entry = self._build_log_entry('DEBUG', message, **extra)
        self.logger.debug(json.dumps(entry, indent=2) if extra else message)
    
    def info(self, message: str, **extra):
        """Log info message with context"""
        entry = self._build_log_entry('INFO', message, **extra)
        self.logger.info(message if not extra else json.dumps(entry, indent=2))
    
    def warning(self, message: str, **extra):
        """Log warning message with context"""
        self._warning_count += 1
        entry = self._build_log_entry('WARNING', message, **extra)
        entry['warning_number'] = self._warning_count
        self.logger.warning(json.dumps(entry, indent=2))
    
    def error(self, message: str, exception: Optional[Exception] = None, **extra):
        """
        Log error message with context and optional exception.
        
        Args:
            message: Error message
            exception: Optional exception object
            **extra: Additional context
        """
        self._error_count += 1
        
        entry = self._build_log_entry('ERROR', message, **extra)
        entry['error_id'] = str(uuid.uuid4())[:8]
        entry['error_number'] = self._error_count
        
        if exception:
            entry['exception'] = {
                'type': type(exception).__name__,
                'message': str(exception),
                'traceback': traceback.format_exc()
            }
        
        self.logger.error(json.dumps(entry, indent=2))
        return entry['error_id']  # Return error ID for user display
    
    def critical(self, message: str, exception: Optional[Exception] = None, **extra):
        """Log critical message with context"""
        entry = self._build_log_entry('CRITICAL', message, **extra)
        entry['error_id'] = str(uuid.uuid4())[:8]
        
        if exception:
            entry['exception'] = {
                'type': type(exception).__name__,
                'message': str(exception),
                'traceback': traceback.format_exc()
            }
        
        self.logger.critical(json.dumps(entry, indent=2))
        return entry['error_id']
    
    def log_operation_start(self, **context):
        """Log the start of an operation"""
        self.set_context(**context)
        self.info(f"Starting operation: {self.context.get('operation')}")
    
    def log_operation_end(self, success: bool = True, **metrics):
        """
        Log the end of an operation with metrics.
        
        Args:
            success: Whether operation succeeded
            **metrics: Operation metrics (rows_processed, time_taken, etc.)
        """
        end_time = datetime.now()
        start_time = datetime.fromisoformat(self.context.get('start_time'))
        duration_seconds = (end_time - start_time).total_seconds()
        
        log_data = {
            'duration_seconds': round(duration_seconds, 2),
            'success': success,
            'errors': self._error_count,
            'warnings': self._warning_count,
            **metrics
        }
        
        if success:
            self.info(
                f"Operation completed: {self.context.get('operation')} "
                f"in {duration_seconds:.2f}s",
                **log_data
            )
        else:
            self.error(
                f"Operation failed: {self.context.get('operation')} "
                f"after {duration_seconds:.2f}s",
                **log_data
            )
    
    def get_correlation_id(self) -> str:
        """Get the correlation ID for this logger"""
        return self.context.get('correlation_id', '')
    
    def get_stats(self) -> Dict[str, int]:
        """Get logging statistics"""
        return {
            'errors': self._error_count,
            'warnings': self._warning_count
        }


def create_context_logger(operation: str, **initial_context) -> ContextLogger:
    """
    Factory function to create a context logger.
    
    Args:
        operation: Name of the operation
        **initial_context: Initial context values
    
    Returns:
        ContextLogger instance
    
    Example:
        logger = create_context_logger(
            "process_lmd_data",
            input_file="data.csv",
            output_file="output.csv"
        )
    """
    base_logger = logging.getLogger(__name__)
    ctx_logger = ContextLogger(base_logger, operation)
    
    if initial_context:
        ctx_logger.set_context(**initial_context)
    
    return ctx_logger
