"""
Logger setup module for DataCleaner application.
Provides comprehensive logging for both development and production (EXE) environments.
"""

import logging
import os
import sys
import traceback
from datetime import datetime
from pathlib import Path


class ApplicationLogger:
    """Centralized logging setup for the application"""
    
    def __init__(self, app_name="DataCleaner"):
        self.app_name = app_name
        self.log_dir = None
        self.log_file = None
        self.logger = None
        
    def setup_logging(self, log_level=logging.INFO):
        """
        Setup comprehensive logging for the application
        
        Args:
            log_level: Logging level (default: INFO)
        """
        # Determine if running as EXE or script
        if getattr(sys, 'frozen', False):
            # Running as EXE - get directory where EXE is located
            app_dir = Path(sys.executable).parent
            is_exe = True
        else:
            # Running as script - get script directory
            app_dir = Path(__file__).parent.parent
            is_exe = False
            
        # Create logs directory
        self.log_dir = app_dir / "logs"
        self.log_dir.mkdir(exist_ok=True)
        
        # Create log filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file = self.log_dir / f"{self.app_name}_{timestamp}.log"
        
        # Setup root logger
        self.logger = logging.getLogger()
        self.logger.setLevel(log_level)
        
        # Clear any existing handlers
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
        
        # Create formatters
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        simple_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        
        # File handler (always enabled)
        file_handler = logging.FileHandler(self.log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(detailed_formatter)
        self.logger.addHandler(file_handler)
        
        # Console handler (always enabled for debugging)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(simple_formatter)
        self.logger.addHandler(console_handler)
        
        # Log startup information
        self.logger.info("="*80)
        self.logger.info(f"{self.app_name} - Application Starting")
        self.logger.info(f"Running as: {'EXE' if is_exe else 'Script'}")
        self.logger.info(f"Python version: {sys.version}")
        self.logger.info(f"Working directory: {os.getcwd()}")
        self.logger.info(f"Application directory: {app_dir}")
        self.logger.info(f"Log file: {self.log_file}")
        self.logger.info(f"Log level: {logging.getLevelName(log_level)}")
        self.logger.info("="*80)
        
        return self.logger
    
    def log_exception(self, exc_info=None, context=""):
        """
        Log exception with full traceback
        
        Args:
            exc_info: Exception info (default: current exception)
            context: Additional context information
        """
        if self.logger:
            if context:
                self.logger.error(f"Exception in {context}:")
            else:
                self.logger.error("Unhandled exception:")
            
            if exc_info is None:
                exc_info = sys.exc_info()
            
            # Log the full traceback
            tb_lines = traceback.format_exception(*exc_info)
            for line in tb_lines:
                self.logger.error(line.rstrip())
    
    def log_system_info(self):
        """Log detailed system information for debugging"""
        if not self.logger:
            return
            
        self.logger.info("System Information:")
        self.logger.info(f"  OS: {os.name}")
        self.logger.info(f"  Platform: {sys.platform}")
        self.logger.info(f"  Architecture: {os.uname() if hasattr(os, 'uname') else 'N/A'}")
        self.logger.info(f"  Python executable: {sys.executable}")
        self.logger.info(f"  Python path: {sys.path[:3]}...")  # First 3 entries
        self.logger.info(f"  Environment variables (selected):")
        
        for key in ['PATH', 'PYTHONPATH', 'TEMP', 'TMP']:
            value = os.environ.get(key, 'Not set')
            # Truncate very long paths
            if len(value) > 200:
                value = value[:200] + "..."
            self.logger.info(f"    {key}: {value}")
    
    def get_log_file_path(self):
        """Return the path to the current log file"""
        return self.log_file
    
    def cleanup_old_logs(self, max_age_days=7, max_count=20):
        """
        Clean up old log files
        
        Args:
            max_age_days: Maximum age of log files to keep
            max_count: Maximum number of log files to keep
        """
        if not self.log_dir or not self.log_dir.exists():
            return
            
        try:
            # Get all log files
            log_files = list(self.log_dir.glob(f"{self.app_name}_*.log"))
            
            # Sort by modification time (newest first)
            log_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
            
            # Remove files beyond max_count
            if len(log_files) > max_count:
                for old_file in log_files[max_count:]:
                    old_file.unlink()
                    if self.logger:
                        self.logger.info(f"Removed old log file: {old_file.name}")
            
            # Remove files older than max_age_days
            import time
            max_age_seconds = max_age_days * 24 * 3600
            current_time = time.time()
            
            for log_file in log_files:
                if current_time - log_file.stat().st_mtime > max_age_seconds:
                    log_file.unlink()
                    if self.logger:
                        self.logger.info(f"Removed expired log file: {log_file.name}")
                        
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Failed to cleanup old logs: {e}")

# Global logger instance
_app_logger = None

def setup_application_logging(log_level=logging.INFO):
    """
    Setup application-wide logging
    
    Args:
        log_level: Logging level
        
    Returns:
        Logger instance
    """
    global _app_logger
    
    if _app_logger is None:
        _app_logger = ApplicationLogger()
    
    logger = _app_logger.setup_logging(log_level)
    _app_logger.log_system_info()
    _app_logger.cleanup_old_logs()
    
    return logger

def get_application_logger():
    """Get the application logger instance"""
    global _app_logger
    return _app_logger

def log_exception(exc_info=None, context=""):
    """Convenience function to log exceptions"""
    global _app_logger
    if _app_logger:
        _app_logger.log_exception(exc_info, context)

def setup_exception_handler():
    """Setup global exception handler for unhandled exceptions"""
    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            # Allow keyboard interrupt to work normally
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        
        # Log the exception
        log_exception((exc_type, exc_value, exc_traceback), "Unhandled exception")
        
        # Also call the original handler
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
    
    sys.excepthook = handle_exception