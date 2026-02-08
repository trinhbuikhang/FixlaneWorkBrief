"""
Test Logging Helper Module
Tests for utils/logging_helper.py
"""

import logging

import pytest

from utils.logging_helper import LoggingHelper, LogLevel


class TestLoggingHelper:
    """Test suite for LoggingHelper class"""
    
    def setup_method(self):
        """Setup for each test"""
        self.logger = logging.getLogger('test_logger')
        self.logger.setLevel(logging.DEBUG)
        
        # Clear any existing handlers
        self.logger.handlers.clear()
    
    def test_log_data_info(self, caplog):
        """Test data information logging"""
        with caplog.at_level(logging.INFO):
            LoggingHelper.log_data_info(
                self.logger,
                "Test data",
                row_count=1000,
                column_count=5,
                memory_mb=10.5
            )
        
        assert "Test data" in caplog.text
        assert "1,000 rows" in caplog.text
        assert "5 columns" in caplog.text
        assert "10.5 MB" in caplog.text
    
    def test_log_warning_with_threshold_exceeded(self, caplog):
        """Test warning when threshold exceeded"""
        with caplog.at_level(logging.WARNING):
            LoggingHelper.log_warning_with_threshold(
                self.logger,
                "Memory usage",
                current_value=90.0,
                threshold=80.0,
                unit="MB"
            )
        
        assert "exceeds threshold" in caplog.text
        assert "90.0MB" in caplog.text
        assert "80.0MB" in caplog.text
    
    def test_log_warning_with_threshold_not_exceeded(self, caplog):
        """Test no warning when threshold not exceeded"""
        with caplog.at_level(logging.DEBUG):
            LoggingHelper.log_warning_with_threshold(
                self.logger,
                "Memory usage",
                current_value=70.0,
                threshold=80.0,
                unit="MB"
            )
        
        # Should be DEBUG level, not WARNING
        assert logging.WARNING not in [rec.levelno for rec in caplog.records]
    
    def test_log_progress_milestone(self, caplog):
        """Test progress milestone logging"""
        with caplog.at_level(logging.INFO):
            # Log at 0%, 10%, 50%, 100%
            LoggingHelper.log_progress_milestone(self.logger, 0, 100, "Test operation")
            LoggingHelper.log_progress_milestone(self.logger, 10, 100, "Test operation")
            LoggingHelper.log_progress_milestone(self.logger, 50, 100, "Test operation")
            LoggingHelper.log_progress_milestone(self.logger, 100, 100, "Test operation")
        
        assert "0/100 (0%)" in caplog.text
        assert "10/100 (10%)" in caplog.text
        assert "50/100 (50%)" in caplog.text
        assert "100/100 (100%)" in caplog.text
    
    def test_log_function_call_decorator_success(self, caplog):
        """Test function call decorator for successful execution"""
        
        @LoggingHelper.log_function_call(self.logger)
        def test_function(x, y):
            return x + y
        
        with caplog.at_level(logging.DEBUG):
            result = test_function(2, 3)
        
        assert result == 5
        assert "Entering test_function()" in caplog.text
        assert "Exiting test_function() successfully" in caplog.text
    
    def test_log_function_call_decorator_exception(self, caplog):
        """Test function call decorator for exception"""
        
        @LoggingHelper.log_function_call(self.logger)
        def failing_function():
            raise ValueError("Test error")
        
        with caplog.at_level(logging.ERROR):
            with pytest.raises(ValueError):
                failing_function()
        
        assert "Exception in failing_function()" in caplog.text
        assert "Test error" in caplog.text
    
    def test_log_performance_context_manager_success(self, caplog):
        """Test performance logging context manager"""
        with caplog.at_level(logging.INFO):
            with LoggingHelper.log_performance(self.logger, "Test operation"):
                # Simulate some work
                pass
        
        assert "Starting: Test operation" in caplog.text
        assert "Completed: Test operation" in caplog.text
    
    def test_log_performance_context_manager_exception(self, caplog):
        """Test performance logging with exception"""
        with caplog.at_level(logging.ERROR):
            with pytest.raises(RuntimeError):
                with LoggingHelper.log_performance(self.logger, "Test operation"):
                    raise RuntimeError("Test error")
        
        assert "Failed: Test operation" in caplog.text
        assert "RuntimeError" in caplog.text


class TestLogLevel:
    """Test suite for LogLevel constants"""
    
    def test_log_level_values(self):
        """Test log level constant values"""
        assert LogLevel.DEBUG == logging.DEBUG
        assert LogLevel.INFO == logging.INFO
        assert LogLevel.WARNING == logging.WARNING
        assert LogLevel.ERROR == logging.ERROR
        assert LogLevel.CRITICAL == logging.CRITICAL
    
    def test_get_level_name(self):
        """Test getting level name"""
        assert LogLevel.get_level_name(logging.DEBUG) == 'DEBUG'
        assert LogLevel.get_level_name(logging.INFO) == 'INFO'
        assert LogLevel.get_level_name(logging.WARNING) == 'WARNING'
        assert LogLevel.get_level_name(logging.ERROR) == 'ERROR'
        assert LogLevel.get_level_name(logging.CRITICAL) == 'CRITICAL'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
