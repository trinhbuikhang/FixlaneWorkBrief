"""
Test Base Processor Module
Tests for utils/base_processor.py
"""

import polars as pl
import pytest

from utils.base_processor import (
    BaseProcessor,
    CancellableProcessor,
    ProcessingCancelled,
)


class TestProcessor(BaseProcessor):
    """Concrete implementation of BaseProcessor for testing"""
    
    def process(self, *args, **kwargs):
        """Test implementation"""
        return "processed"


class TestBaseProcessor:
    """Test suite for BaseProcessor class"""
    
    def test_initialization(self):
        """Test processor initialization"""
        processor = TestProcessor()
        assert processor.progress_callback is None
        
        def callback(msg, progress=None):
            pass
        
        processor_with_callback = TestProcessor(progress_callback=callback)
        assert processor_with_callback.progress_callback == callback
    
    def test_emit_progress(self):
        """Test progress emission"""
        messages = []
        
        def callback(msg, progress=None):
            messages.append(msg)
        
        processor = TestProcessor(progress_callback=callback)
        processor._emit_progress("Test message")
        
        assert len(messages) == 1
        assert messages[0] == "Test message"
    
    def test_validate_file_exists(self, tmp_path):
        """Test file existence validation"""
        processor = TestProcessor()
        
        # Create a test file
        test_file = tmp_path / "test.csv"
        test_file.write_text("test")
        
        # Should pass for existing file
        assert processor._validate_file_exists(str(test_file)) is True
        
        # Should fail for non-existent file
        assert processor._validate_file_exists("nonexistent.csv") is False
    
    def test_remove_duplicate_testdateutc(self):
        """Test duplicate removal"""
        processor = TestProcessor()
        
        df = pl.DataFrame({
            'TestDateUTC': ['2024-01-01', '2024-01-01', '2024-01-02'],
            'Value': [1, 2, 3]
        })
        
        result_df, removed_count = processor._remove_duplicate_testdateutc(df)
        
        assert len(result_df) == 2  # 1 duplicate removed
        assert removed_count == 1
    
    def test_remove_duplicate_missing_column(self):
        """Test duplicate removal when column doesn't exist"""
        processor = TestProcessor()
        
        df = pl.DataFrame({
            'Column1': [1, 2, 3],
            'Column2': ['a', 'b', 'c']
        })
        
        result_df, removed_count = processor._remove_duplicate_testdateutc(df)
        
        assert len(result_df) == len(df)
        assert removed_count == 0
    
    def test_validate_columns_exist(self):
        """Test column validation"""
        processor = TestProcessor()
        
        df = pl.DataFrame({
            'Column1': [1, 2, 3],
            'Column2': ['a', 'b', 'c']
        })
        
        # Should pass when all columns exist
        assert processor._validate_columns_exist(df, ['Column1', 'Column2']) is True
        
        # Should fail when column is missing
        assert processor._validate_columns_exist(df, ['Column1', 'Column3']) is False
    
    def test_standardize_boolean_columns(self):
        """Test boolean column standardization"""
        processor = TestProcessor()
        
        df = pl.DataFrame({
            'Bool1': ['True', 'False', '1', '0'],
            'Bool2': ['true', 'false', 'T', 'F']
        })
        
        result = processor._standardize_boolean_columns(df, ['Bool1', 'Bool2'])
        
        # Check that values are standardized
        assert result['Bool1'].dtype == pl.Boolean
        assert result['Bool1'][0] is True
        assert result['Bool1'][1] is False
    
    def test_format_utilities(self):
        """Test formatting utility methods"""
        processor = TestProcessor()
        
        assert processor._format_number(1000) == "1,000"
        assert processor._format_number(1000000) == "1,000,000"
        
        assert processor._format_percentage(0.5) == "50.00%"
        assert processor._format_percentage(0.333, decimals=1) == "33.3%"


class TestCancellableProcessor:
    """Test suite for CancellableProcessor class"""
    
    class TestCancellable(CancellableProcessor):
        """Test implementation of cancellable processor"""
        
        def process(self, iterations=10):
            """Process with cancellation support"""
            results = []
            for i in range(iterations):
                self._check_cancellation()
                results.append(i)
            return results
    
    def test_cancellation(self):
        """Test cancellation functionality"""
        processor = self.TestCancellable()
        
        # Should not be cancelled initially
        assert processor.is_cancelled() is False
        
        # Request cancellation
        processor.cancel()
        assert processor.is_cancelled() is True
        
        # Processing should raise exception
        with pytest.raises(ProcessingCancelled):
            processor.process()
    
    def test_cancellation_reset(self):
        """Test cancellation reset"""
        processor = self.TestCancellable()
        
        processor.cancel()
        assert processor.is_cancelled() is True
        
        processor.reset_cancellation()
        assert processor.is_cancelled() is False
        
        # Should be able to process again
        result = processor.process(iterations=5)
        assert len(result) == 5


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
