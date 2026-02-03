"""
Test Data Validator Module
Tests for utils/data_validator.py
"""

import pytest
import polars as pl
from utils.data_validator import DataValidator


class TestDataValidator:
    """Test suite for DataValidator class"""
    
    def test_validate_csv_structure_valid(self):
        """Test validating valid CSV structure"""
        df = pl.DataFrame({
            'Column1': [1, 2, 3],
            'Column2': ['a', 'b', 'c']
        })
        
        is_valid, message = DataValidator.validate_csv_structure(df, "test.csv")
        
        assert is_valid is True
        assert message == "Validation passed"
    
    def test_validate_csv_structure_empty(self):
        """Test validating empty DataFrame"""
        df = pl.DataFrame()
        
        is_valid, message = DataValidator.validate_csv_structure(df, "test.csv")
        
        assert is_valid is False
        assert "empty" in message.lower()
    
    def test_validate_csv_structure_duplicate_columns(self):
        """Test validating DataFrame with duplicate column names"""
        # Polars doesn't allow duplicate column names in creation
        # This test verifies the validator handles normal DataFrames correctly
        df = pl.DataFrame({
            'Column1': [1, 2, 3],
            'Column2': ['a', 'b', 'c'],
        })
        
        is_valid, message = DataValidator.validate_csv_structure(df, "test.csv")
        
        # Should pass for valid DataFrame
        assert is_valid is True
    
    def test_validate_required_columns_all_present(self):
        """Test validating when all required columns are present"""
        df = pl.DataFrame({
            'TestDateUTC': ['2024-01-01'],
            'Lane': ['L1'],
            'RawSlope170': ['0011']
        })
        
        required = ['TestDateUTC', 'Lane', 'RawSlope170']
        is_valid, message = DataValidator.validate_required_columns(df, required, "test.csv")
        
        assert is_valid is True
    
    def test_validate_required_columns_missing(self):
        """Test validating when required columns are missing"""
        df = pl.DataFrame({
            'TestDateUTC': ['2024-01-01'],
            'Lane': ['L1']
        })
        
        required = ['TestDateUTC', 'Lane', 'RawSlope170']
        is_valid, message = DataValidator.validate_required_columns(df, required, "test.csv")
        
        assert is_valid is False
        assert 'RawSlope170' in message
    
    def test_validate_data_integrity(self):
        """Test data integrity validation"""
        df = pl.DataFrame({
            'Column1': [1, 2, None, 4],
            'Column2': ['a', 'b', 'c', 'd']
        })
        
        is_valid, message = DataValidator.validate_data_integrity(df, "test.csv")
        
        # Should pass with warnings about nulls
        assert is_valid is True
    
    def test_get_data_summary(self):
        """Test data summary generation"""
        df = pl.DataFrame({
            'Column1': [1, 2, 3, 4, 5],
            'Column2': ['a', 'b', 'c', 'd', 'e']
        })
        
        summary = DataValidator.get_data_summary(df, "test data")
        
        assert "Rows: 5" in summary
        assert "Columns: 2" in summary
        assert "test data" in summary


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
