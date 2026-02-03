"""
Test Data Integrity Module
Tests for utils/data_integrity.py
"""

import pytest
import polars as pl
from pathlib import Path
from utils.data_integrity import DataIntegrityChecker


class TestDataIntegrityChecker:
    """Test suite for DataIntegrityChecker class"""
    
    @pytest.fixture
    def sample_dataframe(self):
        """Create sample DataFrame for testing"""
        return pl.DataFrame({
            'Column1': [1, 2, 3, 4, 5],
            'Column2': ['a', 'b', 'c', 'd', 'e'],
            'Column3': [1.1, 2.2, 3.3, 4.4, 5.5]
        })
    
    def test_calculate_file_checksum(self, tmp_path, sample_dataframe):
        """Test file checksum calculation"""
        test_file = tmp_path / "test.csv"
        sample_dataframe.write_csv(test_file)
        
        # Calculate SHA256 checksum
        checksum = DataIntegrityChecker.calculate_file_checksum(str(test_file))
        
        assert checksum is not None
        assert len(checksum) == 64  # SHA256 produces 64 hex characters
        
        # Checksum should be consistent
        checksum2 = DataIntegrityChecker.calculate_file_checksum(str(test_file))
        assert checksum == checksum2
    
    def test_calculate_different_algorithms(self, tmp_path, sample_dataframe):
        """Test different hash algorithms"""
        test_file = tmp_path / "test.csv"
        sample_dataframe.write_csv(test_file)
        
        md5 = DataIntegrityChecker.calculate_file_checksum(str(test_file), 'md5')
        sha1 = DataIntegrityChecker.calculate_file_checksum(str(test_file), 'sha1')
        sha256 = DataIntegrityChecker.calculate_file_checksum(str(test_file), 'sha256')
        
        assert len(md5) == 32
        assert len(sha1) == 40
        assert len(sha256) == 64
        assert md5 != sha1 != sha256
    
    def test_verify_csv_integrity_valid(self, tmp_path, sample_dataframe):
        """Test CSV integrity verification for valid file"""
        test_file = tmp_path / "test.csv"
        sample_dataframe.write_csv(test_file)
        
        is_valid, message = DataIntegrityChecker.verify_csv_integrity(
            sample_dataframe,
            str(test_file)
        )
        
        assert is_valid is True
        assert "successfully" in message.lower()
    
    def test_verify_csv_integrity_row_count_mismatch(self, tmp_path, sample_dataframe):
        """Test CSV integrity with row count mismatch"""
        test_file = tmp_path / "test.csv"
        
        # Write different data
        modified_df = sample_dataframe.head(3)
        modified_df.write_csv(test_file)
        
        is_valid, message = DataIntegrityChecker.verify_csv_integrity(
            sample_dataframe,
            str(test_file)
        )
        
        assert is_valid is False
        assert "row count mismatch" in message.lower()
    
    def test_verify_csv_integrity_column_mismatch(self, tmp_path):
        """Test CSV integrity with column mismatch"""
        test_file = tmp_path / "test.csv"
        
        original_df = pl.DataFrame({
            'Column1': [1, 2, 3],
            'Column2': ['a', 'b', 'c']
        })
        
        different_df = pl.DataFrame({
            'Column1': [1, 2, 3],
            'Column3': ['x', 'y', 'z']  # Different column name
        })
        
        different_df.write_csv(test_file)
        
        is_valid, message = DataIntegrityChecker.verify_csv_integrity(
            original_df,
            str(test_file)
        )
        
        assert is_valid is False
        assert "column" in message.lower() and "mismatch" in message.lower()
    
    def test_verify_csv_integrity_data_mismatch(self, tmp_path):
        """Test CSV integrity with data value mismatch"""
        test_file = tmp_path / "test.csv"
        
        original_df = pl.DataFrame({
            'Column1': [1, 2, 3],
            'Column2': ['a', 'b', 'c']
        })
        
        modified_df = pl.DataFrame({
            'Column1': [1, 999, 3],  # Different value
            'Column2': ['a', 'b', 'c']
        })
        
        modified_df.write_csv(test_file)
        
        is_valid, message = DataIntegrityChecker.verify_csv_integrity(
            original_df,
            str(test_file),
            check_all_rows=True
        )
        
        assert is_valid is False
        assert "data mismatch" in message.lower()
    
    def test_verify_csv_integrity_nonexistent_file(self, sample_dataframe):
        """Test verification with nonexistent file"""
        is_valid, message = DataIntegrityChecker.verify_csv_integrity(
            sample_dataframe,
            "nonexistent.csv"
        )
        
        assert is_valid is False
        assert "does not exist" in message.lower()
    
    def test_verify_file_size_valid(self, tmp_path, sample_dataframe):
        """Test file size verification"""
        test_file = tmp_path / "test.csv"
        sample_dataframe.write_csv(test_file)
        
        file_size = test_file.stat().st_size
        
        is_valid, message = DataIntegrityChecker.verify_file_size(
            str(test_file),
            expected_min_size=file_size - 100,
            expected_max_size=file_size + 100
        )
        
        assert is_valid is True
        assert "within expected range" in message.lower()
    
    def test_verify_file_size_too_small(self, tmp_path, sample_dataframe):
        """Test file size too small"""
        test_file = tmp_path / "test.csv"
        sample_dataframe.write_csv(test_file)
        
        is_valid, message = DataIntegrityChecker.verify_file_size(
            str(test_file),
            expected_min_size=999999
        )
        
        assert is_valid is False
        assert "too small" in message.lower()
    
    def test_verify_file_size_too_large(self, tmp_path, sample_dataframe):
        """Test file size too large"""
        test_file = tmp_path / "test.csv"
        sample_dataframe.write_csv(test_file)
        
        is_valid, message = DataIntegrityChecker.verify_file_size(
            str(test_file),
            expected_max_size=10  # Very small limit
        )
        
        assert is_valid is False
        assert "too large" in message.lower()
    
    def test_save_and_verify_checksum_file(self, tmp_path, sample_dataframe):
        """Test saving and verifying checksum file"""
        test_file = tmp_path / "test.csv"
        sample_dataframe.write_csv(test_file)
        
        # Calculate and save checksum
        checksum = DataIntegrityChecker.calculate_file_checksum(str(test_file))
        DataIntegrityChecker.save_checksum_file(str(test_file), checksum)
        
        # Verify checksum file exists
        checksum_file = Path(f"{test_file}.sha256")
        assert checksum_file.exists()
        
        # Verify checksum
        is_valid, message = DataIntegrityChecker.verify_checksum_file(str(test_file))
        assert is_valid is True
        assert "matches" in message.lower()
    
    def test_verify_checksum_file_mismatch(self, tmp_path, sample_dataframe):
        """Test checksum verification with file modification"""
        test_file = tmp_path / "test.csv"
        sample_dataframe.write_csv(test_file)
        
        # Save checksum
        checksum = DataIntegrityChecker.calculate_file_checksum(str(test_file))
        DataIntegrityChecker.save_checksum_file(str(test_file), checksum)
        
        # Modify the file
        modified_df = sample_dataframe.with_columns(pl.lit(999).alias('Column4'))
        modified_df.write_csv(test_file)
        
        # Verify should fail
        is_valid, message = DataIntegrityChecker.verify_checksum_file(str(test_file))
        assert is_valid is False
        assert "mismatch" in message.lower()
    
    def test_create_integrity_report(self, tmp_path, sample_dataframe):
        """Test creating comprehensive integrity report"""
        test_file = tmp_path / "test.csv"
        sample_dataframe.write_csv(test_file)
        
        report = DataIntegrityChecker.create_integrity_report(
            str(test_file),
            sample_dataframe
        )
        
        assert report['file_path'] == str(test_file)
        assert report['timestamp'] is not None
        assert report['checksum'] is not None
        assert report['file_size_bytes'] is not None
        assert report['data_integrity'] is not None
        assert report['overall_status'] == 'pass'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
