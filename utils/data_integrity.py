"""
Data Integrity Checker Module
Provides utilities to verify data integrity after write operations.
"""

import hashlib
import logging
import random
from pathlib import Path
from typing import Optional, Tuple

# ⚡ Lazy import for Polars (heavy library)
from utils.lazy_imports import polars as pl

logger = logging.getLogger(__name__)


class DataIntegrityChecker:
    """Verify data integrity for CSV files"""
    
    @staticmethod
    def calculate_file_checksum(file_path: str, algorithm: str = 'sha256') -> str:
        """
        Calculate file checksum using specified algorithm.
        
        Args:
            file_path: Path to file
            algorithm: Hash algorithm ('md5', 'sha1', 'sha256', 'sha512')
        
        Returns:
            Hexadecimal checksum string
        
        Example:
            checksum = DataIntegrityChecker.calculate_file_checksum('data.csv')
            print(f"SHA256: {checksum}")
        """
        try:
            hasher = hashlib.new(algorithm)
            
            with open(file_path, 'rb') as f:
                # Read in chunks to handle large files
                for chunk in iter(lambda: f.read(8192), b''):
                    hasher.update(chunk)
            
            checksum = hasher.hexdigest()
            logger.debug(f"Calculated {algorithm} checksum for {file_path}: {checksum[:16]}...")
            
            return checksum
            
        except Exception as e:
            logger.error(f"Failed to calculate checksum for {file_path}: {e}")
            raise
    
    @staticmethod
    def verify_csv_integrity(
        original_df: pl.DataFrame,
        output_file: str,
        sample_size: int = 1000,
        check_all_rows: bool = False
    ) -> Tuple[bool, str]:
        """
        Verify written CSV matches original DataFrame.
        
        Args:
            original_df: Original DataFrame before writing
            output_file: Path to written CSV file
            sample_size: Number of rows to sample check (ignored if check_all_rows=True)
            check_all_rows: If True, check all rows (slower but thorough)
        
        Returns:
            Tuple of (is_valid, message)
        
        Example:
            df.write_csv('output.csv')
            is_valid, msg = DataIntegrityChecker.verify_csv_integrity(df, 'output.csv')
            if not is_valid:
                raise RuntimeError(f"Integrity check failed: {msg}")
        """
        try:
            logger.debug(f"Verifying integrity of {output_file}")
            
            # Check file exists
            if not Path(output_file).exists():
                return False, f"Output file does not exist: {output_file}"
            
            # Read back the file - let Polars infer types
            written_df = pl.read_csv(output_file)
            
            # Convert both to same types for comparison (all string)
            original_str = original_df.select([pl.col(c).cast(pl.Utf8) for c in original_df.columns])
            written_str = written_df.select([pl.col(c).cast(pl.Utf8) for c in written_df.columns])
            
            # Check row count
            if len(written_df) != len(original_df):
                return False, (
                    f"Row count mismatch: "
                    f"original={len(original_df):,}, "
                    f"written={len(written_df):,}"
                )
            
            # Check column count
            if len(written_df.columns) != len(original_df.columns):
                return False, (
                    f"Column count mismatch: "
                    f"original={len(original_df.columns)}, "
                    f"written={len(written_df.columns)}"
                )
            
            # Check column names
            if written_str.columns != original_str.columns:
                missing = set(original_str.columns) - set(written_str.columns)
                extra = set(written_str.columns) - set(original_str.columns)
                msg_parts = []
                if missing:
                    msg_parts.append(f"missing columns: {missing}")
                if extra:
                    msg_parts.append(f"extra columns: {extra}")
                return False, f"Column mismatch - {', '.join(msg_parts)}"
            
            # Check data integrity
            if check_all_rows:
                # Check all rows (thorough but slow)
                logger.debug(f"Checking all {len(original_str):,} rows")
                
                for idx in range(len(original_str)):
                    if idx % 10000 == 0:
                        logger.debug(f"Verified {idx:,} rows...")
                    
                    original_row = original_str.row(idx)
                    written_row = written_str.row(idx)
                    
                    if original_row != written_row:
                        return False, f"Data mismatch at row {idx}"
                
                logger.info(f"All {len(original_df):,} rows verified successfully")
                
            else:
                # Sample check (faster)
                total_rows = len(original_str)
                actual_sample_size = min(sample_size, total_rows)
                
                logger.debug(f"Sampling {actual_sample_size:,} rows from {total_rows:,}")
                
                # Use random sampling
                random.seed(42)  # Reproducible sampling
                sample_indices = random.sample(range(total_rows), actual_sample_size)
                
                for idx in sample_indices:
                    original_row = original_str.row(idx)
                    written_row = written_str.row(idx)
                    
                    if original_row != written_row:
                        return False, f"Data mismatch at row {idx}"
                
                logger.info(
                    f"Sample verification passed: "
                    f"{actual_sample_size:,} rows checked"
                )
            
            return True, "Integrity verified successfully"
            
        except Exception as e:
            logger.error(f"Integrity verification failed: {e}", exc_info=True)
            return False, f"Verification error: {str(e)}"
    
    @staticmethod
    def verify_file_size(
        file_path: str,
        expected_min_size: Optional[int] = None,
        expected_max_size: Optional[int] = None
    ) -> Tuple[bool, str]:
        """
        Verify file size is within expected range.
        
        Args:
            file_path: Path to file
            expected_min_size: Minimum expected size in bytes (optional)
            expected_max_size: Maximum expected size in bytes (optional)
        
        Returns:
            Tuple of (is_valid, message)
        """
        try:
            file_size = Path(file_path).stat().st_size
            
            if expected_min_size and file_size < expected_min_size:
                return False, (
                    f"File too small: {file_size} bytes < "
                    f"expected minimum {expected_min_size} bytes"
                )
            
            if expected_max_size and file_size > expected_max_size:
                return False, (
                    f"File too large: {file_size} bytes > "
                    f"expected maximum {expected_max_size} bytes"
                )
            
            logger.debug(f"File size OK: {file_size:,} bytes")
            return True, f"File size within expected range: {file_size:,} bytes"
            
        except Exception as e:
            return False, f"Failed to check file size: {e}"
    
    @staticmethod
    def create_integrity_report(file_path: str, original_df: pl.DataFrame) -> dict:
        """
        Create comprehensive integrity report.
        
        Args:
            file_path: Path to file to verify
            original_df: Original DataFrame
        
        Returns:
            Dictionary with integrity information
        """
        report = {
            'file_path': str(file_path),
            'timestamp': None,
            'checksum': None,
            'file_size_bytes': None,
            'row_count_match': None,
            'column_count_match': None,
            'data_integrity': None,
            'overall_status': 'unknown'
        }
        
        try:
            from datetime import datetime
            report['timestamp'] = datetime.now().isoformat()
            
            # File size
            report['file_size_bytes'] = Path(file_path).stat().st_size
            
            # Checksum
            report['checksum'] = DataIntegrityChecker.calculate_file_checksum(file_path)
            
            # Data integrity
            is_valid, message = DataIntegrityChecker.verify_csv_integrity(
                original_df, 
                file_path,
                sample_size=500
            )
            
            report['data_integrity'] = {
                'is_valid': is_valid,
                'message': message
            }
            
            # Overall status
            if is_valid:
                report['overall_status'] = 'pass'
                logger.info(f"Integrity report: {file_path} - PASS")
            else:
                report['overall_status'] = 'fail'
                logger.error(f"Integrity report: {file_path} - FAIL: {message}")
            
        except Exception as e:
            report['overall_status'] = 'error'
            report['error'] = str(e)
            logger.error(f"Failed to create integrity report: {e}", exc_info=True)
        
        return report
    
    @staticmethod
    def save_checksum_file(file_path: str, checksum: str, algorithm: str = 'sha256'):
        """
        Save checksum to companion file.
        
        Args:
            file_path: Original file path
            checksum: Checksum string
            algorithm: Hash algorithm used
        
        Example:
            checksum = DataIntegrityChecker.calculate_file_checksum('data.csv')
            DataIntegrityChecker.save_checksum_file('data.csv', checksum)
            # Creates data.csv.sha256
        """
        checksum_file = f"{file_path}.{algorithm}"
        
        try:
            with open(checksum_file, 'w') as f:
                f.write(f"{checksum}  {Path(file_path).name}\n")
            
            logger.debug(f"Saved checksum to {checksum_file}")
            
        except Exception as e:
            logger.error(f"Failed to save checksum file: {e}")
    
    @staticmethod
    def verify_checksum_file(file_path: str, algorithm: str = 'sha256') -> Tuple[bool, str]:
        """
        Verify file against saved checksum.
        
        Args:
            file_path: Path to file
            algorithm: Hash algorithm
        
        Returns:
            Tuple of (is_valid, message)
        """
        checksum_file = f"{file_path}.{algorithm}"
        
        try:
            # Read saved checksum
            if not Path(checksum_file).exists():
                return False, f"Checksum file not found: {checksum_file}"
            
            with open(checksum_file, 'r') as f:
                saved_checksum = f.read().strip().split()[0]
            
            # Calculate current checksum
            current_checksum = DataIntegrityChecker.calculate_file_checksum(
                file_path, 
                algorithm
            )
            
            # Compare
            if saved_checksum == current_checksum:
                logger.info(f"Checksum verification passed for {file_path}")
                return True, "Checksum matches"
            else:
                logger.error(
                    f"Checksum mismatch for {file_path}: "
                    f"saved={saved_checksum[:16]}..., "
                    f"current={current_checksum[:16]}..."
                )
                return False, "Checksum mismatch - file may be corrupted"
            
        except Exception as e:
            return False, f"Checksum verification failed: {e}"


if __name__ == '__main__':
    # Example usage
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Create test DataFrame
    test_df = pl.DataFrame({
        'Column1': [1, 2, 3, 4, 5],
        'Column2': ['a', 'b', 'c', 'd', 'e']
    })
    
    # Write to file
    test_file = 'test_integrity.csv'
    test_df.write_csv(test_file)
    
    # Verify integrity
    is_valid, message = DataIntegrityChecker.verify_csv_integrity(test_df, test_file)
    print(f"Integrity check: {is_valid} - {message}")
    
    # Calculate and save checksum
    checksum = DataIntegrityChecker.calculate_file_checksum(test_file)
    print(f"Checksum: {checksum}")
    DataIntegrityChecker.save_checksum_file(test_file, checksum)
    
    # Verify checksum
    is_valid, message = DataIntegrityChecker.verify_checksum_file(test_file)
    print(f"Checksum verification: {is_valid} - {message}")
    
    # Clean up
    import os
    os.remove(test_file)
    os.remove(f"{test_file}.sha256")
