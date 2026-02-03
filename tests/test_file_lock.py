"""
Test File Locking Module
Tests for utils/file_lock.py
"""

import pytest
import time
from pathlib import Path
from utils.file_lock import FileLock, FileLockTimeout, is_file_locked


class TestFileLock:
    """Test suite for FileLock class"""
    
    def test_file_lock_basic(self, tmp_path):
        """Test basic file locking"""
        test_file = tmp_path / "test.csv"
        test_file.write_text("test data")
        
        with FileLock(str(test_file), timeout=5):
            assert is_file_locked(str(test_file))
        
        assert not is_file_locked(str(test_file))
    
    def test_file_lock_prevents_concurrent_access(self, tmp_path):
        """Test that file lock prevents concurrent access"""
        test_file = tmp_path / "test.csv"
        test_file.write_text("test data")
        
        with FileLock(str(test_file), timeout=1):
            # Try to acquire another lock
            with pytest.raises(FileLockTimeout):
                with FileLock(str(test_file), timeout=0.5):
                    pass
    
    def test_file_lock_auto_release(self, tmp_path):
        """Test that lock is automatically released"""
        test_file = tmp_path / "test.csv"
        test_file.write_text("test data")
        
        # Acquire and release lock
        with FileLock(str(test_file)):
            pass
        
        # Should be able to acquire again
        with FileLock(str(test_file)):
            assert is_file_locked(str(test_file))
    
    def test_file_lock_exception_handling(self, tmp_path):
        """Test that lock is released even on exception"""
        test_file = tmp_path / "test.csv"
        test_file.write_text("test data")
        
        try:
            with FileLock(str(test_file)):
                raise ValueError("Test error")
        except ValueError:
            pass
        
        # Lock should be released
        assert not is_file_locked(str(test_file))
    
    def test_is_file_locked(self, tmp_path):
        """Test checking if file is locked"""
        test_file = tmp_path / "test.csv"
        test_file.write_text("test data")
        
        assert not is_file_locked(str(test_file))
        
        with FileLock(str(test_file)):
            assert is_file_locked(str(test_file))
        
        assert not is_file_locked(str(test_file))


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
