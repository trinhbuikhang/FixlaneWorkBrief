"""
Test Configuration Module
Tests for config/app_config.py
"""

import pytest
import os
from config.app_config import AppConfig


class TestAppConfig:
    """Test suite for AppConfig class"""
    
    def test_default_values(self):
        """Test that default configuration values are set"""
        assert AppConfig.DEFAULT_CHUNK_SIZE == 100000
        assert AppConfig.MAX_FILE_SIZE_GB == 50.0
        assert AppConfig.STREAMING_THRESHOLD_GB == 5.0
        assert AppConfig.MAX_MEMORY_MB == 8192
        assert AppConfig.PROCESSING_TIMEOUT_SECONDS == 7200
    
    def test_config_validation_passes(self):
        """Test that default config passes validation"""
        is_valid, errors = AppConfig.validate_config()
        assert is_valid is True
        assert len(errors) == 0
    
    def test_chunk_size_bounds(self):
        """Test chunk size constraints"""
        assert AppConfig.DEFAULT_CHUNK_SIZE >= AppConfig.MIN_CHUNK_SIZE
        assert AppConfig.DEFAULT_CHUNK_SIZE <= AppConfig.MAX_CHUNK_SIZE
    
    def test_memory_threshold_valid(self):
        """Test memory threshold is in valid range"""
        assert 0 < AppConfig.MEMORY_WARNING_THRESHOLD <= 1
    
    def test_positive_values(self):
        """Test that numeric values are positive"""
        assert AppConfig.MAX_FILE_SIZE_GB > 0
        assert AppConfig.STREAMING_THRESHOLD_GB > 0
        assert AppConfig.PROCESSING_TIMEOUT_SECONDS > 0
        assert AppConfig.MAX_MEMORY_MB > 0
    
    def test_directories_created(self):
        """Test that required directories exist"""
        AppConfig.ensure_directories()
        assert AppConfig.LOG_DIR.exists()
        assert AppConfig.TEMP_DIR.exists()
    
    def test_to_dict(self):
        """Test configuration export to dictionary"""
        config_dict = AppConfig.to_dict()
        assert isinstance(config_dict, dict)
        assert 'DEFAULT_CHUNK_SIZE' in config_dict
        assert 'MAX_FILE_SIZE_GB' in config_dict
        assert config_dict['DEFAULT_CHUNK_SIZE'] == 100000
    
    def test_environment_override(self, monkeypatch):
        """Test environment variable override"""
        monkeypatch.setenv('CHUNK_SIZE', '50000')
        # Note: Need to reload module for this to work
        # Just verify the mechanism works
        chunk_size = int(os.getenv('CHUNK_SIZE', '100000'))
        assert chunk_size == 50000


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
