"""
Centralized Application Configuration
Provides single source of truth for all configuration values
"""

import logging
import os
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class AppConfig:
    """
    Centralized application configuration.
    
    All configuration values should be accessed through this class.
    Supports environment variable overrides for deployment flexibility.
    """
    
    # ═══════════════════════════════════════════════════════════════
    # ENVIRONMENT
    # ═══════════════════════════════════════════════════════════════
    
    ENVIRONMENT = os.getenv('APP_ENVIRONMENT', 'production')
    DEBUG_MODE = os.getenv('DEBUG_MODE', 'False').lower() == 'true'
    
    # ═══════════════════════════════════════════════════════════════
    # PROCESSING SETTINGS
    # ═══════════════════════════════════════════════════════════════
    
    # Default chunk size for processing
    DEFAULT_CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', '100000'))
    
    # Minimum and maximum chunk sizes
    MIN_CHUNK_SIZE = 1_000
    MAX_CHUNK_SIZE = 1_000_000
    
    # File size threshold for streaming mode (GB)
    STREAMING_THRESHOLD_GB = float(os.getenv('STREAMING_THRESHOLD_GB', '5.0'))
    
    # Maximum allowed file size (GB)
    MAX_FILE_SIZE_GB = float(os.getenv('MAX_FILE_SIZE_GB', '50.0'))
    
    # ═══════════════════════════════════════════════════════════════
    # MEMORY SETTINGS
    # ═══════════════════════════════════════════════════════════════
    
    # Maximum memory usage (MB)
    MAX_MEMORY_MB = int(os.getenv('MAX_MEMORY_MB', '8192'))  # 8GB
    
    # Memory warning threshold (percentage)
    MEMORY_WARNING_THRESHOLD = float(os.getenv('MEMORY_WARNING_THRESHOLD', '0.8'))  # 80%
    
    # Garbage collection frequency (number of chunks)
    GC_FREQUENCY = int(os.getenv('GC_FREQUENCY', '10'))  # Every 10 chunks
    
    # ═══════════════════════════════════════════════════════════════
    # TIMEOUT SETTINGS
    # ═══════════════════════════════════════════════════════════════
    
    # Processing timeout (seconds)
    PROCESSING_TIMEOUT_SECONDS = int(os.getenv('PROCESSING_TIMEOUT', '7200'))  # 2 hours
    
    # File lock timeout (seconds)
    FILE_LOCK_TIMEOUT_SECONDS = int(os.getenv('FILE_LOCK_TIMEOUT', '60'))  # 1 minute
    
    # ═══════════════════════════════════════════════════════════════
    # RATE LIMITING
    # ═══════════════════════════════════════════════════════════════
    
    # Maximum operations per window
    RATE_LIMIT_MAX_OPERATIONS = int(os.getenv('RATE_LIMIT_OPS', '3'))
    
    # Rate limit window (seconds)
    RATE_LIMIT_WINDOW_SECONDS = int(os.getenv('RATE_LIMIT_WINDOW', '60'))
    
    # ═══════════════════════════════════════════════════════════════
    # BACKUP SETTINGS
    # ═══════════════════════════════════════════════════════════════
    
    # Maximum number of backups to keep
    MAX_BACKUPS = int(os.getenv('MAX_BACKUPS', '5'))
    
    # Enable automatic backups
    AUTO_BACKUP_ENABLED = os.getenv('AUTO_BACKUP', 'True').lower() == 'true'
    
    # Verify writes before finalizing
    VERIFY_WRITES = os.getenv('VERIFY_WRITES', 'True').lower() == 'true'
    
    # ═══════════════════════════════════════════════════════════════
    # UI SETTINGS
    # ═══════════════════════════════════════════════════════════════
    
    # Window dimensions
    WINDOW_MIN_WIDTH = int(os.getenv('WINDOW_MIN_WIDTH', '800'))
    WINDOW_MIN_HEIGHT = int(os.getenv('WINDOW_MIN_HEIGHT', '600'))
    WINDOW_DEFAULT_WIDTH = int(os.getenv('WINDOW_DEFAULT_WIDTH', '1200'))
    WINDOW_DEFAULT_HEIGHT = int(os.getenv('WINDOW_DEFAULT_HEIGHT', '800'))
    
    # Theme
    UI_THEME = os.getenv('UI_THEME', 'default')
    
    # ═══════════════════════════════════════════════════════════════
    # LOGGING SETTINGS
    # ═══════════════════════════════════════════════════════════════
    
    # Log level
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    # Log retention (days)
    LOG_RETENTION_DAYS = int(os.getenv('LOG_RETENTION_DAYS', '30'))
    
    # Max log file size (MB)
    MAX_LOG_FILE_SIZE_MB = int(os.getenv('MAX_LOG_SIZE_MB', '10'))
    
    # Number of backup log files
    LOG_BACKUP_COUNT = int(os.getenv('LOG_BACKUP_COUNT', '5'))
    
    # ═══════════════════════════════════════════════════════════════
    # FILE PATHS
    # ═══════════════════════════════════════════════════════════════
    
    # Base directory (project root)
    BASE_DIR = Path(__file__).parent.parent
    
    # Logs directory
    LOG_DIR = BASE_DIR / 'logs'
    
    # Temporary files directory
    TEMP_DIR = BASE_DIR / 'temp'
    
    # Config directory
    CONFIG_DIR = BASE_DIR / 'config'
    
    # ═══════════════════════════════════════════════════════════════
    # DATA VALIDATION
    # ═══════════════════════════════════════════════════════════════
    
    # Maximum rows in CSV (DoS prevention)
    MAX_CSV_ROWS = int(os.getenv('MAX_CSV_ROWS', '100000000'))  # 100M
    
    # Maximum columns in CSV
    MAX_CSV_COLUMNS = int(os.getenv('MAX_CSV_COLUMNS', '10000'))
    
    # Maximum column name length
    MAX_COLUMN_NAME_LENGTH = int(os.getenv('MAX_COL_NAME_LEN', '500'))
    
    # ═══════════════════════════════════════════════════════════════
    # METHODS
    # ═══════════════════════════════════════════════════════════════
    
    @classmethod
    def ensure_directories(cls):
        """Ensure required directories exist"""
        for directory in [cls.LOG_DIR, cls.TEMP_DIR]:
            directory.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Ensured directory exists: {directory}")
    
    @classmethod
    def validate_config(cls) -> tuple[bool, list[str]]:
        """
        Validate configuration values.
        
        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors = []
        
        # Validate chunk size
        if cls.DEFAULT_CHUNK_SIZE < cls.MIN_CHUNK_SIZE:
            errors.append(f"DEFAULT_CHUNK_SIZE ({cls.DEFAULT_CHUNK_SIZE}) < MIN ({cls.MIN_CHUNK_SIZE})")
        if cls.DEFAULT_CHUNK_SIZE > cls.MAX_CHUNK_SIZE:
            errors.append(f"DEFAULT_CHUNK_SIZE ({cls.DEFAULT_CHUNK_SIZE}) > MAX ({cls.MAX_CHUNK_SIZE})")
        
        # Validate thresholds
        if not 0 < cls.MEMORY_WARNING_THRESHOLD <= 1:
            errors.append(f"MEMORY_WARNING_THRESHOLD must be between 0 and 1")
        
        # Validate file sizes
        if cls.MAX_FILE_SIZE_GB <= 0:
            errors.append(f"MAX_FILE_SIZE_GB must be positive")
        if cls.STREAMING_THRESHOLD_GB <= 0:
            errors.append(f"STREAMING_THRESHOLD_GB must be positive")
        
        # Validate timeouts
        if cls.PROCESSING_TIMEOUT_SECONDS <= 0:
            errors.append(f"PROCESSING_TIMEOUT_SECONDS must be positive")
        
        # Validate rate limiting
        if cls.RATE_LIMIT_MAX_OPERATIONS <= 0:
            errors.append(f"RATE_LIMIT_MAX_OPERATIONS must be positive")
        if cls.RATE_LIMIT_WINDOW_SECONDS <= 0:
            errors.append(f"RATE_LIMIT_WINDOW_SECONDS must be positive")
        
        # Validate backup settings
        if cls.MAX_BACKUPS < 0:
            errors.append(f"MAX_BACKUPS must be non-negative")
        
        # Validate UI dimensions
        if cls.WINDOW_MIN_WIDTH < 640:
            errors.append(f"WINDOW_MIN_WIDTH too small (minimum 640)")
        if cls.WINDOW_MIN_HEIGHT < 480:
            errors.append(f"WINDOW_MIN_HEIGHT too small (minimum 480)")
        
        is_valid = len(errors) == 0
        
        if is_valid:
            logger.info("Configuration validation passed")
        else:
            logger.error(f"Configuration validation failed: {errors}")
        
        return is_valid, errors
    
    @classmethod
    def load_from_file(cls, config_file: Optional[str] = None):
        """
        Load configuration from JSON file.
        
        Args:
            config_file: Path to configuration file (optional)
        """
        if config_file is None:
            config_file = cls.CONFIG_DIR / 'app_config.json'
        
        if not os.path.exists(config_file):
            logger.info(f"Config file not found: {config_file}, using defaults")
            return
        
        try:
            import json
            with open(config_file, 'r') as f:
                config = json.load(f)
            
            # Update class attributes from config file
            for key, value in config.items():
                if hasattr(cls, key):
                    setattr(cls, key, value)
                    logger.debug(f"Loaded config: {key} = {value}")
                else:
                    logger.warning(f"Unknown config key: {key}")
            
            logger.info(f"Configuration loaded from {config_file}")
            
        except Exception as e:
            logger.error(f"Failed to load config file: {e}")
    
    @classmethod
    def to_dict(cls) -> dict:
        """
        Export configuration as dictionary.
        
        Returns:
            Dictionary of configuration values
        """
        config = {}
        
        for attr in dir(cls):
            if attr.isupper():  # Only constants (uppercase attributes)
                value = getattr(cls, attr)
                # Convert Path objects to strings
                if isinstance(value, Path):
                    value = str(value)
                config[attr] = value
        
        return config
    
    @classmethod
    def print_config(cls):
        """Print current configuration (for debugging)"""
        print("=" * 60)
        print("Application Configuration")
        print("=" * 60)
        
        for key, value in sorted(cls.to_dict().items()):
            print(f"{key:35s} = {value}")
        
        print("=" * 60)


# Initialize directories on module import
AppConfig.ensure_directories()

# Validate configuration
is_valid, errors = AppConfig.validate_config()
if not is_valid:
    logger.warning(f"Configuration validation warnings: {errors}")
