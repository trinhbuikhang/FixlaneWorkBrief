"""
Configuration module for Fixlane WorkBrief application.
Contains all settings, constants, and column mappings.
"""

import logging
from pathlib import Path
from typing import Dict, List


class Config:
    """Application configuration class."""
    
    # Application info
    APP_NAME = "Fixlane WorkBrief Processor"
    APP_VERSION = "6.0.0"
    
    # Logging configuration
    LOG_LEVEL = logging.INFO
    LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
    
    # File processing settings
    CHUNK_SIZE = 10000  # Process records in chunks
    PROGRESS_UPDATE_INTERVAL = 1000  # Update progress every N records
    
    # Column mappings for standardizing column names across different files
    COLUMN_MAPPINGS = {
        'RoadName': ['Road Name', 'RoadName', 'road_name', 'roadname'],
        'RoadID': ['Road ID', 'RoadID', 'road_id', 'roadid'],
        'Lane': ['Lane', 'lane'],
        'Chainage': ['location', 'Chainage', 'chainage'],
        'TestDateUTC': ['TestDateUTC', 'Test Date UTC', 'test_date_utc'],
        # Lane fixes specific columns
        'From': ['From', 'from', 'FROM', 'Start Time', 'start_time'],
        'To': ['To', 'to', 'TO', 'End Time', 'end_time'],
        'Ignore': ['Ignore', 'ignore', 'IGNORE', 'Skip', 'skip'],
        # Additional common columns
        'Plate': ['Plate', 'plate', 'PLATE', 'License Plate', 'license_plate'],
        'RegionID': ['RegionID', 'regionid', 'REGIONID', 'Region ID', 'region_id'],
        'Travel': ['Travel', 'travel', 'TRAVEL', 'Direction', 'direction']
    }
    
    # Required columns for processing
    REQUIRED_COLUMNS = {
        'laneFixes': ['From', 'To', 'Lane', 'Ignore'],
        'combined_LMD': ['TestDateUTC', 'Lane', 'RoadName'],
        'workbrief': ['RoadName', 'Lane']
    }
    
    # File extensions
    SUPPORTED_FILE_EXTENSIONS = ['.csv', '.xlsx', '.xls']
    
    # GUI settings
    GUI_THEME = 'light'
    WINDOW_MIN_SIZE = (800, 600)
    WINDOW_DEFAULT_SIZE = (1000, 700)
    
    # Processing threads
    MAX_WORKER_THREADS = 1  # Keep single-threaded for data processing
    
    @classmethod
    def get_output_filename(cls, input_path: str, suffix: str) -> str:
        """Generate output filename with timestamp."""
        from datetime import datetime
        
        input_path = Path(input_path)
        timestamp = datetime.now().strftime("%Y%m%d")
        
        return str(input_path.parent / f"{input_path.stem}_{suffix}_{timestamp}.csv")
    
    @classmethod
    def validate_file_extension(cls, file_path: str) -> bool:
        """Check if file has supported extension."""
        return Path(file_path).suffix.lower() in cls.SUPPORTED_FILE_EXTENSIONS


class LogConfig:
    """Logging configuration utilities."""
    
    @staticmethod
    def setup_logging(level=Config.LOG_LEVEL):
        """Setup basic logging configuration."""
        logging.basicConfig(
            level=level,
            format=Config.LOG_FORMAT,
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('fixlane_app.log', mode='a')
            ]
        )
        
        # Suppress pandas warnings
        logging.getLogger('pandas').setLevel(logging.WARNING)
        
        return logging.getLogger(__name__)


class Messages:
    """User-facing messages and error strings."""
    
    # Success messages
    SUCCESS_LANE_UPDATE = "Lane update completed successfully!"
    SUCCESS_WORKBRIEF = "Workbrief processing completed successfully!"
    
    # Error messages
    ERROR_FILE_NOT_FOUND = "File not found: {}"
    ERROR_COLUMN_MISSING = "Required column '{}' not found in file"
    ERROR_TIMESTAMP_PARSE = "Failed to parse timestamps in file"
    ERROR_PROCESSING = "An error occurred during processing: {}"
    ERROR_INVALID_FILE = "Invalid file format. Supported formats: {}"
    
    # Info messages
    INFO_PROCESSING_START = "Processing started..."
    INFO_FILE_LOADED = "File loaded: {} rows"
    INFO_TIMESTAMP_DETECTED = "Timestamp format detected: {}"
    INFO_PROGRESS = "Processing: {:.1f}% complete"
    
    # Warnings
    WARNING_TIMESTAMPS_FAILED = "{} timestamps failed to parse"
    WARNING_NO_MATCHES = "No matching records found"
    
    @classmethod
    def format_supported_extensions(cls):
        """Get formatted list of supported file extensions."""
        return ", ".join(Config.SUPPORTED_FILE_EXTENSIONS)