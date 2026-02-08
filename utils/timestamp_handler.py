"""
Timestamp detection and parsing module for Fixlane WorkBrief application.
Handles automatic detection and parsing of various timestamp formats.
"""

import logging
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class TimestampFormat:
    """Data class for timestamp format information."""
    pattern: str
    formats: List[str]
    name: str
    description: str


class TimestampHandler:
    """Handles automatic detection and parsing of various timestamp formats."""
    
    def __init__(self):
        self.timestamp_patterns = self._initialize_patterns()
    
    def _initialize_patterns(self) -> List[TimestampFormat]:
        """Initialize comprehensive timestamp format patterns."""
        return [
            TimestampFormat(
                pattern=r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?Z?$',
                formats=['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%S.%f', 
                        '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%dT%H:%M:%S'],
                name='ISO 8601',
                description='ISO 8601 format (e.g., 2024-10-29T00:20:36.103Z)'
            ),
            TimestampFormat(
                pattern=r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?[+-]\d{2}:?\d{2}$',
                formats=['%Y-%m-%dT%H:%M:%S.%f%z', '%Y-%m-%dT%H:%M:%S%z'],
                name='ISO 8601 with timezone',
                description='ISO 8601 with timezone offset'
            ),
            TimestampFormat(
                pattern=r'^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{2}(\.\d{1,6})?$',
                formats=['%d/%m/%Y %H:%M:%S.%f', '%d/%m/%Y %H:%M:%S', 
                        '%m/%d/%Y %H:%M:%S.%f', '%m/%d/%Y %H:%M:%S'],
                name='DD/MM/YYYY or MM/DD/YYYY',
                description='European or US format with slashes'
            ),
            TimestampFormat(
                pattern=r'^\d{1,2}-\d{1,2}-\d{4} \d{1,2}:\d{2}:\d{2}(\.\d{1,6})?$',
                formats=['%d-%m-%Y %H:%M:%S.%f', '%d-%m-%Y %H:%M:%S',
                        '%m-%d-%Y %H:%M:%S.%f', '%m-%d-%Y %H:%M:%S'],
                name='DD-MM-YYYY dashed',
                description='Date format with dashes'
            ),
            TimestampFormat(
                pattern=r'^\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}:\d{2}(\.\d{1,6})?$',
                formats=['%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S'],
                name='YYYY-MM-DD space separated',
                description='ISO date with space instead of T'
            ),
            TimestampFormat(
                pattern=r'^\d{1,2}/\d{1,2}/\d{2} \d{1,2}:\d{2}:\d{2}(\.\d{1,6})?$',
                formats=['%d/%m/%y %H:%M:%S.%f', '%d/%m/%y %H:%M:%S',
                        '%m/%d/%y %H:%M:%S.%f', '%m/%d/%y %H:%M:%S'],
                name='DD/MM/YY short year',
                description='Short year format with slashes'
            ),
            TimestampFormat(
                pattern=r'^\d{10}(\.\d{1,6})?$',
                formats=['unix_seconds'],
                name='Unix timestamp (seconds)',
                description='Unix timestamp in seconds'
            ),
            TimestampFormat(
                pattern=r'^\d{13}$',
                formats=['unix_milliseconds'],
                name='Unix timestamp (milliseconds)',
                description='Unix timestamp in milliseconds'
            ),
            TimestampFormat(
                pattern=r'^\d{4}-\d{2}-\d{2}$',
                formats=['%Y-%m-%d'],
                name='Date only YYYY-MM-DD',
                description='Date only format'
            ),
            TimestampFormat(
                pattern=r'^\d{1,2}/\d{1,2}/\d{4}$',
                formats=['%d/%m/%Y', '%m/%d/%Y'],
                name='Date only DD/MM/YYYY',
                description='Date only with slashes'
            )
        ]
    
    def detect_format(self, sample_values: List[str]) -> Optional[TimestampFormat]:
        """
        Detect timestamp format from sample values.
        
        Args:
            sample_values: List of sample timestamp strings
            
        Returns:
            TimestampFormat object if detected, None otherwise
        """
        logger.info(f"Detecting timestamp format from {len(sample_values)} samples")
        logger.debug(f"Sample values: {sample_values[:3]}")
        
        for format_info in self.timestamp_patterns:
            pattern = format_info.pattern
            
            # Check if any sample matches this pattern
            matches = [bool(re.match(pattern, str(val))) for val in sample_values]
            match_rate = sum(matches) / len(matches) if matches else 0
            
            # If at least 80% of samples match, consider it detected
            if match_rate >= 0.8:
                logger.info(f"Detected timestamp format: {format_info.name} "
                           f"(match rate: {match_rate:.1%})")
                return format_info
        
        logger.warning("No timestamp format pattern detected")
        return None
    
    def parse_timestamps(self, series: pd.Series, 
                        detected_format: Optional[TimestampFormat] = None,
                        column_name: str = "timestamp") -> pd.Series:
        """
        Parse timestamps using detected format or auto-detection.
        
        Args:
            series: Pandas series containing timestamp strings
            detected_format: Previously detected format (optional)
            column_name: Name of column for logging
            
        Returns:
            Pandas series with parsed datetime objects
        """
        logger.info(f"Parsing timestamps for column: {column_name}")
        
        # Auto-detect format if not provided
        if detected_format is None:
            sample_values = series.dropna().head(10).astype(str).tolist()
            if not sample_values:
                logger.error(f"No valid timestamp values found in {column_name}")
                return pd.to_datetime(series, errors='coerce')
            
            detected_format = self.detect_format(sample_values)
        
        # Parse using detected format
        if detected_format:
            parsed_series = self._parse_with_format(series, detected_format)
            if parsed_series is not None:
                success_rate = parsed_series.notna().sum() / len(parsed_series) * 100
                logger.info(f"Parsing success rate: {success_rate:.1f}%")
                return parsed_series
        
        # Fallback to automatic parsing
        return self._parse_with_fallback(series, column_name)
    
    def _parse_with_format(self, series: pd.Series, 
                          format_info: TimestampFormat) -> Optional[pd.Series]:
        """Parse timestamps using a specific format."""
        logger.info(f"Parsing with format: {format_info.name}")
        
        # Handle special cases (Unix timestamps)
        if 'unix_seconds' in format_info.formats:
            logger.info("Parsing as Unix timestamp (seconds)")
            return pd.to_datetime(pd.to_numeric(series, errors='coerce'), 
                                unit='s', errors='coerce')
        
        elif 'unix_milliseconds' in format_info.formats:
            logger.info("Parsing as Unix timestamp (milliseconds)")
            return pd.to_datetime(pd.to_numeric(series, errors='coerce'), 
                                unit='ms', errors='coerce')
        
        # Try each format in the list
        for fmt in format_info.formats:
            try:
                logger.debug(f"Trying format: {fmt}")
                parsed_series = pd.to_datetime(series, format=fmt, errors='coerce')
                
                # Check if parsing was successful (more than 50% valid)
                if parsed_series.notna().sum() > len(parsed_series) * 0.5:
                    logger.info(f"Successfully parsed with format: {fmt}")
                    return parsed_series
                else:
                    logger.debug(f"Format {fmt} resulted in too many NaT values")
                    
            except Exception as e:
                logger.debug(f"Format {fmt} failed: {e}")
                continue
        
        logger.warning(f"All formats failed for {format_info.name}")
        return None
    
    def _parse_with_fallback(self, series: pd.Series, column_name: str) -> pd.Series:
        """Parse timestamps using pandas automatic detection as fallback."""
        logger.info(f"Using fallback parsing for {column_name}")
        
        fallback_options = [
            {'infer_datetime_format': True, 'errors': 'coerce'},
            {'infer_datetime_format': True, 'errors': 'coerce', 'utc': True},
            {'errors': 'coerce', 'dayfirst': True},
            {'errors': 'coerce', 'dayfirst': False},
        ]
        
        for i, options in enumerate(fallback_options):
            try:
                result = pd.to_datetime(series, **options)
                success_rate = result.notna().sum() / len(result) * 100
                
                if success_rate > 50:
                    logger.info(f"Fallback method {i+1} successful: {success_rate:.1f}%")
                    return result
                    
            except Exception as e:
                logger.debug(f"Fallback method {i+1} failed: {e}")
                continue
        
        # Last resort - basic coercion
        logger.warning("All fallback methods failed, using basic coercion")
        return pd.to_datetime(series, errors='coerce')
    
    def detect_and_parse_timestamps(self, series: pd.Series, 
                                  column_name: str = "timestamp") -> Tuple[pd.Series, Optional[str]]:
        """
        Main method to detect and parse timestamps.
        
        Args:
            series: Pandas series containing timestamp strings
            column_name: Name of column for logging
            
        Returns:
            Tuple of (parsed_series, detected_format_name)
        """
        # Get sample values for detection
        sample_values = series.dropna().head(10).astype(str).tolist()
        
        if not sample_values:
            logger.error(f"No valid timestamp values found in {column_name}")
            return pd.to_datetime(series, errors='coerce'), None
        
        # Detect format
        detected_format = self.detect_format(sample_values)
        format_name = detected_format.name if detected_format else None
        
        # Parse timestamps
        parsed_series = self.parse_timestamps(series, detected_format, column_name)
        
        # Log results
        failed_count = parsed_series.isna().sum()
        if failed_count > 0:
            logger.warning(f"{failed_count} timestamps failed to parse in {column_name}")
            
            # Show sample of failed timestamps for debugging
            failed_mask = parsed_series.isna() & series.notna()
            if failed_mask.any():
                failed_samples = series[failed_mask].head(3).tolist()
                logger.warning(f"Sample failed timestamps: {failed_samples}")
        
        return parsed_series, format_name
    
    def get_supported_formats_summary(self) -> str:
        """Get a summary of all supported timestamp formats."""
        summary_lines = ["Supported timestamp formats:"]
        for format_info in self.timestamp_patterns:
            summary_lines.append(f"• {format_info.name}: {format_info.description}")
        
        return "\n".join(summary_lines)
    
    def is_iso_format(self, sample_values: List[str]) -> bool:
        """Check if the timestamps appear to be in ISO format (contains 'T')."""
        return any('T' in str(val) for val in sample_values)


# Global instance for easy access
timestamp_handler = TimestampHandler()