"""
Security validation utilities for DataCleaner application.
Provides input validation, path sanitization, and security checks.
"""

import re
import os
import logging
from pathlib import Path
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)


class SecurityValidator:
    """Security validation utilities"""
    
    # Maximum file size: 50GB (configurable)
    MAX_FILE_SIZE_GB = 50
    
    # Allowed file extensions
    ALLOWED_EXTENSIONS = ['.csv']
    
    @staticmethod
    def sanitize_column_name(column_name: str) -> str:
        """
        Sanitize column name to prevent injection attacks.
        
        Args:
            column_name: Column name to validate
            
        Returns:
            Sanitized column name
            
        Raises:
            ValueError: If column name contains invalid characters
        """
        # Only allow alphanumeric, underscore, and spaces
        if not re.match(r'^[a-zA-Z0-9_\s]+$', column_name):
            raise ValueError(f"Invalid column name: {column_name}")
        return column_name
    
    @staticmethod
    def sanitize_file_path(
        file_path: str, 
        allowed_extensions: Optional[List[str]] = None,
        max_size_gb: Optional[float] = None
    ) -> Tuple[bool, str, Optional[Path]]:
        """
        Sanitize and validate file path.
        
        Args:
            file_path: Path to validate
            allowed_extensions: List of allowed extensions (default: ['.csv'])
            max_size_gb: Maximum file size in GB (default: 50)
            
        Returns:
            Tuple of (is_valid, error_message, sanitized_path)
            - is_valid: True if path is valid
            - error_message: Error description if invalid, empty string if valid
            - sanitized_path: Resolved Path object if valid, None if invalid
        """
        if allowed_extensions is None:
            allowed_extensions = SecurityValidator.ALLOWED_EXTENSIONS
        
        if max_size_gb is None:
            max_size_gb = SecurityValidator.MAX_FILE_SIZE_GB
        
        try:
            # Resolve path (handles relative paths, symlinks, etc.)
            path = Path(file_path).resolve(strict=True)
            
            # Check if file exists and is actually a file
            if not path.is_file():
                return False, "Selected path is not a valid file", None
            
            # Check file extension
            if path.suffix.lower() not in allowed_extensions:
                allowed = ', '.join(allowed_extensions)
                return False, f"Invalid file type. Allowed types: {allowed}", None
            
            # Check file size (prevent DoS)
            file_size_gb = path.stat().st_size / (1024 ** 3)
            if file_size_gb > max_size_gb:
                return False, f"File size ({file_size_gb:.1f}GB) exceeds limit ({max_size_gb}GB)", None
            
            # Additional security check: ensure file is readable
            if not os.access(path, os.R_OK):
                return False, "File is not readable. Please check permissions.", None
            
            logger.info(f"File path validated successfully: {path}")
            return True, "", path
            
        except FileNotFoundError:
            return False, "File not found", None
        except PermissionError:
            return False, "Permission denied. Check file permissions.", None
        except OSError as e:
            return False, f"Invalid file path: {str(e)}", None
        except Exception as e:
            logger.error(f"Unexpected error validating path: {e}")
            return False, f"Invalid file selection: {str(e)}", None
    
    @staticmethod
    def validate_output_path(
        output_path: str,
        allowed_extensions: Optional[List[str]] = None
    ) -> Tuple[bool, str, Optional[Path]]:
        """
        Validate output file path (file doesn't need to exist yet).
        
        Args:
            output_path: Output path to validate
            allowed_extensions: List of allowed extensions
            
        Returns:
            Tuple of (is_valid, error_message, sanitized_path)
        """
        if allowed_extensions is None:
            allowed_extensions = SecurityValidator.ALLOWED_EXTENSIONS
        
        try:
            path = Path(output_path).resolve()
            
            # Check parent directory exists and is writable
            if not path.parent.exists():
                return False, "Output directory does not exist", None
            
            if not os.access(path.parent, os.W_OK):
                return False, "Output directory is not writable", None
            
            # Check file extension
            if path.suffix.lower() not in allowed_extensions:
                allowed = ', '.join(allowed_extensions)
                return False, f"Invalid output file type. Allowed: {allowed}", None
            
            # Warn if file exists (will be overwritten)
            if path.exists():
                logger.warning(f"Output file already exists and will be overwritten: {path}")
            
            return True, "", path
            
        except Exception as e:
            logger.error(f"Error validating output path: {e}")
            return False, f"Invalid output path: {str(e)}", None


class UserFriendlyError:
    """
    Generate user-friendly error messages without exposing internal details.
    
    Enhanced for Phase 2:
    - Sanitizes file paths in error messages
    - Removes stack trace information
    - Provides actionable guidance
    - Includes error ID for support
    """
    
    # Error ID counter for tracking
    _error_counter = 0
    
    @staticmethod
    def _sanitize_message(message: str) -> str:
        """
        Sanitize error message to remove sensitive information.
        
        Removes:
        - Full file paths (keeps only filename)
        - Stack traces
        - Internal variable names
        - System paths
        """
        import re
        
        # Replace full Windows paths with just filename
        message = re.sub(r'[A-Za-z]:\\[^:\n]+\\([^\\:\n]+)', r'\1', message)
        
        # Replace Unix paths with just filename
        message = re.sub(r'/[\w/]+/([^\s/:]+)', r'\1', message)
        
        # Remove stack trace markers
        message = re.sub(r'File ".*", line \d+', '', message)
        message = re.sub(r'Traceback.*:\n', '', message)
        
        # Remove Python internal references
        message = re.sub(r'<[\w\s.]+at 0x[0-9a-fA-F]+>', '', message)
        
        return message.strip()
    
    @staticmethod
    def format_error(exception: Exception, user_message: str = None, context: str = None) -> str:
        """
        Format error for user display without exposing sensitive information.
        
        Args:
            exception: The exception that occurred
            user_message: Optional custom user-friendly message
            context: Optional context about what operation failed
            
        Returns:
            User-friendly error message with actionable guidance
        """
        import traceback
        import uuid
        
        # Generate unique error ID for correlation with logs
        error_id = str(uuid.uuid4())[:8]
        UserFriendlyError._error_counter += 1
        
        # Log full details internally (for debugging)
        logger.error(f"Error ID {error_id}: {context or 'Unknown context'}")
        logger.error(f"Exception type: {type(exception).__name__}")
        logger.error(f"Exception message: {str(exception)}")
        logger.error(f"Full traceback:\n{traceback.format_exc()}")
        
        # Generate user-friendly message with actionable guidance
        if user_message:
            message = user_message
        else:
            # Enhanced messages with actionable guidance
            error_messages = {
                FileNotFoundError: (
                    "The selected file could not be found.\n"
                    "• Please check if the file still exists\n"
                    "• Ensure the file hasn't been moved or deleted"
                ),
                PermissionError: (
                    "Permission denied when accessing the file.\n"
                    "• Close the file in Excel or other programs\n"
                    "• Check if you have read/write permissions\n"
                    "• Try running the application as administrator"
                ),
                MemoryError: (
                    "Insufficient memory to process the file.\n"
                    "• Close other applications to free up memory\n"
                    "• Try processing a smaller file\n"
                    "• The application will use streaming mode for large files"
                ),
                ValueError: (
                    "Invalid data format detected in the file.\n"
                    "• Ensure the file is a properly formatted CSV\n"
                    "• Check for missing or corrupted data\n"
                    "• Verify the file has the expected columns"
                ),
                TimeoutError: (
                    "Operation timed out. The file may be too large.\n"
                    "• Try processing a smaller file\n"
                    "• Close other running processes\n"
                    "• Contact support if the problem persists"
                ),
                OSError: (
                    "File system error occurred.\n"
                    "• Check available disk space\n"
                    "• Verify file permissions\n"
                    "• Ensure the disk is not full or write-protected"
                ),
                IOError: (
                    "Input/output error while accessing the file.\n"
                    "• Check if the file is corrupted\n"
                    "• Verify the storage device is working\n"
                    "• Try copying the file to a different location"
                ),
            }
            
            base_message = error_messages.get(
                type(exception), 
                "An unexpected error occurred during processing."
            )
            
            # Sanitize exception message and add if it provides useful info
            sanitized_ex_msg = UserFriendlyError._sanitize_message(str(exception))
            if sanitized_ex_msg and len(sanitized_ex_msg) < 200:
                message = f"{base_message}\n\nDetails: {sanitized_ex_msg}"
            else:
                message = base_message
        
        # Add context if provided
        if context:
            clean_context = UserFriendlyError._sanitize_message(context)
            message = f"{message}\n\nOperation: {clean_context}"
        
        # Add error ID for support
        message += f"\n\n{'─'*50}"
        message += f"\nError ID: {error_id}"
        message += f"\nCheck logs for details: logs/application.log"
        
        return message


class InputValidator:
    """Validate user inputs"""
    
    @staticmethod
    def validate_chunk_size(chunk_size: int) -> Tuple[bool, str]:
        """
        Validate chunk size parameter.
        
        Args:
            chunk_size: Chunk size to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        MIN_CHUNK_SIZE = 100
        MAX_CHUNK_SIZE = 10_000_000
        
        if not isinstance(chunk_size, int):
            return False, "Chunk size must be an integer"
        
        if chunk_size < MIN_CHUNK_SIZE:
            return False, f"Chunk size must be at least {MIN_CHUNK_SIZE:,}"
        
        if chunk_size > MAX_CHUNK_SIZE:
            return False, f"Chunk size cannot exceed {MAX_CHUNK_SIZE:,}"
        
        return True, ""
    
    @staticmethod
    def validate_column_list(columns: List[str]) -> Tuple[bool, str]:
        """
        Validate list of column names.
        
        Args:
            columns: List of column names
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not isinstance(columns, list):
            return False, "Columns must be provided as a list"
        
        if len(columns) == 0:
            return False, "At least one column must be selected"
        
        if len(columns) > 1000:
            return False, "Too many columns selected (max: 1000)"
        
        # Validate each column name
        for col in columns:
            if not isinstance(col, str):
                return False, f"Column name must be string, got: {type(col)}"
            
            if len(col) == 0:
                return False, "Column name cannot be empty"
            
            if len(col) > 256:
                return False, f"Column name too long: {col[:50]}..."
        
        return True, ""
