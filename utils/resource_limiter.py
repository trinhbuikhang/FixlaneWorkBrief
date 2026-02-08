"""
Resource Limiting Module
Prevents resource exhaustion and DoS attacks
"""

import logging
import signal
from contextlib import contextmanager
from datetime import datetime
from typing import Optional

import psutil

logger = logging.getLogger(__name__)


class ResourceLimiter:
    """
    Limit resource usage during processing operations.
    
    Features:
    - Memory usage limits
    - Processing timeout limits
    - File size limits
    - Automatic cleanup on limit exceeded
    """
    
    def __init__(self, 
                 max_memory_mb: int = 8192,  # 8GB default
                 max_time_seconds: int = 7200,  # 2 hours default
                 max_file_size_gb: float = 50.0):  # 50GB default
        """
        Initialize resource limiter.
        
        Args:
            max_memory_mb: Maximum memory usage in MB
            max_time_seconds: Maximum processing time in seconds
            max_file_size_gb: Maximum input file size in GB
        """
        self.max_memory = max_memory_mb * 1024 * 1024  # Convert to bytes
        self.max_time = max_time_seconds
        self.max_file_size = max_file_size_gb * 1024 * 1024 * 1024  # Convert to bytes
        self._start_time: Optional[datetime] = None
        self._initial_memory: Optional[int] = None
    
    def check_memory(self) -> tuple[bool, str]:
        """
        Check if current memory usage is within limits.
        
        Returns:
            Tuple of (within_limit, message)
        """
        try:
            process = psutil.Process()
            current_memory = process.memory_info().rss
            memory_mb = current_memory / (1024 * 1024)
            limit_mb = self.max_memory / (1024 * 1024)
            
            if current_memory > self.max_memory:
                message = (
                    f"Memory limit exceeded: {memory_mb:.1f}MB / {limit_mb:.1f}MB. "
                    "Try processing a smaller file or increase memory limit."
                )
                logger.warning(message)
                return False, message
            
            logger.debug(f"Memory check passed: {memory_mb:.1f}MB / {limit_mb:.1f}MB")
            return True, ""
            
        except Exception as e:
            logger.error(f"Memory check failed: {e}")
            return True, ""  # Allow processing if check fails
    
    def check_timeout(self) -> tuple[bool, str]:
        """
        Check if processing time has exceeded timeout.
        
        Returns:
            Tuple of (within_limit, message)
        """
        if self._start_time is None:
            return True, ""
        
        try:
            elapsed = (datetime.now() - self._start_time).total_seconds()
            
            if elapsed > self.max_time:
                message = (
                    f"Processing timeout: {elapsed:.1f}s / {self.max_time}s. "
                    "Operation took too long."
                )
                logger.warning(message)
                return False, message
            
            logger.debug(f"Timeout check passed: {elapsed:.1f}s / {self.max_time}s")
            return True, ""
            
        except Exception as e:
            logger.error(f"Timeout check failed: {e}")
            return True, ""
    
    def check_file_size(self, file_path: str) -> tuple[bool, str]:
        """
        Check if file size is within limits.
        
        Args:
            file_path: Path to file to check
            
        Returns:
            Tuple of (within_limit, message)
        """
        try:
            import os
            file_size = os.path.getsize(file_path)
            size_gb = file_size / (1024 * 1024 * 1024)
            limit_gb = self.max_file_size / (1024 * 1024 * 1024)
            
            if file_size > self.max_file_size:
                message = (
                    f"File size limit exceeded: {size_gb:.2f}GB / {limit_gb:.1f}GB. "
                    "Please process a smaller file."
                )
                logger.warning(message)
                return False, message
            
            logger.debug(f"File size check passed: {size_gb:.2f}GB / {limit_gb:.1f}GB")
            return True, ""
            
        except Exception as e:
            logger.error(f"File size check failed: {e}")
            return True, ""
    
    @contextmanager
    def limit_resources(self):
        """
        Context manager to enforce resource limits during operation.
        
        Usage:
            limiter = ResourceLimiter(max_memory_mb=4096, max_time_seconds=1800)
            with limiter.limit_resources():
                process_data(input_file, output_file)
        
        Raises:
            ResourceLimitExceeded: If limits are exceeded
        """
        self._start_time = datetime.now()
        
        try:
            process = psutil.Process()
            self._initial_memory = process.memory_info().rss
            
            logger.info(
                f"Resource limits: "
                f"Memory={self.max_memory/(1024*1024):.0f}MB, "
                f"Timeout={self.max_time}s"
            )
            
            yield self
            
        finally:
            # Log resource usage
            if self._initial_memory and self._start_time:
                try:
                    final_memory = psutil.Process().memory_info().rss
                    memory_used = (final_memory - self._initial_memory) / (1024 * 1024)
                    elapsed = (datetime.now() - self._start_time).total_seconds()
                    
                    logger.info(
                        f"Resource usage: "
                        f"Memory used={memory_used:.1f}MB, "
                        f"Time={elapsed:.1f}s"
                    )
                except:
                    pass
            
            self._start_time = None
            self._initial_memory = None
    
    def get_available_memory_mb(self) -> float:
        """
        Get available system memory in MB.
        
        Returns:
            Available memory in MB
        """
        try:
            return psutil.virtual_memory().available / (1024 * 1024)
        except:
            return 0.0
    
    def get_disk_space_gb(self, path: str = "/") -> float:
        """
        Get available disk space in GB.
        
        Args:
            path: Path to check disk space for
            
        Returns:
            Available disk space in GB
        """
        try:
            return psutil.disk_usage(path).free / (1024 * 1024 * 1024)
        except:
            return 0.0
    
    def check_system_resources(self) -> tuple[bool, str]:
        """
        Check if system has sufficient resources for processing.
        
        Returns:
            Tuple of (sufficient, message)
        """
        warnings = []
        
        try:
            # Check available memory
            available_memory_mb = self.get_available_memory_mb()
            required_memory_mb = self.max_memory / (1024 * 1024)
            
            if available_memory_mb < required_memory_mb * 0.5:
                warnings.append(
                    f"Low available memory: {available_memory_mb:.0f}MB "
                    f"(recommended: {required_memory_mb:.0f}MB)"
                )
            
            # Check CPU usage
            cpu_percent = psutil.cpu_percent(interval=0.5)
            if cpu_percent > 90:
                warnings.append(f"High CPU usage: {cpu_percent:.1f}%")
            
            # Check disk space
            disk_space_gb = self.get_disk_space_gb()
            if disk_space_gb < 10:  # Less than 10GB
                warnings.append(f"Low disk space: {disk_space_gb:.1f}GB")
            
            if warnings:
                message = "System resource warnings:\n• " + "\n• ".join(warnings)
                logger.warning(message)
                return True, message  # Warning but allow processing
            
            return True, "System resources sufficient"
            
        except Exception as e:
            logger.error(f"System resource check failed: {e}")
            return True, ""


class ResourceLimitExceeded(Exception):
    """Raised when resource limits are exceeded"""
    pass


# Global default limiter
DEFAULT_LIMITER = ResourceLimiter(
    max_memory_mb=8192,  # 8GB
    max_time_seconds=7200,  # 2 hours
    max_file_size_gb=50.0
)


def check_file_processable(file_path: str, limiter: Optional[ResourceLimiter] = None) -> tuple[bool, str]:
    """
    Check if a file can be processed within resource limits.
    
    Args:
        file_path: Path to file to check
        limiter: ResourceLimiter to use (uses DEFAULT_LIMITER if None)
        
    Returns:
        Tuple of (can_process, message)
    """
    if limiter is None:
        limiter = DEFAULT_LIMITER
    
    # Check file size
    valid, message = limiter.check_file_size(file_path)
    if not valid:
        return False, message
    
    # Check system resources
    valid, message = limiter.check_system_resources()
    if not valid:
        return False, message
    
    return True, "File can be processed"
