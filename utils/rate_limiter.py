"""
Rate Limiting Module
Prevents operation spam and resource exhaustion
"""

import logging
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Tuple, Dict
from threading import Lock

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Rate limiter to prevent operation spam.
    
    Features:
    - Sliding window rate limiting
    - Per-operation limits
    - Thread-safe
    - Automatic cleanup of old entries
    """
    
    def __init__(self, max_operations: int = 5, window_seconds: int = 60):
        """
        Initialize rate limiter.
        
        Args:
            max_operations: Maximum number of operations allowed in window
            window_seconds: Time window in seconds
        """
        self.max_operations = max_operations
        self.window = timedelta(seconds=window_seconds)
        self.operations: Dict[str, list] = defaultdict(list)
        self._lock = Lock()
    
    def check_rate_limit(self, operation_id: str) -> Tuple[bool, str, int]:
        """
        Check if operation is within rate limit.
        
        Args:
            operation_id: Unique identifier for the operation type
            
        Returns:
            Tuple of (allowed, message, retry_after_seconds)
        """
        with self._lock:
            now = datetime.now()
            
            # Remove old operations outside window
            self.operations[operation_id] = [
                op_time for op_time in self.operations[operation_id]
                if now - op_time < self.window
            ]
            
            # Check limit
            current_count = len(self.operations[operation_id])
            
            if current_count >= self.max_operations:
                # Calculate retry time
                oldest_operation = min(self.operations[operation_id])
                retry_time = oldest_operation + self.window
                retry_after = int((retry_time - now).total_seconds())
                
                message = (
                    f"Rate limit exceeded. "
                    f"Maximum {self.max_operations} operations per {self.window.seconds} seconds. "
                    f"Please wait {retry_after} seconds."
                )
                
                logger.warning(
                    f"Rate limit hit for '{operation_id}': "
                    f"{current_count}/{self.max_operations} operations"
                )
                
                return False, message, retry_after
            
            # Record this operation
            self.operations[operation_id].append(now)
            
            remaining = self.max_operations - (current_count + 1)
            logger.debug(
                f"Rate limit check passed for '{operation_id}': "
                f"{current_count + 1}/{self.max_operations} operations, "
                f"{remaining} remaining"
            )
            
            return True, "", 0
    
    def reset(self, operation_id: str):
        """
        Reset rate limit for a specific operation.
        
        Args:
            operation_id: Operation to reset
        """
        with self._lock:
            if operation_id in self.operations:
                del self.operations[operation_id]
                logger.info(f"Reset rate limit for '{operation_id}'")
    
    def reset_all(self):
        """Reset all rate limits"""
        with self._lock:
            self.operations.clear()
            logger.info("Reset all rate limits")
    
    def get_current_count(self, operation_id: str) -> int:
        """
        Get current operation count within window.
        
        Args:
            operation_id: Operation to check
            
        Returns:
            Number of operations in current window
        """
        with self._lock:
            now = datetime.now()
            
            # Remove old operations
            self.operations[operation_id] = [
                op_time for op_time in self.operations[operation_id]
                if now - op_time < self.window
            ]
            
            return len(self.operations[operation_id])
    
    def get_remaining(self, operation_id: str) -> int:
        """
        Get remaining operations allowed in current window.
        
        Args:
            operation_id: Operation to check
            
        Returns:
            Number of operations remaining
        """
        current = self.get_current_count(operation_id)
        return max(0, self.max_operations - current)


class GlobalRateLimiter:
    """
    Global rate limiter singleton for application-wide rate limiting.
    """
    
    _instance = None
    _lock = Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._limiters = {}
        return cls._instance
    
    def get_limiter(self, name: str, max_ops: int = 3, window_sec: int = 60) -> RateLimiter:
        """
        Get or create a rate limiter.
        
        Args:
            name: Name of the rate limiter
            max_ops: Maximum operations per window
            window_sec: Window size in seconds
            
        Returns:
            RateLimiter instance
        """
        if name not in self._limiters:
            self._limiters[name] = RateLimiter(max_ops, window_sec)
        return self._limiters[name]
    
    def check(self, limiter_name: str, operation_id: str) -> Tuple[bool, str, int]:
        """
        Convenience method to check rate limit.
        
        Args:
            limiter_name: Name of the limiter to use
            operation_id: Operation identifier
            
        Returns:
            Tuple of (allowed, message, retry_after)
        """
        limiter = self.get_limiter(limiter_name)
        return limiter.check_rate_limit(operation_id)


# Default rate limiters for different operations
# Can be customized per tab/operation

# Processing operations (heavy)
PROCESSING_LIMITER = RateLimiter(max_operations=3, window_seconds=60)

# File selection operations (light)
FILE_SELECTION_LIMITER = RateLimiter(max_operations=10, window_seconds=10)

# General UI operations
UI_OPERATION_LIMITER = RateLimiter(max_operations=20, window_seconds=10)
