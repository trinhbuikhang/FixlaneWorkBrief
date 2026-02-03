"""
Test Rate Limiter Module
Tests for utils/rate_limiter.py
"""

import pytest
import time
from utils.rate_limiter import RateLimiter


class TestRateLimiter:
    """Test suite for RateLimiter class"""
    
    def test_rate_limiter_allows_within_limit(self):
        """Test that operations within limit are allowed"""
        limiter = RateLimiter(max_operations=3, window_seconds=60)
        
        # First 3 operations should be allowed
        for i in range(3):
            allowed, message, retry_after = limiter.check_rate_limit("test_op")
            assert allowed is True
            assert message == ""
            assert retry_after == 0
    
    def test_rate_limiter_blocks_over_limit(self):
        """Test that operations over limit are blocked"""
        limiter = RateLimiter(max_operations=2, window_seconds=60)
        
        # First 2 should pass
        limiter.check_rate_limit("test_op")
        limiter.check_rate_limit("test_op")
        
        # Third should fail
        allowed, message, retry_after = limiter.check_rate_limit("test_op")
        assert allowed is False
        assert "Rate limit exceeded" in message
        assert retry_after > 0
    
    def test_rate_limiter_sliding_window(self):
        """Test sliding window behavior"""
        limiter = RateLimiter(max_operations=2, window_seconds=1)
        
        # Use up limit
        limiter.check_rate_limit("test_op")
        limiter.check_rate_limit("test_op")
        
        # Should be blocked
        allowed, _, _ = limiter.check_rate_limit("test_op")
        assert allowed is False
        
        # Wait for window to slide
        time.sleep(1.1)
        
        # Should be allowed again
        allowed, _, _ = limiter.check_rate_limit("test_op")
        assert allowed is True
    
    def test_rate_limiter_reset(self):
        """Test rate limiter reset"""
        limiter = RateLimiter(max_operations=2, window_seconds=60)
        
        # Use up limit
        limiter.check_rate_limit("test_op")
        limiter.check_rate_limit("test_op")
        
        # Should be blocked
        allowed, _, _ = limiter.check_rate_limit("test_op")
        assert allowed is False
        
        # Reset
        limiter.reset("test_op")
        
        # Should be allowed again
        allowed, _, _ = limiter.check_rate_limit("test_op")
        assert allowed is True
    
    def test_rate_limiter_different_operations(self):
        """Test that different operations have separate limits"""
        limiter = RateLimiter(max_operations=2, window_seconds=60)
        
        # Use up limit for operation A
        limiter.check_rate_limit("op_a")
        limiter.check_rate_limit("op_a")
        
        # Operation B should still be allowed
        allowed, _, _ = limiter.check_rate_limit("op_b")
        assert allowed is True
    
    def test_get_remaining(self):
        """Test getting remaining operations"""
        limiter = RateLimiter(max_operations=5, window_seconds=60)
        
        assert limiter.get_remaining("test_op") == 5
        
        limiter.check_rate_limit("test_op")
        assert limiter.get_remaining("test_op") == 4
        
        limiter.check_rate_limit("test_op")
        assert limiter.get_remaining("test_op") == 3


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
