"""
Test Cancellation Handler Module
Tests for utils/cancellation_handler.py
"""

import time

import pytest

from utils.cancellation_handler import (
    CancellationRequested,
    CancellationToken,
    ProgressTracker,
)


class TestCancellationToken:
    """Test suite for CancellationToken class"""
    
    def test_initial_state(self):
        """Test token initial state"""
        token = CancellationToken()
        assert token.is_cancelled() is False
    
    def test_cancel(self):
        """Test cancellation request"""
        token = CancellationToken()
        token.cancel()
        assert token.is_cancelled() is True
    
    def test_reset(self):
        """Test reset after cancellation"""
        token = CancellationToken()
        token.cancel()
        assert token.is_cancelled() is True
        
        token.reset()
        assert token.is_cancelled() is False
    
    def test_check_cancelled_not_cancelled(self):
        """Test check_cancelled when not cancelled"""
        token = CancellationToken()
        # Should not raise exception
        token.check_cancelled()
    
    def test_check_cancelled_when_cancelled(self):
        """Test check_cancelled raises exception when cancelled"""
        token = CancellationToken()
        token.cancel()
        
        with pytest.raises(CancellationRequested):
            token.check_cancelled()
    
    def test_cancel_callback(self):
        """Test cancel callback is invoked"""
        callback_invoked = {'value': False}
        
        def callback():
            callback_invoked['value'] = True
        
        token = CancellationToken()
        token.set_cancel_callback(callback)
        token.cancel()
        
        assert callback_invoked['value'] is True


class TestProgressTracker:
    """Test suite for ProgressTracker class"""
    
    def test_initialization(self):
        """Test progress tracker initialization"""
        tracker = ProgressTracker(total_items=100)
        assert tracker.total_items == 100
        assert tracker.current_item == 0
    
    def test_update_progress(self):
        """Test updating progress"""
        tracker = ProgressTracker(total_items=100)
        tracker.update(50)
        assert tracker.current_item == 50
    
    def test_progress_callback(self):
        """Test progress callback is invoked"""
        messages = []
        percentages = []
        
        def callback(message, percentage):
            messages.append(message)
            percentages.append(percentage)
        
        tracker = ProgressTracker(
            total_items=100,
            callback=callback,
            log_interval=10
        )
        
        # Update to trigger callbacks at 0%, 10%, 20%
        tracker.update(0, "Start")
        tracker.update(10, "10%")
        tracker.update(20, "20%")
        tracker.update(15, "15% (should not log)")
        
        # Should have logged at 0%, 10%, 20%
        assert len(messages) >= 3
        assert 0.0 in percentages
        assert 10.0 in percentages
        assert 20.0 in percentages
    
    def test_complete(self):
        """Test completion"""
        tracker = ProgressTracker(total_items=100)
        tracker.complete("Done")
        assert tracker.current_item == 100
    
    def test_zero_total_items(self):
        """Test tracker with zero items"""
        tracker = ProgressTracker(total_items=0)
        tracker.update(0)  # Should not raise exception
        assert tracker.current_item == 0


class TestCancellationIntegration:
    """Integration tests for cancellation in processing"""
    
    def test_cancellable_operation(self):
        """Test cancellable operation pattern"""
        token = CancellationToken()
        processed_items = []
        
        def process_items(items, cancellation_token):
            for item in items:
                cancellation_token.check_cancelled()
                processed_items.append(item)
                time.sleep(0.01)  # Simulate work
        
        items = list(range(100))
        
        # Process without cancellation
        process_items(items[:10], token)
        assert len(processed_items) == 10
        
        # Process with cancellation
        token.cancel()
        with pytest.raises(CancellationRequested):
            process_items(items[10:20], token)
        
        # Should have stopped immediately
        assert len(processed_items) == 10  # No new items processed
    
    def test_progress_with_cancellation(self):
        """Test progress tracking with cancellation"""
        token = CancellationToken()
        progress_updates = []
        
        def callback(message, percentage):
            progress_updates.append((message, percentage))
        
        tracker = ProgressTracker(
            total_items=100,
            callback=callback,
            log_interval=10
        )
        
        # Process with cancellation in middle
        for i in range(50):
            if i == 25:
                token.cancel()
            
            try:
                token.check_cancelled()
                tracker.update(i + 1, f"Item {i + 1}")
            except CancellationRequested:
                break
        
        # Should have updates up to cancellation point
        assert len(progress_updates) > 0
        assert all(pct <= 30.0 for _, pct in progress_updates)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
