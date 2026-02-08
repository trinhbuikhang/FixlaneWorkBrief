"""
Cancellation Handler Module
Provides GUI integration for operation cancellation.
"""

import logging
from typing import Callable, Optional

from PyQt6.QtCore import QObject, QThread, pyqtSignal
from PyQt6.QtWidgets import QMessageBox, QPushButton, QWidget

logger = logging.getLogger(__name__)


class CancellationToken:
    """
    Token that can be passed to operations to check for cancellation.
    Thread-safe cancellation mechanism.
    """
    
    def __init__(self):
        self._is_cancelled = False
        self._cancel_callback: Optional[Callable] = None
    
    def cancel(self):
        """Request cancellation"""
        self._is_cancelled = True
        logger.info("Cancellation requested via token")
        
        if self._cancel_callback:
            try:
                self._cancel_callback()
            except Exception as e:
                logger.error(f"Error in cancel callback: {e}")
    
    def is_cancelled(self) -> bool:
        """Check if cancellation was requested"""
        return self._is_cancelled
    
    def reset(self):
        """Reset cancellation state"""
        self._is_cancelled = False
        logger.debug("Cancellation token reset")
    
    def set_cancel_callback(self, callback: Callable):
        """Set callback to be called when cancel() is invoked"""
        self._cancel_callback = callback
    
    def check_cancelled(self):
        """
        Check cancellation and raise exception if cancelled.
        
        Raises:
            CancellationRequested: If cancellation was requested
        """
        if self._is_cancelled:
            raise CancellationRequested("Operation was cancelled by user")


class CancellationRequested(Exception):
    """Exception raised when operation is cancelled"""
    pass


class CancellableWorker(QThread):
    """
    QThread worker with built-in cancellation support.
    
    Signals:
        progress: Emitted during processing (message: str, percentage: float)
        finished: Emitted when processing completes (success: bool, message: str)
        cancelled: Emitted when processing is cancelled
    """
    
    progress = pyqtSignal(str, float)  # message, percentage
    finished = pyqtSignal(bool, str)   # success, message
    cancelled = pyqtSignal()
    
    def __init__(self, parent: Optional[QObject] = None):
        super().__init__(parent)
        self.cancellation_token = CancellationToken()
        self._processor = None
    
    def set_processor(self, processor):
        """
        Set the processor instance.
        
        Args:
            processor: Instance with cancel() method
        """
        self._processor = processor
        
        # Link token to processor
        if hasattr(processor, 'cancel'):
            self.cancellation_token.set_cancel_callback(processor.cancel)
    
    def cancel(self):
        """Request cancellation of the operation"""
        logger.info("Cancelling worker operation")
        self.cancellation_token.cancel()
        
        # Also cancel processor if available
        if self._processor and hasattr(self._processor, 'cancel'):
            self._processor.cancel()
    
    def is_cancelled(self) -> bool:
        """Check if operation was cancelled"""
        return self.cancellation_token.is_cancelled()
    
    def emit_progress(self, message: str, percentage: Optional[float] = None):
        """Emit progress update"""
        if percentage is None:
            percentage = -1.0  # Indeterminate
        self.progress.emit(message, percentage)
    
    def run(self):
        """
        Override this method in subclasses to implement processing logic.
        
        Example:
            def run(self):
                try:
                    for i in range(100):
                        self.cancellation_token.check_cancelled()
                        # Do work...
                        self.emit_progress(f"Processing {i}/100", i)
                    
                    self.finished.emit(True, "Completed successfully")
                    
                except CancellationRequested:
                    self.cancelled.emit()
                    self.finished.emit(False, "Operation cancelled by user")
                    
                except Exception as e:
                    self.finished.emit(False, str(e))
        """
        raise NotImplementedError("Subclasses must implement run()")


class CancellationUI:
    """
    UI helper for cancellation button management.
    
    Manages the state of cancel button and provides user confirmation.
    """
    
    @staticmethod
    def setup_cancel_button(
        button: QPushButton,
        worker: CancellableWorker,
        confirm_cancellation: bool = True,
        parent: Optional[QWidget] = None
    ):
        """
        Setup cancel button to work with cancellable worker.
        
        Args:
            button: Cancel button widget
            worker: CancellableWorker instance
            confirm_cancellation: Ask user to confirm cancellation
            parent: Parent widget for confirmation dialog
        """
        def on_cancel_clicked():
            if confirm_cancellation:
                reply = QMessageBox.question(
                    parent,
                    'Confirm Cancellation',
                    'Are you sure you want to cancel this operation?\n\n'
                    'Any progress will be lost.',
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                    QMessageBox.StandardButton.No
                )
                
                if reply == QMessageBox.StandardButton.No:
                    return
            
            # Cancel the operation
            worker.cancel()
            button.setEnabled(False)
            button.setText("Cancelling...")
            logger.info("User confirmed cancellation")
        
        button.clicked.connect(on_cancel_clicked)
        button.setEnabled(True)
        button.setText("Cancel")
    
    @staticmethod
    def enable_cancel_button(button: QPushButton):
        """Enable cancel button at start of operation"""
        button.setEnabled(True)
        button.setText("Cancel")
    
    @staticmethod
    def disable_cancel_button(button: QPushButton):
        """Disable cancel button at end of operation"""
        button.setEnabled(False)
        button.setText("Cancel")


class ProgressTracker:
    """
    Helper class to track and report progress.
    
    Example:
        tracker = ProgressTracker(total_items=1000, callback=emit_progress)
        
        for i, item in enumerate(items):
            tracker.update(i + 1, f"Processing {item}")
            # Do work...
    """
    
    def __init__(
        self,
        total_items: int,
        callback: Optional[Callable[[str, float], None]] = None,
        log_interval: int = 10
    ):
        """
        Initialize progress tracker.
        
        Args:
            total_items: Total number of items to process
            callback: Callback function(message, percentage)
            log_interval: Log progress every N percent
        """
        self.total_items = total_items
        self.callback = callback
        self.log_interval = log_interval
        self.current_item = 0
        self.last_logged_percentage = -1
    
    def update(self, current: int, message: Optional[str] = None):
        """
        Update progress.
        
        Args:
            current: Current item number
            message: Progress message (optional)
        """
        self.current_item = current
        
        if self.total_items == 0:
            percentage = 0.0
        else:
            percentage = (current / self.total_items) * 100
        
        # Log at intervals
        current_log_level = int(percentage / self.log_interval) * self.log_interval
        
        if current_log_level > self.last_logged_percentage:
            self.last_logged_percentage = current_log_level
            
            if message is None:
                message = f"Progress: {current:,}/{self.total_items:,}"
            
            if self.callback:
                self.callback(message, percentage)
            
            logger.debug(f"{message} ({percentage:.1f}%)")
    
    def complete(self, message: str = "Completed"):
        """Mark progress as complete"""
        self.update(self.total_items, message)


if __name__ == '__main__':
    # Example usage
    import sys
    import time

    from PyQt6.QtWidgets import QApplication, QPushButton
    
    logging.basicConfig(level=logging.DEBUG)
    
    class ExampleWorker(CancellableWorker):
        def run(self):
            try:
                tracker = ProgressTracker(
                    total_items=100,
                    callback=self.emit_progress
                )
                
                for i in range(100):
                    # Check for cancellation
                    self.cancellation_token.check_cancelled()
                    
                    # Simulate work
                    time.sleep(0.1)
                    
                    # Update progress
                    tracker.update(i + 1, f"Processing item {i + 1}")
                
                self.finished.emit(True, "Completed successfully!")
                
            except CancellationRequested:
                logger.info("Operation was cancelled")
                self.cancelled.emit()
                self.finished.emit(False, "Operation cancelled by user")
                
            except Exception as e:
                logger.error(f"Error: {e}", exc_info=True)
                self.finished.emit(False, f"Error: {e}")
    
    # Test
    app = QApplication(sys.argv)
    
    worker = ExampleWorker()
    cancel_btn = QPushButton("Cancel")
    
    CancellationUI.setup_cancel_button(cancel_btn, worker)
    
    def on_finished(success, message):
        print(f"Finished: success={success}, message={message}")
        app.quit()
    
    worker.finished.connect(on_finished)
    worker.progress.connect(lambda msg, pct: print(f"{msg} ({pct:.1f}%)"))
    
    cancel_btn.show()
    worker.start()
    
    sys.exit(app.exec())
