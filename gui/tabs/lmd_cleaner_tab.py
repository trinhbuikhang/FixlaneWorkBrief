import gc
import logging
import os
import sys
import time

from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtWidgets import (
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from utils.data_processor import process_data
from utils.security import SecurityValidator, UserFriendlyError


class QTextEditHandler(logging.Handler):
    """Custom logging handler to write logs to QTextEdit."""
    def __init__(self, text_edit):
        super().__init__()
        self.text_edit = text_edit

    def emit(self, record):
        msg = self.format(record)
        self.text_edit.append(msg)
        # Flush UI updates immediately while processing
        from PyQt6.QtWidgets import QApplication
        QApplication.processEvents()

class ProcessingWorker(QThread):
    log_message = pyqtSignal(str)
    done = pyqtSignal(bool, str)

    def __init__(self, input_file: str, output_file: str, timeout: int = 7200):
        super().__init__()
        self.input_file = input_file
        self.output_file = output_file
        self.timeout = timeout
        self._is_cancelled = False
        self._start_time = None

    def cancel(self):
        """Request cancellation of processing"""
        self._is_cancelled = True

    def run(self):
        import traceback
        self._start_time = time.time()
        temp_files = []
        
        try:
            # Process with timeout awareness
            process_data(self.input_file, self.output_file, self.log_message.emit)
            
            # Check if operation took too long
            elapsed = time.time() - self._start_time
            if elapsed > self.timeout:
                raise TimeoutError(f"Processing timeout after {self.timeout}s")
            
            if self._is_cancelled:
                self.done.emit(False, "Processing cancelled by user")
            else:
                self.done.emit(True, "")
                
        except MemoryError as e:
            error_msg = UserFriendlyError.format_error(
                e, 
                "Out of memory. Try using streaming mode or closing other applications.",
                "LMD Data Processing"
            )
            self.done.emit(False, error_msg)
            logging.error(f"Memory error during processing: {e}")
            
        except TimeoutError as e:
            error_msg = UserFriendlyError.format_error(
                e,
                f"Processing timeout. Operation took longer than {self.timeout//60} minutes.",
                "LMD Data Processing"
            )
            self.done.emit(False, error_msg)
            logging.error(f"Timeout during processing: {e}")
            
        except (FileNotFoundError, PermissionError, OSError) as e:
            error_msg = UserFriendlyError.format_error(e, context="LMD Data Processing")
            self.done.emit(False, error_msg)
            logging.error(f"File system error: {e}")
            
        except KeyboardInterrupt:
            self.done.emit(False, "Processing interrupted by user")
            logging.warning("Processing interrupted by user")
            
        except Exception as e:
            error_msg = UserFriendlyError.format_error(e, context="LMD Data Processing")
            self.done.emit(False, error_msg)
            logging.error(f"Unexpected error: {traceback.format_exc()}")
            
        finally:
            # Clean up resources
            for temp_file in temp_files:
                try:
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
                except:
                    pass
            
            gc.collect()  # Force garbage collection


class LMDCleanerTab(QWidget):
    def __init__(self):
        super().__init__()
        self.processing_active = False
        self.worker = None
        self.initUI()
    
    def update_status(self, message):
        """Safely update status label if it exists"""
        if self.status_label is not None:
            self.status_label.setText(message)

    def initUI(self):
        layout = QVBoxLayout()
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(15)

        # Title
        title_label = QLabel("LMD Data Cleaner")
        title_label.setObjectName("titleLabel")
        layout.addWidget(title_label)

        # Description
        desc_label = QLabel(
            "This tool cleans LMD data by applying the following filters:\n"
            "- Remove rows with empty RawSlope170 and RawSlope270\n"
            "- Remove rows where TrailingFactor < 0.15\n"
            "- Remove rows where abs(tsdSlopeMinY)/tsdSlopeMaxY < 0.15\n"
            "- Remove rows where Lane contains 'SK'\n"
            "- Remove rows where Ignore is true"
        )
        desc_label.setWordWrap(True)
        desc_label.setObjectName("descriptionLabel")
        layout.addWidget(desc_label)

        # File Selection GroupBox
        files_group = QGroupBox("File Selection")
        files_layout = QVBoxLayout(files_group)
        files_layout.setSpacing(10)

        # Input file section
        input_layout = QHBoxLayout()
        input_layout.setSpacing(10)

        # Fixed width label for alignment
        input_label = QLabel("Input:")
        input_label.setFixedWidth(140)  # Standardized width for consistency
        input_layout.addWidget(input_label)

        self.input_edit = QLineEdit()
        self.input_edit.setPlaceholderText("Select the combined_lmd CSV file")
        self.input_edit.setMinimumWidth(300)  # Minimum width for consistency
        input_layout.addWidget(self.input_edit, 1)  # Stretch factor 1

        self.input_btn = QPushButton("Browse...")
        self.input_btn.setFixedWidth(100)  # Fixed width for button
        self.input_btn.clicked.connect(self.select_input)
        input_layout.addWidget(self.input_btn)

        files_layout.addLayout(input_layout)

        # Output file section
        output_layout = QHBoxLayout()
        output_layout.setSpacing(10)

        # Fixed width label for alignment (same as input)
        output_label = QLabel("Output:")
        output_label.setFixedWidth(140)  # Standardized width for consistency
        output_layout.addWidget(output_label)

        self.output_edit = QLineEdit()
        self.output_edit.setPlaceholderText("Select where to save cleaned data")
        self.output_edit.setMinimumWidth(300)  # Same minimum width as input
        output_layout.addWidget(self.output_edit, 1)  # Stretch factor 1

        self.output_btn = QPushButton("Browse...")
        self.output_btn.setFixedWidth(100)  # Same fixed width as input button
        self.output_btn.clicked.connect(self.select_output)
        output_layout.addWidget(self.output_btn)

        files_layout.addLayout(output_layout)
        layout.addWidget(files_group)

        # Process button
        self.process_btn = QPushButton("Process Data")
        self.process_btn.setObjectName("processButton")  # For special styling
        self.process_btn.clicked.connect(self.handle_process_click)
        layout.addWidget(self.process_btn)

        # Status label will be set by main window
        self.status_label = None

        # Log section
        layout.addWidget(QLabel("Processing Log:"))
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        layout.addWidget(self.log_text)

        # Progress bar - Slim and at bottom
        self.progress = QProgressBar()
        self.progress.setValue(0)
        self.progress.setMaximumHeight(8)  # Make it slim and elegant
        layout.addWidget(self.progress)

        self.setLayout(layout)

    def select_input(self):
        file_name, _ = QFileDialog.getOpenFileName(
            self, "Select Input CSV File", "", "CSV Files (*.csv);;All Files (*)"
        )
        if file_name:
            # Validate file path
            is_valid, error_msg, validated_path = SecurityValidator.sanitize_file_path(file_name)
            
            if not is_valid:
                QMessageBox.critical(self, "Invalid File", error_msg)
                logging.error(f"File validation failed: {error_msg}")
                return
            
            self.input_edit.setText(str(validated_path))
            
            # Auto-suggest output file
            import os
            base_name = os.path.splitext(str(validated_path))[0]
            self.output_edit.setText(f"{base_name}_cleaned.csv")

    def select_output(self):
        file_name, _ = QFileDialog.getSaveFileName(
            self, "Select Output CSV File", "", "CSV Files (*.csv);;All Files (*)"
        )
        if file_name:
            # Validate output path
            is_valid, error_msg, validated_path = SecurityValidator.validate_output_path(file_name)
            
            if not is_valid:
                QMessageBox.critical(self, "Invalid Output Path", error_msg)
                logging.error(f"Output path validation failed: {error_msg}")
                return
            
            self.output_edit.setText(str(validated_path))

    def handle_process_click(self):
        """Handle process button click - start processing or (placeholder) cancel"""
        if self.processing_active:
            # Placeholder: real cancellation would require cooperative checks inside worker
            self.log_text.append("Cancel requested, but cancellation is not implemented for streaming yet.")
            QMessageBox.information(self, "Cancel", "Cancellation during streaming is not supported yet.")
        else:
            self.process_data()

    def reset_process_button(self):
        """Reset process button to initial state"""
        self.process_btn.setText("Process Data")
        self.process_btn.setObjectName("processButton")
        self.process_btn.setStyleSheet("")
        self.process_btn.setEnabled(True)
        self.process_btn.style().unpolish(self.process_btn)
        self.process_btn.style().polish(self.process_btn)

    def _append_log_from_worker(self, message: str):
        """Safely append log from worker thread"""
        self.log_text.append(message)
        from PyQt6.QtWidgets import QApplication
        QApplication.processEvents()

    def _worker_finished(self, success: bool, error_message: str):
        """Handle worker completion"""
        if success:
            self.progress.setValue(100)
            QMessageBox.information(self, "Success", "Data processing completed successfully!")
        else:
            self.progress.setValue(0)
            self.log_text.append(f"Processing error: {error_message}")
            QMessageBox.critical(self, "Processing Error", f"An error occurred during processing:\n{error_message}")

        self.processing_active = False
        self.worker = None
        self.reset_process_button()

    def process_data(self):
        input_file = self.input_edit.text().strip()
        output_file = self.output_edit.text().strip()

        if not input_file:
            QMessageBox.warning(self, "Input Required", "Please select an input CSV file.")
            return
        if not output_file:
            QMessageBox.warning(self, "Output Required", "Please specify an output CSV file.")
            return

        import os
        if not os.path.exists(input_file):
            QMessageBox.critical(self, "File Not Found", f"Input file does not exist: {input_file}")
            return

        self.progress.setValue(0)
        self.log_text.clear()
        self.processing_active = True
        self.process_btn.setText("Cancel")
        self.process_btn.setObjectName("warningButton")
        self.process_btn.setStyleSheet("")
        self.process_btn.style().unpolish(self.process_btn)
        self.process_btn.style().polish(self.process_btn)

        # Start worker thread to keep UI responsive
        self.worker = ProcessingWorker(input_file, output_file)
        self.worker.log_message.connect(self._append_log_from_worker)
        self.worker.done.connect(self._worker_finished)
        self.worker.start()
