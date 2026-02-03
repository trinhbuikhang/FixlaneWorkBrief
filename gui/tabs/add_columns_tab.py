import sys
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QPushButton, QFileDialog, QListWidget, QLineEdit,
    QProgressBar, QMessageBox, QFrame, QTextEdit,
    QSplitter, QGroupBox, QSpacerItem, QSizePolicy
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QFont
import polars as pl
import os
from datetime import datetime
import gc

try:
    from utils.add_columns_processor import AddColumnsProcessor
    from utils.memory_efficient_processor import MemoryEfficientAddColumnsProcessor
    from utils.security import SecurityValidator, UserFriendlyError
    import psutil
except ImportError as e:
    print(f"ERROR: Failed to import processors: {e}")
    print(f"Current working directory: {os.getcwd()}")
    import sys
    print(f"Python path: {sys.path}")
    raise

class AddColumnsTab(QWidget):
    def __init__(self):
        super().__init__()
        self.combined_lmd = None
        self.combined_details_file = None
        self.last_directory = {"lmd": "", "details": ""}
        self.DEFAULT_CHUNK_SIZE = 100000
        self.processing_active = False
        self.initUI()
    
    def update_status(self, message):
        """Safely update status label if it exists"""
        if self.status_label is not None:
            self.status_label.setText(message)

    def initUI(self):
        layout = QVBoxLayout()
        layout.setContentsMargins(20, 20, 20, 20)  # Standardized margins
        layout.setSpacing(15)  # Standardized spacing

        # Header Section - Compact
        header_layout = QVBoxLayout()
        header_layout.setSpacing(4)

        # Title
        title_label = QLabel("Add Columns Processor")
        title_label.setObjectName("titleLabel")
        header_layout.addWidget(title_label)

        # Description - More compact
        desc_label = QLabel("Add selected columns from LMD data using intelligent timestamp matching")
        desc_label.setWordWrap(True)
        desc_label.setObjectName("descriptionLabel")
        header_layout.addWidget(desc_label)

        layout.addLayout(header_layout)

        # File Selection - Standardized layout
        files_group = QGroupBox("File Selection")
        files_layout = QVBoxLayout()
        files_layout.setSpacing(10)

        # Details file
        details_layout = QHBoxLayout()
        details_layout.setSpacing(10)
        details_label = QLabel("Combined Details:")
        details_label.setFixedWidth(140)  # Standardized width
        details_layout.addWidget(details_label)

        self.details_edit = QLineEdit()
        self.details_edit.setPlaceholderText("Select main data file...")
        self.details_edit.setReadOnly(True)
        self.details_edit.setMinimumWidth(300)
        details_layout.addWidget(self.details_edit, 1)

        self.details_button = QPushButton("Browse...")
        self.details_button.setFixedWidth(100)  # Standardized width
        self.details_button.clicked.connect(self.select_combined_details_file)
        details_layout.addWidget(self.details_button)

        files_layout.addLayout(details_layout)

        # LMD file
        lmd_layout = QHBoxLayout()
        lmd_layout.setSpacing(10)
        lmd_label = QLabel("Combined LMD:")
        lmd_label.setFixedWidth(140)  # Standardized width
        lmd_layout.addWidget(lmd_label)

        self.lmd_edit = QLineEdit()
        self.lmd_edit.setPlaceholderText("Select source data file...")
        self.lmd_edit.setReadOnly(True)
        self.lmd_edit.setMinimumWidth(300)
        lmd_layout.addWidget(self.lmd_edit, 1)

        self.open_button = QPushButton("Browse...")
        self.open_button.setFixedWidth(100)  # Standardized width
        self.open_button.clicked.connect(self.open_combined_lmd)
        lmd_layout.addWidget(self.open_button)

        files_layout.addLayout(lmd_layout)

        files_group.setLayout(files_layout)
        layout.addWidget(files_group)

        # Process button - Standardized position
        self.process_button = QPushButton("Process Data")
        self.process_button.setObjectName("processButton")
        self.process_button.clicked.connect(self.handle_process_click)
        layout.addWidget(self.process_button)

        # Main Content with Splitter
        splitter = QSplitter(Qt.Orientation.Horizontal)
        
        # Left Panel - Column Selection
        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        left_layout.setContentsMargins(0, 0, 0, 0)
        
        # Column Selection Group
        columns_group = QGroupBox("Column Selection")
        columns_layout = QVBoxLayout()
        
        # Controls header
        controls_layout = QHBoxLayout()
        columns_header = QLabel("Available Columns")
        columns_header.setStyleSheet("font-weight: 600; font-size: 13px; color: #323130;")
        controls_layout.addWidget(columns_header)
        
        columns_layout.addLayout(controls_layout)

        # Column list
        self.column_listbox = QListWidget()
        self.column_listbox.setSelectionMode(QListWidget.SelectionMode.MultiSelection)
        self.column_listbox.itemSelectionChanged.connect(self.update_selection_count)
        columns_layout.addWidget(self.column_listbox)

        # Selection counter
        self.selection_label = QLabel("No columns available")
        self.selection_label.setStyleSheet("font-size: 11px; color: #605E5C; padding: 8px;")
        columns_layout.addWidget(self.selection_label)
        
        columns_group.setLayout(columns_layout)
        left_layout.addWidget(columns_group)
        
        # Right Panel - Processing Log
        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)
        right_layout.setContentsMargins(0, 0, 0, 0)
        
        # Log Group
        log_group = QGroupBox("Processing Log")
        log_layout = QVBoxLayout()
        
        self.log_text = QTextEdit()
        self.log_text.setObjectName("logArea")
        self.log_text.setReadOnly(True)
        self.log_text.setPlaceholderText("Processing logs will appear here...")
        log_layout.addWidget(self.log_text)
        
        log_group.setLayout(log_layout)
        right_layout.addWidget(log_group)
        
        # Add to splitter with standard proportions (40:60)
        splitter.addWidget(left_widget)
        splitter.addWidget(right_widget)
        splitter.setSizes([320, 480])  # 40% left, 60% right - consistent with Client Feedback
        splitter.setMinimumHeight(250)  # Ensure minimum usable height
        
        layout.addWidget(splitter, 1)  # Give splitter maximum available space

        # Progress bar - Slim and at bottom
        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)
        self.progress_bar.setMaximumHeight(8)  # Make it slim and elegant
        layout.addWidget(self.progress_bar)

        # Status label will be set by main window
        self.status_label = None

        self.setLayout(layout)

    def get_default_directory(self, file_type):
        """Get default directory for file selection."""
        # Check if J:\Processing exists
        processing_dir = "J:\\Processing"
        if os.path.exists(processing_dir):
            return processing_dir
        
        # Fall back to last used directory or root
        return self.last_directory.get(file_type, "/")

    def open_combined_lmd(self):
        initial_dir = self.get_default_directory("lmd")
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select Combined_LMD file", initial_dir, "CSV Files (*.csv)"
        )
        if file_path:
            try:
                # Validate file path
                is_valid, error_msg, validated_path = SecurityValidator.sanitize_file_path(file_path)
                
                if not is_valid:
                    QMessageBox.critical(self, "Invalid File", error_msg)
                    logging.error(f"LMD file validation failed: {error_msg}")
                    return
                
                self.last_directory["lmd"] = os.path.dirname(str(validated_path))
                self.combined_lmd = {"file_path": str(validated_path)}

                # Read only the header
                temp_df = pl.read_csv(str(validated_path), n_rows=0, null_values=['∞'], infer_schema_length=0)
                self.update_column_listbox(temp_df)

                self.lmd_edit.setText(os.path.basename(str(validated_path)))  # Update QLineEdit with filename
                QMessageBox.information(self, "Success", f"Combined_LMD file loaded: {os.path.basename(str(validated_path))}")

            except pl.exceptions.NoDataError:
                QMessageBox.critical(self, "Data Error", "The file is empty or has no valid CSV content")
            except Exception as e:
                error_msg = UserFriendlyError.format_error(e, context="Loading LMD file")
                QMessageBox.critical(self, "Error", error_msg)

    def update_column_listbox(self, temp_df):
        self.column_listbox.clear()
        for column in temp_df.columns:
            self.column_listbox.addItem(column)
        self.update_selection_count()

    def update_selection_count(self):
        """Update the selection count label."""
        selected_count = len(self.column_listbox.selectedItems())
        total_count = self.column_listbox.count()
        self.selection_label.setText(f"Selected: {selected_count}/{total_count} columns")

    def select_combined_details_file(self):
        initial_dir = self.get_default_directory("details")
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select Combined Details file", initial_dir, "CSV Files (*.csv)"
        )
        if file_path:
            try:
                # Validate file path
                is_valid, error_msg, validated_path = SecurityValidator.sanitize_file_path(file_path)
                
                if not is_valid:
                    QMessageBox.critical(self, "Invalid File", error_msg)
                    logging.error(f"Details file validation failed: {error_msg}")
                    return

                self.combined_details_file = str(validated_path)
                self.last_directory["details"] = os.path.dirname(str(validated_path))
                self.details_edit.setText(os.path.basename(str(validated_path)))  # Update QLineEdit instead of QLabel
                
            except Exception as e:
                error_msg = UserFriendlyError.format_error(e, context="Selecting Details file")
                QMessageBox.critical(self, "Error", error_msg)

    def handle_process_click(self):
        """Handle process button click - start processing or cancel if already running"""
        if self.processing_active:
            self.cancel_processing()
        else:
            self.update_combined_details()

    def update_combined_details(self):
        if self.processing_active:
            return

        if not self.combined_details_file:
            QMessageBox.warning(self, "Warning", "Please select a Combined Details file.")
            return

        if self.combined_lmd is None or "file_path" not in self.combined_lmd:
            QMessageBox.warning(self, "Warning", "Please load a Combined_LMD file first.")
            return

        selected_items = self.column_listbox.selectedItems()
        selected_columns = [item.text() for item in selected_items]
        if not selected_columns:
            QMessageBox.warning(self, "Warning", "Please select at least one column.")
            return

        # Use default chunk size
        chunk_size = self.DEFAULT_CHUNK_SIZE

        self.update_status("Processing, please wait...")
        self.progress_bar.setRange(0, 0)  # Indeterminate
        self.process_button.setText("Cancel")
        self.process_button.setObjectName("warningButton")
        self.process_button.setStyleSheet("")  # Clear style to apply new object name
        self.process_button.style().unpolish(self.process_button)
        self.process_button.style().polish(self.process_button)
        self.processing_active = True
        
        # Clear log and add start message
        self.log_text.clear()
        from datetime import datetime
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.append(f"[{timestamp}] Starting add columns processing...")
        self.log_text.append(f"[{timestamp}] Selected columns: {', '.join(selected_columns)}")

        lmd_file_path = self.combined_lmd["file_path"]

        # Start processing thread
        self.worker = ProcessingWorker(selected_columns, self.combined_details_file, lmd_file_path, chunk_size)
        self.worker.progress.connect(self.update_progress)
        self.worker.finished.connect(self.processing_finished)
        self.worker.error.connect(self.processing_error)
        self.worker.start()

    def cancel_processing(self):
        if self.processing_active:
            self.processing_active = False
            self.progress_bar.setRange(0, 100)  # Reset to normal range
            self.progress_bar.setValue(0)  # Reset progress
            self.update_status("Cancelling... Please wait")
            self.reset_process_button()
            QMessageBox.information(self, "Processing Cancelled", "Processing will be cancelled after the current chunk completes.")

    def reset_process_button(self):
        """Reset process button to initial state"""
        self.process_button.setText("Process Data")
        self.process_button.setObjectName("processButton")
        self.process_button.setStyleSheet("")  # Clear style to apply new object name
        self.process_button.style().unpolish(self.process_button)
        self.process_button.style().polish(self.process_button)

    def update_progress(self, message, progress=None):
        self.update_status(message)
        # Also append to log text
        from datetime import datetime
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.append(f"[{timestamp}] {message}")
        
        if progress is not None:
            self.progress_bar.setRange(0, 100)
            self.progress_bar.setValue(int(progress))
        
        # Force GUI update to show log immediately
        from PyQt6.QtWidgets import QApplication
        QApplication.processEvents()

    def processing_finished(self, output_filename):
        self.processing_active = False
        self.progress_bar.setRange(0, 100)  # Reset to normal range
        self.progress_bar.setValue(100)  # Show completion
        self.update_status("Ready")
        self.reset_process_button()
        
        # Add completion message to log
        from datetime import datetime
        timestamp = datetime.now().strftime("%H:%M:%S")
        if output_filename:
            self.log_text.append(f"[{timestamp}] ✓ Processing completed successfully!")
            self.log_text.append(f"[{timestamp}] Output file: {output_filename}")
            QMessageBox.information(self, "Success", f"Combined Details updated successfully! File saved as {output_filename}")
        else:
            self.log_text.append(f"[{timestamp}] ✗ Processing completed with errors")
        gc.collect()

    def processing_error(self, error_msg):
        self.processing_active = False
        self.progress_bar.setRange(0, 100)  # Reset to normal range
        self.progress_bar.setValue(0)  # Reset progress
        self.update_status("Ready")
        self.reset_process_button()
        
        # Add error message to log
        from datetime import datetime
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.append(f"[{timestamp}] ✗ ERROR: {error_msg}")
        
        QMessageBox.critical(self, "Error", f"An error occurred while processing the files: {error_msg}")
        gc.collect()

class ProcessingWorker(QThread):
    progress = pyqtSignal(str, int)
    finished = pyqtSignal(str)
    error = pyqtSignal(str)

    def __init__(self, selected_columns, details_file, lmd_file_path, chunk_size):
        super().__init__()
        self.selected_columns = selected_columns
        self.details_file = details_file
        self.lmd_file_path = lmd_file_path
        self.chunk_size = chunk_size

    def _choose_processor(self) -> tuple:
        """Choose appropriate processor based on file sizes and available memory."""
        try:
            # Get file sizes
            lmd_size = os.path.getsize(self.lmd_file_path) / (1024 ** 3)  # GB
            details_size = os.path.getsize(self.details_file) / (1024 ** 3)  # GB
            total_size = lmd_size + details_size
            
            # Get available memory
            virtual_memory = psutil.virtual_memory()
            available_gb = virtual_memory.available / (1024 ** 3)
            
            processor_type = "standard"
            reason = ""
            
            # Decision logic
            if total_size > available_gb * 0.6:  # If files > 60% of available memory
                processor_type = "memory_efficient"
                reason = f"Large files ({total_size:.1f}GB) vs available memory ({available_gb:.1f}GB)"
            elif lmd_size > 15:  # If LMD > 15GB
                processor_type = "memory_efficient"
                reason = f"Large LMD file ({lmd_size:.1f}GB)"
            elif total_size > 20:  # If total > 20GB
                processor_type = "memory_efficient"
                reason = f"Large total size ({total_size:.1f}GB)"
            else:
                reason = f"Files manageable ({total_size:.1f}GB) with {available_gb:.1f}GB available"
            
            return processor_type, reason, lmd_size, details_size, available_gb
            
        except Exception as e:
            # Fallback to standard processor if analysis fails
            return "standard", f"Analysis failed: {e}", 0, 0, 0

    def run(self):
        try:
            def progress_callback(message, progress=None):
                self.progress.emit(message, progress)

            # Choose appropriate processor
            processor_type, reason, lmd_size, details_size, available_gb = self._choose_processor()
            
            # Emit processor selection info
            self.progress.emit(f"🔍 PROCESSOR SELECTION:", None)
            self.progress.emit(f"   • LMD file: {lmd_size:.2f} GB", None)
            self.progress.emit(f"   • Details file: {details_size:.2f} GB", None)
            self.progress.emit(f"   • Available memory: {available_gb:.2f} GB", None)
            self.progress.emit(f"   • Selected: {processor_type.upper()} processor", None)
            self.progress.emit(f"   • Reason: {reason}", None)
            self.progress.emit("", None)
            
            # Create and run appropriate processor
            if processor_type == "memory_efficient":
                processor = MemoryEfficientAddColumnsProcessor(progress_callback)
                output_file_path = processor.process_add_columns(
                    self.lmd_file_path, self.details_file, self.selected_columns, self.chunk_size
                )
            else:
                processor = AddColumnsProcessor(progress_callback)
                output_file_path = processor.process_files(
                    self.lmd_file_path, self.details_file, self.selected_columns, self.chunk_size
                )

            if output_file_path:
                output_filename = os.path.basename(output_file_path)
                self.finished.emit(output_filename)
            else:
                self.finished.emit("")

        except Exception as e:
            self.error.emit(str(e))
