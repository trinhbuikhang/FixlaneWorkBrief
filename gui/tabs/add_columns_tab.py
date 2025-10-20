import sys
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QPushButton, QFileDialog, QListWidget, QLineEdit,
    QProgressBar, QMessageBox, QFrame, QTextEdit
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
import polars as pl
import os
from datetime import datetime
import gc

try:
    from utils.add_columns_processor import AddColumnsProcessor
except ImportError as e:
    print(f"ERROR: Failed to import add columns processor: {e}")
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

    def initUI(self):
        layout = QVBoxLayout()
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(15)

        # Button to select Combined Details file (moved to top)
        details_layout = QHBoxLayout()
        details_layout.setSpacing(10)

        details_label = QLabel("Combined Details:")
        details_label.setFixedWidth(110)  # Slightly reduced width for better balance
        details_layout.addWidget(details_label)

        self.details_edit = QLineEdit()
        self.details_edit.setPlaceholderText("Select Combined Details CSV file")
        self.details_edit.setMinimumWidth(250)
        self.details_edit.setReadOnly(True)  # Make it read-only like input field
        details_layout.addWidget(self.details_edit, 1)  # Stretch factor 1

        self.details_button = QPushButton("Browse...")
        self.details_button.setFixedWidth(100)  # Increased width for better text display
        self.details_button.clicked.connect(self.select_combined_details_file)
        details_layout.addWidget(self.details_button)

        layout.addLayout(details_layout)

        # Button to select Combined_LMD file
        lmd_layout = QHBoxLayout()
        lmd_layout.setSpacing(10)

        lmd_label = QLabel("Combined LMD:")
        lmd_label.setFixedWidth(110)  # Same width as details label for consistency
        lmd_layout.addWidget(lmd_label)

        self.lmd_edit = QLineEdit()
        self.lmd_edit.setPlaceholderText("Select Combined LMD CSV file")
        self.lmd_edit.setMinimumWidth(250)
        self.lmd_edit.setReadOnly(True)  # Make it read-only like input field
        lmd_layout.addWidget(self.lmd_edit, 1)  # Stretch factor 1

        self.open_button = QPushButton("Browse...")
        self.open_button.setFixedWidth(100)  # Increased width for better text display
        self.open_button.clicked.connect(self.open_combined_lmd)
        lmd_layout.addWidget(self.open_button)

        layout.addLayout(lmd_layout)

        # Instruction label
        label = QLabel("Select columns from Combined_LMD to add to Combined Details")
        label.setWordWrap(True)
        label.setObjectName("descriptionLabel")  # Match styling with other tabs
        layout.addWidget(label)

        # Frame containing ListWidget
        listbox_frame = QFrame()
        listbox_layout = QVBoxLayout()
        listbox_frame.setLayout(listbox_layout)
        layout.addWidget(listbox_frame)

        # Column selection controls
        column_controls_layout = QHBoxLayout()
        column_controls_layout.addWidget(QLabel("Available Columns:"))
        column_controls_layout.addStretch()
        
        self.select_all_btn = QPushButton("Select All")
        self.select_all_btn.setMinimumWidth(90)  # Set minimum width for proper text display
        self.select_all_btn.clicked.connect(self.select_all_columns)
        column_controls_layout.addWidget(self.select_all_btn)
        
        self.clear_all_btn = QPushButton("Clear All")
        self.clear_all_btn.setMinimumWidth(90)  # Set minimum width for proper text display
        self.clear_all_btn.clicked.connect(self.clear_all_columns)
        column_controls_layout.addWidget(self.clear_all_btn)
        
        listbox_layout.addLayout(column_controls_layout)

        self.column_listbox = QListWidget()
        self.column_listbox.setSelectionMode(QListWidget.SelectionMode.MultiSelection)
        # Remove height limit to allow expansion
        # self.column_listbox.setMaximumHeight(200)
        self.column_listbox.itemSelectionChanged.connect(self.update_selection_count)
        listbox_layout.addWidget(self.column_listbox)

        # Selection count label
        self.selection_label = QLabel("Selected: 0 columns")
        listbox_layout.addWidget(self.selection_label)

        # Buttons
        button_layout = QHBoxLayout()
        self.update_button = QPushButton("Update Combined Details")
        self.update_button.setObjectName("processButton")  # Match styling with other process buttons
        self.update_button.clicked.connect(self.update_combined_details)
        self.cancel_button = QPushButton("Cancel")
        self.cancel_button.setMinimumWidth(80)  # Set minimum width for proper text display
        self.cancel_button.clicked.connect(self.cancel_processing)
        self.cancel_button.setEnabled(False)
        button_layout.addWidget(self.update_button)
        button_layout.addWidget(self.cancel_button)
        button_layout.addStretch()
        layout.addLayout(button_layout)

        # Status and progress
        self.status_label = QLabel("Ready")
        self.status_label.setObjectName("statusLabel")  # Match styling with other tabs
        layout.addWidget(self.status_label)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 0)  # Indeterminate
        self.progress_bar.setVisible(False)  # Hide initially
        layout.addWidget(self.progress_bar)

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
                self.last_directory["lmd"] = os.path.dirname(file_path)
                self.combined_lmd = {"file_path": file_path}

                if not os.path.exists(file_path):
                    raise FileNotFoundError(f"File not found: {file_path}")

                if not os.access(file_path, os.R_OK):
                    raise PermissionError(f"No permission to read file: {file_path}")

                # Read only the header
                temp_df = pl.read_csv(file_path, n_rows=0, null_values=['∞'], infer_schema_length=0)
                self.update_column_listbox(temp_df)

                self.lmd_edit.setText(os.path.basename(file_path))  # Update QLineEdit with filename
                QMessageBox.information(self, "Success", f"Combined_LMD file loaded: {os.path.basename(file_path)}")

            except FileNotFoundError as e:
                QMessageBox.critical(self, "File Error", str(e))
            except PermissionError as e:
                QMessageBox.critical(self, "Permission Error", str(e))
            except pl.exceptions.NoDataError:
                QMessageBox.critical(self, "Data Error", "The file is empty or has no valid CSV content")
            except Exception as e:
                QMessageBox.critical(self, "Error", f"An error occurred while reading the Combined_LMD file: {e}")

    def update_column_listbox(self, temp_df):
        self.column_listbox.clear()
        for column in temp_df.columns:
            self.column_listbox.addItem(column)
        self.update_selection_count()

    def select_all_columns(self):
        """Select all columns in the list."""
        for i in range(self.column_listbox.count()):
            self.column_listbox.item(i).setSelected(True)

    def clear_all_columns(self):
        """Clear all column selections."""
        self.column_listbox.clearSelection()

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
                if not os.path.exists(file_path):
                    raise FileNotFoundError(f"File not found: {file_path}")

                if not os.access(file_path, os.R_OK):
                    raise PermissionError(f"No permission to read file: {file_path}")

                self.combined_details_file = file_path
                self.last_directory["details"] = os.path.dirname(file_path)
                self.details_edit.setText(os.path.basename(file_path))  # Update QLineEdit instead of QLabel
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Error selecting file: {e}")

    def update_combined_details(self):
        if self.processing_active:
            QMessageBox.information(self, "Process Running", "A process is already running. Please wait for it to complete.")
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

        self.status_label.setText("Processing, please wait...")
        self.progress_bar.setVisible(True)  # Show progress bar when processing starts
        self.progress_bar.setRange(0, 0)  # Indeterminate
        self.update_button.setEnabled(False)
        self.cancel_button.setEnabled(True)
        self.processing_active = True

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
            self.progress_bar.setVisible(False)  # Hide progress bar when cancelling
            self.status_label.setText("Cancelling... Please wait")
            QMessageBox.information(self, "Processing Cancelled", "Processing will be cancelled after the current chunk completes.")

    def update_progress(self, message, progress=None):
        self.status_label.setText(message)
        if progress is not None:
            self.progress_bar.setRange(0, 100)
            self.progress_bar.setValue(int(progress))

    def processing_finished(self, output_filename):
        self.processing_active = False
        self.progress_bar.setVisible(False)  # Hide progress bar when finished
        self.progress_bar.setRange(0, 0)
        self.status_label.setText("Ready")
        self.update_button.setEnabled(True)
        self.cancel_button.setEnabled(False)
        if output_filename:
            QMessageBox.information(self, "Success", f"Combined Details updated successfully! File saved as {output_filename}")
        gc.collect()

    def processing_error(self, error_msg):
        self.processing_active = False
        self.progress_bar.setVisible(False)  # Hide progress bar on error
        self.progress_bar.setRange(0, 0)
        self.status_label.setText("Ready")
        self.update_button.setEnabled(True)
        self.cancel_button.setEnabled(False)
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

    def run(self):
        try:
            def progress_callback(message, progress=None):
                self.progress.emit(message, progress)

            processor = AddColumnsProcessor(progress_callback)
            output_file_path = processor.process_files(self.lmd_file_path, self.details_file, self.selected_columns, self.chunk_size)

            if output_file_path:
                output_filename = os.path.basename(output_file_path)
                self.finished.emit(output_filename)
            else:
                self.finished.emit("")

        except Exception as e:
            self.error.emit(str(e))
