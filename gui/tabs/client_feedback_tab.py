import sys
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QLineEdit, QPushButton, QFileDialog, QTextEdit,
    QProgressBar, QMessageBox, QGroupBox, QListWidget,
    QListWidgetItem, QCheckBox
)
from PyQt6.QtCore import Qt
import logging
import polars as pl
import os

try:
    from utils.client_feedback_processor import ClientFeedbackProcessor
except ImportError as e:
    print(f"ERROR: Failed to import client feedback modules: {e}")
    print(f"Current working directory: {os.getcwd()}")
    print(f"Python path: {sys.path}")
    raise

class QTextEditHandler(logging.Handler):
    """Custom logging handler to write logs to QTextEdit."""
    def __init__(self, text_edit):
        super().__init__()
        self.text_edit = text_edit

    def emit(self, record):
        msg = self.format(record)
        self.text_edit.append(msg)

class ClientFeedbackTab(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()

    def initUI(self):
        layout = QVBoxLayout()
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(15)

        # Title
        title_label = QLabel("Client Feedback Processor")
        title_label.setObjectName("titleLabel")
        layout.addWidget(title_label)

        # Description
        desc_label = QLabel("This tool adds client feedback data to LMD files by matching road sections and adding treatment information.")
        desc_label.setWordWrap(True)
        desc_label.setObjectName("descriptionLabel")
        layout.addWidget(desc_label)

        # File inputs
        files_group = QGroupBox("File Selection")
        files_layout = QVBoxLayout()

        # Combined LMD file (always required)
        lmd_layout = QHBoxLayout()
        lmd_layout.setSpacing(10)
        lmd_label = QLabel("Combined LMD:")
        lmd_label.setFixedWidth(140)
        lmd_layout.addWidget(lmd_label)

        self.lmd_edit = QLineEdit()
        self.lmd_edit.setPlaceholderText("Select combined LMD CSV file")
        self.lmd_edit.setMinimumWidth(300)
        lmd_layout.addWidget(self.lmd_edit, 1)

        self.lmd_btn = QPushButton("Browse...")
        self.lmd_btn.setFixedWidth(100)
        self.lmd_btn.clicked.connect(self.select_lmd_file)
        lmd_layout.addWidget(self.lmd_btn)
        files_layout.addLayout(lmd_layout)

        # Client feedback file
        feedback_layout = QHBoxLayout()
        feedback_layout.setSpacing(10)
        feedback_label = QLabel("Client Feedback:")
        feedback_label.setFixedWidth(140)
        feedback_layout.addWidget(feedback_label)

        self.feedback_edit = QLineEdit()
        self.feedback_edit.setText(r"J:\Processing\AT Rehab AWTs\AT Gen Mtce - West CSS - checked_chainage_km.csv")  # Default path
        self.feedback_edit.setMinimumWidth(300)
        feedback_layout.addWidget(self.feedback_edit, 1)

        self.feedback_btn = QPushButton("Browse...")
        self.feedback_btn.setFixedWidth(100)
        self.feedback_btn.clicked.connect(self.select_feedback_file)
        feedback_layout.addWidget(self.feedback_btn)
        files_layout.addLayout(feedback_layout)

        # Output file section
        output_layout = QHBoxLayout()
        output_layout.setSpacing(10)

        output_label = QLabel("Output:")
        output_label.setFixedWidth(140)
        output_layout.addWidget(output_label)

        self.output_edit = QLineEdit()
        self.output_edit.setPlaceholderText("Select where to save processed data")
        self.output_edit.setMinimumWidth(300)
        output_layout.addWidget(self.output_edit, 1)

        self.output_btn = QPushButton("Browse...")
        self.output_btn.setFixedWidth(100)
        self.output_btn.clicked.connect(self.select_output)
        output_layout.addWidget(self.output_btn)
        files_layout.addLayout(output_layout)

        files_group.setLayout(files_layout)
        layout.addWidget(files_group)

        # Column selection
        columns_group = QGroupBox("Client Feedback Columns")
        columns_layout = QVBoxLayout()
        
        columns_label = QLabel("Available columns in client feedback file:")
        columns_layout.addWidget(columns_label)
        
        self.columns_list = QListWidget()
        self.columns_list.setMaximumHeight(150)
        columns_layout.addWidget(self.columns_list)
        
        columns_group.setLayout(columns_layout)
        layout.addWidget(columns_group)

        # Process button
        self.process_btn = QPushButton("Process Client Feedback")
        self.process_btn.setObjectName("processButton")
        self.process_btn.clicked.connect(self.process_data)
        layout.addWidget(self.process_btn)

        # Progress bar
        self.progress = QProgressBar()
        self.progress.setValue(0)
        layout.addWidget(self.progress)

        # Status label for current operation
        self.status_label = QLabel("Ready")
        self.status_label.setObjectName("statusLabel")
        layout.addWidget(self.status_label)

        # Log section
        layout.addWidget(QLabel("Processing Log:"))
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        layout.addWidget(self.log_text)

        self.setLayout(layout)

    def load_columns(self, file_path):
        """Load and display columns from client feedback file."""
        try:
            if not os.path.exists(file_path):
                self.columns_list.clear()
                return
            
            # Read the first row to get column names, ignore errors
            df = pl.read_csv(file_path, n_rows=1, ignore_errors=True)
            columns = df.columns
            
            self.columns_list.clear()
            for col in columns:
                item = QListWidgetItem(col)
                item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
                item.setCheckState(Qt.CheckState.Unchecked)
                self.columns_list.addItem(item)
                
        except Exception as e:
            self.columns_list.clear()
            QMessageBox.warning(self, "Error", f"Failed to load columns: {str(e)}")

    def get_selected_columns(self):
        """Get list of selected columns from the columns list."""
        selected_columns = []
        for i in range(self.columns_list.count()):
            item = self.columns_list.item(i)
            if item.checkState() == Qt.CheckState.Checked:
                selected_columns.append(item.text())
        return selected_columns

    def _emit_progress(self, message: str, progress: float = None):
        """Emit progress update to GUI."""
        if progress is not None:
            self.progress.setValue(int(progress))
        self.log_text.append(message)
        # Force GUI update to prevent freezing
        from PyQt6.QtWidgets import QApplication
        QApplication.processEvents()

    def select_lmd_file(self):
        file_name, _ = QFileDialog.getOpenFileName(
            self, "Select Combined LMD CSV File", "", "CSV Files (*.csv);;All Files (*)"
        )
        if file_name:
            self.lmd_edit.setText(file_name)
            # Auto-suggest output file
            import os
            base_name = os.path.splitext(file_name)[0]
            self.output_edit.setText(f"{base_name}_with_feedback.csv")

    def select_feedback_file(self):
        file_name, _ = QFileDialog.getOpenFileName(
            self, "Select Client Feedback CSV File", "", "CSV Files (*.csv);;All Files (*)"
        )
        if file_name:
            self.feedback_edit.setText(file_name)
            self.load_columns(file_name)

    def select_output(self):
        file_name, _ = QFileDialog.getSaveFileName(
            self, "Select Output CSV File", "", "CSV Files (*.csv);;All Files (*)"
        )
        if file_name:
            self.output_edit.setText(file_name)

    def process_data(self):
        lmd_file = self.lmd_edit.text().strip()
        feedback_file = self.feedback_edit.text().strip()
        output_file = self.output_edit.text().strip()

        # Validate inputs
        if not lmd_file:
            QMessageBox.warning(self, "Input Required", "Please select a Combined LMD file.")
            return

        if not feedback_file:
            QMessageBox.warning(self, "Input Required", "Please select a Client Feedback file.")
            return

        if not output_file:
            QMessageBox.warning(self, "Output Required", "Please specify an output file.")
            return

        # Check file existence
        if not os.path.exists(lmd_file):
            QMessageBox.critical(self, "File Not Found", f"Combined LMD file does not exist: {lmd_file}")
            return

        if not os.path.exists(feedback_file):
            QMessageBox.critical(self, "File Not Found", f"Client feedback file does not exist: {feedback_file}")
            return

        self.progress.setValue(0)
        self.log_text.clear()
        self.status_label.setText("Initializing...")
        self.process_btn.setEnabled(False)
        self.process_btn.setText("Processing Client Feedback...")

        # Set up logging to QTextEdit
        handler = QTextEditHandler(self.log_text)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger = logging.getLogger()
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        try:
            self.progress.setValue(10)

            # Create progress callback
            def progress_callback(message, progress=None):
                if progress is not None:
                    self.progress.setValue(int(progress))
                # Update status label with current operation
                if "Progress:" in message:
                    progress_info = message.split(" - ")[0] if " - " in message else message
                    self.status_label.setText(f"Processing: {progress_info}")
                elif "Status:" in message:
                    self.status_label.setText(message.replace("Status: ", ""))
                elif any(keyword in message.lower() for keyword in ["starting", "loading", "initializing", "completed", "processing"]):
                    self.status_label.setText(message)
                # Only log important messages, not every progress update
                if "Progress:" not in message and "Processing records:" not in message:
                    self.log_text.append(message)
                # Force GUI update to prevent freezing
                from PyQt6.QtWidgets import QApplication
                QApplication.processEvents()

            result = None

            logging.info("Starting client feedback processing...")
            processor = ClientFeedbackProcessor(progress_callback)

            # Always use chunking for better memory efficiency
            chunk_size = 10000  # Default chunk size for large files
            # Get selected extra columns
            extra_columns = self.get_selected_columns()
            if extra_columns:
                logging.info(f"Adding extra columns: {extra_columns}")
            
            self._emit_progress(f"Processing with chunking (chunk size: {chunk_size})...")
            processed_df = processor.process_with_chunking(lmd_file, feedback_file, chunk_size, extra_columns)

            if processed_df is not None:
                # Prepare data for CSV output with proper boolean formatting
                processed_df = processor._prepare_for_csv_output(processed_df)
                # Write CSV and remove trailing newline
                processed_df.write_csv(output_file)
                # Remove trailing newline if it exists
                with open(output_file, 'rb+') as f:
                    f.seek(0, 2)  # Seek to end
                    if f.tell() > 0:
                        f.seek(-1, 2)  # Seek to last character
                        if f.read(1) == b'\n':
                            f.seek(-1, 2)
                            f.truncate()  # Remove the trailing newline
                result = output_file
                logging.info("Client feedback processing completed")
            else:
                logging.error("Client feedback processing returned None")

            if result:
                self.progress.setValue(100)
                self.status_label.setText("✅ Complete")
                QMessageBox.information(self, "Success", "Client feedback processing completed successfully!")
                logging.info("Client feedback processing completed successfully")
            else:
                self.progress.setValue(0)
                self.status_label.setText("❌ Failed")
                error_msg = "Client feedback processing failed. Check the log for details."
                QMessageBox.critical(self, "Processing Failed", error_msg)
                logging.error(error_msg)

        except Exception as e:
            error_msg = f"An error occurred during processing: {str(e)}"
            logging.error(error_msg)
            logging.error(f"Error type: {type(e).__name__}")
            import traceback
            logging.error(f"Traceback: {traceback.format_exc()}")
            QMessageBox.critical(self, "Processing Error", error_msg)
            self.progress.setValue(0)
            self.status_label.setText("❌ Error occurred")

        finally:
            logger.removeHandler(handler)
            self.process_btn.setEnabled(True)
            self.process_btn.setText("Process Client Feedback")
            if self.progress.value() != 100 and self.progress.value() != 0:
                self.status_label.setText("Ready")