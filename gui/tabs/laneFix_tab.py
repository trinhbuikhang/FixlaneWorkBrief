import sys
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QLineEdit, QPushButton, QFileDialog, QTextEdit,
    QProgressBar, QMessageBox, QComboBox, QCheckBox,
    QGroupBox, QRadioButton, QButtonGroup
)
from PyQt6.QtCore import Qt
import logging
import polars as pl
import os

try:
    from config.laneFix_config import Config, Messages
    from utils.laneFix_polar_data_processor import (
        PolarsLaneFixProcessor,
        PolarsWorkbriefProcessor,
        PolarsCombinedProcessor
    )
except ImportError as e:
    print(f"ERROR: Failed to import laneFix modules: {e}")
    print(f"Current working directory: {os.getcwd()}")
    print(f"Python path: {sys.path}")
    # Re-raise the error so the application fails with proper error message
    raise

class QTextEditHandler(logging.Handler):
    """Custom logging handler to write logs to QTextEdit."""
    def __init__(self, text_edit):
        super().__init__()
        self.text_edit = text_edit

    def emit(self, record):
        msg = self.format(record)
        self.text_edit.append(msg)

class LaneFixTab(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()

    def initUI(self):
        layout = QVBoxLayout()
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(15)

        # Title
        title_label = QLabel("Lane Fix Processor")
        title_label.setObjectName("titleLabel")
        layout.addWidget(title_label)

        # Processing mode selection
        mode_group = QGroupBox("Processing Mode")
        mode_layout = QVBoxLayout()

        self.mode_group = QButtonGroup()
        self.lane_fix_only_radio = QRadioButton("Lane Fixes Only (Combined LMD + Lane Fixes)")
        self.workbrief_only_radio = QRadioButton("Workbrief Only (Combined LMD + Workbrief)")
        self.combined_radio = QRadioButton("Complete Processing (All three files)")

        self.combined_radio.setChecked(True)  # Default to complete processing

        self.mode_group.addButton(self.lane_fix_only_radio, 1)
        self.mode_group.addButton(self.workbrief_only_radio, 2)
        self.mode_group.addButton(self.combined_radio, 3)

        mode_layout.addWidget(self.lane_fix_only_radio)
        mode_layout.addWidget(self.workbrief_only_radio)
        mode_layout.addWidget(self.combined_radio)
        mode_group.setLayout(mode_layout)
        layout.addWidget(mode_group)

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

        # Lane fixes file
        lane_layout = QHBoxLayout()
        lane_layout.setSpacing(10)
        lane_label = QLabel("Lane Fixes:")
        lane_label.setFixedWidth(140)
        lane_layout.addWidget(lane_label)

        self.lane_edit = QLineEdit()
        self.lane_edit.setPlaceholderText("Select lane fixes CSV file")
        self.lane_edit.setMinimumWidth(300)
        lane_layout.addWidget(self.lane_edit, 1)

        self.lane_btn = QPushButton("Browse...")
        self.lane_btn.setFixedWidth(100)
        self.lane_btn.clicked.connect(self.select_lane_file)
        lane_layout.addWidget(self.lane_btn)
        files_layout.addLayout(lane_layout)

        # Workbrief file
        workbrief_layout = QHBoxLayout()
        workbrief_layout.setSpacing(10)
        workbrief_label = QLabel("Workbrief:")
        workbrief_label.setFixedWidth(140)
        workbrief_layout.addWidget(workbrief_label)

        self.workbrief_edit = QLineEdit()
        self.workbrief_edit.setPlaceholderText("Select workbrief CSV file")
        self.workbrief_edit.setMinimumWidth(300)
        workbrief_layout.addWidget(self.workbrief_edit, 1)

        self.workbrief_btn = QPushButton("Browse...")
        self.workbrief_btn.setFixedWidth(100)
        self.workbrief_btn.clicked.connect(self.select_workbrief_file)
        workbrief_layout.addWidget(self.workbrief_btn)
        files_layout.addLayout(workbrief_layout)

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

        # Process button
        self.process_btn = QPushButton("Process Lane Fixes")
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

        # Update UI based on selected mode
        self.mode_group.buttonClicked.connect(self.update_ui_for_mode)
        self.update_ui_for_mode()

    def _emit_progress(self, message: str, progress: float = None):
        """Emit progress update to GUI."""
        if progress is not None:
            self.progress.setValue(int(progress))
        self.log_text.append(message)
        # Force GUI update to prevent freezing
        from PyQt6.QtWidgets import QApplication
        QApplication.processEvents()

    def update_ui_for_mode(self):
        """Update UI elements based on selected processing mode."""
        mode = self.mode_group.checkedId()

        if mode == 1:  # Lane fixes only
            self.lane_edit.setEnabled(True)
            self.lane_btn.setEnabled(True)
            self.workbrief_edit.setEnabled(False)
            self.workbrief_btn.setEnabled(False)
            self.process_btn.setText("Process Lane Fixes")
        elif mode == 2:  # Workbrief only
            self.lane_edit.setEnabled(False)
            self.lane_btn.setEnabled(False)
            self.workbrief_edit.setEnabled(True)
            self.workbrief_btn.setEnabled(True)
            self.process_btn.setText("Process Workbrief")
        else:  # Combined processing
            self.lane_edit.setEnabled(True)
            self.lane_btn.setEnabled(True)
            self.workbrief_edit.setEnabled(True)
            self.workbrief_btn.setEnabled(True)
            self.process_btn.setText("Complete Processing")

    def select_lmd_file(self):
        file_name, _ = QFileDialog.getOpenFileName(
            self, "Select Combined LMD CSV File", "", "CSV Files (*.csv);;All Files (*)"
        )
        if file_name:
            self.lmd_edit.setText(file_name)
            # Auto-suggest output file
            import os
            base_name = os.path.splitext(file_name)[0]
            mode = self.mode_group.checkedId()
            suffix = "lane_fixed" if mode == 1 else "workbrief_processed" if mode == 2 else "complete_processed"
            self.output_edit.setText(f"{base_name}_{suffix}.csv")

    def select_lane_file(self):
        file_name, _ = QFileDialog.getOpenFileName(
            self, "Select Lane Fixes CSV File", "", "CSV Files (*.csv);;All Files (*)"
        )
        if file_name:
            self.lane_edit.setText(file_name)

    def select_workbrief_file(self):
        file_name, _ = QFileDialog.getOpenFileName(
            self, "Select Workbrief CSV File", "", "CSV Files (*.csv);;All Files (*)"
        )
        if file_name:
            self.workbrief_edit.setText(file_name)

    def select_output(self):
        file_name, _ = QFileDialog.getSaveFileName(
            self, "Select Output CSV File", "", "CSV Files (*.csv);;All Files (*)"
        )
        if file_name:
            self.output_edit.setText(file_name)

    def process_data(self):
        lmd_file = self.lmd_edit.text().strip()
        lane_file = self.lane_edit.text().strip()
        workbrief_file = self.workbrief_edit.text().strip()
        output_file = self.output_edit.text().strip()

        mode = self.mode_group.checkedId()

        # Validate inputs based on mode
        if not lmd_file:
            QMessageBox.warning(self, "Input Required", "Please select a Combined LMD file.")
            return

        if mode == 1 and not lane_file:  # Lane fixes only
            QMessageBox.warning(self, "Input Required", "Please select a Lane Fixes file.")
            return

        if mode == 2 and not workbrief_file:  # Workbrief only
            QMessageBox.warning(self, "Input Required", "Please select a Workbrief file.")
            return

        if mode == 3 and (not lane_file or not workbrief_file):  # Combined
            QMessageBox.warning(self, "Input Required", "Please select both Lane Fixes and Workbrief files.")
            return

        if not output_file:
            QMessageBox.warning(self, "Output Required", "Please specify an output file.")
            return

        # Check file existence
        if not os.path.exists(lmd_file):
            QMessageBox.critical(self, "File Not Found", f"Combined LMD file does not exist: {lmd_file}")
            return

        if mode in [1, 3] and not os.path.exists(lane_file):
            QMessageBox.critical(self, "File Not Found", f"Lane fixes file does not exist: {lane_file}")
            return

        if mode in [2, 3] and not os.path.exists(workbrief_file):
            QMessageBox.critical(self, "File Not Found", f"Workbrief file does not exist: {workbrief_file}")
            return

        self.progress.setValue(0)
        self.log_text.clear()
        self.status_label.setText("Initializing...")
        self.process_btn.setEnabled(False)

        mode_text = "Lane Fixes" if mode == 1 else "Workbrief" if mode == 2 else "Complete Processing"
        self.process_btn.setText(f"Processing {mode_text}...")

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
                    # Extract progress info for status
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

            if mode == 1:  # Lane fixes only
                logging.info("Starting lane fixes processing...")
                self._emit_progress("Initializing lane fixes processor...")
                processor = PolarsLaneFixProcessor(progress_callback)
                result = processor.process(lane_file, lmd_file)
                logging.info("Lane fixes processing completed")
            elif mode == 2:  # Workbrief only
                logging.info("Starting workbrief processing...")
                self._emit_progress("Loading LMD data for workbrief processing...")
                # For workbrief processing, we need to read the LMD data first
                lmd_df = pl.read_csv(lmd_file, ignore_errors=True, infer_schema_length=0)
                processor = PolarsWorkbriefProcessor(progress_callback)
                processed_df = processor.process_in_memory(lmd_df, workbrief_file)
                if processed_df is not None:
                    # Prepare data for CSV output with proper boolean formatting
                    processed_df = processor._prepare_for_csv_output(processed_df)
                    processed_df.write_csv(output_file)
                    result = output_file
                    logging.info("Workbrief processing completed")
                else:
                    logging.error("Workbrief processing returned None")
            else:  # Combined processing
                logging.info("Starting combined processing...")
                self._emit_progress("Initializing combined processor...")
                processor = PolarsCombinedProcessor(progress_callback)
                result = processor.process(lmd_file, lane_file, workbrief_file)
                logging.info("Combined processing completed")

            if result:
                self.progress.setValue(100)
                self.status_label.setText("✅ Complete")
                QMessageBox.information(self, "Success", f"{mode_text} completed successfully!")
                logging.info(f"{mode_text} completed successfully")
            else:
                self.progress.setValue(0)
                self.status_label.setText("❌ Failed")
                error_msg = f"{mode_text} failed. Check the log for details."
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
            mode_text = "Lane Fixes" if mode == 1 else "Workbrief" if mode == 2 else "Complete Processing"
            self.process_btn.setText(f"Process {mode_text}")
            if self.progress.value() != 100 and self.progress.value() != 0:
                self.status_label.setText("Ready")