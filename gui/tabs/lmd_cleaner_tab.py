import gc
import logging
import os
import time
from datetime import datetime
from pathlib import Path

from PyQt6.QtCore import QThread, pyqtSignal
from PyQt6.QtWidgets import (
    QCheckBox,
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QRadioButton,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from utils.data_processor import process_data, merge_then_clean_folder
from utils.security import SecurityValidator, UserFriendlyError

# ---------------------------------------------------------------------------
# File-based progress log so progress can be tracked even when the GUI
# is unresponsive or when running as exe.  Lives next to the application logs.
# ---------------------------------------------------------------------------
_file_logger = None

def _get_file_logger() -> logging.Logger:
    """Lazy-create a dedicated file logger for LMD processing progress."""
    global _file_logger
    if _file_logger is not None:
        return _file_logger

    _file_logger = logging.getLogger("lmd_cleaner_progress")
    _file_logger.setLevel(logging.DEBUG)
    _file_logger.propagate = False  # don't duplicate to root

    try:
        # Put log next to application logs
        from config.app_config import AppConfig
        log_dir = AppConfig.LOG_DIR
    except Exception:
        log_dir = Path(__file__).resolve().parent.parent.parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "lmd_processing.log"

    handler = logging.FileHandler(str(log_path), mode="a", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s  %(message)s"))
    _file_logger.addHandler(handler)
    _file_logger.info("=" * 60)
    _file_logger.info("LMD Processing log initialised  (file: %s)", log_path)
    return _file_logger


class QTextEditHandler(logging.Handler):
    """Custom logging handler to write logs to QTextEdit. Throttles processEvents to reduce UI overhead."""
    _last_process_events = 0
    _throttle_ms = 80  # Max ~12 processEvents/sec instead of hundreds

    def __init__(self, text_edit):
        super().__init__()
        self.text_edit = text_edit

    def emit(self, record):
        msg = self.format(record)
        self.text_edit.append(msg)
        # Throttle processEvents to avoid slowing down processing (was called every log line)
        from PyQt6.QtWidgets import QApplication
        now_ms = int(time.time() * 1000)
        if now_ms - self._last_process_events >= self._throttle_ms:
            self._last_process_events = now_ms
            QApplication.processEvents()

class ProcessingWorker(QThread):
    """
    Background worker for data processing.

    **No data-processing logic lives here** – everything is delegated to
    ``utils.data_processor`` (``process_data`` for single files,
    ``merge_and_clean_folder`` for folder mode).
    """

    log_message = pyqtSignal(str)
    progress_percent = pyqtSignal(int)  # 0-100 for progress bar
    done = pyqtSignal(bool, str)

    def __init__(self, input_path: str, output_file: str,
                 is_folder: bool = False, timeout: int = 7200):
        super().__init__()
        self.input_path = input_path
        self.output_file = output_file
        self.is_folder = is_folder
        self.timeout = timeout
        self._is_cancelled = False
        self._start_time = None

    # -- helpers ----------------------------------------------------------

    def _log(self, message: str):
        """Emit timestamped line to GUI + on-disk log."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_message.emit(f"[{timestamp}] {message}")
        try:
            _get_file_logger().info(message)
        except Exception:
            pass

    def cancel(self):
        """Request cancellation."""
        self._is_cancelled = True

    def _is_cancel_requested(self) -> bool:
        return self._is_cancelled

    # -- main entry -------------------------------------------------------

    def run(self):
        import traceback
        self._start_time = time.time()

        # Throttle GUI updates to reduce overhead (progress + log) during heavy processing
        _last_progress_emit = [0.0]  # use list so closure can mutate
        _last_log_emit = [0.0]
        _log_throttle_sec = 0.15   # batch log to GUI at most ~6–7/sec
        _progress_throttle_sec = 0.2  # progress bar at most 5/sec

        def progress_callback(msg, percent=None):
            now = time.time()
            self.log_message.emit(msg)  # always emit so log is complete; slot side throttles processEvents
            if percent is not None and (now - _last_progress_emit[0]) >= _progress_throttle_sec:
                _last_progress_emit[0] = now
                self.progress_percent.emit(min(100, max(0, int(percent))))

        try:
            if self.is_folder:
                self._log("Processing folder (fast merge then clean)...")
                merge_then_clean_folder(
                    folder_path=self.input_path,
                    output_file=self.output_file,
                    progress_callback=progress_callback,
                    cancel_check=self._is_cancel_requested,
                )
                self._log("Folder processing complete")
            else:
                self._log("Processing single file...")
                process_data(
                    self.input_path,
                    self.output_file,
                    progress_callback=progress_callback,
                )

            # Timeout guard
            elapsed = time.time() - self._start_time
            if elapsed > self.timeout:
                raise TimeoutError(f"Processing timeout after {self.timeout}s")

            if self._is_cancelled:
                self.done.emit(False, "Processing cancelled by user")
            else:
                self.progress_percent.emit(100)  # Ensure bar reaches 100% (throttle may have skipped last update)
                self.done.emit(True, "")

        except MemoryError as e:
            error_msg = UserFriendlyError.format_error(
                e,
                "Out of memory. Try closing other applications.",
                "LMD Data Processing",
            )
            self.done.emit(False, error_msg)
            logging.error("Memory error during processing: %s", e)

        except TimeoutError as e:
            error_msg = UserFriendlyError.format_error(
                e,
                f"Processing timeout ({self.timeout // 60} min).",
                "LMD Data Processing",
            )
            self.done.emit(False, error_msg)
            logging.error("Timeout during processing: %s", e)

        except (FileNotFoundError, PermissionError, OSError) as e:
            error_msg = UserFriendlyError.format_error(
                e, context="LMD Data Processing"
            )
            self.done.emit(False, error_msg)
            logging.error("File system error: %s", e)

        except KeyboardInterrupt:
            self.done.emit(False, "Processing interrupted by user")
            logging.warning("Processing interrupted by user")

        except Exception as e:
            error_msg = UserFriendlyError.format_error(
                e, context="LMD Data Processing"
            )
            self.done.emit(False, error_msg)
            logging.error("Unexpected error: %s", traceback.format_exc())

        finally:
            gc.collect()
            try:
                _get_file_logger().info(
                    "ProcessingWorker.run() finished  (folder=%s)", self.is_folder
                )
            except Exception:
                pass


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

        # Input Type Selection
        type_group = QGroupBox("Input Type")
        type_layout = QHBoxLayout(type_group)
        type_layout.setSpacing(20)

        self.file_radio = QRadioButton("Single CSV File")
        self.file_radio.setChecked(True)
        self.folder_radio = QRadioButton("Folder with CSV Files")

        type_layout.addWidget(self.file_radio)
        type_layout.addWidget(self.folder_radio)
        type_layout.addStretch()
        layout.addWidget(type_group)

        # File Selection GroupBox
        files_group = QGroupBox("File/Folder Selection")
        files_layout = QVBoxLayout(files_group)
        files_layout.setSpacing(10)

        # Input section
        input_layout = QHBoxLayout()
        input_layout.setSpacing(10)

        input_label = QLabel("Input:")
        input_label.setFixedWidth(140)
        input_layout.addWidget(input_label)

        self.input_edit = QLineEdit()
        self.input_edit.setPlaceholderText("Select CSV file or folder containing CSV files")
        self.input_edit.setMinimumWidth(300)
        input_layout.addWidget(self.input_edit, 1)

        self.input_btn = QPushButton("Browse...")
        self.input_btn.setFixedWidth(100)
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

        # Skip confirmation (faster repeat runs)
        self.skip_confirm_cb = QCheckBox("Skip confirmation dialog (this session)")
        self.skip_confirm_cb.setToolTip("When checked, Process starts immediately without asking to confirm.")
        layout.addWidget(self.skip_confirm_cb)

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
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        self.progress.setMaximumHeight(8)  # Make it slim and elegant
        self.progress.setFormat("%p%")
        layout.addWidget(self.progress)

        self.setLayout(layout)

    def select_input(self):
        if self.file_radio.isChecked():
            # Select single file (default dir from env or empty)
            default_dir = os.environ.get("DATA_CLEANER_INPUT_DIR", "")
            if default_dir and not os.path.isdir(default_dir):
                default_dir = ""
            if not default_dir:
                default_dir = os.path.expanduser("~") if os.path.isdir(os.path.expanduser("~")) else ""
            file_name, _ = QFileDialog.getOpenFileName(
                self, "Select Input CSV File", default_dir, "CSV Files (*.csv);;All Files (*)"
            )
            if file_name:
                is_valid, error_msg, validated_path = SecurityValidator.sanitize_file_path(file_name)
                if not is_valid:
                    QMessageBox.critical(self, "Invalid File", error_msg)
                    return
                self.input_edit.setText(str(validated_path))
                # Auto-suggest output
                base_name = os.path.splitext(str(validated_path))[0]
                self.output_edit.setText(f"{base_name}_cleaned.csv")
        else:
            # Select folder
            folder_name = QFileDialog.getExistingDirectory(
                self, "Select Folder Containing CSV Files", ""
            )
            if folder_name:
                # Validate folder path manually
                try:
                    from pathlib import Path
                    folder_path = Path(folder_name).resolve()
                    if not folder_path.exists():
                        QMessageBox.critical(self, "Invalid Folder", "Folder does not exist")
                        return
                    if not folder_path.is_dir():
                        QMessageBox.critical(self, "Invalid Folder", "Selected path is not a directory")
                        return
                    if not os.access(folder_path, os.R_OK):
                        QMessageBox.critical(self, "Invalid Folder", "Folder is not readable")
                        return
                    self.input_edit.setText(str(folder_path))
                    # Auto-suggest output: place the merged file inside the same folder
                    output_name = f"{folder_path.name}_merged_cleaned.csv"
                    self.output_edit.setText(str(folder_path / output_name))
                except Exception as e:
                    QMessageBox.critical(self, "Invalid Folder", f"Error validating folder: {str(e)}")
                    return

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

    # Max lines to keep in log widget (avoid slowdown with very long runs)
    _LOG_MAX_LINES = 800
    _log_process_events_count = 0
    _log_process_events_interval = 6  # Call processEvents only every N messages

    def _append_log_from_worker(self, message: str):
        """Safely append log from worker thread; trim old lines and throttle processEvents."""
        self.log_text.append(message)
        # Trim if too long so QTextEdit doesn't slow down
        doc = self.log_text.document()
        if doc.blockCount() > self._LOG_MAX_LINES:
            from PyQt6.QtGui import QTextCursor
            cursor = QTextCursor(doc)
            cursor.movePosition(QTextCursor.MoveOperation.Start)
            cursor.movePosition(
                QTextCursor.MoveOperation.Down,
                QTextCursor.MoveMode.KeepAnchor,
                doc.blockCount() - self._LOG_MAX_LINES,
            )
            cursor.removeSelectedText()
        self._log_process_events_count += 1
        if self._log_process_events_count >= self._log_process_events_interval:
            self._log_process_events_count = 0
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
        input_path = self.input_edit.text().strip()
        output_file = self.output_edit.text().strip()

        if not input_path:
            QMessageBox.warning(self, "Input Required", "Please select an input file or folder.")
            return
        if not output_file:
            QMessageBox.warning(self, "Output Required", "Please specify an output CSV file.")
            return

        # Detect input type
        is_folder = False
        if os.path.isdir(input_path):
            is_folder = True
        elif os.path.isfile(input_path):
            is_folder = False
        else:
            QMessageBox.critical(self, "Invalid Input", f"Input path does not exist: {input_path}")
            return

        # Show confirmation dialog
        if is_folder:
            # Count CSV files in folder
            csv_count = len([f for f in os.listdir(input_path) if f.lower().endswith('.csv')])
            if csv_count == 0:
                QMessageBox.critical(self, "No CSV Files", f"No CSV files found in folder: {input_path}")
                return

            msg = (f"Folder Processing Mode (Fast merge then clean)\n\n"
                   f"Found {csv_count} CSV file(s) in the selected folder.\n"
                   f"The system will:\n"
                   f"1. Fast merge: combine all CSVs (byte copy, header from first file)\n"
                   f"2. Clean once: filter and deduplicate the combined file\n\n"
                   f"Faster than cleaning each file then merging.\n\n"
                   f"Continue?")
        else:
            msg = ("Single File Processing Mode\n\n"
                   f"The system will clean and deduplicate the selected CSV file.\n\n"
                   f"Continue with file processing?")

        if not self.skip_confirm_cb.isChecked():
            reply = QMessageBox.question(
                self, "Confirm Processing Mode",
                msg,
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.Yes
            )
            if reply != QMessageBox.StandardButton.Yes:
                return

        self.progress.setValue(0)
        self.log_text.clear()
        self._log_process_events_count = 0
        self.processing_active = True
        self.process_btn.setText("Cancel")
        self.process_btn.setObjectName("warningButton")
        self.process_btn.setStyleSheet("")
        self.process_btn.style().unpolish(self.process_btn)
        self.process_btn.style().polish(self.process_btn)

        # Start worker thread to keep UI responsive
        self.worker = ProcessingWorker(input_path, output_file, is_folder)
        self.worker.log_message.connect(self._append_log_from_worker)
        self.worker.progress_percent.connect(self.progress.setValue)
        self.worker.done.connect(self._worker_finished)
        self.worker.start()
