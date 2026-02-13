"""
Polygon Selector tab: point-in-polygon processing.
Native PyQt6 UI (same pattern as LMD Cleaner / Add Columns).
Requires: shapely, polars (no PyQt6-WebEngine).
"""
import logging
import os
from datetime import datetime
from pathlib import Path

from PyQt6.QtCore import QThread, pyqtSignal
from PyQt6.QtWidgets import (
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

from gui.ui_constants import (
    BROWSE_BUTTON_WIDTH,
    GROUP_SPACING,
    INPUT_MIN_WIDTH,
    LABEL_FIXED_WIDTH,
    LAYOUT_MARGINS,
    LAYOUT_SPACING,
)

logger = logging.getLogger(__name__)

# Default folder for Polygon CSV (WKT) file selection when no previous path was used
DEFAULT_POLYGON_FOLDER = r"J:\- RPP Calibrations\RPP Regions"


class PolygonWorker(QThread):
    """Background worker for polygon processing (single file or batch)."""

    log_message = pyqtSignal(str)
    progress_percent = pyqtSignal(int)
    done = pyqtSignal(bool, str)

    def __init__(self, polygon_path: str, data_path: str, is_batch: bool):
        super().__init__()
        self.polygon_path = Path(polygon_path)
        self.data_path = Path(data_path)
        self.is_batch = is_batch

    def _log(self, msg: str) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_message.emit(f"[{ts}] {msg}")

    def run(self) -> None:
        try:
            from utils.polygon_processor import (
                _get_col_by_lower,
                process_folder_batch,
                process_single_csv_against_polygons,
                read_csv_as_strings,
                save_per_polygon_results,
                validate_polygon_file,
            )
        except ImportError as e:
            self.log_message.emit(f"Error: Missing dependency - {e}")
            self.done.emit(False, "Install shapely and polars.")
            return

        try:
            self.progress_percent.emit(0)
            if self.is_batch:
                self._log("Starting batch processing...")
                out_path = [None]

                def output_cb(msg: str) -> None:
                    self._log(msg)
                    if "Results saved in:" in msg:
                        out_path[0] = msg.split("Results saved in:", 1)[-1].strip()

                def progress_cb(percent: int) -> None:
                    self.progress_percent.emit(min(100, max(0, percent)))

                process_folder_batch(
                    self.data_path,
                    self.polygon_path,
                    validate_all=False,
                    output_cb=output_cb,
                    progress_cb=progress_cb,
                )
                result = out_path[0] or str(self.data_path / "batch_results")
                self._log(f"Batch complete. Results: {result}")
                self.progress_percent.emit(100)
                self.done.emit(True, result)
            else:
                self._log("Starting single file processing...")
                self.progress_percent.emit(10)
                polygon_df = validate_polygon_file(self.polygon_path, full_validate=False)
                self.progress_percent.emit(25)
                data_df = read_csv_as_strings(self.data_path)
                lon_col = _get_col_by_lower(data_df, "longitude") or _get_col_by_lower(data_df, "lon")
                lat_col = _get_col_by_lower(data_df, "latitude") or _get_col_by_lower(data_df, "lat")
                if not lon_col or not lat_col:
                    raise ValueError("Data file must have longitude/latitude (or lon/lat) columns.")
                self._log(f"Using columns: {lon_col}, {lat_col}")
                self.progress_percent.emit(40)
                results = process_single_csv_against_polygons(data_df, polygon_df, lon_col, lat_col)
                self.progress_percent.emit(70)
                output_dir = self.data_path.parent / "single_file_results"
                save_per_polygon_results(results, self.data_path, output_dir)
                self._log(f"Single file complete. Results: {output_dir}")
                self.progress_percent.emit(100)
                self.done.emit(True, str(output_dir))
        except Exception as e:
            logger.exception("Polygon processing failed")
            self._log(f"Error: {e}")
            self.done.emit(False, str(e))


class PolygonSelectorTab(QWidget):
    """Native PyQt6 tab for Polygon Selector (point-in-polygon)."""

    def __init__(self):
        super().__init__()
        self.status_label = None
        self.worker = None
        self._last_dir = {"polygon": "", "data": ""}
        self.init_ui()

    def update_status(self, message: str) -> None:
        if self.status_label:
            self.status_label.setText(message)

    def init_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(*LAYOUT_MARGINS)
        layout.setSpacing(LAYOUT_SPACING)

        title = QLabel("Polygon Selector")
        title.setObjectName("titleLabel")
        layout.addWidget(title)

        # Mode
        mode_group = QGroupBox("Mode")
        mode_layout = QHBoxLayout(mode_group)
        self.batch_radio = QRadioButton("Batch (folder of CSV files)")
        self.batch_radio.setChecked(True)
        self.single_radio = QRadioButton("Single file")
        self.batch_radio.toggled.connect(self._on_mode_changed)
        mode_layout.addWidget(self.batch_radio)
        mode_layout.addWidget(self.single_radio)
        mode_layout.addStretch()
        layout.addWidget(mode_group)

        # File selection
        files_group = QGroupBox("File Selection")
        files_layout = QVBoxLayout(files_group)
        files_layout.setSpacing(GROUP_SPACING)

        # Polygon file (required for both modes)
        poly_layout = QHBoxLayout()
        poly_layout.setSpacing(GROUP_SPACING)
        poly_label = QLabel("Polygon CSV (WKT):")
        poly_label.setFixedWidth(LABEL_FIXED_WIDTH)
        poly_layout.addWidget(poly_label)
        self.polygon_edit = QLineEdit()
        self.polygon_edit.setPlaceholderText("CSV with WKT column (and optional id, CouncilName)")
        self.polygon_edit.setReadOnly(True)
        self.polygon_edit.setMinimumWidth(INPUT_MIN_WIDTH)
        poly_layout.addWidget(self.polygon_edit, 1)
        self.polygon_btn = QPushButton("Browse...")
        self.polygon_btn.setFixedWidth(BROWSE_BUTTON_WIDTH)
        self.polygon_btn.clicked.connect(self._select_polygon_file)
        poly_layout.addWidget(self.polygon_btn)
        files_layout.addLayout(poly_layout)

        # Data folder (batch) / Data file (single)
        data_layout = QHBoxLayout()
        data_layout.setSpacing(GROUP_SPACING)
        self.data_label = QLabel("Data folder:")
        self.data_label.setFixedWidth(LABEL_FIXED_WIDTH)
        data_layout.addWidget(self.data_label)
        self.data_edit = QLineEdit()
        self.data_edit.setPlaceholderText("Folder with CSV files (batch) or single CSV file")
        self.data_edit.setReadOnly(True)
        self.data_edit.setMinimumWidth(INPUT_MIN_WIDTH)
        data_layout.addWidget(self.data_edit, 1)
        self.data_btn = QPushButton("Browse...")
        self.data_btn.setFixedWidth(BROWSE_BUTTON_WIDTH)
        self.data_btn.clicked.connect(self._select_data)
        data_layout.addWidget(self.data_btn)
        files_layout.addLayout(data_layout)
        layout.addWidget(files_group)

        # Buttons
        btn_layout = QHBoxLayout()
        self.clear_btn = QPushButton("Clear paths")
        self.clear_btn.clicked.connect(self._clear_paths)
        self.process_btn = QPushButton("Process")
        self.process_btn.setObjectName("processButton")
        self.process_btn.clicked.connect(self._start_processing)
        btn_layout.addWidget(self.clear_btn)
        btn_layout.addWidget(self.process_btn)
        btn_layout.addStretch()
        layout.addLayout(btn_layout)

        # Log
        layout.addWidget(QLabel("Processing log:"))
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        layout.addWidget(self.log_text)

        # Progress
        self.progress = QProgressBar()
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        self.progress.setMaximumHeight(8)
        self.progress.setFormat("%p%")
        layout.addWidget(self.progress)

        self._on_mode_changed(True)

    def _on_mode_changed(self, batch: bool) -> None:
        if self.batch_radio.isChecked():
            self.data_label.setText("Data folder:")
            self.data_edit.setPlaceholderText("Folder containing CSV files")
        else:
            self.data_label.setText("Data file:")
            self.data_edit.setPlaceholderText("Single CSV file with longitude/latitude columns")

    def _select_polygon_file(self) -> None:
        start = self._last_dir["polygon"] or (DEFAULT_POLYGON_FOLDER if os.path.isdir(DEFAULT_POLYGON_FOLDER) else os.path.expanduser("~"))
        path, _ = QFileDialog.getOpenFileName(
            self, "Select Polygon CSV (WKT)", start, "CSV (*.csv);;All (*)"
        )
        if path:
            self._last_dir["polygon"] = str(Path(path).parent)
            self.polygon_edit.setText(path)

    def _select_data(self) -> None:
        start = self._last_dir["data"] or os.path.expanduser("~")
        if self.batch_radio.isChecked():
            path = QFileDialog.getExistingDirectory(self, "Select Data Folder", start)
        else:
            path, _ = QFileDialog.getOpenFileName(self, "Select Data CSV", start, "CSV (*.csv);;All (*)")
        if path:
            self._last_dir["data"] = str(Path(path).parent)
            self.data_edit.setText(path)

    def _clear_paths(self) -> None:
        self.polygon_edit.clear()
        self.data_edit.clear()
        self.update_status("Paths cleared")

    def _start_processing(self) -> None:
        polygon_path = self.polygon_edit.text().strip()
        data_path = self.data_edit.text().strip()
        if not polygon_path:
            QMessageBox.warning(self, "Polygon Selector", "Please select a polygon CSV file (with WKT column).")
            return
        if not data_path:
            QMessageBox.warning(
                self, "Polygon Selector",
                "Please select a data folder (batch) or data file (single).",
            )
            return
        polygon_p = Path(polygon_path)
        data_p = Path(data_path)
        is_batch = self.batch_radio.isChecked()
        if is_batch and not data_p.is_dir():
            QMessageBox.warning(self, "Polygon Selector", "In batch mode, data path must be a folder.")
            return
        if not is_batch and not data_p.is_file():
            QMessageBox.warning(self, "Polygon Selector", "In single mode, data path must be a file.")
            return
        if not polygon_p.is_file():
            QMessageBox.warning(self, "Polygon Selector", "Polygon file not found.")
            return

        self.log_text.clear()
        self.progress.setValue(0)
        self.process_btn.setEnabled(False)
        self.update_status("Processing...")

        self.worker = PolygonWorker(polygon_path, data_path, is_batch)
        self.worker.log_message.connect(self._append_log)
        self.worker.progress_percent.connect(self.progress.setValue)
        self.worker.done.connect(self._on_done)
        self.worker.start()

    def _append_log(self, msg: str) -> None:
        """Only append to the tab's Processing log. Status bar is updated only on start/done/error."""
        self.log_text.append(msg)

    def _on_done(self, success: bool, message: str) -> None:
        self.process_btn.setEnabled(True)
        if success:
            self.update_status(f"Done: {message[:60]}...")
            QMessageBox.information(self, "Polygon Selector", f"Processing completed.\nResults: {message}")
        else:
            self.update_status("Error")
            QMessageBox.critical(self, "Polygon Selector", f"Processing failed:\n{message}")
