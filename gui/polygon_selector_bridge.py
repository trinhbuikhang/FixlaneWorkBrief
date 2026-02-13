"""
Bridge between Polygon Selector web UI (JavaScript) and Python processing.
Used by the Polygon Selector tab.
"""
import json
import logging
import threading
from pathlib import Path

from PyQt6.QtCore import QObject, pyqtSignal, pyqtSlot
from PyQt6.QtWidgets import QFileDialog

logger = logging.getLogger(__name__)

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
    logger.warning("Polygon processor not available: %s", e)

    def _get_col_by_lower(*args, **kwargs):
        return None

    def process_folder_batch(*args, **kwargs):
        pass

    def process_single_csv_against_polygons(*args, **kwargs):
        return []

    def read_csv_as_strings(*args, **kwargs):
        return None

    def save_per_polygon_results(*args, **kwargs):
        pass

    def validate_polygon_file(*args, **kwargs):
        return None


class PolygonSelectorBridge(QObject):
    """Bridge for file operations and polygon processing from the web UI."""

    progress_updated = pyqtSignal(str)
    processing_completed = pyqtSignal(str)
    error_occurred = pyqtSignal(str)
    file_selected = pyqtSignal(str, dict)

    def __init__(self):
        super().__init__()
        self.reset_state()

    def reset_state(self) -> None:
        self.polygon_file_path = None
        self.data_folder_path = None
        self.single_file_path = None

    @pyqtSlot(str)
    def select_polygon_file(self, message: str) -> None:
        path, _ = QFileDialog.getOpenFileName(None, "Select Polygon CSV File", "", "CSV files (*.csv)")
        if path:
            self.polygon_file_path = Path(path)
            self.file_selected.emit("polygon_file_selected", {"path": path, "name": self.polygon_file_path.name})

    @pyqtSlot(str)
    def select_data_folder(self, message: str) -> None:
        folder_path = QFileDialog.getExistingDirectory(None, "Select Data Folder")
        if folder_path:
            self.data_folder_path = Path(folder_path)
            csv_files = list(self.data_folder_path.glob("*.csv"))
            self.file_selected.emit("data_folder_selected", {
                "path": folder_path,
                "name": self.data_folder_path.name,
                "csv_count": len(csv_files),
            })

    @pyqtSlot(str)
    def select_single_file(self, message: str) -> None:
        path, _ = QFileDialog.getOpenFileName(None, "Select CSV Data File", "", "CSV files (*.csv)")
        if path:
            self.single_file_path = Path(path)
            self.file_selected.emit("single_file_selected", {"path": path, "name": self.single_file_path.name})

    @pyqtSlot()
    def clear_files(self) -> None:
        self.reset_state()
        self.file_selected.emit("files_cleared", {})

    @pyqtSlot(str)
    def log_message(self, message: str) -> None:
        logger.debug("JS: %s", message)

    @pyqtSlot(str)
    def start_single_processing(self, message: str) -> None:
        self._process_single_file()

    @pyqtSlot(str)
    def start_batch_processing(self, message: str) -> None:
        self._process_batch()

    def _process_single_file(self) -> None:
        if not self.polygon_file_path or not self.single_file_path:
            self.error_occurred.emit("Please select both polygon file and data file first")
            return

        def run() -> None:
            try:
                self.progress_updated.emit("Starting single file processing...")
                polygon_df = validate_polygon_file(self.polygon_file_path, full_validate=False)
                data_df = read_csv_as_strings(self.single_file_path)
                lon_col = _get_col_by_lower(data_df, "longitude") or _get_col_by_lower(data_df, "lon")
                lat_col = _get_col_by_lower(data_df, "latitude") or _get_col_by_lower(data_df, "lat")
                if not lon_col or not lat_col:
                    raise ValueError(f"Missing longitude/latitude columns in {self.single_file_path.name}")
                self.progress_updated.emit(f"Using columns: {lon_col}, {lat_col}")
                results = process_single_csv_against_polygons(data_df, polygon_df, lon_col, lat_col)
                output_dir = self.single_file_path.parent / "single_file_results"
                save_per_polygon_results(results, self.single_file_path, output_dir)
                self.processing_completed.emit(str(output_dir))
            except Exception as e:
                self.error_occurred.emit(f"Single file processing failed: {e}")

        threading.Thread(target=run, daemon=True).start()

    def _process_batch(self) -> None:
        if not self.polygon_file_path or not self.data_folder_path:
            self.error_occurred.emit("Please select both polygon file and data folder first")
            return

        def run() -> None:
            try:
                self.progress_updated.emit("Starting batch processing...")
                batch_output_path = [None]

                def output_cb(msg: str) -> None:
                    self.progress_updated.emit(msg)
                    if "Results saved in:" in msg:
                        batch_output_path[0] = msg.split("Results saved in:", 1)[-1].strip()

                process_folder_batch(
                    self.data_folder_path,
                    self.polygon_file_path,
                    validate_all=False,
                    output_cb=output_cb,
                )
                out = batch_output_path[0] or str(self.data_folder_path / "batch_results")
                self.processing_completed.emit(out)
            except Exception as e:
                self.error_occurred.emit(f"Batch processing failed: {e}")

        threading.Thread(target=run, daemon=True).start()

    @pyqtSlot(str)
    def clearCache(self, message: str = "") -> None:
        try:
            self.reset_state()
            self.processing_completed.emit("Cache cleared.")
        except Exception as e:
            self.error_occurred.emit(str(e))
