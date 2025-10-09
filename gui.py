"""
Modern PyQt6 GUI for Fixlane WorkBrief application.
Provides a clean, user-friendly interface for data processing operations.
"""

import sys
import os
import logging
import signal
from pathlib import Path
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
    QGridLayout, QPushButton, QLabel, QLineEdit, QFileDialog, 
    QTextEdit, QProgressBar, QTabWidget, QGroupBox, QMessageBox,
    QFrame, QSplitter, QMenuBar, QMenu, QStatusBar, QToolBar,
    QComboBox, QCheckBox
)
from PyQt6.QtCore import (
    Qt, QThread, pyqtSignal, QTimer, QSettings, QSize
)
from PyQt6.QtGui import (
    QFont, QIcon, QPalette, QColor, QAction, QPixmap
)

import logging
from config import Config, Messages
from timestamp_handler import timestamp_handler


logger = logging.getLogger(__name__)


class ProcessingThread(QThread):
    """Thread for running data processing operations."""
    
    progress_updated = pyqtSignal(str, float)
    finished = pyqtSignal(str)  # Output path or error message
    error_occurred = pyqtSignal(str)
    
    def __init__(self, processor, *args):
        super().__init__()
        self.processor = processor
        self.args = args
    
    def run(self):
        """Run the processing operation in a separate thread."""
        try:
            # Set up progress callback
            self.processor.progress_callback = self._progress_callback
            
            # Run the processor
            result = self.processor.process(*self.args)
            
            if result:
                self.finished.emit(result)
            else:
                self.error_occurred.emit("Processing failed - check logs for details")
                
        except Exception as e:
            logger.error(f"Processing thread error: {e}", exc_info=True)
            self.error_occurred.emit(str(e))
    
    def _progress_callback(self, message: str, progress: float = None):
        """Emit progress update."""
        if progress is None:
            progress = -1  # Indeterminate progress
        self.progress_updated.emit(message, progress)


class FileSelectionWidget(QGroupBox):
    """Widget for file selection with browse button."""
    
    def __init__(self, title: str, file_types: str = "CSV Files (*.csv)"):
        super().__init__(title)
        self.file_types = file_types
        self.setup_ui()
    
    def setup_ui(self):
        """Setup the file selection UI."""
        layout = QHBoxLayout()
        layout.setSpacing(8)  # Compact spacing
        layout.setContentsMargins(10, 8, 10, 8)  # Reduce margins
        
        self.path_edit = QLineEdit()
        self.path_edit.setPlaceholderText("Select file...")
        self.path_edit.setStyleSheet("""
            QLineEdit {
                padding: 6px;
                border: 1px solid #ddd;
                border-radius: 4px;
                font-size: 11px;
            }
            QLineEdit:focus {
                border: 2px solid #4CAF50;
            }
        """)
        
        self.browse_btn = QPushButton("Browse")
        self.browse_btn.clicked.connect(self.browse_file)
        self.browse_btn.setFixedSize(70, 28)  # Compact size
        self.browse_btn.setStyleSheet("""
            QPushButton {
                background-color: #f8f9fa;
                border: 1px solid #ddd;
                border-radius: 4px;
                padding: 4px 8px;
                font-size: 10px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #e9ecef;
                border-color: #adb5bd;
            }
            QPushButton:pressed {
                background-color: #dee2e6;
            }
        """)
        
        layout.addWidget(self.path_edit)
        layout.addWidget(self.browse_btn)
        
        self.setLayout(layout)
        
        # Modern group box styling
        self.setStyleSheet("""
            QGroupBox {
                font-weight: bold;
                font-size: 11px;
                border: 1px solid #ddd;
                border-radius: 6px;
                margin: 4px 0px;
                padding-top: 8px;
                background-color: #fafafa;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 8px;
                padding: 0 4px 0 4px;
                color: #333;
            }
        """)
    
    def browse_file(self):
        """Open file dialog to select file."""
        file_path, _ = QFileDialog.getOpenFileName(
            self, f"Select {self.title()}", "", self.file_types
        )
        if file_path:
            self.path_edit.setText(file_path)
    
    def get_path(self) -> str:
        """Get the selected file path."""
        return self.path_edit.text().strip()
    
    def set_path(self, path: str):
        """Set the file path."""
        self.path_edit.setText(path)
    
    def is_valid(self) -> bool:
        """Check if a valid file path is selected."""
        path = self.get_path()
        return path and os.path.exists(path)


class LogWidget(QTextEdit):
    """Custom log widget with formatting."""
    
    def __init__(self):
        super().__init__()
        self.setup_ui()
    
    def setup_ui(self):
        """Setup the log widget UI."""
        self.setReadOnly(True)
        self.setMaximumHeight(200)
        
        # Set monospace font
        font = QFont("Consolas", 9)
        font.setFixedPitch(True)
        self.setFont(font)
        
        # Set background color
        palette = self.palette()
        palette.setColor(QPalette.ColorRole.Base, QColor(248, 248, 248))
        self.setPalette(palette)
    
    def append_message(self, message: str, level: str = "INFO"):
        """Append a formatted log message."""
        from datetime import datetime
        
        timestamp = datetime.now().strftime("%H:%M:%S")
        
        # Color coding based on level
        if level == "ERROR":
            color = "red"
        elif level == "WARNING":
            color = "orange"
        elif level == "SUCCESS":
            color = "green"
        else:
            color = "black"
        
        formatted_msg = f'<span style="color: gray">[{timestamp}]</span> ' \
                       f'<span style="color: {color}; font-weight: bold">{level}:</span> ' \
                       f'<span style="color: {color}">{message}</span>'
        
        self.append(formatted_msg)
        
        # Auto-scroll to bottom
        self.verticalScrollBar().setValue(
            self.verticalScrollBar().maximum()
        )


class CombinedProcessingTab(QWidget):
    """Tab widget for combined lane fixing and workbrief processing."""
    
    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window
        self.processing_thread = None
        self.setup_ui()
    
    def setup_ui(self):
        """Setup the combined processing tab UI."""
        layout = QVBoxLayout()
        layout.setSpacing(6)  # Compact vertical spacing
        layout.setContentsMargins(12, 12, 12, 8)  # Modern margins
        
        # File selection section with compact layout
        files_group = QGroupBox("Input Files")
        files_group.setStyleSheet("""
            QGroupBox {
                font-weight: bold;
                font-size: 12px;
                border: 2px solid #e9ecef;
                border-radius: 8px;
                margin: 8px 0px;
                padding-top: 12px;
                background-color: #ffffff;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 12px;
                padding: 0 6px 0 6px;
                color: #495057;
            }
        """)
        
        files_layout = QVBoxLayout()
        files_layout.setSpacing(4)  # Tight spacing between file widgets
        files_layout.setContentsMargins(8, 8, 8, 8)
        
        # Combined LMD file first (main data file to be processed)
        self.combined_lmd_widget = FileSelectionWidget("1. Combined LMD File (Main Data)")
        self.combined_lmd_widget.setToolTip("Select the main LMD data file that contains the lane information to be updated")
        
        # Lane fixes file second (correction data file)
        self.lane_fixes_widget = FileSelectionWidget("2. Lane Fixes File (Corrections)")  
        self.lane_fixes_widget.setToolTip("Select the lane fixes file that contains correction data (From, To, Lane, Ignore columns)")
        
        # Workbrief file third (final processing)
        self.workbrief_widget = FileSelectionWidget("3. Workbrief File (Final Processing)")
        self.workbrief_widget.setToolTip("Select the workbrief file for final data processing")
        
        files_layout.addWidget(self.combined_lmd_widget)
        files_layout.addWidget(self.lane_fixes_widget)
        files_layout.addWidget(self.workbrief_widget)
        
        files_group.setLayout(files_layout)
        layout.addWidget(files_group)
        
        # Process button with modern styling
        button_container = QWidget()
        button_layout = QHBoxLayout(button_container)
        button_layout.setContentsMargins(0, 8, 0, 0)
        
        self.process_btn = QPushButton("🚀 Process Data")
        self.process_btn.clicked.connect(self.process_complete_workflow)
        self.process_btn.setToolTip("Process LMD data through Lane Fixes and Workbrief for complete output")
        self.process_btn.setFixedHeight(45)  # Modern button height
        self.process_btn.setStyleSheet("""
            QPushButton {
                background: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                    stop: 0 #52c41a, stop: 1 #389e0d);
                color: white;
                font-weight: bold;
                font-size: 13px;
                border: none;
                border-radius: 6px;
                padding: 12px 24px;
            }
            QPushButton:hover {
                background: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                    stop: 0 #73d13d, stop: 1 #52c41a);
            }
            QPushButton:pressed {
                background: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                    stop: 0 #389e0d, stop: 1 #237804);
            }
            QPushButton:disabled {
                background-color: #d9d9d9;
                color: #999999;
            }
        """)
        
        button_layout.addWidget(self.process_btn)
        layout.addWidget(button_container)
        
        self.setLayout(layout)
    
    def process_complete_workflow(self):
        """Start complete processing workflow."""
        # Validate all inputs
        if not self.combined_lmd_widget.is_valid():
            QMessageBox.warning(self, "Invalid Input", 
                              "Please select a valid Combined LMD file first (main data file).")
            return
        
        if not self.lane_fixes_widget.is_valid():
            QMessageBox.warning(self, "Invalid Input", 
                              "Please select a valid Lane Fixes file (corrections file).")
            return
            
        if not self.workbrief_widget.is_valid():
            QMessageBox.warning(self, "Invalid Input", 
                              "Please select a valid Workbrief file (final processing).")
            return
        
        # Start processing
        self._start_processing()
    
    def _start_processing(self):
        """Start the processing thread."""
        from data_processor import CombinedProcessor
        
        self.process_btn.setEnabled(False)
        self.main_window.log_widget.append_message("Starting data processing...", "INFO")

        # Create processor and thread
        processor = CombinedProcessor()
        self.processing_thread = ProcessingThread(
            processor,
            self.combined_lmd_widget.get_path(),
            self.lane_fixes_widget.get_path(),
            self.workbrief_widget.get_path()
        )
        
        # Connect signals
        self.processing_thread.progress_updated.connect(self.main_window.update_progress)
        self.processing_thread.finished.connect(self._processing_finished)
        self.processing_thread.error_occurred.connect(self._processing_error)
        
        # Start thread
        self.processing_thread.start()
    
    def _processing_finished(self, output_path: str):
        """Handle successful processing completion."""
        self.process_btn.setEnabled(True)
        self.main_window.progress_bar.setVisible(False)
        
        self.main_window.log_widget.append_message(
            f"Complete processing workflow finished successfully! Output: {output_path}", "SUCCESS"
        )
        
        # Show success dialog
        QMessageBox.information(
            self, "Success", 
            f"Complete processing workflow completed successfully!\\n\\nFinal output file: {output_path}\\n\\n" +
            f"This file contains:\\n" +
            f"• Original LMD data\\n" + 
            f"• Lane corrections applied\\n" +
            f"• Workbrief processing completed"
        )
    
    def _processing_error(self, error_message: str):
        """Handle processing error."""
        self.process_btn.setEnabled(True)
        self.main_window.progress_bar.setVisible(False)
        
        self.main_window.log_widget.append_message(f"Error: {error_message}", "ERROR")
        
        QMessageBox.critical(self, "Error", f"Processing failed: {error_message}")



class MainWindow(QMainWindow):
    """Main application window."""
    
    def __init__(self):
        super().__init__()
        self.settings = QSettings('FixlaneApp', 'Settings')
        self.setup_ui()
        self.load_settings()
    
    def setup_ui(self):
        """Setup the main window UI."""
        self.setWindowTitle(f"{Config.APP_NAME} v{Config.APP_VERSION}")
        self.setMinimumSize(*Config.WINDOW_MIN_SIZE)
        self.resize(*Config.WINDOW_DEFAULT_SIZE)
        
        # Set application icon (if available)
        self.setWindowIcon(QIcon())  # Add your icon path here
        
        # Create central widget
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # Main layout
        main_layout = QVBoxLayout(central_widget)
        
        # Create splitter for main content and logs
        splitter = QSplitter(Qt.Orientation.Vertical)
        
        # Create tab widget
        self.tab_widget = QTabWidget()
        
        # Add tabs
        self.combined_processing_tab = CombinedProcessingTab(self)
        
        self.tab_widget.addTab(self.combined_processing_tab, "Complete Data Processing")
        
        # Create log section
        log_section = self._create_log_section()
        
        # Add to splitter
        splitter.addWidget(self.tab_widget)
        splitter.addWidget(log_section)
        
        # Set splitter proportions - make log section closer
        splitter.setStretchFactor(0, 2)  # Main content gets moderate space
        splitter.setStretchFactor(1, 1)  # Log section gets proportional space
        
        main_layout.addWidget(splitter)
        
        # Create menu bar
        self.create_menu_bar()
        
        # Create status bar
        self.create_status_bar()
        
        # Apply styling
        self.apply_styling()
    
    def _create_log_section(self) -> QGroupBox:
        """Create the log section widget."""
        log_group = QGroupBox("Processing Log")
        layout = QVBoxLayout()
        
        # Create log widget
        self.log_widget = LogWidget()
        layout.addWidget(self.log_widget)
        
        # Create progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)
        
        log_group.setLayout(layout)
        return log_group
    
    def create_menu_bar(self):
        """Create the application menu bar."""
        menubar = self.menuBar()
        
        # File menu
        file_menu = menubar.addMenu('File')
        
        # Clear logs action
        clear_logs_action = QAction('Clear Logs', self)
        clear_logs_action.triggered.connect(self.clear_logs)
        file_menu.addAction(clear_logs_action)
        
        file_menu.addSeparator()
        
        # Exit action
        exit_action = QAction('Exit', self)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)
        
        # Help menu
        help_menu = menubar.addMenu('Help')
        
        # Timestamp formats action
        formats_action = QAction('Supported Timestamp Formats', self)
        formats_action.triggered.connect(self.show_timestamp_formats)
        help_menu.addAction(formats_action)
        
        # About action
        about_action = QAction('About', self)
        about_action.triggered.connect(self.show_about)
        help_menu.addAction(about_action)
    
    def create_status_bar(self):
        """Create the status bar."""
        self.status_bar = self.statusBar()
        self.status_bar.showMessage("Ready")
    
    def apply_styling(self):
        """Apply custom styling to the application."""
        self.setStyleSheet("""
            QMainWindow {
                background-color: #f5f5f5;
            }
            QGroupBox {
                font-weight: bold;
                border: 2px solid #cccccc;
                border-radius: 5px;
                margin-top: 10px;
                padding-top: 10px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px 0 5px;
            }
            QTabWidget::pane {
                border: 1px solid #cccccc;
                background-color: white;
            }
            QTabBar::tab {
                background-color: #e1e1e1;
                padding: 8px 16px;
                margin-right: 2px;
            }
            QTabBar::tab:selected {
                background-color: white;
                border-bottom: 2px solid #2196F3;
            }
        """)
    
    def update_progress(self, message: str, progress: float):
        """Update progress bar and log."""
        self.log_widget.append_message(message, "INFO")
        
        if progress >= 0:
            self.progress_bar.setVisible(True)
            self.progress_bar.setValue(int(progress))
        else:
            self.progress_bar.setVisible(True)
            self.progress_bar.setRange(0, 0)  # Indeterminate progress
        
        self.status_bar.showMessage(message)
    
    def clear_logs(self):
        """Clear the log widget."""
        self.log_widget.clear()
        self.log_widget.append_message("Logs cleared", "INFO")
    
    def show_timestamp_formats(self):
        """Show supported timestamp formats dialog."""
        formats_text = timestamp_handler.get_supported_formats_summary()
        
        msg_box = QMessageBox(self)
        msg_box.setWindowTitle("Supported Timestamp Formats")
        msg_box.setText("The application automatically detects and supports these timestamp formats:")
        msg_box.setDetailedText(formats_text)
        msg_box.setIcon(QMessageBox.Icon.Information)
        msg_box.exec()
    
    def show_about(self):
        """Show about dialog."""
        about_text = f"""
        <h3>{Config.APP_NAME}</h3>
        <p>Version: {Config.APP_VERSION}</p>
        <p>A modern application for processing lane fixes and workbrief data.</p>
        <p><b>Features:</b></p>
        <ul>
            <li>Automatic timestamp format detection</li>
            <li>Lane fixing operations</li>
            <li>Workbrief data processing</li>
            <li>Modern PyQt6 interface</li>
        </ul>
        """
        
        QMessageBox.about(self, "About", about_text)
    
    def load_settings(self):
        """Load application settings."""
        # Restore window geometry
        geometry = self.settings.value('geometry')
        if geometry:
            self.restoreGeometry(geometry)
        
        # Restore window state
        state = self.settings.value('windowState')
        if state:
            self.restoreState(state)
    
    def save_settings(self):
        """Save application settings."""
        self.settings.setValue('geometry', self.saveGeometry())
        self.settings.setValue('windowState', self.saveState())
    
    def closeEvent(self, event):
        """Handle window close event."""
        self.save_settings()
        event.accept()


class FixlaneApp(QApplication):
    """Main application class."""
    
    def __init__(self, sys_argv):
        super().__init__(sys_argv)
        self.setApplicationName(Config.APP_NAME)
        self.setApplicationVersion(Config.APP_VERSION)
        self.setOrganizationName("FixlaneApp")
        
        # Setup logging
        from config import LogConfig
        LogConfig.setup_logging()
        
        # Setup signal handling for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # Timer to allow signals to be processed by Qt
        self._timer = QTimer()
        self._timer.timeout.connect(lambda: None)  # Just to keep event loop active
        self._timer.start(100)  # Check every 100ms
        
        # Create main window
        self.main_window = MainWindow()
        
        # Setup application styling
        self.setStyle('Fusion')  # Use Fusion style for modern look
    
    def _signal_handler(self, signum, frame):
        """Handle system signals gracefully."""
        logging.info(f"Received signal {signum}, shutting down gracefully...")
        self.quit()
        
    def run(self):
        """Run the application."""
        try:
            self.main_window.show()
            return self.exec()
        except KeyboardInterrupt:
            # Handle Ctrl+C gracefully
            self.quit()
            return 130
        except Exception as e:
            logging.error(f"Application error: {e}", exc_info=True)
            return 1


def main():
    """Main entry point."""
    app = FixlaneApp(sys.argv)
    return app.run()


if __name__ == "__main__":
    sys.exit(main())