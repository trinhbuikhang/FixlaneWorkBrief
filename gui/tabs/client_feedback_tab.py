import logging
import os
import sys
from datetime import datetime

from utils.lazy_imports import polars as pl
from PyQt6.QtCore import Qt
from gui.ui_constants import (
    BROWSE_BUTTON_WIDTH,
    GROUP_SPACING,
    INPUT_MIN_WIDTH,
    LABEL_FIXED_WIDTH,
    LAYOUT_MARGINS,
    LAYOUT_SPACING,
)
from PyQt6.QtWidgets import (
    QCheckBox,
    QFileDialog,
    QFrame,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QSplitter,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

logger = logging.getLogger(__name__)

try:
    from utils.client_feedback_processor import ClientFeedbackProcessor
    from utils.security import SecurityValidator, UserFriendlyError
except ImportError as e:
    logger.exception("Failed to import client feedback modules: %s", e)
    logger.debug("Current working directory: %s", os.getcwd())
    logger.debug("Python path: %s", sys.path)
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
        self.processing_active = False
        self.initUI()
    
    def update_status(self, message):
        """Safely update status label if it exists"""
        if self.status_label is not None:
            self.status_label.setText(message)

    def initUI(self):
        layout = QVBoxLayout()
        layout.setContentsMargins(*LAYOUT_MARGINS)
        layout.setSpacing(LAYOUT_SPACING)

        # Title
        title_label = QLabel("Client Feedback Processor")
        title_label.setObjectName("titleLabel")
        layout.addWidget(title_label)

        # File Selection GroupBox
        files_group = QGroupBox("File Selection")
        files_layout = QVBoxLayout(files_group)
        files_layout.setSpacing(GROUP_SPACING)

        # Combined LMD file
        lmd_layout = QHBoxLayout()
        lmd_layout.setSpacing(GROUP_SPACING)
        lmd_label = QLabel("Combined LMD:")
        lmd_label.setFixedWidth(LABEL_FIXED_WIDTH)
        lmd_layout.addWidget(lmd_label)

        self.lmd_edit = QLineEdit()
        self.lmd_edit.setPlaceholderText("Select combined LMD CSV file")
        self.lmd_edit.setMinimumWidth(INPUT_MIN_WIDTH)
        self.lmd_edit.setReadOnly(True)
        lmd_layout.addWidget(self.lmd_edit, 1)

        self.lmd_btn = QPushButton("Browse...")
        self.lmd_btn.setFixedWidth(BROWSE_BUTTON_WIDTH)
        self.lmd_btn.clicked.connect(self.select_lmd_file)
        lmd_layout.addWidget(self.lmd_btn)
        files_layout.addLayout(lmd_layout)

        # Client feedback file
        feedback_layout = QHBoxLayout()
        feedback_layout.setSpacing(GROUP_SPACING)
        feedback_label = QLabel("Client Feedback:")
        feedback_label.setFixedWidth(LABEL_FIXED_WIDTH)
        feedback_layout.addWidget(feedback_label)

        self.feedback_edit = QLineEdit()
        self.feedback_edit.setPlaceholderText("Select client feedback CSV file")
        self.feedback_edit.setMinimumWidth(INPUT_MIN_WIDTH)
        self.feedback_edit.setReadOnly(True)
        feedback_layout.addWidget(self.feedback_edit, 1)

        self.feedback_btn = QPushButton("Browse...")
        self.feedback_btn.setFixedWidth(BROWSE_BUTTON_WIDTH)
        self.feedback_btn.clicked.connect(self.select_feedback_file)
        feedback_layout.addWidget(self.feedback_btn)
        files_layout.addLayout(feedback_layout)

        layout.addWidget(files_group)

        # Process button - Standardized position
        self.process_btn = QPushButton("Process Data")
        self.process_btn.setObjectName("processButton")
        self.process_btn.clicked.connect(self.handle_process_click)
        layout.addWidget(self.process_btn)

        # Create splitter to split columns list and log
        splitter = QSplitter(Qt.Orientation.Horizontal)
        
        # Left side - Column selection
        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        left_layout.setContentsMargins(0, 0, 0, 0)
        
        # Column Selection Group
        columns_group = QGroupBox("Column Selection")
        columns_layout = QVBoxLayout()
        
        # Controls header
        controls_layout = QHBoxLayout()
        columns_header = QLabel("Available Columns")
        columns_header.setStyleSheet("font-weight: 600; font-size: 13px; color: #6d4847;")
        controls_layout.addWidget(columns_header)
        
        columns_layout.addLayout(controls_layout)

        self.columns_list = QListWidget()
        # Use multi-selection mode like Add Columns tab
        self.columns_list.setSelectionMode(QListWidget.SelectionMode.MultiSelection)
        self.columns_list.itemSelectionChanged.connect(self.update_selection_count)
        columns_layout.addWidget(self.columns_list)

        # Selection counter
        self.selection_label = QLabel("No columns available")
        self.selection_label.setStyleSheet("font-size: 11px; color: #8b5e5d; padding: 8px;")
        columns_layout.addWidget(self.selection_label)
        
        columns_group.setLayout(columns_layout)
        left_layout.addWidget(columns_group)

        # Right side - Processing log
        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)
        right_layout.setContentsMargins(0, 0, 0, 0)
        
        # Log Group
        log_group = QGroupBox("Processing Log")
        log_layout = QVBoxLayout()
        
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        log_layout.addWidget(self.log_text)
        
        log_group.setLayout(log_layout)
        right_layout.addWidget(log_group)

        # Add to splitter with standard proportions (40:60)
        splitter.addWidget(left_widget)
        splitter.addWidget(right_widget)
        splitter.setSizes([320, 480])  # 40% left, 60% right - consistent with Add Columns
        
        layout.addWidget(splitter, 1)

        # Progress bar - Slim and at bottom
        self.progress = QProgressBar()
        self.progress.setValue(0)
        self.progress.setMaximumHeight(8)  # Make it slim and elegant
        layout.addWidget(self.progress)

        # Status label will be set by main window
        self.status_label = None

        self.setLayout(layout)

    def load_columns(self, file_path):
        """Load and display columns from client feedback file with default selections."""
        try:
            if not os.path.exists(file_path):
                self.columns_list.clear()
                self.update_selection_count()
                return

            # Read the first row to get column names, ignore errors
            df = pl.read_csv(file_path, n_rows=1, ignore_errors=True)
            columns = df.columns

            # Define standard/default columns that should be pre-selected
            default_columns = {
                'Site Description', 'Treatment 2024', 'Treatment 2025', 
                'Treatment 2026', 'Terminal', 'Foamed Bitumen %', 
                'Cement %', 'Lime %'
            }
            
            # System columns that should not be included in selection
            system_columns = {
                'road_id', 'region_id', 'project_id', 'Road Name', 'Region', 'Region ID',
                'Start Chainage (km)', 'End Chainage (km)', 'Start Chainage', 'End Chainage',
                'locFrom', 'locTo', 'LocFrom', 'LocTo',  # Chainage variants
                'wheelpath', 'Wheelpath', 'WheelPath', 'WHEELPATH',  # Wheelpath matching column
                'Lanes', 'lanes', 'LANES', 'Lane_Type'  # Lanes matching column (plural - for 'All' logic)
            }

            self.columns_list.clear()
            
            # Add columns and mark default ones as selected
            for col in columns:
                if col not in system_columns:  # Only show non-system columns
                    item = QListWidgetItem(col)
                    
                    # Check if this is a default column
                    if col in default_columns:
                        # Style default columns differently
                        item.setToolTip(f"Default column: {col}")
                        # Make default columns bold
                        font = item.font()
                        font.setBold(True)
                        item.setFont(font)
                    else:
                        # Style additional columns
                        item.setToolTip(f"Additional column found in file: {col}")
                    
                    self.columns_list.addItem(item)
                    
                    # Set selection AFTER adding to list
                    if col in default_columns:
                        item.setSelected(True)
            
            self.update_selection_count()
            
            # Show summary in log
            total_cols = len(columns)
            system_cols = len([col for col in columns if col in system_columns])
            available_cols = total_cols - system_cols
            default_found = len([col for col in columns if col in default_columns and col not in system_columns])
            additional_found = available_cols - default_found
            
            self.log_text.append(f"📋 Feedback file analysis:")
            self.log_text.append(f"   • Total columns: {total_cols}")
            self.log_text.append(f"   • System columns (excluded): {system_cols}")
            self.log_text.append(f"   • Available for selection: {available_cols}")
            self.log_text.append(f"   • Default columns found: {default_found} (pre-selected)")
            self.log_text.append(f"   • Additional columns found: {additional_found}")
            if default_found > 0:
                self.log_text.append(f"✓ Default columns are highlighted in bold and pre-selected")
            if additional_found > 0:
                self.log_text.append(f"💡 You can select additional columns as needed")

        except Exception as e:
            self.columns_list.clear()
            self.update_selection_count()
            QMessageBox.warning(self, "Error", f"Failed to load columns: {str(e)}")

    def update_selection_count(self):
        """Update the selection count label."""
        selected_count = len(self.columns_list.selectedItems())
        total_count = self.columns_list.count()
        if total_count == 0:
            self.selection_label.setText("No columns available")
        else:
            self.selection_label.setText(f"Selected: {selected_count}/{total_count} columns")

    def get_selected_columns(self):
        """Get list of selected columns from the columns list."""
        selected_items = self.columns_list.selectedItems()
        return [item.text() for item in selected_items]

    def _emit_progress(self, message: str, progress: float = None):
        """Emit progress update to GUI."""
        if progress is not None:
            self.progress.setValue(int(progress))
        self.log_text.append(message)
        # Force GUI update to prevent freezing
        from PyQt6.QtWidgets import QApplication
        QApplication.processEvents()

    def select_lmd_file(self):
        # Set default directory to J:/Processing if it exists
        import os
        default_dir = "J:/Processing" if os.path.exists("J:/Processing") else ""
        file_name, _ = QFileDialog.getOpenFileName(
            self, "Select Combined LMD CSV File", default_dir, "CSV Files (*.csv);;All Files (*)"
        )
        if file_name:
            # Validate file path
            is_valid, error_msg, validated_path = SecurityValidator.sanitize_file_path(file_name)
            
            if not is_valid:
                QMessageBox.critical(self, "Invalid File", error_msg)
                logging.error(f"LMD file validation failed: {error_msg}")
                return
            
            self.lmd_edit.setText(str(validated_path))
            # File selected - ready for processing (output will be auto-generated)

    def select_feedback_file(self):
        # Set default directory to J:/Processing if it exists
        import os
        default_dir = "J:/Processing" if os.path.exists("J:/Processing") else ""
        file_name, _ = QFileDialog.getOpenFileName(
            self, "Select Client Feedback CSV File", default_dir, "CSV Files (*.csv);;All Files (*)"
        )
        if file_name:
            # Validate file path
            is_valid, error_msg, validated_path = SecurityValidator.sanitize_file_path(file_name)
            
            if not is_valid:
                QMessageBox.critical(self, "Invalid File", error_msg)
                logging.error(f"Feedback file validation failed: {error_msg}")
                return
            
            self.feedback_edit.setText(str(validated_path))
            self.load_columns(str(validated_path))

    def handle_process_click(self):
        """Handle process button click - start processing or cancel if already running"""
        if self.processing_active:
            self.cancel_processing()
        else:
            self.process_data()

    def cancel_processing(self):
        """Cancel the current processing"""
        if self.processing_active:
            self.processing_active = False
            self.reset_process_button()
            self.progress.setValue(0)
            self.log_text.append("Processing cancelled by user.")
            QMessageBox.information(self, "Cancelled", "Processing has been cancelled.")

    def reset_process_button(self):
        """Reset process button to initial state"""
        self.process_btn.setText("Process Data")
        self.process_btn.setObjectName("processButton")
        self.process_btn.setStyleSheet("")
        self.process_btn.style().unpolish(self.process_btn)
        self.process_btn.style().polish(self.process_btn)

    def process_data(self):
        lmd_file = self.lmd_edit.text().strip()
        feedback_file = self.feedback_edit.text().strip()

        # Validate inputs
        if not lmd_file:
            QMessageBox.warning(self, "Input Required", "Please select a Combined LMD file.")
            return

        if not feedback_file:
            QMessageBox.warning(self, "Input Required", "Please select a Client Feedback file.")
            return

        # Generate output filename automatically (same location as LMD file + suffix)
        import os
        lmd_dir = os.path.dirname(lmd_file)
        lmd_name = os.path.splitext(os.path.basename(lmd_file))[0]
        output_file = os.path.join(lmd_dir, f"{lmd_name}_with_feedback.csv")

        # Check file existence
        if not os.path.exists(lmd_file):
            QMessageBox.critical(self, "File Not Found", f"Combined LMD file does not exist: {lmd_file}")
            return

        if not os.path.exists(feedback_file):
            QMessageBox.critical(self, "File Not Found", f"Client feedback file does not exist: {feedback_file}")
            return

        self.progress.setValue(0)
        self.log_text.clear()
        self.update_status("Initializing...")
        self.processing_active = True
        self.process_btn.setText("Cancel")
        self.process_btn.setObjectName("warningButton")
        self.process_btn.setStyleSheet("")
        self.process_btn.style().unpolish(self.process_btn)
        self.process_btn.style().polish(self.process_btn)

        # Log the output file location
        self.log_text.append(f"Output will be saved to: {output_file}")
        self.log_text.append(f"Processing started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

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
                    self.update_status(f"Processing: {progress_info}")
                elif "Status:" in message:
                    self.update_status(message.replace("Status: ", ""))
                elif any(keyword in message.lower() for keyword in ["starting", "loading", "initializing", "completed", "processing"]):
                    self.update_status(message)
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
                processed_df.write_csv(output_file, line_terminator='\r\n')
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
                self.update_status("✅ Complete")
                success_msg = f"Client feedback processing completed successfully!\n\nOutput saved to:\n{output_file}"
                QMessageBox.information(self, "Success", success_msg)
                logging.info(f"Client feedback processing completed successfully. Output saved to: {output_file}")
            else:
                self.progress.setValue(0)
                self.update_status("❌ Failed")
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
            self.update_status("❌ Error occurred")

        finally:
            logger.removeHandler(handler)
            self.processing_active = False
            self.reset_process_button()
            if self.progress.value() != 100 and self.progress.value() != 0:
                self.update_status("Ready")