import sys
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QLineEdit, QPushButton, QFileDialog, QTextEdit,
    QProgressBar, QMessageBox, QTabWidget
)
from PyQt6.QtCore import Qt
from utils.data_processor import process_data
import logging
from PyQt6.QtWidgets import QTextEdit

class QTextEditHandler(logging.Handler):
    """Custom logging handler to write logs to QTextEdit."""
    def __init__(self, text_edit):
        super().__init__()
        self.text_edit = text_edit

    def emit(self, record):
        msg = self.format(record)
        self.text_edit.append(msg)

class LMDCleanerTab(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()

    def initUI(self):
        layout = QVBoxLayout()
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(15)
        layout.setSpacing(15)

        # Description
        desc_label = QLabel("This tool cleans LMD data by applying the following filters:\n"
                           "• Remove rows with empty rawSlope170 and rawSlope270\n"
                           "• Remove rows where trailingFactor < 0.15\n"
                           "• Remove rows where abs(tsdSlopeMinY)/tsdSlopeMaxY < 0.15\n"
                           "• Remove rows where Lane contains 'SK'\n"
                           "• Remove rows where Ignore is true")
        desc_label.setWordWrap(True)
        desc_label.setObjectName("descriptionLabel")
        layout.addWidget(desc_label)

        # Input file section
        input_layout = QHBoxLayout()
        input_layout.setSpacing(10)

        # Fixed width label for alignment
        input_label = QLabel("Input:")
        input_label.setFixedWidth(80)  # Fixed width for consistent alignment
        input_layout.addWidget(input_label)

        self.input_edit = QLineEdit()
        self.input_edit.setPlaceholderText("Select the combined_lmd CSV file")
        self.input_edit.setMinimumWidth(300)  # Minimum width for consistency
        input_layout.addWidget(self.input_edit, 1)  # Stretch factor 1

        self.input_btn = QPushButton("Browse...")
        self.input_btn.setFixedWidth(100)  # Fixed width for button
        self.input_btn.clicked.connect(self.select_input)
        input_layout.addWidget(self.input_btn)

        layout.addLayout(input_layout)

        # Output file section
        output_layout = QHBoxLayout()
        output_layout.setSpacing(10)

        # Fixed width label for alignment (same as input)
        output_label = QLabel("Output:")
        output_label.setFixedWidth(80)  # Same fixed width as input label
        output_layout.addWidget(output_label)

        self.output_edit = QLineEdit()
        self.output_edit.setPlaceholderText("Select where to save cleaned data")
        self.output_edit.setMinimumWidth(300)  # Same minimum width as input
        output_layout.addWidget(self.output_edit, 1)  # Stretch factor 1

        self.output_btn = QPushButton("Browse...")
        self.output_btn.setFixedWidth(100)  # Same fixed width as input button
        self.output_btn.clicked.connect(self.select_output)
        output_layout.addWidget(self.output_btn)

        layout.addLayout(output_layout)

        # Process button
        self.process_btn = QPushButton("Process Data")
        self.process_btn.setObjectName("processButton")  # For special styling
        self.process_btn.clicked.connect(self.process_data)
        layout.addWidget(self.process_btn)

        # Progress bar
        self.progress = QProgressBar()
        self.progress.setValue(0)
        layout.addWidget(self.progress)

        # Log section
        layout.addWidget(QLabel("Processing Log:"))
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        layout.addWidget(self.log_text)

        self.setLayout(layout)

    def select_input(self):
        file_name, _ = QFileDialog.getOpenFileName(
            self, "Select Input CSV File", "", "CSV Files (*.csv);;All Files (*)"
        )
        if file_name:
            self.input_edit.setText(file_name)
            # Auto-suggest output file
            import os
            base_name = os.path.splitext(file_name)[0]
            self.output_edit.setText(f"{base_name}_cleaned.csv")

    def select_output(self):
        file_name, _ = QFileDialog.getSaveFileName(
            self, "Select Output CSV File", "", "CSV Files (*.csv);;All Files (*)"
        )
        if file_name:
            self.output_edit.setText(file_name)

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
        self.process_btn.setEnabled(False)
        self.process_btn.setText("Processing...")

        # Set up logging to QTextEdit
        handler = QTextEditHandler(self.log_text)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger = logging.getLogger()
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

        try:
            self.progress.setValue(10)
            process_data(input_file, output_file)
            self.progress.setValue(100)
            QMessageBox.information(self, "Success", "Data processing completed successfully!")

        except Exception as e:
            QMessageBox.critical(self, "Processing Error", f"An error occurred during processing:\n{str(e)}")
            self.progress.setValue(0)

        finally:
            logger.removeHandler(handler)
            self.process_btn.setEnabled(True)
            self.process_btn.setText("Process Data")