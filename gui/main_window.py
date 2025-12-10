import sys
import logging
from PyQt6.QtWidgets import QApplication, QWidget, QVBoxLayout, QTabWidget, QLabel, QFrame
from PyQt6.QtCore import Qt
from gui.tabs.lmd_cleaner_tab import LMDCleanerTab
from gui.tabs.laneFix_tab import LaneFixTab
from gui.tabs.client_feedback_tab import ClientFeedbackTab
from gui.tabs.add_columns_tab import AddColumnsTab
from gui.styles import apply_stylesheet, MAIN_STYLESHEET

class DataCleanerApp(QWidget):
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(__name__)
        self.logger.info("Initializing DataCleanerApp...")
        try:
            self.initUI()
            self.logger.info("DataCleanerApp initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize DataCleanerApp: {e}")
            from utils.logger_setup import log_exception
            log_exception(context="DataCleanerApp.__init__")
            raise

    def initUI(self):
        try:
            self.logger.info("Setting window title and geometry...")
            self.setWindowTitle("Data Processing Tool")
            
            # Auto-size to half screen width and 90% screen height
            from PyQt6.QtWidgets import QApplication
            screen = QApplication.primaryScreen().geometry()
            width = screen.width() // 2  # Half screen width
            height = int(screen.height() * 0.9)  # 90% screen height to keep window controls visible
            
            # Position on right half of screen
            x = screen.width() // 2
            y = int(screen.height() * 0.05)  # Small margin from top
            
            self.setGeometry(x, y, width, height)
            self.setMinimumSize(800, 600)  # Minimum size for usability
            self.logger.info(f"Window geometry set: {width}x{height} at ({x}, {y})")

            # Apply the modern stylesheet
            self.logger.info("Applying stylesheet...")
            apply_stylesheet(self)

            layout = QVBoxLayout()
            layout.setContentsMargins(20, 20, 20, 20)
            layout.setSpacing(15)

            # Create tab widget
            self.logger.info("Creating tab widget...")
            self.tab_widget = QTabWidget()
            layout.addWidget(self.tab_widget)

            # Add LMD Cleaner tab
            self.logger.info("Creating LMD Cleaner tab...")
            self.lmd_cleaner_tab = LMDCleanerTab()
            self.tab_widget.addTab(self.lmd_cleaner_tab, "LMD Cleaner")

            # Add Lane Fix tab
            self.logger.info("Creating Lane Fix tab...")
            self.lane_fix_tab = LaneFixTab()
            self.tab_widget.addTab(self.lane_fix_tab, "Lane Fix")

            # Add Client Feedback tab
            self.logger.info("Creating Client Feedback tab...")
            self.client_feedback_tab = ClientFeedbackTab()
            self.tab_widget.addTab(self.client_feedback_tab, "Client Feedback")

            # Add Add Columns tab
            self.logger.info("Creating Add Columns tab...")
            self.add_columns_tab = AddColumnsTab()
            self.tab_widget.addTab(self.add_columns_tab, "Add Columns")

            # Global status bar at bottom
            self.logger.info("Creating status bar...")
            status_container = QFrame()
            status_container.setObjectName("statusContainer")
            status_container.setFrameStyle(QFrame.Shape.StyledPanel)
            status_container.setFixedHeight(30)
            
            status_layout = QVBoxLayout(status_container)
            status_layout.setContentsMargins(10, 5, 10, 5)
            
            self.global_status_label = QLabel("Ready")
            self.global_status_label.setObjectName("globalStatusLabel")
            status_layout.addWidget(self.global_status_label)
            
            layout.addWidget(status_container)

            # Connect tab status updates to global status
            self.logger.info("Connecting tab status updates...")
            self.connect_tab_status_updates()

            self.setLayout(layout)
            self.logger.info("UI initialization completed successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize UI: {e}")
            from utils.logger_setup import log_exception
            log_exception(context="DataCleanerApp.initUI")
            raise
    
    def connect_tab_status_updates(self):
        """Connect all tab status labels to the global status label"""
        # Connect each tab's status updates to global status
        if hasattr(self.lmd_cleaner_tab, 'status_label'):
            self.lmd_cleaner_tab.status_label = self.global_status_label
        if hasattr(self.lane_fix_tab, 'status_label'):
            self.lane_fix_tab.status_label = self.global_status_label
        if hasattr(self.client_feedback_tab, 'status_label'):
            self.client_feedback_tab.status_label = self.global_status_label
        if hasattr(self.add_columns_tab, 'status_label'):
            self.add_columns_tab.status_label = self.global_status_label

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setApplicationName("Data Processing Tool")
    app.setApplicationVersion("1.0")
    app.setOrganizationName("PyDeveloper")

    window = DataCleanerApp()
    window.show()
    sys.exit(app.exec())