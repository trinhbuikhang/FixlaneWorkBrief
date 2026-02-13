import logging
import sys

from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import (
    QApplication,
    QFrame,
    QLabel,
    QVBoxLayout,
    QWidget,
)

from gui.lazy_loader import LazyTabWidget
from gui.icons import get_tab_icon

# Note: Stylesheet is now applied in main.py for better startup performance
# No need to import or apply stylesheet here

# ⚡ LAZY IMPORTS - Tabs will be imported when needed
# from gui.tabs.add_columns_tab import AddColumnsTab
# from gui.tabs.client_feedback_tab import ClientFeedbackTab
# from gui.tabs.laneFix_tab import LaneFixTab
# from gui.tabs.lmd_cleaner_tab import LMDCleanerTab


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
            self.setWindowTitle("Data Processing Tool v2.0")
            
            # Set window icon
            from gui.icons import get_app_icon
            self.setWindowIcon(get_app_icon())
            
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

            # Note: Stylesheet is applied in main.py for optimal startup performance

            layout = QVBoxLayout()
            layout.setContentsMargins(20, 20, 20, 20)
            layout.setSpacing(15)

            # Create status bar FIRST (needed by tabs)
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

            # ⚡ Create LAZY tab widget - tabs loaded on demand
            self.logger.info("Creating lazy tab widget...")
            self.tab_widget = LazyTabWidget()
            layout.addWidget(self.tab_widget)

            # ⚡ Add tabs with LAZY LOADING - only load when user clicks
            self.logger.info("Adding lazy tabs (not loading content yet)...")
            
            # Tab 0: LMD Cleaner - Load immediately (default tab)
            self.tab_widget.add_lazy_tab(
                "LMD Cleaner",
                lambda: self._create_lmd_cleaner_tab(),
                load_immediately=True,  # Load first tab immediately
                icon=get_tab_icon("LMD Cleaner"),
                tooltip="Clean and process LMD survey data - filters invalid records and adds calculated fields"
            )

            # Tab 1: Lane Fix - Lazy load
            self.tab_widget.add_lazy_tab(
                "Lane Fix",
                lambda: self._create_lane_fix_tab(),
                load_immediately=False,
                icon=get_tab_icon("Lane Fix"),
                tooltip="Fix lane numbering and chainage issues in LMD data using reference coordinates"
            )
            
            # Tab 2: Client Feedback - Lazy load
            self.tab_widget.add_lazy_tab(
                "Client Feedback",
                lambda: self._create_client_feedback_tab(),
                load_immediately=False,
                icon=get_tab_icon("Client Feedback"),
                tooltip="Match and merge client feedback data with LMD records based on region, road, and chainage"
            )
            
            # Tab 3: Add Columns - Lazy load
            self.tab_widget.add_lazy_tab(
                "Add Columns",
                lambda: self._create_add_columns_tab(),
                load_immediately=False,
                icon=get_tab_icon("Add Columns"),
                tooltip="Add custom columns to LMD data from external CSV files based on matching criteria"
            )
            
            # Tab 4: Help - Lazy load
            self.tab_widget.add_lazy_tab(
                "Help",
                lambda: self._create_help_tab(),
                load_immediately=False,
                icon=get_tab_icon("Help"),
                tooltip="User guide and documentation - learn how to use each processing tool effectively"
            )
            
            self.logger.info(f"Added {self.tab_widget.count()} lazy tabs")

            # Add status bar to layout
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
    
    # ⚡ TAB FACTORY METHODS - Import only when needed
    def _create_lmd_cleaner_tab(self):
        """Factory method to create LMD Cleaner tab"""
        self.logger.info("Loading LMD Cleaner tab...")
        from gui.tabs.lmd_cleaner_tab import LMDCleanerTab
        tab = LMDCleanerTab()
        self._connect_tab_status(tab)
        return tab
    
    def _create_lane_fix_tab(self):
        """Factory method to create Lane Fix tab"""
        self.logger.info("Loading Lane Fix tab...")
        from gui.tabs.laneFix_tab import LaneFixTab
        tab = LaneFixTab()
        self._connect_tab_status(tab)
        return tab
    
    def _create_client_feedback_tab(self):
        """Factory method to create Client Feedback tab"""
        self.logger.info("Loading Client Feedback tab...")
        from gui.tabs.client_feedback_tab import ClientFeedbackTab
        tab = ClientFeedbackTab()
        self._connect_tab_status(tab)
        return tab
    
    def _create_add_columns_tab(self):
        """Factory method to create Add Columns tab"""
        self.logger.info("Loading Add Columns tab...")
        from gui.tabs.add_columns_tab import AddColumnsTab
        tab = AddColumnsTab()
        self._connect_tab_status(tab)
        return tab
    
    def _create_help_tab(self):
        """Factory method to create Help tab"""
        self.logger.info("Loading Help tab...")
        from gui.tabs.help_tab import HelpTab
        return HelpTab()  # Help tab doesn't have status updates
    
    def _connect_tab_status(self, tab):
        """Connect tab status label to global status"""
        if hasattr(tab, 'status_label'):
            tab.status_label = self.global_status_label
    
    def connect_tab_status_updates(self):
        """
        Connect all tab status labels to the global status label.
        
        ⚡ Note: With lazy loading, tabs are not created yet.
        Status connection will be done in factory methods.
        """
        # With lazy loading, no need to connect immediately
        # Connection will be done in _connect_tab_status()
        pass

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setApplicationName("Data Processing Tool")
    app.setApplicationVersion("1.0")
    app.setOrganizationName("PyDeveloper")

    window = DataCleanerApp()
    window.show()
    sys.exit(app.exec())