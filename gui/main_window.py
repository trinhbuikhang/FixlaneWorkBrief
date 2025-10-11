import sys
from PyQt6.QtWidgets import QApplication, QWidget, QVBoxLayout, QTabWidget
from PyQt6.QtCore import Qt
from gui.tabs.lmd_cleaner_tab import LMDCleanerTab
from gui.tabs.laneFix_tab import LaneFixTab
from gui.styles import apply_stylesheet, MAIN_STYLESHEET

class DataCleanerApp(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()

    def initUI(self):
        self.setWindowTitle("Data Cleaner - LMD Processing Tool")
        self.setGeometry(100, 100, 900, 700)

        # Apply the modern stylesheet
        apply_stylesheet(self)

        layout = QVBoxLayout()
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(15)

        # Create tab widget
        self.tab_widget = QTabWidget()
        layout.addWidget(self.tab_widget)

        # Add LMD Cleaner tab
        self.lmd_cleaner_tab = LMDCleanerTab()
        self.tab_widget.addTab(self.lmd_cleaner_tab, "LMD Cleaner")

        # Add Lane Fix tab
        self.lane_fix_tab = LaneFixTab()
        self.tab_widget.addTab(self.lane_fix_tab, "Lane Fix")

        self.setLayout(layout)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setApplicationName("Data Cleaner")
    app.setApplicationVersion("1.0")
    app.setOrganizationName("PyDeveloper")

    window = DataCleanerApp()
    window.show()
    sys.exit(app.exec())