#!/usr/bin/env python3
"""
Data Cleaner Application

A PyQt6 application for cleaning binviewer CSV data based on specific criteria.
"""

import sys
from PyQt6.QtWidgets import QApplication
from gui.main_window import DataCleanerApp

def main():
    app = QApplication(sys.argv)
    app.setApplicationName("Data Cleaner")
    app.setApplicationVersion("1.0")
    app.setOrganizationName("PyDeveloper")

    window = DataCleanerApp()
    window.show()

    sys.exit(app.exec())

if __name__ == "__main__":
    main()