#!/usr/bin/env python3
"""
Data Processing Tool

A PyQt6 application for cleaning binviewer CSV data based on specific criteria.

Optimization: Heavy imports are deferred until after QApplication creation
to improve startup time and get window visible faster.
"""

import logging
import sys

# ⚡ MINIMAL IMPORTS - Only what's needed for QApplication
from PyQt6.QtWidgets import QApplication

# ⚡ DEFERRED IMPORTS - Will be imported after QApplication is ready
# from gui.main_window import DataCleanerApp
# from gui.modern_styles import MODERN_STYLESHEET
# from PyQt6.QtWidgets import QMessageBox

# Import our logging setup
try:
    from utils.logger_setup import (
        get_application_logger,
        setup_application_logging,
        setup_exception_handler,
    )
except ImportError as e:
    print(f"Failed to import logging setup: {e}")
    # Fallback basic logging
    logging.basicConfig(level=logging.INFO)

def main():
    # Setup comprehensive logging first
    try:
        logger = setup_application_logging(logging.INFO)
        setup_exception_handler()
        logger.info("Application logging initialized successfully")
    except Exception as e:
        print(f"Failed to setup logging: {e}")
        # Continue with basic logging
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger()
    
    try:
        # ⚡ Step 1: Create QApplication ASAP (very fast)
        logger.info("Creating QApplication...")
        app = QApplication(sys.argv)
        app.setApplicationName("Data Processing Tool")
        app.setApplicationVersion("1.0")
        app.setOrganizationName("PyDeveloper")
        
        # ⚡ Step 2: Import heavy modules AFTER QApplication exists
        # This allows the app to start faster and show window sooner
        logger.info("Loading GUI modules...")
        from gui.main_window import DataCleanerApp
        from gui.modern_styles import MINIMAL_STYLESHEET, MODERN_STYLESHEET
        
        logger.info("Applying minimal stylesheet...")
        # ⚡ Apply minimal stylesheet FIRST for fast startup
        app.setStyleSheet(MINIMAL_STYLESHEET)

        logger.info("Creating main window...")
        window = DataCleanerApp()
        
        logger.info("Showing main window...")
        window.show()
        
        # ⚡ Apply full stylesheet AFTER window is visible
        # This doesn't block the UI from appearing
        logger.info("Loading full stylesheet...")
        app.setStyleSheet(MODERN_STYLESHEET)

        logger.info("Starting event loop...")
        result = app.exec()
        
        logger.info(f"Application exited with code: {result}")
        return result
        
    except Exception as e:
        # Log the exception
        if 'logger' in locals():
            logger.error(f"Fatal error in main(): {e}")
            from utils.logger_setup import log_exception
            log_exception(context="main() function")
        else:
            print(f"Fatal error in main(): {e}")
            import traceback
            traceback.print_exc()
        
        # Show error dialog if possible
        try:
            if 'app' in locals():
                # ⚡ Import QMessageBox only when needed (error case)
                from PyQt6.QtWidgets import QMessageBox
                
                msg = QMessageBox()
                msg.setIcon(QMessageBox.Icon.Critical)
                msg.setWindowTitle("Fatal Error")
                msg.setText(f"A fatal error occurred:\n\n{str(e)}")
                msg.setInformativeText("Please check the log file for detailed information.")
                
                # Add log file path if available
                app_logger = get_application_logger()
                if app_logger and app_logger.get_log_file_path():
                    msg.setDetailedText(f"Log file: {app_logger.get_log_file_path()}")
                
                msg.exec()
        except:
            pass  # If we can't show dialog, just continue
        
        return 1

if __name__ == "__main__":
    sys.exit(main())