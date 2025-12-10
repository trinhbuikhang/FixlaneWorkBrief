#!/usr/bin/env python3
"""
Data Processing Tool

A PyQt6 application for cleaning binviewer CSV data based on specific criteria.
"""

import sys
import logging
from PyQt6.QtWidgets import QApplication, QMessageBox
from gui.main_window import DataCleanerApp
from gui.modern_styles import MODERN_STYLESHEET

# Import our logging setup
try:
    from utils.logger_setup import setup_application_logging, setup_exception_handler, get_application_logger
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
        logger.info("Creating QApplication...")
        app = QApplication(sys.argv)
        app.setApplicationName("Data Processing Tool")
        app.setApplicationVersion("1.0")
        app.setOrganizationName("PyDeveloper")
        
        logger.info("Applying modern stylesheet...")
        # Apply modern stylesheet
        app.setStyleSheet(MODERN_STYLESHEET)

        logger.info("Creating main window...")
        window = DataCleanerApp()
        
        logger.info("Showing main window...")
        window.show()

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