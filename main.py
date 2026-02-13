#!/usr/bin/env python3
"""
Data Processing Tool

A PyQt6 application for cleaning binviewer CSV data based on specific criteria.

CRITICAL for Windows/EXE:
  multiprocessing.freeze_support() and set_start_method('spawn') MUST be called
  before ANY other import that might use multiprocessing, otherwise PyInstaller
  executables will spawn duplicate GUI windows and eventually crash the system.
"""

import multiprocessing
import os
import sys

# ── MUST be the very first thing in __main__ ──
if __name__ == "__main__":
    multiprocessing.freeze_support()
    # Explicitly set spawn (Windows default, but be safe for all platforms)
    try:
        multiprocessing.set_start_method("spawn", force=True)
    except RuntimeError:
        pass  # already set

# ---------------------------------------------------------------------------
# Bootstrap crash log: write to file immediately, flush after each line.
# On crash (including segfault), open logs/startup_crash.log to see last step.
# ---------------------------------------------------------------------------
_CRASH_LOG_PATH = None
_CRASH_LOG_FILE = None

def _crash_log_path():
    global _CRASH_LOG_PATH
    if _CRASH_LOG_PATH is not None:
        return _CRASH_LOG_PATH
    try:
        if getattr(sys, "frozen", False):
            base = os.path.dirname(sys.executable)
        else:
            base = os.path.dirname(os.path.abspath(__file__))
        log_dir = os.path.join(base, "logs")
        os.makedirs(log_dir, exist_ok=True)
        _CRASH_LOG_PATH = os.path.join(log_dir, "startup_crash.log")
    except Exception:
        _CRASH_LOG_PATH = os.path.join(os.getcwd(), "startup_crash.log")
    return _CRASH_LOG_PATH

def _bootstrap_log(msg):
    """Write msg to crash log file and flush immediately (so log is available after crash)."""
    global _CRASH_LOG_FILE
    try:
        if _CRASH_LOG_FILE is None:
            path = _crash_log_path()
            _CRASH_LOG_FILE = open(path, "a", encoding="utf-8")
            from datetime import datetime
            _CRASH_LOG_FILE.write("\n" + "="*60 + "\n")
            _CRASH_LOG_FILE.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "  START\n")
            _CRASH_LOG_FILE.write("(If last line is 'entering event loop' with no 'first tick' -> crash during Qt first paint)\n")
            _CRASH_LOG_FILE.flush()
            # Write log path to a hint file so user can find it when app crashes
            try:
                hint_dir = os.path.dirname(os.path.dirname(path))  # parent of logs = project root
                hint_file = os.path.join(hint_dir, "CRASH_LOG_LOCATION.txt")
                with open(hint_file, "w", encoding="utf-8") as f:
                    f.write("When the app crashes, open the log file at:\n\n")
                    f.write(path + "\n")
            except Exception:
                pass
        from datetime import datetime
        line = datetime.now().strftime("%H:%M:%S.%f")[:12] + "  " + msg + "\n"
        _CRASH_LOG_FILE.write(line)
        _CRASH_LOG_FILE.flush()
    except Exception:
        pass

def _bootstrap_excepthook(etype, value, tb):
    """Write any uncaught exception to crash log."""
    _bootstrap_log("UNCAUGHT: " + etype.__name__ + ": " + str(value))
    try:
        import traceback
        for line in traceback.format_exc().splitlines():
            _bootstrap_log("  " + line)
    except Exception:
        pass
    sys.__excepthook__(etype, value, tb)

if __name__ == "__main__":
    sys.excepthook = _bootstrap_excepthook
    import atexit
    def _log_exit():
        try:
            path = _crash_log_path()
            with open(path, "a", encoding="utf-8") as f:
                f.write("process exit (atexit)\n")
                f.flush()
        except Exception:
            pass
    atexit.register(_log_exit)

import logging

# ⚡ MINIMAL IMPORTS - Only what's needed for QApplication
from PyQt6.QtWidgets import QApplication

# Import our logging setup
try:
    from utils.logger_setup import (
        get_application_logger,
        setup_application_logging,
        setup_exception_handler,
    )
except ImportError as e:
    print(f"Failed to import logging setup: {e}")
    logging.basicConfig(level=logging.INFO)

def main():
    # Log every step to startup_crash.log (flush immediately) so it's readable after crash
    _bootstrap_log("main() entered")

    # Catch native crashes (segfault, SIGABRT...) and write to same log file
    try:
        import faulthandler
        if _CRASH_LOG_FILE is not None:
            faulthandler.enable(file=_CRASH_LOG_FILE, all_threads=True)
            _bootstrap_log("faulthandler enabled (native crash will be logged)")
    except Exception as e:
        _bootstrap_log("faulthandler not available: " + str(e))

    # Setup comprehensive logging first
    try:
        _bootstrap_log("calling setup_application_logging...")
        logger = setup_application_logging(logging.INFO)
        setup_exception_handler()
        logger.info("Application logging initialized successfully")
        _bootstrap_log("setup_application_logging OK")
    except Exception as e:
        _bootstrap_log("setup_application_logging FAILED: " + str(e))
        print(f"Failed to setup logging: {e}")
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger()

    try:
        _bootstrap_log("Creating QApplication...")
        app = QApplication(sys.argv)
        app.setApplicationName("Data Processing Tool")
        app.setApplicationVersion("2.0.1")
        app.setOrganizationName("PyDeveloper")
        _bootstrap_log("QApplication created")

        _bootstrap_log("Importing gui.main_window...")
        from gui.main_window import DataCleanerApp
        _bootstrap_log("Importing rose_gold_styles...")
        from gui.rose_gold_styles import ROSE_GOLD_STYLESHEET
        _bootstrap_log("Importing icons...")
        from gui.icons import get_app_icon
        _bootstrap_log("GUI imports OK")

        logger.info("Setting application icon...")
        app.setWindowIcon(get_app_icon())
        logger.info("Applying Rose Gold stylesheet...")
        app.setStyleSheet(ROSE_GOLD_STYLESHEET)
        _bootstrap_log("Styles applied")

        logger.info("Creating main window...")
        _bootstrap_log("Creating DataCleanerApp()...")
        window = DataCleanerApp()
        _bootstrap_log("DataCleanerApp() created")

        logger.info("Showing main window...")
        window.show()
        _bootstrap_log("window.show() done - entering event loop")

        # Log right after first event loop tick (if crash before that, this line won't appear)
        from PyQt6.QtCore import QTimer
        def _on_first_tick():
            _bootstrap_log("event loop: first tick (Qt processed first frame)")
        QTimer.singleShot(0, _on_first_tick)

        try:
            import pyi_splash
            pyi_splash.close()
            logger.info("Splash screen closed")
        except (ImportError, ModuleNotFoundError):
            pass
        except Exception as e:
            logger.warning("Could not close splash: %s", e)

        result = app.exec()
        _bootstrap_log("event loop exited, code=" + str(result))
        return result

    except Exception as e:
        _bootstrap_log("FATAL in main: " + type(e).__name__ + ": " + str(e))
        try:
            import traceback
            _bootstrap_log(traceback.format_exc())
        except Exception:
            pass
        if "logger" in dir():
            try:
                logger.error("Fatal error in main(): %s", e)
                from utils.logger_setup import log_exception
                log_exception(context="main() function")
            except Exception:
                pass
        else:
            print("Fatal error in main():", e)
            import traceback
            traceback.print_exc()

        try:
            if "app" in dir():
                from PyQt6.QtWidgets import QMessageBox
                msg = QMessageBox()
                msg.setIcon(QMessageBox.Icon.Critical)
                msg.setWindowTitle("Fatal Error")
                msg.setText("A fatal error occurred:\n\n" + str(e))
                msg.setInformativeText("Log file (copy path to open): " + _crash_log_path())
                try:
                    app_logger = get_application_logger()
                    if app_logger and getattr(app_logger, "get_log_file_path", None) and app_logger.get_log_file_path():
                        msg.setDetailedText("Log: " + app_logger.get_log_file_path() + "\nCrash log: " + _crash_log_path())
                except Exception:
                    msg.setDetailedText("Crash log: " + _crash_log_path())
                msg.exec()
        except Exception as dialog_err:
            _bootstrap_log("Error dialog failed: " + str(dialog_err))
        return 1
    finally:
        try:
            if _CRASH_LOG_FILE is not None:
                _bootstrap_log("main() exit")
                _CRASH_LOG_FILE.close()
        except Exception:
            pass

if __name__ == "__main__":
    if multiprocessing.current_process().name == "MainProcess":
        sys.exit(main())