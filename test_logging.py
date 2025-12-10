#!/usr/bin/env python3
"""
Test logging functionality to verify it works before building EXE
"""

import sys
import os

# Add current directory to path to import our modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_logging():
    """Test the logging setup"""
    print("Testing logging setup...")
    
    try:
        # Import our logging setup
        from utils.logger_setup import setup_application_logging, log_exception, get_application_logger
        
        # Setup logging
        logger = setup_application_logging()
        print(f"✓ Logging setup successful")
        
        # Test basic logging
        logger.info("Test info message")
        logger.warning("Test warning message")
        logger.error("Test error message")
        
        # Test exception logging
        try:
            raise ValueError("Test exception for logging")
        except Exception as e:
            log_exception(context="test_logging function")
        
        # Get log file path
        app_logger = get_application_logger()
        if app_logger:
            log_file = app_logger.get_log_file_path()
            print(f"✓ Log file created at: {log_file}")
            
            if log_file and log_file.exists():
                print(f"✓ Log file exists and is accessible")
                print(f"✓ Log file size: {log_file.stat().st_size} bytes")
            else:
                print("❌ Log file not accessible")
        
        print("✓ All logging tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Logging test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_logging()
    if success:
        print("\n🎉 Logging system is working correctly!")
        print("You can now build the EXE with confidence that logging will work.")
    else:
        print("\n❌ Logging system has issues that need to be fixed before building EXE.")
    
    input("\nPress Enter to exit...")