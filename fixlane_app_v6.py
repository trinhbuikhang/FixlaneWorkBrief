#!/usr/bin/env python3
"""
Fixlane WorkBrief Processor - Modern PyQt6 Application
Main entry point for the refactored application.
"""

import sys
import os
import logging
from pathlib import Path

# Add the current directory to Python path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

def setup_environment():
    """Setup the application environment."""
    # Ensure required directories exist
    log_dir = current_dir / "logs"
    log_dir.mkdir(exist_ok=True)
    
    # Setup logging
    from config import LogConfig
    LogConfig.setup_logging()
    
    logger = logging.getLogger(__name__)
    logger.info("=" * 50)
    logger.info("Fixlane WorkBrief Processor Starting")
    logger.info("=" * 50)
    
    return logger

def check_dependencies():
    """Check if all required dependencies are available."""
    required_packages = [
        'polars',
        'pandas', 
        'PyQt6'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        print(f"Error: Missing required packages: {', '.join(missing_packages)}")
        print("Please install them using:")
        print(f"pip install {' '.join(missing_packages)}")
        return False
    
    return True

def main():
    """Main application entry point."""
    # Check dependencies first
    if not check_dependencies():
        return 1
    
    try:
        # Setup environment
        logger = setup_environment()
        
        # Import and run the GUI application
        from gui import FixlaneApp
        
        logger.info("Starting GUI application...")
        app = FixlaneApp(sys.argv)
        
        # Run the application
        exit_code = app.run()
        
        logger.info(f"Application exited with code: {exit_code}")
        return exit_code
        
    except KeyboardInterrupt:
        print("\\nApplication interrupted by user")
        return 130
        
    except Exception as e:
        print(f"Fatal error: {e}")
        logging.error(f"Fatal error: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())