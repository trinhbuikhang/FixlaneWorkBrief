#!/usr/bin/env python3
"""
Build script with debug console enabled for troubleshooting EXE issues
"""

import PyInstaller.__main__
import os
import sys

def build_exe_debug():
    """Build the application with console enabled for debugging."""

    # Get the directory of this script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Main script path
    main_script = os.path.join(script_dir, 'main.py')

    # Build command arguments for DEBUG version
    args = [
        main_script,
        '--onefile',                    # Create a single executable file
        '--console',                    # SHOW console window for debugging
        '--name=DataProcessingTool_Debug', # Name of the executable
        '--clean',                      # Clean cache before building
        '--noconfirm',                  # Don't ask for confirmation
        '--exclude-module=PyQt5',       # Exclude PyQt5 to avoid conflicts
        '--debug=all',                  # Enable all debug options
    ]

    # Add icon if it exists
    icon_path = os.path.join(script_dir, 'icon.ico')
    if os.path.exists(icon_path):
        args.extend(['--icon', icon_path])

    # Add hidden imports that PyInstaller might miss
    hidden_imports = [
        'PyQt6.QtCore',
        'PyQt6.QtGui', 
        'PyQt6.QtWidgets',
        'polars',
        'pandas',
        'logging',
        'logging.handlers',
        'traceback',
        'pathlib',
        'utils.logger_setup',
        'utils.client_feedback_processor',
        'utils.add_columns_processor',
        'utils.data_processor',
        'utils.laneFix_polar_data_processor',
        'utils.timestamp_handler',
        'gui.main_window',
        'gui.modern_styles',
        'gui.styles',
        'gui.tabs.client_feedback_tab',
        'gui.tabs.add_columns_tab',
        'gui.tabs.lmd_cleaner_tab',
        'gui.tabs.laneFix_tab',
        'config.laneFix_config',
    ]

    for imp in hidden_imports:
        args.extend(['--hidden-import', imp])

    print("Building DEBUG executable with PyInstaller...")
    print("This version will show console output for debugging.")
    print(f"Arguments: {' '.join(args)}")

    try:
        # Run PyInstaller
        PyInstaller.__main__.run(args)
        
        print("\n" + "="*80)
        print("Build completed!")
        print("The executable is in the 'dist' folder.")
        print("DEBUG version will show console window with detailed logging.")
        print("="*80)
        
    except Exception as e:
        print(f"Build failed: {e}")
        return False
    
    return True

def build_exe_release():
    """Build the application without console for release."""

    # Get the directory of this script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Main script path
    main_script = os.path.join(script_dir, 'main.py')

    # Build command arguments for RELEASE version
    args = [
        main_script,
        '--onefile',                    # Create a single executable file
        '--windowed',                   # Don't show console window
        '--name=DataProcessingTool',    # Name of the executable
        '--clean',                      # Clean cache before building
        '--noconfirm',                  # Don't ask for confirmation
        '--exclude-module=PyQt5',       # Exclude PyQt5 to avoid conflicts
    ]

    # Add icon if it exists
    icon_path = os.path.join(script_dir, 'icon.ico')
    if os.path.exists(icon_path):
        args.extend(['--icon', icon_path])

    # Add hidden imports that PyInstaller might miss
    hidden_imports = [
        'PyQt6.QtCore',
        'PyQt6.QtGui', 
        'PyQt6.QtWidgets',
        'polars',
        'pandas',
        'logging',
        'logging.handlers',
        'traceback',
        'pathlib',
        'utils.logger_setup',
        'utils.client_feedback_processor',
        'utils.add_columns_processor',
        'utils.data_processor',
        'utils.laneFix_polar_data_processor',
        'utils.timestamp_handler',
        'gui.main_window',
        'gui.modern_styles',
        'gui.styles',
        'gui.tabs.client_feedback_tab',
        'gui.tabs.add_columns_tab',
        'gui.tabs.lmd_cleaner_tab',
        'gui.tabs.laneFix_tab',
        'config.laneFix_config',
    ]

    for imp in hidden_imports:
        args.extend(['--hidden-import', imp])

    print("Building RELEASE executable with PyInstaller...")
    print("This version will NOT show console window.")
    print(f"Arguments: {' '.join(args)}")

    try:
        # Run PyInstaller
        PyInstaller.__main__.run(args)
        
        print("\n" + "="*80)
        print("Build completed!")
        print("The executable is in the 'dist' folder.")
        print("RELEASE version will log to file only (check logs folder).")
        print("="*80)
        
    except Exception as e:
        print(f"Build failed: {e}")
        return False
    
    return True

if __name__ == "__main__":
    print("Data Processing Tool - Build Script")
    print("="*50)
    print("1. Build DEBUG version (with console)")
    print("2. Build RELEASE version (no console)")
    print("3. Build both versions")
    
    choice = input("\nEnter your choice (1/2/3): ").strip()
    
    if choice == "1":
        build_exe_debug()
    elif choice == "2":
        build_exe_release()
    elif choice == "3":
        print("\nBuilding DEBUG version first...")
        if build_exe_debug():
            print("\nBuilding RELEASE version...")
            build_exe_release()
    else:
        print("Invalid choice. Building DEBUG version by default...")
        build_exe_debug()
    
    print("\nBuild process completed!")
    input("Press Enter to exit...")