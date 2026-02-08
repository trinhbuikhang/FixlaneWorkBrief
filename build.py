#!/usr/bin/env python3
"""
Build script to create executable for Data Processing Tool
"""

import os
import sys

import PyInstaller.__main__


def build_exe():
    """Build the application into a standalone executable."""

    # Get the directory of this script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Main script path
    main_script = os.path.join(script_dir, 'main.py')

    # Build command arguments
    args = [
        main_script,
        '--onefile',                    # Create a single executable file
        '--windowed',                   # Don't show console window
        '--name=DataProcessingTool',    # Name of the executable
        '--clean',                      # Clean cache before building
        '--noconfirm',                  # Don't ask for confirmation
        '--exclude-module=PyQt5',       # Exclude PyQt5 to avoid conflicts
        # Add data files if needed (e.g., config files, icons)
        # '--add-data', 'config;config',  # For Windows
        # '--add-data', 'config:config',  # For Unix
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
        'pandas',  # In case polars uses it
        'logging',
        'logging.handlers',
        'traceback',
        'pathlib',
    ]

    for imp in hidden_imports:
        args.extend(['--hidden-import', imp])

    print("Building executable with PyInstaller...")
    print(f"Command: {' '.join(args)}")

    # Run PyInstaller
    PyInstaller.__main__.run(args)

    print("\nBuild completed!")
    print("Executable created in 'dist' folder")

if __name__ == "__main__":
    build_exe()