"""
Optimized Build Script for Data Processing Tool

This script creates a minimal executable by excluding unnecessary libraries
that PyInstaller incorrectly auto-detects.

Target: Reduce from ~800MB to 150-200MB
"""

import PyInstaller.__main__
import sys

# List of heavy libraries to EXCLUDE (not used in our app)
EXCLUDE_MODULES = [
    # NOTE: pandas + numpy are INCLUDED because timestamp_handler needs pandas for Lane Fix
    # pandas will automatically bundle the minimal numpy it needs
    
    # Data science libraries (we use polars + pandas for timestamps)
    # 'pandas', # KEEP - needed for timestamp parsing in Lane Fix
    # 'numpy',  # KEEP - pandas dependency
    'scipy',
    
    # Machine learning frameworks (not used)
    'torch',
    'torchvision', 
    'tensorflow',
    'tensorboard',
    'keras',
    'sklearn',
    'scikit-learn',
    
    # Plotting libraries (not used)
    'matplotlib',
    'seaborn',
    'plotly',
    
    # Jupyter/IPython (not used)
    'IPython',
    'jupyter',
    'jupyter_client',
    'jupyter_core',
    'notebook',
    'nbconvert',
    'nbformat',
    
    # Other unnecessary modules
    'PIL',
    'Pillow',
    'cv2',
    'opencv',
    'sympy',
    
    # Qt5 (we use Qt6)
    'PyQt5',
    'PySide2',
    'PySide6',
    
    # Testing frameworks
    'pytest',
    'unittest',
    'nose',
    
    # Documentation tools
    'sphinx',
    'docutils',
]

def build_optimized_exe():
    """Build optimized executable with minimal dependencies"""
    
    print("=" * 70)
    print("OPTIMIZED BUILD - Data Processing Tool")
    print("=" * 70)
    print(f"\nExcluding {len(EXCLUDE_MODULES)} unnecessary modules...")
    print("\nBuilding optimized executable...\n")
    
    # Build exclude arguments
    exclude_args = []
    for module in EXCLUDE_MODULES:
        exclude_args.extend(['--exclude-module', module])
    
    # PyInstaller arguments
    args = [
        'main.py',
        '--name=DataProcessingTool_Optimized',
        '--onefile',
        '--noconsole',
        '--clean',
        
        # Hidden imports (only what we actually need)
        '--hidden-import=PyQt6.QtCore',
        '--hidden-import=PyQt6.QtGui', 
        '--hidden-import=PyQt6.QtWidgets',
        '--hidden-import=polars',
        '--hidden-import=logging.handlers',
        
        # Optimize
        '--strip',  # Strip debug symbols
        '--noupx',  # Disable UPX (can cause issues)
        
        # Add exclude arguments
        *exclude_args,
    ]
    
    try:
        PyInstaller.__main__.run(args)
        print("\n" + "=" * 70)
        print("BUILD COMPLETED SUCCESSFULLY!")
        print("=" * 70)
        print("\nCheck dist/DataProcessingTool_Optimized.exe")
        print("\nCompare size with original build:")
        print("  Original: ~722MB")
        print("  Target: 150-200MB")
        return 0
        
    except Exception as e:
        print(f"\n❌ Build failed: {e}", file=sys.stderr)
        return 1

if __name__ == '__main__':
    sys.exit(build_optimized_exe())
