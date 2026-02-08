"""
Lazy Import Wrapper for Heavy Libraries

Defers importing heavy libraries until they are actually needed.
Reduces application startup time.
"""

import importlib
import logging
from typing import Any

logger = logging.getLogger(__name__)


class LazyImporter:
    """
    Wrapper class to lazy load a module.
    
    Module is only imported on first attribute access.
    
    Example:
        polars = LazyImporter('polars')
        # polars not imported yet
        
        df = polars.read_csv('file.csv')
        # polars imported at this point
    """
    
    def __init__(self, module_name: str):
        self._module_name = module_name
        self._module = None
        self._import_attempted = False
    
    def _load_module(self):
        """Load module if not already loaded"""
        if not self._import_attempted:
            self._import_attempted = True
            try:
                logger.debug(f"Lazy loading module: {self._module_name}")
                self._module = importlib.import_module(self._module_name)
                logger.debug(f"Successfully loaded: {self._module_name}")
            except ImportError as e:
                logger.error(f"Failed to import {self._module_name}: {e}")
                raise
    
    def __getattr__(self, name: str) -> Any:
        """
        Intercept attribute access and load module if needed
        """
        if self._module is None:
            self._load_module()
        
        return getattr(self._module, name)
    
    def __dir__(self):
        """Support for dir() và autocomplete"""
        if self._module is None:
            self._load_module()
        return dir(self._module)


class LazyClass:
    """
    Lazy load a specific class from a module.
    
    Useful when you only need one class from a large module.
    
    Example:
        DataCleaner = LazyClass('utils.data_processor', 'DataCleaner')
        # Module not imported yet
        
        cleaner = DataCleaner()
        # Module imported and class returned
    """
    
    def __init__(self, module_name: str, class_name: str):
        self._module_name = module_name
        self._class_name = class_name
        self._class = None
    
    def _load_class(self):
        """Load class if not already loaded"""
        if self._class is None:
            logger.debug(f"Lazy loading class: {self._module_name}.{self._class_name}")
            module = importlib.import_module(self._module_name)
            self._class = getattr(module, self._class_name)
    
    def __call__(self, *args, **kwargs):
        """Instantiate class when called"""
        if self._class is None:
            self._load_class()
        return self._class(*args, **kwargs)
    
    def __getattr__(self, name: str) -> Any:
        """Access class attributes/methods"""
        if self._class is None:
            self._load_class()
        return getattr(self._class, name)


# ============================================================================
# PRE-CONFIGURED LAZY IMPORTS
# Common heavy libraries used in the app
# ============================================================================

# Polars - main data processing library (very heavy)
polars = LazyImporter('polars')

# Pandas - if used (heavy)
pandas = LazyImporter('pandas')

# NumPy - if used
numpy = LazyImporter('numpy')


def get_lazy_import(module_name: str) -> LazyImporter:
    """
    Factory function để tạo lazy importer.
    
    Args:
        module_name: Tên module cần lazy load
    
    Returns:
        LazyImporter instance
    
    Example:
        csv_module = get_lazy_import('csv')
        reader = csv_module.DictReader(file)
    """
    return LazyImporter(module_name)


def preload_module(module_name: str) -> None:
    """
    Preload a module in the background.
    
    Useful for loading modules that will be needed later,
    without blocking startup.
    
    Args:
        module_name: Name of the module to preload
    """
    try:
        importlib.import_module(module_name)
        logger.info(f"Preloaded module: {module_name}")
    except ImportError as e:
        logger.warning(f"Failed to preload {module_name}: {e}")


# ============================================================================
# USAGE EXAMPLES
# ============================================================================

if __name__ == "__main__":
    # Example 1: Lazy module import
    print("Creating lazy importer for polars...")
    pl = LazyImporter('polars')
    print("polars not imported yet")
    
    # Module sẽ được import ở đây
    print("Accessing polars.DataFrame...")
    df_class = pl.DataFrame
    print(f"polars imported! DataFrame class: {df_class}")
    
    # Example 2: Lazy class import
    print("\nCreating lazy class importer...")
    # (giả sử có class này)
    # Processor = LazyClass('utils.base_processor', 'BaseProcessor')
    # processor = Processor()  # Import happens here
