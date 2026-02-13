"""
Lazy Loading Helper for PyQt6 Tabs

Defers loading tab content until user clicks on that tab.
Reduces startup time and initial memory footprint.
"""

import logging
from typing import Dict, Callable, Optional
from PyQt6.QtWidgets import QWidget, QTabWidget, QVBoxLayout, QLabel
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QIcon

logger = logging.getLogger(__name__)


class LazyTabWidget(QTabWidget):
    """
    TabWidget with lazy loading capability.
    
    Tab content is only loaded when:
    1. User clicks on that tab for the first time, OR
    2. Explicitly preloaded via method call
    
    Example:
        tabs = LazyTabWidget()
        tabs.add_lazy_tab("LMD Cleaner", lambda: LMDCleanerTab())
        tabs.add_lazy_tab("Lane Fix", lambda: LaneFixTab())
    """
    
    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)
        
        # Track loaded tabs: {index: loaded_widget}
        self._loaded_tabs: Dict[int, QWidget] = {}
        
        # Tab factories: {index: factory_function}
        self._tab_factories: Dict[int, Callable[[], QWidget]] = {}
        
        # Tab names for logging
        self._tab_names: Dict[int, str] = {}
        
        # Tab icons: {index: QIcon}
        self._tab_icons: Dict[int, Optional[QIcon]] = {}
        
        # Tab tooltips: {index: tooltip_text}
        self._tab_tooltips: Dict[int, Optional[str]] = {}
        
        # Connect signal
        self.currentChanged.connect(self._on_tab_changed)
        
        logger.info("LazyTabWidget initialized")
    
    def add_lazy_tab(
        self, 
        tab_name: str, 
        factory: Callable[[], QWidget],
        load_immediately: bool = False,
        icon: Optional[QIcon] = None,
        tooltip: Optional[str] = None
    ) -> int:
        """
        Add a tab with lazy loading.
        
        Args:
            tab_name: Display name of the tab
            factory: Function that returns the tab widget (lambda: TabClass())
            load_immediately: True to load immediately, False for lazy loading
            icon: Optional icon for the tab
            tooltip: Optional tooltip text for the tab
        
        Returns:
            Index of the added tab
        """
        # Create placeholder widget
        placeholder = self._create_placeholder_widget(tab_name)
        
        # Add placeholder to tab widget (with or without icon)
        if icon:
            index = self.addTab(placeholder, icon, tab_name)
        else:
            index = self.addTab(placeholder, tab_name)
        
        # Set tooltip if provided
        if tooltip:
            self.setTabToolTip(index, tooltip)
        
        # Store factory, name, icon, and tooltip
        self._tab_factories[index] = factory
        self._tab_names[index] = tab_name
        self._tab_icons[index] = icon
        self._tab_tooltips[index] = tooltip
        
        logger.debug(f"Added lazy tab '{tab_name}' at index {index}")
        
        # Load immediately if requested
        if load_immediately:
            self._load_tab(index)
        
        return index
    
    def _create_placeholder_widget(self, tab_name: str) -> QWidget:
        """
        Create a simple placeholder widget.
        
        Displayed until the real tab is loaded.
        """
        placeholder = QWidget()
        layout = QVBoxLayout(placeholder)
        
        label = QLabel(f"Loading {tab_name}...")
        label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        label.setStyleSheet("""
            QLabel {
                color: #666;
                font-size: 14px;
                padding: 40px;
            }
        """)
        
        layout.addWidget(label)
        
        return placeholder
    
    def _on_tab_changed(self, index: int):
        """
        Callback when user switches tabs.
        
        Load tab content if not already loaded.
        """
        if index >= 0:  # Valid index
            self._load_tab(index)
    
    def _load_tab(self, index: int):
        """
        Load tab content if not already loaded.
        
        Args:
            index: Index of the tab to load
        """
        # Already loaded -> skip
        if index in self._loaded_tabs:
            return  # Don't log again
        
        # No factory -> skip
        if index not in self._tab_factories:
            logger.warning(f"No factory for tab {index}")
            return
        
        tab_name = self._tab_names.get(index, f"Tab {index}")
        
        # Mark as loaded IMMEDIATELY to prevent re-entry during widget creation
        self._loaded_tabs[index] = None  # Temporary placeholder
        
        try:
            logger.info(f"Loading tab: {tab_name}")
            
            # Call factory to create real widget
            widget = self._tab_factories[index]()
            
            # Save current tab index to restore after replacement
            current_index = self.currentIndex()
            
            # Get stored icon and tooltip
            icon = self._tab_icons.get(index)
            tooltip = self._tab_tooltips.get(index)
            
            # Block signals during tab replacement to prevent recursion
            self.blockSignals(True)
            
            # Replace placeholder with real widget
            self.removeTab(index)
            
            # Insert with icon if available
            if icon:
                self.insertTab(index, widget, icon, tab_name)
            else:
                self.insertTab(index, widget, tab_name)
            
            # Restore tooltip if available
            if tooltip:
                self.setTabToolTip(index, tooltip)
            
            # Restore current tab if it was the one we just loaded
            if current_index == index:
                self.setCurrentIndex(index)
            
            # Re-enable signals
            self.blockSignals(False)
            
            # Update loaded tabs with actual widget
            self._loaded_tabs[index] = widget
            
            logger.info(f"Successfully loaded tab: {tab_name}")
            
        except Exception as e:
            logger.error(f"Failed to load tab '{tab_name}': {e}")
            
            # Save current tab index
            current_index = self.currentIndex()
            
            # Get stored icon
            icon = self._tab_icons.get(index)
            
            # Block signals during error widget insertion
            self.blockSignals(True)
            
            # Show error in placeholder
            error_widget = self._create_error_widget(tab_name, str(e))
            self.removeTab(index)
            
            # Insert error widget with icon if available
            if icon:
                self.insertTab(index, error_widget, icon, f"⚠️ {tab_name}")
            else:
                self.insertTab(index, error_widget, f"⚠️ {tab_name}")
            
            # Restore current tab if it was the one that failed
            if current_index == index:
                self.setCurrentIndex(index)
            
            # Re-enable signals
            self.blockSignals(False)
    
    def _create_error_widget(self, tab_name: str, error: str) -> QWidget:
        """
        Create widget to display error when tab loading fails.
        """
        error_widget = QWidget()
        layout = QVBoxLayout(error_widget)
        
        label = QLabel(f"Failed to load {tab_name}\n\n{error}")
        label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        label.setStyleSheet("""
            QLabel {
                color: #D32F2F;
                font-size: 13px;
                padding: 40px;
            }
        """)
        label.setWordWrap(True)
        
        layout.addWidget(label)
        
        return error_widget
    
    def preload_tab(self, index: int):
        """
        Preload a specific tab.
        
        Useful for loading important tabs in the background.
        
        Args:
            index: Index of the tab to preload
        """
        self._load_tab(index)
    
    def preload_all_tabs(self):
        """
        Preload all tabs.
        
        Useful when app has started and you want to load everything.
        """
        for index in self._tab_factories.keys():
            self._load_tab(index)
    
    def is_tab_loaded(self, index: int) -> bool:
        """
        Check if tab has been loaded.
        
        Args:
            index: Index of the tab
        
        Returns:
            True if loaded, False otherwise
        """
        return index in self._loaded_tabs
    
    def get_loaded_tab(self, index: int) -> Optional[QWidget]:
        """
        Get tab widget if already loaded.
        
        Args:
            index: Index of the tab
        
        Returns:
            Widget if loaded, None otherwise
        """
        return self._loaded_tabs.get(index)
    
    def get_loaded_tabs_count(self) -> int:
        """
        Get number of tabs that have been loaded.
        
        Returns:
            Number of loaded tabs
        """
        return len(self._loaded_tabs)


# ============================================================================
# USAGE EXAMPLE
# ============================================================================

if __name__ == "__main__":
    from PyQt6.QtWidgets import QApplication, QPushButton
    import sys
    
    app = QApplication(sys.argv)
    
    # Create lazy tab widget
    tabs = LazyTabWidget()
    tabs.resize(800, 600)
    
    # Add tabs with lazy loading
    tabs.add_lazy_tab(
        "Tab 1",
        lambda: QPushButton("Content of Tab 1")
    )
    
    tabs.add_lazy_tab(
        "Tab 2", 
        lambda: QPushButton("Content of Tab 2")
    )
    
    tabs.add_lazy_tab(
        "Tab 3",
        lambda: QPushButton("Content of Tab 3")
    )
    
    # Load first tab immediately
    tabs.add_lazy_tab(
        "Tab 4 (Preloaded)",
        lambda: QPushButton("This tab is preloaded"),
        load_immediately=True
    )
    
    tabs.show()
    import logging
    logging.getLogger(__name__).info("Loaded tabs: %s", tabs.get_loaded_tabs_count())
    sys.exit(app.exec())
