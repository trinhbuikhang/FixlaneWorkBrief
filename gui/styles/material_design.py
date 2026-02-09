"""
Material Design Stylesheet for Data Processing Tool

Based on Material Design 3 (Material You) principles:
- Modern color system with primary/secondary/surface colors
- Elevated surfaces with depth
- Consistent spacing and padding
- Smooth animations and transitions
- Accessibility-focused contrast ratios
"""

# Material Design Color Palette
COLORS = {
    # Primary colors (Blue tones - professional, trustworthy)
    'primary': '#1976D2',           # Main brand color
    'primary_light': '#63A4FF',     # Lighter variant
    'primary_dark': '#004BA0',      # Darker variant
    'on_primary': '#FFFFFF',        # Text on primary
    
    # Secondary colors (Teal - accent, complementary)
    'secondary': '#00897B',         # Accent color
    'secondary_light': '#4EBAAA',   # Lighter variant
    'secondary_dark': '#005B4F',    # Darker variant
    'on_secondary': '#FFFFFF',      # Text on secondary
    
    # Surface colors (Background hierarchy)
    'surface': '#FFFFFF',           # Main surface
    'surface_variant': '#F5F5F5',   # Alternate surface
    'background': '#FAFAFA',        # App background
    'on_surface': '#1C1B1F',        # Text on surface
    'on_surface_variant': '#49454F', # Secondary text
    
    # Outline and borders
    'outline': '#E0E0E0',           # Borders, dividers
    'outline_variant': '#E8E8E8',   # Subtle borders
    
    # States
    'hover': '#E3F2FD',             # Hover state
    'pressed': '#BBDEFB',           # Pressed state
    'selected': '#2196F3',          # Selected state
    'disabled': '#BDBDBD',          # Disabled elements
    
    # Semantic colors
    'success': '#4CAF50',           # Success messages
    'warning': '#FF9800',           # Warnings
    'error': '#F44336',             # Errors
    'info': '#2196F3',              # Information
    
    # Shadow colors
    'shadow': 'rgba(0, 0, 0, 0.12)', # Elevation shadows
}

# Elevation levels (Material Design shadow system)
ELEVATION = {
    'level0': 'none',  # Base surface
    'level1': f'0 1px 3px {COLORS["shadow"]}, 0 1px 2px rgba(0,0,0,0.06)',  # Cards
    'level2': f'0 3px 6px {COLORS["shadow"]}, 0 2px 4px rgba(0,0,0,0.08)',  # Raised elements
    'level3': f'0 6px 12px {COLORS["shadow"]}, 0 4px 8px rgba(0,0,0,0.10)', # Modals
    'level4': f'0 12px 24px {COLORS["shadow"]}, 0 8px 16px rgba(0,0,0,0.12)', # Dropdowns
}

# Material Design Stylesheet - Minimal version for fast startup
MATERIAL_MINIMAL = f"""
/* Material Design - Minimal for fast startup */
QMainWindow, QWidget {{
    background-color: {COLORS['background']};
    font-family: 'Segoe UI', 'Roboto', 'Arial', sans-serif;
    font-size: 14px;
    color: {COLORS['on_surface']};
}}

QTabWidget::pane {{
    background-color: {COLORS['surface']};
    border: 1px solid {COLORS['outline']};
    border-radius: 8px;
}}

QTabBar::tab {{
    background-color: {COLORS['surface_variant']};
    padding: 12px 24px;
    border-radius: 8px 8px 0 0;
    margin-right: 4px;
}}

QTabBar::tab:selected {{
    background-color: {COLORS['primary']};
    color: {COLORS['on_primary']};
}}

QPushButton {{
    background-color: {COLORS['primary']};
    color: {COLORS['on_primary']};
    border: none;
    padding: 10px 24px;
    border-radius: 8px;
    font-weight: 500;
}}

QLabel {{
    color: {COLORS['on_surface']};
}}
"""

# Material Design Stylesheet - Full version with all components
MATERIAL_FULL = f"""
/* ====================================
   Material Design 3 - Full Stylesheet
   ==================================== */

/* === BASE STYLES === */
QMainWindow {{
    background-color: {COLORS['background']};
    font-family: 'Segoe UI', 'Roboto', 'Helvetica Neue', 'Arial', sans-serif;
    font-size: 14px;
    color: {COLORS['on_surface']};
}}

QWidget {{
    background-color: transparent;
    color: {COLORS['on_surface']};
    font-size: 14px;
}}

/* === TABS === */
QTabWidget::pane {{
    border: 1px solid {COLORS['outline']};
    background-color: {COLORS['surface']};
    border-radius: 12px;
    padding: 8px;
}}

QTabBar {{
    background-color: transparent;
}}

QTabBar::tab {{
    background-color: {COLORS['surface_variant']};
    color: {COLORS['on_surface_variant']};
    border: none;
    padding: 12px 24px;
    margin-right: 4px;
    border-radius: 8px 8px 0 0;
    font-weight: 500;
    font-size: 14px;
    min-width: 100px;
}}

QTabBar::tab:hover {{
    background-color: {COLORS['hover']};
    color: {COLORS['primary']};
}}

QTabBar::tab:selected {{
    background-color: {COLORS['primary']};
    color: {COLORS['on_primary']};
    font-weight: 600;
}}

/* === BUTTONS === */
QPushButton {{
    background-color: {COLORS['primary']};
    color: {COLORS['on_primary']};
    border: none;
    padding: 10px 24px;
    border-radius: 8px;
    font-weight: 500;
    font-size: 14px;
    min-height: 36px;
}}

QPushButton:hover {{
    background-color: {COLORS['primary_light']};
}}

QPushButton:pressed {{
    background-color: {COLORS['primary_dark']};
}}

QPushButton:disabled {{
    background-color: {COLORS['disabled']};
    color: rgba(0, 0, 0, 0.38);
}}

/* Secondary Button Style */
QPushButton[class="secondary"] {{
    background-color: {COLORS['secondary']};
    color: {COLORS['on_secondary']};
}}

QPushButton[class="secondary"]:hover {{
    background-color: {COLORS['secondary_light']};
}}

QPushButton[class="secondary"]:pressed {{
    background-color: {COLORS['secondary_dark']};
}}

/* Outlined Button Style */
QPushButton[class="outlined"] {{
    background-color: transparent;
    color: {COLORS['primary']};
    border: 2px solid {COLORS['primary']};
}}

QPushButton[class="outlined"]:hover {{
    background-color: {COLORS['hover']};
}}

/* Text Button Style */
QPushButton[class="text"] {{
    background-color: transparent;
    color: {COLORS['primary']};
    border: none;
    padding: 8px 16px;
}}

QPushButton[class="text"]:hover {{
    background-color: {COLORS['hover']};
}}

/* === LABELS === */
QLabel {{
    color: {COLORS['on_surface']};
    background-color: transparent;
    font-size: 14px;
}}

/* Title Labels */
QLabel[objectName="titleLabel"] {{
    font-size: 20px;
    font-weight: 600;
    color: {COLORS['on_surface']};
    margin-bottom: 8px;
}}

/* Subtitle Labels */
QLabel[objectName="subtitleLabel"] {{
    font-size: 16px;
    font-weight: 500;
    color: {COLORS['on_surface']};
    margin-bottom: 4px;
}}

/* Description Labels */
QLabel[objectName="descriptionLabel"] {{
    font-size: 14px;
    color: {COLORS['on_surface_variant']};
    line-height: 1.5;
    background-color: {COLORS['surface_variant']};
    padding: 12px;
    border-radius: 8px;
    border: 1px solid {COLORS['outline_variant']};
}}

/* Status Labels */
QLabel[objectName="statusLabel"] {{
    font-size: 13px;
    color: {COLORS['on_surface_variant']};
    padding: 4px 8px;
    background-color: {COLORS['surface_variant']};
    border-radius: 4px;
}}

/* === TEXT INPUT === */
QLineEdit {{
    background-color: {COLORS['surface']};
    color: {COLORS['on_surface']};
    border: 2px solid {COLORS['outline']};
    border-radius: 8px;
    padding: 10px 16px;
    font-size: 14px;
    min-height: 40px;
}}

QLineEdit:focus {{
    border: 2px solid {COLORS['primary']};
    background-color: {COLORS['surface']};
}}

QLineEdit:hover {{
    border: 2px solid {COLORS['on_surface_variant']};
}}

QLineEdit:disabled {{
    background-color: {COLORS['surface_variant']};
    color: {COLORS['disabled']};
    border: 2px solid {COLORS['outline_variant']};
}}

/* Placeholder text */
QLineEdit::placeholder {{
    color: {COLORS['on_surface_variant']};
    font-style: italic;
}}

/* === TEXT AREA === */
QTextEdit, QPlainTextEdit {{
    background-color: {COLORS['surface']};
    color: {COLORS['on_surface']};
    border: 2px solid {COLORS['outline']};
    border-radius: 8px;
    padding: 12px;
    font-size: 14px;
    font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
}}

QTextEdit:focus, QPlainTextEdit:focus {{
    border: 2px solid {COLORS['primary']};
}}

/* === COMBO BOX === */
QComboBox {{
    background-color: {COLORS['surface']};
    color: {COLORS['on_surface']};
    border: 2px solid {COLORS['outline']};
    border-radius: 8px;
    padding: 10px 16px;
    font-size: 14px;
    min-height: 40px;
}}

QComboBox:hover {{
    border: 2px solid {COLORS['on_surface_variant']};
}}

QComboBox:focus {{
    border: 2px solid {COLORS['primary']};
}}

QComboBox::drop-down {{
    border: none;
    width: 32px;
}}

QComboBox::down-arrow {{
    image: none;
    border-left: 5px solid transparent;
    border-right: 5px solid transparent;
    border-top: 5px solid {COLORS['on_surface']};
    margin-right: 8px;
}}

QComboBox QAbstractItemView {{
    background-color: {COLORS['surface']};
    border: 1px solid {COLORS['outline']};
    border-radius: 8px;
    selection-background-color: {COLORS['primary']};
    selection-color: {COLORS['on_primary']};
    padding: 4px;
}}

/* === SCROLL BAR === */
QScrollBar:vertical {{
    background-color: {COLORS['surface_variant']};
    width: 12px;
    border-radius: 6px;
    margin: 0px;
}}

QScrollBar::handle:vertical {{
    background-color: {COLORS['on_surface_variant']};
    border-radius: 6px;
    min-height: 30px;
}}

QScrollBar::handle:vertical:hover {{
    background-color: {COLORS['primary']};
}}

QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{
    height: 0px;
}}

QScrollBar:horizontal {{
    background-color: {COLORS['surface_variant']};
    height: 12px;
    border-radius: 6px;
    margin: 0px;
}}

QScrollBar::handle:horizontal {{
    background-color: {COLORS['on_surface_variant']};
    border-radius: 6px;
    min-width: 30px;
}}

QScrollBar::handle:horizontal:hover {{
    background-color: {COLORS['primary']};
}}

QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {{
    width: 0px;
}}

/* === GROUP BOX === */
QGroupBox {{
    background-color: {COLORS['surface']};
    border: 1px solid {COLORS['outline']};
    border-radius: 12px;
    padding: 20px;
    margin-top: 16px;
    font-weight: 500;
    font-size: 14px;
}}

QGroupBox::title {{
    subcontrol-origin: margin;
    subcontrol-position: top left;
    padding: 4px 12px;
    background-color: {COLORS['primary']};
    color: {COLORS['on_primary']};
    border-radius: 4px;
    font-weight: 600;
}}

/* === FRAME === */
QFrame {{
    background-color: transparent;
    border: none;
}}

QFrame[frameShape="4"] {{ /* HLine */
    max-height: 1px;
    background-color: {COLORS['outline']};
}}

QFrame[frameShape="5"] {{ /* VLine */
    max-width: 1px;
    background-color: {COLORS['outline']};
}}

/* === CHECKBOX === */
QCheckBox {{
    spacing: 8px;
    color: {COLORS['on_surface']};
    font-size: 14px;
}}

QCheckBox::indicator {{
    width: 20px;
    height: 20px;
    border: 2px solid {COLORS['outline']};
    border-radius: 4px;
    background-color: {COLORS['surface']};
}}

QCheckBox::indicator:hover {{
    border: 2px solid {COLORS['primary']};
}}

QCheckBox::indicator:checked {{
    background-color: {COLORS['primary']};
    border: 2px solid {COLORS['primary']};
    image: url(data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTYiIGhlaWdodD0iMTYiIHZpZXdCb3g9IjAgMCAxNiAxNiIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPHBhdGggZD0iTTEzLjMzMzMgNEw2IDExLjMzMzNMMi42NjY2NyA4IiBzdHJva2U9IndoaXRlIiBzdHJva2Utd2lkdGg9IjIiIHN0cm9rZS1saW5lY2FwPSJyb3VuZCIgc3Ryb2tlLWxpbmVqb2luPSJyb3VuZCIvPgo8L3N2Zz4K);
}}

/* === RADIO BUTTON === */
QRadioButton {{
    spacing: 8px;
    color: {COLORS['on_surface']};
    font-size: 14px;
}}

QRadioButton::indicator {{
    width: 20px;
    height: 20px;
    border: 2px solid {COLORS['outline']};
    border-radius: 10px;
    background-color: {COLORS['surface']};
}}

QRadioButton::indicator:hover {{
    border: 2px solid {COLORS['primary']};
}}

QRadioButton::indicator:checked {{
    background-color: {COLORS['primary']};
    border: 2px solid {COLORS['primary']};
}}

QRadioButton::indicator:checked::after {{
    content: "";
    width: 10px;
    height: 10px;
    border-radius: 5px;
    background-color: {COLORS['on_primary']};
    position: absolute;
    top: 5px;
    left: 5px;
}}

/* === PROGRESS BAR === */
QProgressBar {{
    background-color: {COLORS['surface_variant']};
    border: none;
    border-radius: 8px;
    text-align: center;
    height: 16px;
    color: {COLORS['on_primary']};
    font-weight: 600;
}}

QProgressBar::chunk {{
    background-color: {COLORS['primary']};
    border-radius: 8px;
}}

/* === STATUS BAR === */
QStatusBar {{
    background-color: {COLORS['surface']};
    color: {COLORS['on_surface_variant']};
    border-top: 1px solid {COLORS['outline']};
    padding: 4px;
    font-size: 13px;
}}

/* === MENU BAR === */
QMenuBar {{
    background-color: {COLORS['surface']};
    color: {COLORS['on_surface']};
    border-bottom: 1px solid {COLORS['outline']};
    padding: 4px;
}}

QMenuBar::item {{
    padding: 8px 16px;
    background-color: transparent;
    border-radius: 4px;
}}

QMenuBar::item:selected {{
    background-color: {COLORS['hover']};
    color: {COLORS['primary']};
}}

/* === MENU === */
QMenu {{
    background-color: {COLORS['surface']};
    border: 1px solid {COLORS['outline']};
    border-radius: 8px;
    padding: 4px;
}}

QMenu::item {{
    padding: 8px 24px;
    border-radius: 4px;
}}

QMenu::item:selected {{
    background-color: {COLORS['primary']};
    color: {COLORS['on_primary']};
}}

/* === TABLE === */
QTableWidget {{
    background-color: {COLORS['surface']};
    border: 1px solid {COLORS['outline']};
    border-radius: 8px;
    gridline-color: {COLORS['outline_variant']};
    selection-background-color: {COLORS['primary']};
    selection-color: {COLORS['on_primary']};
}}

QTableWidget::item {{
    padding: 8px;
}}

QTableWidget::item:hover {{
    background-color: {COLORS['hover']};
}}

QHeaderView::section {{
    background-color: {COLORS['primary']};
    color: {COLORS['on_primary']};
    padding: 10px;
    border: none;
    font-weight: 600;
    font-size: 14px;
}}

/* === TOOLTIP === */
QToolTip {{
    background-color: {COLORS['on_surface']};
    color: {COLORS['surface']};
    border: none;
    border-radius: 4px;
    padding: 8px 12px;
    font-size: 13px;
}}

/* === MESSAGE BOX === */
QMessageBox {{
    background-color: {COLORS['surface']};
}}

QMessageBox QLabel {{
    color: {COLORS['on_surface']};
    font-size: 14px;
}}

QMessageBox QPushButton {{
    min-width: 80px;
}}
"""

def get_material_stylesheet(minimal=False):
    """
    Get Material Design stylesheet
    
    Args:
        minimal: If True, returns minimal stylesheet for fast startup
                If False, returns full stylesheet with all components
    
    Returns:
        str: QSS stylesheet string
    """
    return MATERIAL_MINIMAL if minimal else MATERIAL_FULL

def get_color(color_name):
    """
    Get a color from the Material Design palette
    
    Args:
        color_name: Name of the color (e.g., 'primary', 'secondary')
    
    Returns:
        str: Hex color code
    """
    return COLORS.get(color_name, COLORS['primary'])
