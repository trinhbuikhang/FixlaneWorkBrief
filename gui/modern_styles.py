"""
Modern Professional Stylesheet for Data Processing Tool
"""

# ⚡ MINIMAL STYLESHEET - Only essential styles for fast startup
# Loaded immediately to get window visible ASAP
MINIMAL_STYLESHEET = """
/* Essential styles only - for fast startup */
QMainWindow, QWidget {
    background-color: #FAFAFA;
    font-family: 'Segoe UI', Tahoma, Arial, sans-serif;
    font-size: 12px;
}

QTabWidget::pane {
    background-color: white;
    border: 1px solid #E1E1E1;
}

QTabBar::tab {
    background-color: #F8F8F8;
    padding: 8px 16px;
}

QTabBar::tab:selected {
    background-color: white;
    color: #0078D4;
}

QPushButton {
    background-color: #0078D4;
    color: white;
    border: none;
    padding: 8px 16px;
    border-radius: 4px;
}

QLabel {
    color: #323130;
}
"""

# ⚡ FULL STYLESHEET - Complete styling loaded after window shown
# This can take longer without impacting perceived startup time
MODERN_STYLESHEET = """
/* Main Application Styling */
QMainWindow {
    background-color: #FAFAFA;
    font-family: 'Segoe UI', Tahoma, Arial, sans-serif;
}

QTabWidget::pane {
    border: 1px solid #E1E1E1;
    background-color: white;
    border-radius: 8px;
}

QTabBar::tab {
    background-color: #F8F8F8;
    border: 1px solid #E1E1E1;
    padding: 8px 16px;
    margin-right: 2px;
    border-top-left-radius: 6px;
    border-top-right-radius: 6px;
    font-weight: 500;
}

QTabBar::tab:selected {
    background-color: white;
    border-bottom-color: white;
    color: #0078D4;
    font-weight: 600;
}

QTabBar::tab:hover {
    background-color: #F0F6FF;
}

/* Typography - Optimized for half-screen */
QLabel[objectName="titleLabel"] {
    font-size: 16px;
    font-weight: 600;
    color: #323130;
    margin-bottom: 4px;
    padding-left: 4px;
}

QLabel[objectName="descriptionLabel"] {
    font-size: 11px;
    color: #605E5C;
    line-height: 1.3;
    margin-bottom: 8px;
    padding: 8px;
    background-color: #F8F8F8;
    border: 1px solid #EDEBE9;
    border-radius: 4px;
}

QLabel[objectName="statusLabel"] {
    font-size: 12px;
    color: #323130;
    padding: 8px;
    background-color: #F3F2F1;
    border-radius: 4px;
    font-weight: 500;
}

/* Cards and Containers */
QFrame[objectName="fileCard"] {
    background-color: white;
    border: 1px solid #E1E1E1;
    border-radius: 8px;
    padding: 16px;
    margin: 4px;
}

QFrame[objectName="fileCard"]:hover {
    border-color: #C7C7C7;
}

QGroupBox {
    font-weight: 600;
    font-size: 12px;
    color: #323130;
    border: 1px solid #E1E1E1;
    border-radius: 6px;
    margin-top: 8px;
    padding-top: 6px;
    background-color: white;
}

QGroupBox::title {
    subcontrol-origin: margin;
    left: 8px;
    padding: 0 6px 0 6px;
    background-color: white;
}

/* Input Fields */
QLineEdit {
    border: 1px solid #E1E1E1;
    border-radius: 3px;
    padding: 6px 8px;
    font-size: 11px;
    background-color: white;
    selection-background-color: #0078D4;
}

QLineEdit:focus {
    border-color: #0078D4;
    outline: none;
}

QLineEdit:read-only {
    background-color: #F8F8F8;
    color: #605E5C;
}

QLineEdit::placeholder {
    color: #A19F9D;
    font-style: italic;
}

/* Buttons */
QPushButton {
    background-color: #F3F2F1;
    border: 1px solid #C8C6C4;
    border-radius: 3px;
    padding: 6px 12px;
    font-size: 11px;
    font-weight: 500;
    color: #323130;
}

QPushButton:hover {
    background-color: #EDEBE9;
    border-color: #A19F9D;
}

QPushButton:pressed {
    background-color: #E1DFDD;
}

QPushButton[objectName="processButton"] {
    background-color: #0078D4;
    color: white;
    border: 1px solid #106EBE;
    font-weight: 600;
    padding: 10px 20px;
}

QPushButton[objectName="processButton"]:hover {
    background-color: #106EBE;
}

QPushButton[objectName="processButton"]:pressed {
    background-color: #005A9E;
}

QPushButton[objectName="processButton"]:disabled {
    background-color: #C8C6C4;
    color: #A19F9D;
    border-color: #E1DFDD;
}

QPushButton[objectName="successButton"] {
    background-color: #107C10;
    color: white;
    border: 1px solid #0E6C0E;
}

QPushButton[objectName="warningButton"] {
    background-color: #FF8C00;
    color: white;
    border: 1px solid #E67C00;
}

QPushButton[objectName="dangerButton"] {
    background-color: #D13438;
    color: white;
    border: 1px solid #B92B2F;
}

/* Lists and Tables */
QListWidget {
    border: 1px solid #E1E1E1;
    border-radius: 6px;
    background-color: white;
    alternate-background-color: #F8F8F8;
    selection-background-color: #E3F2FD;
}

QListWidget::item {
    padding: 6px 12px;
    border-bottom: 1px solid #F3F2F1;
}

QListWidget::item:selected {
    background-color: #E3F2FD;
    color: #0078D4;
}

QListWidget::item:hover {
    background-color: #F0F6FF;
}

/* Progress Bars */
QProgressBar {
    border: 1px solid #E1E1E1;
    border-radius: 4px;
    text-align: center;
    font-size: 11px;
    font-weight: 500;
    background-color: #F8F8F8;
}

QProgressBar::chunk {
    background-color: #0078D4;
    border-radius: 3px;
}

QProgressBar[objectName="successProgress"]::chunk {
    background-color: #107C10;
}

QProgressBar[objectName="warningProgress"]::chunk {
    background-color: #FF8C00;
}

/* Text Areas */
QTextEdit {
    border: 1px solid #E1E1E1;
    border-radius: 6px;
    background-color: white;
    font-family: 'Consolas', 'Courier New', monospace;
    font-size: 11px;
    line-height: 1.4;
}

QTextEdit[objectName="logArea"] {
    background-color: #1E1E1E;
    color: #D4D4D4;
    border: 1px solid #404040;
}

/* Scrollbars */
QScrollBar:vertical {
    background-color: #F8F8F8;
    width: 12px;
    border-radius: 6px;
}

QScrollBar::handle:vertical {
    background-color: #C8C6C4;
    border-radius: 6px;
    min-height: 20px;
}

QScrollBar::handle:vertical:hover {
    background-color: #A19F9D;
}

/* Splitters */
QSplitter::handle {
    background-color: #E1E1E1;
    width: 4px;
    height: 4px;
}

QSplitter::handle:hover {
    background-color: #C8C6C4;
}

/* Status and Notifications */
QFrame[objectName="statusPanel"] {
    background-color: #F3F2F1;
    border: 1px solid #E1DFDD;
    border-radius: 6px;
    padding: 8px;
}

QFrame[objectName="successPanel"] {
    background-color: #F3FDF3;
    border: 1px solid #C4E7C4;
    color: #107C10;
}

QFrame[objectName="warningPanel"] {
    background-color: #FFF8F0;
    border: 1px solid #FFCC99;
    color: #8A6914;
}

QFrame[objectName="errorPanel"] {
    background-color: #FDF3F4;
    border: 1px solid #F1AEB5;
    color: #A4262C;
}

/* Global Status Bar */
QFrame[objectName="statusContainer"] {
    background-color: #F8F8F8;
    border-top: 1px solid #E1E1E1;
    border-radius: 0px;
}

QLabel[objectName="globalStatusLabel"] {
    font-size: 11px;
    color: #605E5C;
    font-weight: 500;
    padding: 2px 0px;
}

/* Progress Bars - Slim and elegant */
QProgressBar {
    background-color: #F3F2F1;
    border: 1px solid #E1DFDD;
    border-radius: 4px;
    text-align: center;
    font-size: 10px;
    font-weight: 500;
    color: #605E5C;
}

QProgressBar::chunk {
    background-color: #0078D4;
    border-radius: 3px;
    margin: 1px;
}

QProgressBar[value="0"] {
    color: transparent; /* Hide text when empty */
}
"""