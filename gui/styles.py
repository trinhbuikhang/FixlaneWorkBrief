"""
Data Cleaner Application Styles

Modern and attractive stylesheet for the PyQt6 GUI components.
"""

# Main application stylesheet
MAIN_STYLESHEET = """
/* Main Window */
QWidget {
    font-family: 'Segoe UI', 'Helvetica Neue', Arial, sans-serif;
    font-size: 10pt;
    color: #333333;
    background-color: #f8f9fa;
}

/* Tab Widget */
QTabWidget {
    background-color: #ffffff;
    border: 1px solid #dee2e6;
    border-radius: 8px;
}

QTabWidget::pane {
    border: 1px solid #dee2e6;
    border-radius: 8px;
    background-color: #ffffff;
    top: -1px;
}

QTabBar::tab {
    background-color: #f8f9fa;
    border: 1px solid #dee2e6;
    border-bottom: none;
    border-radius: 8px 8px 0 0;
    padding: 12px 24px;
    margin-right: 4px;
    color: #6c757d;
    font-weight: 500;
    min-width: 120px;
}

QTabBar::tab:selected {
    background-color: #ffffff;
    color: #007bff;
    border-bottom: 2px solid #007bff;
    font-weight: 600;
}

QTabBar::tab:hover {
    background-color: #e9ecef;
    color: #495057;
}

/* Buttons */
QPushButton {
    background-color: #007bff;
    color: #ffffff;
    border: none;
    border-radius: 6px;
    padding: 10px 20px;
    font-size: 10pt;
    font-weight: 500;
    min-height: 16px;
}

QPushButton:hover {
    background-color: #0056b3;
}

QPushButton:pressed {
    background-color: #004085;
}

QPushButton:disabled {
    background-color: #6c757d;
    color: #ffffff;
    opacity: 0.6;
}

/* Special Process Button */
QPushButton#processButton {
    background-color: #28a745;
    font-size: 12pt;
    font-weight: 600;
    padding: 12px 24px;
    min-height: 20px;
}

QPushButton#processButton:hover {
    background-color: #1e7e34;
}

QPushButton#processButton:pressed {
    background-color: #155724;
}

/* Line Edits */
QLineEdit {
    border: 2px solid #ced4da;
    border-radius: 6px;
    padding: 8px 12px;
    background-color: #ffffff;
    font-size: 10pt;
    selection-background-color: #007bff;
}

QLineEdit:focus {
    border-color: #007bff;
    outline: none;
}

QLineEdit::placeholder {
    color: #6c757d;
}

/* Text Edit (Log Area) */
QTextEdit {
    border: 2px solid #ced4da;
    border-radius: 6px;
    background-color: #ffffff;
    font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
    font-size: 9pt;
    line-height: 1.4;
    padding: 8px;
}

QTextEdit:focus {
    border-color: #007bff;
}

/* Labels */
QLabel {
    color: #495057;
    font-size: 10pt;
    font-weight: 500;
}

QLabel#titleLabel {
    font-size: 14pt;
    font-weight: 600;
    color: #212529;
    margin-bottom: 8px;
}

QLabel#sectionLabel {
    font-size: 11pt;
    font-weight: 600;
    color: #495057;
    margin-top: 12px;
    margin-bottom: 6px;
}

QLabel#statusLabel {
    font-size: 11pt;
    font-weight: 600;
    color: #007bff;
    background-color: #e7f3ff;
    border: 1px solid #b3d9ff;
    border-radius: 4px;
    padding: 8px 12px;
    margin: 4px 0px;
}

/* Progress Bar */
QProgressBar {
    border: 2px solid #ced4da;
    border-radius: 6px;
    text-align: center;
    background-color: #ffffff;
    font-size: 9pt;
    font-weight: 500;
    color: #495057;
    min-height: 20px;
}

QProgressBar::chunk {
    background-color: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                                      stop:0 #28a745, stop:1 #20c997);
    border-radius: 4px;
}

/* Scroll Bars */
QScrollBar:vertical {
    background-color: #f8f9fa;
    width: 16px;
    border-radius: 8px;
}

QScrollBar::handle:vertical {
    background-color: #dee2e6;
    border-radius: 8px;
    min-height: 30px;
}

QScrollBar::handle:vertical:hover {
    background-color: #adb5bd;
}

QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
    border: none;
    background: none;
}

/* File Dialog */
QFileDialog {
    background-color: #ffffff;
}

QFileDialog QWidget {
    background-color: #ffffff;
}

/* Message Boxes */
QMessageBox {
    background-color: #ffffff;
}

QMessageBox QLabel {
    color: #495057;
    font-size: 10pt;
}

/* Tooltips */
QToolTip {
    background-color: #343a40;
    color: #ffffff;
    border: none;
    border-radius: 4px;
    padding: 6px 8px;
    font-size: 9pt;
}

/* Focus and Selection */
QWidget:focus {
    outline: none;
}

/* Animations - Note: Qt QSS doesn't support CSS transitions */

/* Dark mode support (can be toggled) */
QWidget[darkMode="true"] {
    background-color: #212529;
    color: #ffffff;
}

QWidget[darkMode="true"] QPushButton {
    background-color: #495057;
    color: #ffffff;
}

QWidget[darkMode="true"] QPushButton:hover {
    background-color: #6c757d;
}

QWidget[darkMode="true"] QLineEdit {
    background-color: #343a40;
    border-color: #495057;
    color: #ffffff;
}

QWidget[darkMode="true"] QTextEdit {
    background-color: #343a40;
    border-color: #495057;
    color: #ffffff;
}
"""

# Additional style utilities
def apply_stylesheet(widget, stylesheet=None):
    """Apply stylesheet to a widget"""
    if stylesheet is None:
        stylesheet = MAIN_STYLESHEET
    widget.setStyleSheet(stylesheet)

def set_dark_mode(widget, enabled=True):
    """Enable or disable dark mode"""
    if enabled:
        widget.setProperty("darkMode", "true")
    else:
        widget.setProperty("darkMode", "false")
    widget.style().unpolish(widget)
    widget.style().polish(widget)
    widget.update()

def create_gradient_button_style(primary_color="#007bff", hover_color="#0056b3"):
    """Create a gradient button style"""
    return f"""
    QPushButton {{
        background-color: {primary_color};
        color: #ffffff;
        border: none;
        border-radius: 6px;
        padding: 10px 20px;
        font-size: 10pt;
        font-weight: 500;
    }}
    QPushButton:hover {{
        background-color: {hover_color};
    }}
    """

# Color schemes
COLOR_SCHEMES = {
    "blue": {
        "primary": "#007bff",
        "secondary": "#6c757d",
        "success": "#28a745",
        "danger": "#dc3545",
        "warning": "#ffc107",
        "info": "#17a2b8"
    },
    "green": {
        "primary": "#28a745",
        "secondary": "#6c757d",
        "success": "#20c997",
        "danger": "#dc3545",
        "warning": "#ffc107",
        "info": "#17a2b8"
    },
    "purple": {
        "primary": "#6f42c1",
        "secondary": "#6c757d",
        "success": "#28a745",
        "danger": "#dc3545",
        "warning": "#ffc107",
        "info": "#17a2b8"
    }
}

def get_color_scheme(scheme_name="blue"):
    """Get a color scheme dictionary"""
    return COLOR_SCHEMES.get(scheme_name, COLOR_SCHEMES["blue"])