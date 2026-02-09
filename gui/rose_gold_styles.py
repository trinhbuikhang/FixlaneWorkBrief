"""
Rose Gold Color Scheme for Data Cleaner Application

Elegant rose gold theme with pink/purple tones
"""

ROSE_GOLD_STYLESHEET = """
/* Main Window */
QWidget {
    font-family: 'Segoe UI', 'Helvetica Neue', Arial, sans-serif;
    font-size: 10pt;
    color: #4a2c2a;
    background-color: #fef5f1;
}

/* Tab Widget */
QTabWidget {
    background-color: #fff9f6;
    border: 1px solid #e8c4c1;
    border-radius: 8px;
}

QTabWidget::pane {
    border: 1px solid #e8c4c1;
    border-radius: 8px;
    background-color: #fff9f6;
    top: -1px;
}

QTabBar::tab {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                                stop:0 #f7e0dd, stop:1 #f0d0ce);
    border: 1px solid #e8c4c1;
    border-bottom: none;
    border-radius: 8px 8px 0 0;
    padding: 12px 24px;
    margin-right: 4px;
    color: #7d5e5c;
    font-weight: 500;
    min-width: 120px;
}

QTabBar::tab:selected {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                                stop:0 #fff9f6, stop:1 #fef5f1);
    color: #b76e79;
    border-bottom: 3px solid #c77d8f;
    font-weight: 600;
}

QTabBar::tab:hover {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                                stop:0 #fce8e5, stop:1 #f7dbd8);
    color: #8b5f5d;
}

/* Buttons */
QPushButton {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                                stop:0 #d4888f, stop:1 #b76e79);
    color: #ffffff;
    border: none;
    border-radius: 6px;
    padding: 10px 20px;
    font-size: 10pt;
    font-weight: 500;
    min-height: 16px;
}

QPushButton:hover {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                                stop:0 #c77d8f, stop:1 #a55e6d);
}

QPushButton:pressed {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                                stop:0 #b76e79, stop:1 #8b5160);
}

QPushButton:disabled {
    background-color: #c9afad;
    color: #ffffff;
    opacity: 0.6;
}

/* Special Process Button */
QPushButton#processButton {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                                stop:0 #a8779d, stop:1 #8b5e87);
    font-size: 12pt;
    font-weight: 600;
    padding: 12px 24px;
    min-height: 20px;
}

QPushButton#processButton:hover {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                                stop:0 #96678b, stop:1 #754f73);
}

QPushButton#processButton:pressed {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                                stop:0 #8b5e87, stop:1 #63425e);
}

/* Line Edits */
QLineEdit {
    border: 1px solid #e8c4c1;
    border-radius: 6px;
    padding: 8px 12px;
    background-color: #ffffff;
    font-size: 10pt;
    selection-background-color: #f0b8be;
    color: #4a2c2a;
}

QLineEdit:focus {
    border-color: #c77d8f;
    outline: none;
}

QLineEdit::placeholder {
    color: #b39593;
}

/* Text Edit (Log Area) */
QTextEdit {
    border: 1px solid #e8c4c1;
    border-radius: 6px;
    background-color: #ffffff;
    font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
    font-size: 9pt;
    line-height: 1.4;
    padding: 8px;
    color: #4a2c2a;
}

QTextEdit:focus {
    border-color: #c77d8f;
}

/* Text Browser (Help/Documentation) */
QTextBrowser {
    border: 1px solid #e8c4c1;
    border-radius: 6px;
    background-color: #ffffff;
    font-family: 'Segoe UI', 'Helvetica Neue', Arial, sans-serif;
    font-size: 10pt;
    line-height: 1.6;
    padding: 12px;
    color: #4a2c2a;
}

QTextBrowser:focus {
    border-color: #c77d8f;
}

/* List Widget */
QListWidget {
    border: 1px solid #e8c4c1;
    border-radius: 6px;
    background-color: #ffffff;
    font-size: 10pt;
    selection-background-color: #f0b8be;
    selection-color: #4a2c2a;
    color: #4a2c2a;
    padding: 4px;
}

QListWidget:focus {
    border-color: #c77d8f;
}

QListWidget::item {
    padding: 6px 8px;
    border-radius: 4px;
}

QListWidget::item:selected {
    background-color: #f0b8be;
    color: #4a2c2a;
}

QListWidget::item:hover {
    background-color: #f7e0dd;
}

/* Labels */
QLabel {
    color: #6d4847;
    font-size: 10pt;
    font-weight: 500;
}

QLabel#titleLabel {
    font-size: 14pt;
    font-weight: 600;
    color: #4a2c2a;
    margin-bottom: 8px;
}

QLabel#sectionLabel {
    font-size: 11pt;
    font-weight: 600;
    color: #6d4847;
    margin-top: 12px;
    margin-bottom: 6px;
}

/* Group Boxes */
QGroupBox {
    border: 1px solid #e8c4c1;
    border-radius: 8px;
    margin-top: 12px;
    padding-top: 12px;
    font-weight: 600;
    color: #6d4847;
    background-color: #fff9f6;
}

QGroupBox::title {
    subcontrol-origin: margin;
    subcontrol-position: top left;
    padding: 4px 12px;
    background-color: #fef5f1;
    border: 1px solid #e8c4c1;
    border-radius: 4px;
    color: #8b5e5d;
}

/* Progress Bar */
QProgressBar {
    border: 1px solid #e8c4c1;
    border-radius: 6px;
    text-align: center;
    background-color: #fff9f6;
    color: #6d4847;
    font-weight: 600;
    min-height: 28px;
    padding: 2px;
}

QProgressBar::chunk {
    background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                                stop:0 #d4888f, stop:0.5 #c77d8f, stop:1 #b76e79);
    border-radius: 4px;
}

/* Status Bar Container */
QFrame#statusContainer {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                                stop:0 #f7e0dd, stop:1 #fef5f1);
    border: 1px solid #e8c4c1;
    border-radius: 6px;
    min-height: 32px;
    padding: 4px 8px;
}

QLabel#globalStatusLabel {
    color: #6d4847;
    font-size: 9pt;
    font-weight: 500;
    padding: 2px 8px;
}

/* Scrollbars */
QScrollBar:vertical {
    border: none;
    background-color: #fef5f1;
    width: 12px;
    margin: 0px;
}

QScrollBar::handle:vertical {
    background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                                stop:0 #e8c4c1, stop:1 #d4b1ae);
    border-radius: 6px;
    min-height: 30px;
}

QScrollBar::handle:vertical:hover {
    background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                                stop:0 #d4b1ae, stop:1 #c09f9c);
}

QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
    height: 0px;
}

QScrollBar:horizontal {
    border: none;
    background-color: #fef5f1;
    height: 12px;
    margin: 0px;
}

QScrollBar::handle:horizontal {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                                stop:0 #e8c4c1, stop:1 #d4b1ae);
    border-radius: 6px;
    min-width: 30px;
}

QScrollBar::handle:horizontal:hover {
    background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                                stop:0 #d4b1ae, stop:1 #c09f9c);
}

QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {
    width: 0px;
}

/* Tooltips */
QToolTip {
    background-color: #fff9f6;
    color: #4a2c2a;
    border: 1px solid #c77d8f;
    border-radius: 6px;
    padding: 8px 12px;
    font-size: 9pt;
    opacity: 240;
}

/* Message Boxes */
QMessageBox {
    background-color: #fef5f1;
}

QMessageBox QLabel {
    color: #4a2c2a;
}

QMessageBox QPushButton {
    min-width: 80px;
    padding: 8px 16px;
}

/* Selection Colors */
*::selection {
    background-color: #f0b8be;
    color: #4a2c2a;
}
"""
