"""
Icon system for Data Processing Tool

Provides consistent icons across the application using:
1. Icon files (.ico) from gui/Icon folder
2. Unicode symbols as fallback
3. Font Awesome style icons
4. Material Design icons

For production, uses actual icon files (.ico) for better quality.
"""

import os
from typing import Optional
from PyQt6.QtGui import QIcon, QPixmap, QPainter, QColor, QFont
from PyQt6.QtCore import Qt

# Unicode icon mappings
UNICODE_ICONS = {
    # Tab icons
    'clean': '🧹',  # Broom for cleaning
    'merge': '🔗',  # Link for merging
    'feedback': '💬',  # Speech bubble for feedback
    'columns': '📊',  # Chart for columns
    'help': '📖',  # Book for help/documentation
    
    # Action icons
    'file': '📄',
    'folder': '📁',
    'open': '📂',
    'save': '💾',
    'export': '📤',
    'import': '📥',
    'settings': '⚙️',
    'search': '🔍',
    'filter': '🔎',
    'refresh': '🔄',
    'delete': '🗑️',
    'edit': '✏️',
    'add': '➕',
    'remove': '➖',
    'check': '✅',
    'cross': '❌',
    'warning': '⚠️',
    'info': 'ℹ️',
    'question': '❓',
    
    # Status icons
    'success': '✓',
    'error': '✗',
    'loading': '⟳',
    'pending': '○',
    
    # Navigation
    'back': '◀',
    'forward': '▶',
    'up': '▲',
    'down': '▼',
    'left': '◄',
    'right': '►',
}

# Alternative ASCII-based icons (for better compatibility)
ASCII_ICONS = {
    'clean': '[CLN]',
    'merge': '[MRG]',
    'feedback': '[FBK]',
    'columns': '[COL]',
    'file': '[FILE]',
    'folder': '[FLD]',
    'settings': '[SET]',
    'success': '[OK]',
    'error': '[ERR]',
    'loading': '[...]',
}

# Material Design style icon glyphs (if using Material Icons font)
MATERIAL_ICONS = {
    'clean': '\ue8b8',  # cleaning_services
    'merge': '\ue8d4',  # merge
    'feedback': '\ue87f',  # feedback
    'columns': '\ue24b',  # view_column
    'file': '\ue24d',  # description
    'folder': '\ue2c7',  # folder
    'settings': '\ue8b8',  # settings
}


def get_icon_text(icon_name: str, style: str = 'unicode') -> str:
    """
    Get icon text for a given icon name
    
    Args:
        icon_name: Name of the icon
        style: Icon style ('unicode', 'ascii', 'material')
    
    Returns:
        Icon text string
    """
    if style == 'unicode':
        return UNICODE_ICONS.get(icon_name, '•')
    elif style == 'ascii':
        return ASCII_ICONS.get(icon_name, f'[{icon_name.upper()[:3]}]')
    elif style == 'material':
        return MATERIAL_ICONS.get(icon_name, '')
    return icon_name


def create_text_icon(text: str, 
                     size: int = 24,
                     bg_color: str = '#1976D2',
                     fg_color: str = '#FFFFFF') -> QIcon:
    """
    Create a simple icon from text
    
    Args:
        text: Text to display (emoji or character)
        size: Icon size in pixels
        bg_color: Background color
        fg_color: Foreground (text) color
    
    Returns:
        QIcon with rendered text
    """
    # Create a pixmap with transparent background
    pixmap = QPixmap(size, size)
    pixmap.fill(Qt.GlobalColor.transparent)
    
    # Create painter
    painter = QPainter(pixmap)
    painter.setRenderHint(QPainter.RenderHint.Antialiasing)
    
    # Draw rounded background
    painter.setBrush(QColor(bg_color))
    painter.setPen(Qt.PenStyle.NoPen)
    painter.drawRoundedRect(0, 0, size, size, 4, 4)
    
    # Draw text
    painter.setPen(QColor(fg_color))
    
    # Adjust font size based on text type
    if len(text) == 1 and ord(text) > 127:  # Emoji or unicode
        font_size = int(size * 0.6)
    else:
        font_size = int(size * 0.5)
    
    font = QFont('Segoe UI Emoji', font_size)
    painter.setFont(font)
    
    painter.drawText(0, 0, size, size, 
                    Qt.AlignmentFlag.AlignCenter, 
                    text)
    
    painter.end()
    
    return QIcon(pixmap)


def get_tab_icon(tab_name: str) -> QIcon:
    """
    Get icon for a specific tab
    
    Args:
        tab_name: Name of the tab
    
    Returns:
        QIcon for the tab
    """
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    icon_dir = os.path.join(script_dir, 'Icon')
    
    # Map tab names to icon files
    icon_file_map = {
        'LMD Cleaner': 'data cleaning.ico',
        'Lane Fix': 'Fix lane.ico',
        'Client Feedback': 'client feedback.ico',
        'Add Columns': 'add column.ico',
        'Polygon Selector': 'polygon.ico',
        'Polygon': 'polygon.ico',
        'Help': 'help.ico',
    }
    
    icon_filename = icon_file_map.get(tab_name)
    
    if icon_filename:
        icon_path = os.path.join(icon_dir, icon_filename)
        if os.path.exists(icon_path):
            return QIcon(icon_path)
    
    # Fallback to text-based icon if file not found
    tab_icon_map = {
        'LMD Cleaner': 'clean',
        'Lane Fix': 'merge',
        'Client Feedback': 'feedback',
        'Add Columns': 'columns',
        'Polygon Selector': 'file',
        'Polygon': 'file',
        'Help': 'help',
    }
    
    icon_name = tab_icon_map.get(tab_name, 'file')
    icon_text = get_icon_text(icon_name, style='unicode')
    
    # Use different colors for different tabs
    colors = {
        'LMD Cleaner': ('#1976D2', '#FFFFFF'),  # Blue
        'Lane Fix': ('#00897B', '#FFFFFF'),  # Teal
        'Client Feedback': ('#F57C00', '#FFFFFF'),  # Orange
        'Add Columns': ('#5E35B1', '#FFFFFF'),  # Purple
        'Polygon Selector': ('#00796B', '#FFFFFF'),  # Dark teal
        'Polygon': ('#00796B', '#FFFFFF'),
        'Help': ('#43A047', '#FFFFFF'),  # Green
    }
    
    bg_color, fg_color = colors.get(tab_name, ('#1976D2', '#FFFFFF'))
    
    return create_text_icon(icon_text, size=32, bg_color=bg_color, fg_color=fg_color)


# Convenience functions for common icons
def get_clean_icon() -> QIcon:
    """Get cleaning/LMD Cleaner icon"""
    return get_tab_icon('LMD Cleaner')


def get_merge_icon() -> QIcon:
    """Get merge/Lane Fix icon"""
    return get_tab_icon('Lane Fix')


def get_feedback_icon() -> QIcon:
    """Get feedback icon"""
    return get_tab_icon('Client Feedback')


def get_columns_icon() -> QIcon:
    """Get columns icon"""
    return get_tab_icon('Add Columns')


def get_file_icon() -> QIcon:
    """Get generic file icon"""
    icon_text = get_icon_text('file', style='unicode')
    return create_text_icon(icon_text, size=24)


def get_folder_icon() -> QIcon:
    """Get folder icon"""
    icon_text = get_icon_text('folder', style='unicode')
    return create_text_icon(icon_text, size=24)


def get_settings_icon() -> QIcon:
    """Get settings icon"""
    icon_text = get_icon_text('settings', style='unicode')
    return create_text_icon(icon_text, size=24, bg_color='#757575')


def get_app_icon() -> QIcon:
    """Get application icon for window title"""
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    icon_dir = os.path.join(script_dir, 'Icon')
    icon_path = os.path.join(icon_dir, 'data processing.ico')
    
    # Try to load from file first
    if os.path.exists(icon_path):
        return QIcon(icon_path)
    
    # Fallback to creating text icon
    icon_text = '📊'  # Chart icon represents data processing
    return create_text_icon(icon_text, size=48, bg_color='#b76e79', fg_color='#ffffff')
