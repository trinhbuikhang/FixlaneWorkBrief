"""
UI Design Standards for Data Processing Tool
===========================================

## Layout Structure (Consistent for all tabs):

1. **Header Section:**
   - Title Label (objectName="titleLabel") 
   - Description Label (objectName="descriptionLabel")

2. **Content Section:**
   - File Selection Area
   - Main Processing Area (split or single column based on needs)

3. **Footer Section:**
   - Action Buttons
   - Status Label (objectName="statusLabel") 
   - Progress Bar
   - Log Area (if needed)

## Standard Dimensions:

- **Container Margins:** 20px all sides
- **Layout Spacing:** 15px between sections
- **Label Fixed Width:** 140px (for file selection labels)
- **Button Fixed Width:** 100px (for Browse buttons)
- **Input Min Width:** 300px

## Standard Components:

### File Selection Row:
```python
file_layout = QHBoxLayout()
file_layout.setSpacing(10)

file_label = QLabel("File Type:")
file_label.setFixedWidth(140)
file_layout.addWidget(file_label)

file_edit = QLineEdit()
file_edit.setPlaceholderText("Select file...")
file_edit.setMinimumWidth(300)
file_edit.setReadOnly(True)
file_layout.addWidget(file_edit, 1)

browse_btn = QPushButton("Browse...")
browse_btn.setFixedWidth(100)
file_layout.addWidget(browse_btn)
```

### Action Buttons:
```python
button_layout = QHBoxLayout()
process_btn = QPushButton("Process Data")
process_btn.setObjectName("processButton")
cancel_btn = QPushButton("Cancel")
cancel_btn.setFixedWidth(100)
cancel_btn.setEnabled(False)

button_layout.addWidget(process_btn)
button_layout.addWidget(cancel_btn)
button_layout.addStretch()
```

### Status Section:
```python
status_label = QLabel("Ready")
status_label.setObjectName("statusLabel")

progress_bar = QProgressBar()
progress_bar.setVisible(False)
```

## Tab-Specific Layouts:

### Simple Processing Tabs (LMD Cleaner, etc.):
- Vertical layout
- File selection at top
- Single log area at bottom

### Complex Processing Tabs (Add Columns, Client Feedback):
- Split layout (50/50 horizontal)
- Left: Configuration/Selection
- Right: Log/Preview

## Consistent Naming:

- **process_btn:** Main action button
- **cancel_btn:** Cancel button  
- **status_label:** Status display
- **progress_bar:** Progress indicator
- **log_text:** Log output area
"""