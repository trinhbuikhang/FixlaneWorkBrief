# Data Cleaner

A PyQt6 application for cleaning binviewer combined_lmd CSV data by removing rows based on specific criteria.

## Project Structure

```
DataCleaner/
├── main.py                 # Application entry point
├── gui/
│   ├── __init__.py
│   ├── main_window.py      # Main application window with tabs
│   └── tabs/
│       ├── __init__.py
│       └── lmd_cleaner_tab.py  # LMD Cleaner tab implementation
├── utils/
│   ├── __init__.py
│   └── data_processor.py   # Data processing logic
├── requirements.txt        # Python dependencies
├── README.md              # This file
└── testdata/              # Test data files
```

## Features

- **Modern Tabbed UI**: Clean PyQt6 interface organized in tabs for easy expansion
- **Multiple Processing Tools**:
  - **LMD Cleaner**: Clean binviewer combined_lmd CSV data with advanced filtering
  - **Lane Fix**: Process WorkBrief CSV files with lane fixes and duplicate removal
- **Beautiful Styling**: Custom stylesheet with modern design, proper spacing, and attractive colors
- **Perfect Alignment**: Consistent margins, fixed-width labels, and uniform component sizing
- **Fast Processing**: Uses Polars for efficient data handling
- **Comprehensive Filtering**: Removes rows based on all your specified criteria:
  - Both slope columns empty/NaN
  - Trailing factor < 0.15
  - Slope ratio < 0.15
  - Lane contains "SK"
  - Ignore flag is true
- **Detailed Logging**: Shows exactly how many rows removed for each criterion
- **Format Preservation**: Maintains original CSV structure
- **Robust Error Handling**: Gracefully handles missing columns and malformed data

## Installation

1. Ensure Python 3.8+ is installed
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

1. Run the application:
   ```bash
   python main.py
   ```

2. Select the "LMD Cleaner" tab
3. Select the input CSV file (binviewer combined_lmd output)
4. Choose the output location for the cleaned CSV
5. Click "Process Data" to filter the data

## Filtering Criteria

The application removes rows that match any of the following conditions:

1. **Empty Slopes**: Both 'rawSlope170' and 'rawSlope270' are empty or NaN
2. **Low Trailing Factor**: 'trailingFactor' < 0.15
3. **Low Slope Ratio**: Abs('tsdSlopeMinY') / 'tsdSlopeMaxY' < 0.15
4. **SK Lanes**: 'Lane' contains "SK" (e.g., LSK1, RSK1)
5. **Ignore Flag**: 'Ignore' is true or "true"

## Output

- Cleaned CSV file with filtered data
- Detailed processing log showing how many rows were removed for each criterion
- Original data format preserved

## Development

The application is organized into modules for easy maintenance:

- **gui/**: User interface components
  - **styles.py**: Modern stylesheet definitions for beautiful UI
  - **tabs/**: Individual tab implementations
    - **lmd_cleaner_tab.py**: LMD data cleaning functionality
    - **laneFix_tab.py**: Lane fixing and WorkBrief processing
- **utils/**: Utility functions and data processing
- **main.py**: Application bootstrap

To add new functionality, create new tabs in `gui/tabs/` or add utilities in `utils/`.
To customize the appearance, modify `gui/styles.py` with Qt Style Sheet (QSS) rules.

## Requirements

- PyQt6
- Polars

## License

This project is open source. Feel free to use and modify as needed.

## License

This project is open source. Feel free to use and modify as needed.