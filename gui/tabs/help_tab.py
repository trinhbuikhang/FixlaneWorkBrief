"""
Help Tab - User Guide and Documentation

Provides comprehensive instructions and scenarios for all data processing tools.
"""

import logging
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QTextBrowser, QLabel
)
from PyQt6.QtCore import Qt

logger = logging.getLogger(__name__)


class HelpTab(QWidget):
    """Help and documentation tab with usage guides and scenarios."""
    
    def __init__(self):
        super().__init__()
        self.initUI()
        logger.info("Help tab initialized")
    
    def initUI(self):
        """Initialize the user interface."""
        layout = QVBoxLayout(self)
        from gui.ui_constants import LAYOUT_MARGINS, LAYOUT_SPACING
        layout.setContentsMargins(*LAYOUT_MARGINS)
        layout.setSpacing(LAYOUT_SPACING)
        
        # Title
        title_label = QLabel("User Guide & Documentation")
        title_label.setObjectName("titleLabel")
        layout.addWidget(title_label)
        
        # Help content area
        help_browser = QTextBrowser()
        help_browser.setOpenExternalLinks(False)
        help_browser.setHtml(self._get_help_content())
        layout.addWidget(help_browser)
    
    def _get_help_content(self) -> str:
        """Generate HTML help content."""
        return """
<!DOCTYPE html>
<html>
<head>
<style>
    body {
        font-family: 'Segoe UI', Arial, sans-serif;
        color: #4a2c2a;
        line-height: 1.6;
        padding: 10px;
    }
    h2 {
        color: #b76e79;
        border-bottom: 2px solid #e8c4c1;
        padding-bottom: 8px;
        margin-top: 24px;
        margin-bottom: 12px;
    }
    h3 {
        color: #8b5e5d;
        margin-top: 16px;
        margin-bottom: 8px;
    }
    .scenario {
        background-color: #fff9f6;
        border-left: 4px solid #d4888f;
        padding: 12px;
        margin: 12px 0;
        border-radius: 4px;
    }
    .filter-item {
        background-color: #f7e0dd;
        padding: 8px 12px;
        margin: 6px 0;
        border-radius: 4px;
        font-family: 'Consolas', monospace;
        font-size: 9pt;
    }
    .note {
        background-color: #fce8e5;
        border: 1px solid #e8c4c1;
        padding: 12px;
        margin: 12px 0;
        border-radius: 6px;
    }
    .warning {
        background-color: #fff3cd;
        border: 1px solid #ffc107;
        padding: 12px;
        margin: 12px 0;
        border-radius: 6px;
    }
    ul {
        margin: 8px 0;
        padding-left: 24px;
    }
    li {
        margin: 4px 0;
    }
    code {
        background-color: #f7e0dd;
        padding: 2px 6px;
        border-radius: 3px;
        font-family: 'Consolas', monospace;
        font-size: 9pt;
    }
</style>
</head>
<body>

<h2>📘 LMD Data Cleaner</h2>

<h3>Purpose</h3>
<p>Cleans LMD survey data by removing invalid or problematic records based on predefined quality filters. Supports <strong>single CSV file</strong> or <strong>folder of CSVs</strong> (fast merge then clean).</p>

<h3>Modes</h3>
<ul>
    <li><strong>Single CSV File:</strong> One input file → one cleaned output file.</li>
    <li><strong>Folder with CSV Files:</strong> All CSVs in the folder are merged (byte copy, no full parse) then cleaned in a single pass. Faster for many files. Before merging, the app checks that every file has the <strong>same header (schema)</strong> by reading only the first line of each file; if any file differs, processing stops with a clear error.</li>
</ul>

<h3>Data Cleaning Rules (Applied in order)</h3>

<div class="scenario">
    <strong>Scenario 1: Remove Empty Slope Data</strong>
    <div class="filter-item">Remove rows where both RawSlope170 AND RawSlope270 are empty</div>
    <p><strong>Why:</strong> Slope data is critical for road condition analysis. Records without any slope measurements are incomplete and unusable.</p>
</div>

<div class="scenario">
    <strong>Scenario 2: Remove Low Trailing Factor</strong>
    <div class="filter-item">Remove rows where TrailingFactor < 0.15</div>
    <p><strong>Why:</strong> TrailingFactor indicates measurement reliability. Values below 0.15 suggest unstable or unreliable readings.</p>
</div>

<div class="scenario">
    <strong>Scenario 3: Remove Low Slope Ratio</strong>
    <div class="filter-item">Remove rows where abs(tsdSlopeMinY) / tsdSlopeMaxY < 0.15</div>
    <p><strong>Why:</strong> This ratio indicates slope measurement balance. Low ratios suggest sensor imbalance or measurement errors.</p>
</div>

<div class="scenario">
    <strong>Scenario 4: Remove Shoulder Lane Data</strong>
    <div class="filter-item">Remove rows where Lane contains 'SK'</div>
    <p><strong>Why:</strong> 'SK' (Shoulder) lanes are not part of the main carriageway and are excluded from primary analysis.</p>
</div>

<div class="scenario">
    <strong>Scenario 5: Honor Ignore Flag</strong>
    <div class="filter-item">Remove rows where Ignore = True (or null)</div>
    <p><strong>Why:</strong> Records marked with Ignore=True have been flagged for exclusion during data collection or previous processing.</p>
</div>

<div class="scenario">
    <strong>Scenario 6: Deduplicate by Test Date</strong>
    <div class="filter-item">Keep first occurrence per TestDateUTC</div>
    <p><strong>Why:</strong> Ensures one record per test timestamp in the output.</p>
</div>

<div class="note">
    <strong>💡 Note:</strong> All filters are applied automatically. Output uses <strong>CRLF</strong> line endings for consistency. In folder mode, schema is checked (header-only read) before merge so all files must have the same columns.
</div>

<h2>🔷 Polygon (Point-in-Polygon)</h2>

<h3>Purpose</h3>
<p>Splits CSV data by polygon regions: each row is assigned to a polygon based on its longitude/latitude, and output is written as separate files per polygon (or merged per polygon in batch mode).</p>

<h3>Modes</h3>
<ul>
    <li><strong>Batch:</strong> Select a folder of CSV files. Each file is processed against the polygon set; results are saved under <code>batch_results</code> with per-polygon files and merged summaries.</li>
    <li><strong>Single file:</strong> One data CSV is split by polygons; results go to <code>single_file_results</code> in the same directory as the input file.</li>
</ul>

<h3>Input Requirements</h3>
<ul>
    <li><strong>Polygon CSV:</strong> Must contain a <code>WKT</code> column (Well-Known Text geometry). Optional columns: <code>id</code>, <code>CouncilName</code> (or similar) for naming output files. The default browse folder for this file is <code>J:\\- RPP Calibrations\\RPP Regions</code>.</li>
    <li><strong>Data CSV(s):</strong> Must contain <strong>longitude</strong> and <strong>latitude</strong> columns (or <code>lon</code> / <code>lat</code>). Rows are assigned to the polygon that contains the point.</li>
</ul>

<h3>Output</h3>
<p>Per-polygon CSV files (e.g. <code>filename_id1_RegionName.csv</code>). In batch mode, merged files per polygon are also produced. Progress bar shows progress through files (batch) or steps (single file). Output uses <strong>CRLF</strong> line endings.</p>

<div class="note">
    <strong>💡 Dependencies:</strong> Polygon tab uses <code>shapely</code> and <code>polars</code>. No web browser or PyQt6-WebEngine required.
</div>

<h2>🛣️ Lane Fix Processor</h2>

<h3>Purpose</h3>
<p>Matches and merges lane fix data with LMD survey data to incorporate correction information.</p>

<h3>Processing Modes</h3>
<ul>
    <li><strong>Lane Fixes Only:</strong> Combines LMD data with lane corrections</li>
    <li><strong>Workbrief Only:</strong> Combines LMD data with work brief information</li>
    <li><strong>Complete Processing:</strong> Combines all three files (LMD + Lane Fixes + Workbrief)</li>
</ul>

<div class="note">
    <strong>💡 Matching Strategy:</strong> Uses road ID, region ID, and chainage ranges to match records accurately.
</div>

<h2>📋 Client Feedback Processor</h2>

<h3>Purpose</h3>
<p>Enriches LMD data with client feedback information including treatment recommendations and site descriptions.</p>

<h3>Key Features</h3>
<ul>
    <li><strong>Flexible Column Selection:</strong> Choose which feedback columns to add to your LMD data</li>
    <li><strong>Intelligent Matching:</strong> Uses region ID, road ID, chainage ranges, and optional wheelpath/lanes filters</li>
    <li><strong>Composite Keys:</strong> Handles leading zeros and format variations in road/region IDs</li>
    <li><strong>Match Statistics:</strong> Provides detailed matching metrics in the processing log</li>
</ul>

<div class="scenario">
    <strong>Matching Criteria (Applied in order)</strong>
    <ol>
        <li><strong>Region ID + Road ID:</strong> Normalizes IDs (removes leading zeros) for matching</li>
        <li><strong>Chainage Range:</strong> LMD location must fall within [Start, End] range (auto unit conversion)</li>
        <li><strong>Lane (Optional):</strong> Exact match if 'Lane' column exists in both files</li>
        <li><strong>Wheelpath (Optional):</strong> If 'wheelpath' column exists:
            <ul>
                <li>LMD wheelpath values: <strong>L, R, LWP, RWP</strong> (auto-normalized: L→LWP, R→RWP)</li>
                <li>Feedback wheelpath values: <strong>Both, LWP, RWP</strong></li>
                <li>Feedback = 'Both' → matches any wheelpath (LWP/RWP)</li>
                <li>Feedback = 'LWP' or 'RWP' → exact match required</li>
            </ul>
        </li>
        <li><strong>Lanes (Optional):</strong> If 'Lanes' column exists in feedback file:
            <ul>
                <li>LMD uses <strong>'Lane'</strong> column (e.g., L1, L2, R1, R2)</li>
                <li>Feedback uses <strong>'Lanes'</strong> column (e.g., All, L1, L2, R1, R2)</li>
                <li>Feedback = 'All' → matches any lane value in LMD</li>
                <li>Feedback = specific lane → exact match required (e.g., Lanes='L1' only matches Lane='L1')</li>
            </ul>
        </li>
    </ol>
</div>

<div class="note">
    <strong>💡 Stricter Matching:</strong> The wheelpath and Lanes filters provide more precise control over which records receive feedback data. Only records that satisfy ALL criteria will be matched.
</div>

<div class="warning">
    <strong>⚠️ Important:</strong> If an LMD record matches multiple feedback ranges, only the FIRST match is used. Check your feedback data for overlapping ranges.
</div>

<h2>➕ Add Columns Processor</h2>

<h3>Purpose</h3>
<p>Adds selected columns from a source LMD file to a target file using intelligent timestamp matching.</p>

<h3>Use Cases</h3>
<ul>
    <li>Add missing columns from a complete dataset</li>
    <li>Merge data from different processing stages</li>
    <li>Synchronize related measurements by timestamp</li>
</ul>

<div class="note">
    <strong>💡 Matching Method:</strong> Uses TestDateUTC timestamp for precise record matching between files.
</div>

<h2>⚙️ General Usage Guidelines</h2>

<h3>Tab Order</h3>
<p>Tabs are: <strong>LMD Cleaner</strong>, <strong>Polygon</strong>, <strong>Client Feedback</strong>, <strong>Lane Fix</strong>, <strong>Add Columns</strong>, <strong>Help</strong>. Layout and control sizes are unified across tabs (labels, inputs, Browse buttons, log areas).</p>

<h3>File Format Requirements</h3>
<ul>
    <li><strong>Input Format:</strong> CSV files with UTF-8 encoding (with or without BOM)</li>
    <li><strong>Headers:</strong> First row must contain column names</li>
    <li><strong>Separators:</strong> Comma-separated values</li>
    <li><strong>Date Format:</strong> ISO 8601 for timestamps (YYYY-MM-DD HH:MM:SS)</li>
</ul>

<h3>Performance Tips</h3>
<ul>
    <li>Large files (>1 million rows) may take several minutes to process</li>
    <li>Progress bars show meaningful progress (e.g. LMD folder mode, Polygon batch/single)</li>
    <li>LMD cleaner uses at most half of CPU cores by default (configurable) to reduce risk of overload</li>
    <li>You can cancel long-running operations using the Cancel button</li>
    <li>Processing logs provide detailed information about each step</li>
</ul>

<h3>Output Files</h3>
<ul>
    <li>Output files use <strong>CRLF</strong> line endings consistently across all tools</li>
    <li>Output is saved as specified (same directory as input or chosen path)</li>
    <li>Original files are never modified</li>
    <li>Output filenames include descriptive suffixes (e.g., "_cleaned", "_with_feedback")</li>
    <li>Existing output files are automatically overwritten</li>
</ul>

<h3>Common Issues & Solutions</h3>

<div class="scenario">
    <strong>Issue: Low Match Rate</strong>
    <p><strong>Possible Causes:</strong></p>
    <ul>
        <li>Different road ID or region ID formats</li>
        <li>Chainage unit mismatch (meters vs kilometers)</li>
        <li>Non-overlapping chainage ranges</li>
    </ul>
    <p><strong>Solution:</strong> Check the processing log for detailed matching statistics and verify that your input files use consistent ID formats.</p>
</div>

<div class="scenario">
    <strong>Issue: Application Unresponsive</strong>
    <p><strong>Cause:</strong> Processing very large files</p>
    <p><strong>Solution:</strong> Wait for the progress bar to complete. The application is still working even if it appears frozen.</p>
</div>

<h3>Best Practices</h3>
<ul>
    <li>✅ Always review the processing log after each operation</li>
    <li>✅ Check match rates and statistics to verify data quality</li>
    <li>✅ Keep backup copies of original files</li>
    <li>✅ Use descriptive output filenames to track processing versions</li>
    <li>✅ Review sample output records to verify results</li>
</ul>

<div class="note">
    <strong>💡 Need Help?</strong> Check the processing logs for detailed error messages and statistics. Most issues can be diagnosed from the log output.
</div>

</body>
</html>
"""
