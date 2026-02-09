"""
Demo script to show tooltip functionality
"""
from PyQt6.QtWidgets import QApplication, QTabWidget, QWidget, QVBoxLayout, QLabel
from PyQt6.QtCore import Qt
import sys

if __name__ == "__main__":
    app = QApplication(sys.argv)
    
    # Create simple tab widget
    tabs = QTabWidget()
    tabs.setWindowTitle("Tooltip Demo - Hover over tabs!")
    tabs.resize(800, 600)
    
    # Add tabs with tooltips
    tab1 = QWidget()
    layout1 = QVBoxLayout(tab1)
    label1 = QLabel("Tab 1 Content\n\nHover over the tab names above to see descriptions!")
    label1.setAlignment(Qt.AlignmentFlag.AlignCenter)
    label1.setStyleSheet("font-size: 14px; padding: 40px;")
    layout1.addWidget(label1)
    
    tabs.addTab(tab1, "LMD Cleaner")
    tabs.setTabToolTip(0, "Clean and process LMD survey data - filters invalid records and adds calculated fields")
    
    tab2 = QWidget()
    layout2 = QVBoxLayout(tab2)
    label2 = QLabel("Tab 2 Content")
    label2.setAlignment(Qt.AlignmentFlag.AlignCenter)
    layout2.addWidget(label2)
    
    tabs.addTab(tab2, "Lane Fix")
    tabs.setTabToolTip(1, "Fix lane numbering and chainage issues in LMD data using reference coordinates")
    
    tab3 = QWidget()
    layout3 = QVBoxLayout(tab3)
    label3 = QLabel("Tab 3 Content")
    label3.setAlignment(Qt.AlignmentFlag.AlignCenter)
    layout3.addWidget(label3)
    
    tabs.addTab(tab3, "Client Feedback")
    tabs.setTabToolTip(2, "Match and merge client feedback data with LMD records based on region, road, and chainage")
    
    print("\n" + "="*70)
    print("TOOLTIP DEMO - Hover over tab names to see descriptions!")
    print("="*70)
    print("\n✅ Tab tooltips added:")
    print("  • LMD Cleaner: Shows cleaning functionality")
    print("  • Lane Fix: Shows lane fixing functionality")
    print("  • Client Feedback: Shows feedback matching functionality")
    print("\n💡 Note: Tooltips appear after hovering for ~1 second")
    print("="*70 + "\n")
    
    tabs.show()
    sys.exit(app.exec())
