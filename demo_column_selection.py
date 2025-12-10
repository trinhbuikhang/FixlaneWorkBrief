#!/usr/bin/env python3
"""
Demo script to show the new column selection feature in Client Feedback tab
"""

import os
import polars as pl

def create_sample_feedback_file():
    """Create a sample feedback file with various columns to demo the selection feature"""
    
    # Sample data with various types of columns
    data = {
        'road_id': ['R001', 'R002', 'R003', 'R004', 'R005'],
        'region_id': [1, 2, 1, 3, 2],
        'project_id': ['P001', 'P002', 'P003', 'P004', 'P005'],
        'Road Name': ['Main Street', 'Highway 1', 'Oak Avenue', 'River Road', 'Hill Street'],
        'Start Chainage (km)': [0.0, 2.5, 5.0, 7.5, 10.0],
        'End Chainage (km)': [2.0, 4.5, 6.5, 9.0, 12.0],
        
        # Standard/default columns (will be pre-selected)
        'Site Description': ['Urban arterial', 'Rural highway', 'Residential street', 'Bridge approach', 'Hill section'],
        'Treatment 2024': ['Reseal', '', 'Overlay', '', 'Rehabilitation'],
        'Treatment 2025': ['', 'Overlay', '', 'Reseal', ''],
        'Treatment 2026': ['', '', 'Rehabilitation', '', 'Reseal'],
        'Terminal': ['Yes', 'No', 'No', 'Yes', 'No'],
        'Foamed Bitumen %': [2.5, 0.0, 3.0, 0.0, 2.8],
        'Cement %': [1.5, 0.0, 2.0, 0.0, 1.8],
        'Lime %': [0.5, 0.0, 0.8, 0.0, 0.6],
        
        # Additional columns (will be available for selection but not pre-selected)
        'Traffic Volume': [15000, 8000, 5000, 12000, 6000],
        'Pavement Age': [15, 8, 12, 20, 10],
        'Surface Condition': ['Good', 'Fair', 'Poor', 'Fair', 'Good'],
        'Priority': ['High', 'Medium', 'Low', 'High', 'Medium'],
        'Budget Estimate': [50000, 80000, 120000, 200000, 90000],
        'Contractor': ['ABC Ltd', 'XYZ Corp', 'ABC Ltd', 'DEF Inc', 'XYZ Corp']
    }
    
    # Create DataFrame
    df = pl.DataFrame(data)
    
    # Save to demo file
    demo_file = "demo_client_feedback.csv"
    df.write_csv(demo_file)
    
    print(f"✓ Created demo feedback file: {demo_file}")
    print(f"   Total columns: {len(df.columns)}")
    print(f"   System columns: road_id, region_id, project_id, Road Name, Start/End Chainage")
    print(f"   Default columns (pre-selected): Site Description, Treatment 2024-2026, Terminal, Foamed Bitumen %, Cement %, Lime %")
    print(f"   Additional columns (user can select): Traffic Volume, Pavement Age, Surface Condition, Priority, Budget Estimate, Contractor")
    print()
    print("How to use:")
    print("1. Start the application: python main.py")
    print("2. Go to 'Client Feedback' tab")
    print("3. Select an LMD file")
    print("4. Select this demo feedback file")
    print("5. Notice that default columns are highlighted in bold and pre-selected")
    print("6. You can select/deselect any additional columns as needed")
    print("7. Only selected columns will be added to the output")
    
    return demo_file

if __name__ == "__main__":
    print("Demo: New Column Selection Feature for Client Feedback")
    print("=" * 60)
    
    demo_file = create_sample_feedback_file()
    
    print("\n📋 File contents preview:")
    df = pl.read_csv(demo_file)
    print(df.head(3))
    
    print(f"\n🎯 Next steps:")
    print(f"1. Run: python main.py")
    print(f"2. In Client Feedback tab, select '{demo_file}' as feedback file")
    print(f"3. See the new column selection interface in action!")
    
    input("\nPress Enter to continue...")