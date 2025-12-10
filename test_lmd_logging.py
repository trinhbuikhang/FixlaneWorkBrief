#!/usr/bin/env python3
"""
Test script to verify LMD Cleaner logging functionality
"""

import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_lmd_cleaner_logging():
    """Test the LMD cleaner with logging callback"""
    print("Testing LMD Cleaner logging functionality...")

    try:
        from utils.data_processor import process_data

        # Create a simple logging callback
        def log_callback(message):
            print(f"[LOG] {message}")

        # Test with a small sample file if available
        test_input = "test_input.csv"
        test_output = "test_output.csv"

        # Create a minimal test CSV
        if not os.path.exists(test_input):
            with open(test_input, 'w') as f:
                f.write("col1,col2,RawSlope170,RawSlope270,TrailingFactor,Lane,Ignore\n")
                f.write("data1,data2,1.0,2.0,0.2,L01,false\n")
                f.write("data3,data4,,0.5,L02,true\n")
                f.write("data5,data6,3.0,,0.1,SK01,false\n")
            print(f"Created test input file: {test_input}")

        print("Testing process_data with logging callback...")
        process_data(test_input, test_output, log_callback)

        print("✓ Test completed successfully!")
        print(f"Check output file: {test_output}")

        # Cleanup
        if os.path.exists(test_input):
            os.remove(test_input)
        if os.path.exists(test_output):
            os.remove(test_output)

        return True

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_lmd_cleaner_logging()
    if success:
        print("\n🎉 LMD Cleaner logging is working correctly!")
    else:
        print("\n❌ LMD Cleaner logging has issues.")