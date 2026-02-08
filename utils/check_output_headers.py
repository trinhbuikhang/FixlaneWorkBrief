#!/usr/bin/env python3
"""
Script to check the headers of the chunked processing output CSV file.
This verifies that the output contains the correct columns: Ignore and InBrief.
"""

import os

import pandas as pd


def check_output_headers():
    """Check the headers of the chunked processing output file."""

    # Path to the output file
    output_file = r"C:\Users\du\Desktop\PyDeveloper\FixlaneWorkBrief\test data\PAS\V201_NORTHLAND_0.2m-Reduced_test_complete_chunked_20251010.csv"

    # Check if file exists
    if not os.path.exists(output_file):
        print(f"❌ Output file not found: {output_file}")
        return

    try:
        # Read just the header (first row) to check columns
        df_header = pd.read_csv(output_file, nrows=0)
        columns = df_header.columns.tolist()

        print("📄 Output File Header Check")
        print("=" * 50)
        print(f"File: {output_file}")
        print(f"Total columns: {len(columns)}")
        print("\nColumn headers:")
        for i, col in enumerate(columns, 1):
            print(f"  {i:2d}. {col}")

        print("\n🔍 Checking for required columns:")

        # Check for expected columns
        has_ignore = 'Ignore' in columns
        has_inbrief = 'InBrief' in columns
        has_testdate_ts = 'TestDateUTC_ts' in columns

        print(f"  ✅ Ignore column: {'Found' if has_ignore else 'NOT FOUND'}")
        print(f"  ✅ InBrief column: {'Found' if has_inbrief else 'NOT FOUND'}")
        print(f"  ❌ TestDateUTC_ts column: {'Found (PROBLEM!)' if has_testdate_ts else 'Not found (Good)'}")

        if has_ignore and has_inbrief and not has_testdate_ts:
            print("\n🎉 SUCCESS: Output has correct columns (Ignore and InBrief) without temporary columns!")
        else:
            print("\n⚠️  WARNING: Output columns are not as expected!")

        # Show first few rows to verify data
        print("\n📊 Sample data (first 3 rows):")
        df_sample = pd.read_csv(output_file, nrows=3)
        print(df_sample.to_string(index=False))

    except Exception as e:
        print(f"❌ Error reading file: {e}")

if __name__ == "__main__":
    check_output_headers()