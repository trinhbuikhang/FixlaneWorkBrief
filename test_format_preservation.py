"""
Test script to verify that data formatting is preserved during processing.
Tests:
- Leading zeros (0011 should remain 0011, not become 11)
- Boolean values (False should remain False, not become false)
- Other string values remain unchanged
"""
import os
import polars as pl
import tempfile

# Create test data with formatting that should be preserved
test_data = {
    "TestDateUTC": ["2024-01-01 10:00:00", "2024-01-02 11:00:00", "2024-01-03 12:00:00", "2024-01-04 13:00:00"],
    "RawSlope170": ["0011", "0022", "0033", ""],  # Leading zeros
    "RawSlope270": ["0044", "0055", "", "0077"],  # Leading zeros
    "TrailingFactor": ["0.20", "0.25", "0.10", "0.30"],  # Should filter row 3
    "tsdSlopeMinY": ["-0.50", "-0.60", "-0.10", "-0.40"],
    "tsdSlopeMaxY": ["1.00", "2.00", "3.00", "1.50"],
    "Lane": ["L1", "L2", "SK1", "L3"],  # Should filter row 3
    "Ignore": ["False", "False", "False", "True"],  # Should filter row 4 (case insensitive)
    "CustomColumn": ["00123", "00456", "00789", "00999"],  # More leading zeros
}

df = pl.DataFrame(test_data)

# Create temporary CSV file
with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as f:
    temp_input = f.name
    df.write_csv(f.name)

temp_output = temp_input.replace('.csv', '_output.csv')

print("=" * 60)
print("TESTING FORMAT PRESERVATION")
print("=" * 60)
print(f"\nInput file: {temp_input}")
print(f"Output file: {temp_output}")
print("\nOriginal data:")
print(df)

# Process using the actual processor
from utils.data_processor import process_data

print("\n" + "=" * 60)
print("PROCESSING...")
print("=" * 60)
process_data(temp_input, temp_output)

# Read output and verify
print("\n" + "=" * 60)
print("VERIFICATION")
print("=" * 60)

# Read output AS STRINGS to check formatting
output_df = pl.read_csv(temp_output, schema_overrides={col: pl.Utf8 for col in df.columns})
print("\nOutput data (all columns read as strings):")
print(output_df)

# Verify specific formatting
print("\n" + "=" * 60)
print("FORMAT CHECK")
print("=" * 60)

checks = [
    ("Leading zeros in RawSlope170", output_df["RawSlope170"][0] == "0011"),
    ("Leading zeros in RawSlope270", output_df["RawSlope270"][0] == "0044"),
    ("Leading zeros in CustomColumn", output_df["CustomColumn"][0] == "00123"),
    ("Boolean False preserved", output_df["Ignore"][0] == "False"),
]

all_passed = True
for check_name, result in checks:
    status = "✓ PASS" if result else "✗ FAIL"
    print(f"{status}: {check_name}")
    if not result:
        all_passed = False

print("\n" + "=" * 60)
if all_passed:
    print("✓ ALL FORMATTING TESTS PASSED!")
else:
    print("✗ SOME TESTS FAILED - Check the output above")
print("=" * 60)

# Expected rows after filtering:
# Row 1: Keep (all criteria pass)
# Row 2: Keep (all criteria pass)
# Row 3: Filter (TrailingFactor < 0.15 AND Lane contains 'SK')
# Row 4: Filter (Ignore is True)
expected_rows = 2
actual_rows = len(output_df)
print(f"\nRow count check: Expected {expected_rows}, Got {actual_rows} - {'✓ PASS' if expected_rows == actual_rows else '✗ FAIL'}")

# Cleanup
os.remove(temp_input)
os.remove(temp_output)
print(f"\nTemporary files cleaned up.")
