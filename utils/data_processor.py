import polars as pl
import logging

def process_data(input_file: str, output_file: str, progress_callback=None) -> None:
    """
    Process the CSV file by filtering out rows based on specific criteria.

    Args:
        input_file: Path to the input CSV file
        output_file: Path to save the cleaned CSV file
        progress_callback: Optional callback function for progress updates
    """
    # Use provided progress callback or default logging
    if progress_callback:
        log_func = progress_callback
    else:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        log_func = logging.info

    try:
        # Read the CSV file
        df = pl.read_csv(input_file, truncate_ragged_lines=True, infer_schema=False)
        
        # Remove empty rows (rows where first column is null or empty)
        if len(df) > 0 and len(df.columns) > 0:
            first_col = df.columns[0]
            df = df.filter(df[first_col].is_not_null() & (df[first_col] != ""))
            log_func(f"Removed empty rows, remaining {len(df)} rows")
        original_count = len(df)
        log_func(f"Loaded {original_count} rows from {input_file}")

        # Criterion 0: Remove duplicate TestDateUTC rows (keep first occurrence)
        removed_duplicates = 0
        if 'TestDateUTC' in df.columns:
            before_dedup = len(df)
            # Keep first occurrence of each unique TestDateUTC value
            df = df.unique(subset=['TestDateUTC'], keep='first', maintain_order=True)
            removed_duplicates = before_dedup - len(df)
            if removed_duplicates > 0:
                log_func(f"Removed {removed_duplicates} duplicate TestDateUTC rows")
            else:
                log_func(f"No duplicate TestDateUTC rows found")
        else:
            log_func("Column 'TestDateUTC' not found, skipping duplicate removal")

        # Criterion 1: Remove rows where both 'RawSlope170' and 'rawSlope270' are empty or NaN
        removed1 = 0
        if 'RawSlope170' in df.columns and 'RawSlope270' in df.columns:
            condition1 = (
                (df['RawSlope170'].is_null() | (df['RawSlope170'] == "")) &
                (df['RawSlope270'].is_null() | (df['RawSlope270'] == ""))
            )
            removed1 = df.filter(condition1).height
            df = df.filter(~condition1)
            log_func(f"Removed {removed1} rows where both RawSlope170 and RawSlope270 are empty or NaN")
        else:
            log_func("Columns 'RawSlope170' and/or 'RawSlope270' not found, skipping slope filtering")

        # Criterion 2: Remove rows where 'trailingFactor' < 0.15
        removed2 = 0
        if 'TrailingFactor' in df.columns:
            try:
                condition2 = df['TrailingFactor'].cast(pl.Float64, strict=False) < 0.15
                removed2 = df.filter(condition2).height
                df = df.filter(~condition2)
                log_func(f"Removed {removed2} rows where TrailingFactor < 0.15")
            except Exception as e:
                log_func(f"Could not process TrailingFactor as number: {e}, skipping")
        else:
            log_func("Column 'TrailingFactor' not found, skipping trailing factor filtering")

        # Criterion 3: Remove rows where Abs('tsdSlopeMinY') / 'tsdSlopeMaxY' < 0.15
        removed3 = 0
        if 'tsdSlopeMinY' in df.columns and 'tsdSlopeMaxY' in df.columns:
            try:
                min_y = df['tsdSlopeMinY'].cast(pl.Float64, strict=False)
                max_y = df['tsdSlopeMaxY'].cast(pl.Float64, strict=False)
                ratio_expr = (min_y.abs() / max_y).alias('ratio')
                df_with_ratio = df.with_columns(ratio_expr)
                condition3 = df_with_ratio['ratio'] < 0.15
                removed3 = df_with_ratio.filter(condition3).height
                df = df_with_ratio.filter(~condition3).drop('ratio')
                log_func(f"Removed {removed3} rows where abs(tsdSlopeMinY)/tsdSlopeMaxY < 0.15")
            except Exception as e:
                log_func(f"Could not process slope ratio as numbers: {e}, skipping")
        else:
            log_func("Columns 'tsdSlopeMinY' and/or 'tsdSlopeMaxY' not found, skipping slope ratio filtering")

        # Criterion 4: Remove rows where 'Lane' contains "SK"
        removed4 = 0
        if 'Lane' in df.columns:
            condition4 = df['Lane'].str.contains("SK")
            removed4 = df.filter(condition4).height
            df = df.filter(~condition4)
            log_func(f"Removed {removed4} rows where Lane contains 'SK'")
        else:
            log_func("Column 'Lane' not found, skipping lane filtering")

        # Criterion 5: Remove rows where 'Ignore' is true or True
        removed5 = 0
        if 'Ignore' in df.columns:
            condition5 = df['Ignore'].cast(pl.Utf8).str.to_lowercase() == "true"
            removed5 = df.filter(condition5).height
            df = df.filter(~condition5)
            log_func(f"Removed {removed5} rows where Ignore is true")
        else:
            log_func("Column 'Ignore' not found, skipping ignore filtering")

        final_count = len(df)
        log_func(f"Final dataset has {final_count} rows (removed {original_count - final_count} total)")

        # Preserve boolean formatting for Ignore column
        if 'Ignore' in df.columns and df['Ignore'].dtype == pl.Boolean:
            df = df.with_columns(
                pl.when(df['Ignore'])
                .then(pl.lit("True"))
                .otherwise(pl.lit("False"))
                .alias('Ignore')
            )

        # Save the cleaned data
        # Convert all columns to string to preserve formatting
        df = df.with_columns([
            pl.col(col).cast(pl.Utf8).alias(col) for col in df.columns
        ])
        
        # Write CSV without trailing newline
        df.write_csv(output_file, include_header=True)
        
        # Remove trailing empty lines by reading and rewriting
        with open(output_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Remove trailing newlines and empty lines
        lines = content.rstrip('\n\r').split('\n')
        # Remove empty lines from the end
        while lines and lines[-1].strip() == '':
            lines.pop()
        
        # Only rewrite if there were trailing empty lines
        if len(lines) < len(content.split('\n')):
            cleaned_content = '\n'.join(lines)  # No trailing newline
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(cleaned_content)
        
        log_func(f"Saved cleaned data to {output_file}")

    except Exception as e:
        if progress_callback:
            progress_callback(f"Error processing data: {str(e)}")
        else:
            logging.error(f"Error processing data: {str(e)}")
        raise