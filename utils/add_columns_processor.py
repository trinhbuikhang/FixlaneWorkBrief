"""
Add Columns Processor for Data Processing Tool
Handles adding columns from LMD data to Details data using Polars.
"""

import polars as pl
import logging
from typing import Optional, Callable, List
from pathlib import Path
import os
from datetime import datetime

logger = logging.getLogger(__name__)


class AddColumnsProcessor:
    """Handles adding columns from LMD data to Details data using Polars."""

    def __init__(self, progress_callback: Optional[Callable] = None):
        """
        Initialize add columns processor.

        Args:
            progress_callback: Optional callback function for progress updates
        """
        self.progress_callback = progress_callback

    def _emit_progress(self, message: str, progress: float = None):
        """Emit progress update if callback is available."""
        if self.progress_callback:
            self.progress_callback(message, progress)
        logger.info(message)

    def _validate_file_exists(self, file_path: str) -> bool:
        """Validate that file exists."""
        if not os.path.exists(file_path):
            error_msg = f"ERROR: File not found: {file_path}"
            logger.error(error_msg)
            self._emit_progress(error_msg)
            return False
        return True

    def process_files(self, lmd_file_path: str, details_file_path: str,
                     selected_columns: List[str], chunk_size: int = 10000) -> Optional[str]:
        """
        Process files to add columns from LMD to Details data.

        Args:
            lmd_file_path: Path to Combined_LMD CSV file
            details_file_path: Path to Combined Details CSV file
            selected_columns: List of columns to add/update
            chunk_size: Number of rows to process per chunk

        Returns:
            Path to output file if successful, None otherwise
        """
        try:
            self._emit_progress("=" * 80)
            self._emit_progress("STARTING ADD COLUMNS PROCESSING")
            self._emit_progress("=" * 80)

            # Validate files exist
            if not self._validate_file_exists(lmd_file_path) or not self._validate_file_exists(details_file_path):
                return None

            # Get total row count for progress tracking
            try:
                with open(details_file_path, 'r', encoding='utf-8') as f:
                    row_count = sum(1 for _ in f) - 1  # subtract header
                self._emit_progress(f"✓ Details file row count: {row_count:,}")
                self._emit_progress(f"✓ Processing chunk size: {chunk_size:,}")
            except Exception as e:
                logger.warning(f"Could not count rows: {e}")
                row_count = 0

            self._emit_progress("")

            # STEP 1: Load LMD data
            self._emit_progress("STEP 1: LOADING LMD DATA INTO MEMORY")
            self._emit_progress("-" * 80)
            lmd_lookup = self._load_lmd_data(lmd_file_path, selected_columns, chunk_size)

            if lmd_lookup is None:
                return None

            lmd_unique_keys = lmd_lookup.select(['Filename', 'lmd_sequence_num']).n_unique()
            self._emit_progress(f"✓ LMD data loaded: {len(lmd_lookup):,} rows with {lmd_unique_keys} unique keys")
            self._emit_progress("")

            # STEP 2: Prepare output file
            self._emit_progress("STEP 2: PREPARING OUTPUT FILE")
            self._emit_progress("-" * 80)
            base_path = os.path.dirname(details_file_path)
            current_date = datetime.now().strftime("%Y-%m-%d")
            base_name = os.path.splitext(os.path.basename(details_file_path))[0]
            output_file_name = f"{base_name}_updated_{current_date}.csv"
            output_file_path = os.path.join(base_path, output_file_name)

            # Check write permissions
            if os.path.exists(output_file_path) and not os.access(output_file_path, os.W_OK):
                error_msg = f"Permission denied: Cannot write to file {output_file_path}"
                logger.error(error_msg)
                self._emit_progress(error_msg)
                return None

            if not os.access(base_path, os.W_OK):
                error_msg = f"Permission denied: Cannot write to directory {base_path}"
                logger.error(error_msg)
                self._emit_progress(error_msg)
                return None

            self._emit_progress(f"✓ Output file: {output_file_path}")
            self._emit_progress("")

            # STEP 3: Process details file
            self._emit_progress("STEP 3: PROCESSING DETAILS FILE AND JOINING WITH LMD")
            self._emit_progress("-" * 80)
            result = self._process_details_file(
                details_file_path, output_file_path, lmd_lookup,
                selected_columns, chunk_size, row_count
            )

            if result:
                self._emit_progress("")
                self._emit_progress("=" * 80)
                self._emit_progress("✓ ADD COLUMNS PROCESSING COMPLETED SUCCESSFULLY")
                self._emit_progress("=" * 80)
                return output_file_path
            else:
                self._emit_progress("✗ ADD COLUMNS PROCESSING FAILED")
                return None

        except Exception as e:
            error_msg = f"Add columns processing failed: {str(e)}"
            logger.error(f"{error_msg}\n{e}", exc_info=True)
            self._emit_progress(error_msg)
            return None

    def _load_lmd_data(self, lmd_file_path: str, selected_columns: List[str],
                      chunk_size: int) -> Optional[pl.DataFrame]:
        """Load LMD data and create lookup DataFrame."""
        try:
            key_columns = ['Filename', 'lmd_sequence_num']
            columns_to_read = key_columns + selected_columns + ['TestDateUTC']

            # Get schema to preserve original formats
            lmd_sample = pl.read_csv(lmd_file_path, n_rows=0, null_values=['∞'], infer_schema_length=0)
            lmd_schema_overrides = {col: pl.Utf8 for col in lmd_sample.columns}

            # Read LMD data in chunks and collect all data
            lmd_chunks = []
            processed_rows = 0

            self._emit_progress("Reading LMD file in chunks...")
            batched_reader = pl.read_csv_batched(
                lmd_file_path, 
                batch_size=chunk_size, 
                null_values=['∞'], 
                schema_overrides=lmd_schema_overrides, 
                infer_schema_length=0
            )
            
            for chunk in batched_reader.next_batches(chunk_size):
                if chunk.is_empty():
                    break

                # Select only needed columns
                chunk = chunk.select(columns_to_read)
                lmd_chunks.append(chunk)
                processed_rows += len(chunk)

                self._emit_progress(f"  Loaded {processed_rows:,} rows from LMD")

            if not lmd_chunks:
                logger.error("No LMD data could be loaded")
                return None

            # Concatenate all chunks
            lmd_df = pl.concat(lmd_chunks, how='vertical')
            self._emit_progress(f"✓ Concatenated {len(lmd_chunks)} chunks")

            # Create timestamp for asof join
            lmd_df = lmd_df.with_columns(
                (pl.col("TestDateUTC").str.strptime(pl.Datetime, format="%d/%m/%Y %H:%M:%S%.f", strict=False)
                .cast(pl.Int64) / 1_000_000).alias("_timestamp")
            )

            # Create lookup DataFrame with unique keys - keep first occurrence for each key
            lmd_lookup = lmd_df.unique(subset=['Filename', 'lmd_sequence_num'], keep='first').sort(['Filename', '_timestamp'])
            
            self._emit_progress(f"✓ Created lookup with unique keys: {len(lmd_lookup):,} rows")

            return lmd_lookup

        except Exception as e:
            logger.error(f"Failed to load LMD data: {e}")
            self._emit_progress(f"✗ Error loading LMD data: {e}")
            return None

    def _process_details_file(self, details_file_path: str, output_file_path: str,
                            lmd_lookup: pl.DataFrame, selected_columns: List[str],
                            chunk_size: int, total_rows: int) -> bool:
        """Process details file in chunks and write output using join_asof."""
        try:
            # Read sample to get existing columns
            details_sample = pl.read_csv(details_file_path, n_rows=0, null_values=['∞'], infer_schema_length=0)
            existing_columns = details_sample.columns
            
            # Create schema overrides to preserve original formats
            details_schema_overrides = {col: pl.Utf8 for col in existing_columns}

            common_columns = [col for col in selected_columns if col in existing_columns]
            new_columns = [col for col in selected_columns if col not in existing_columns]

            self._emit_progress(f"Updating {len(common_columns)} existing columns")
            self._emit_progress(f"Adding {len(new_columns)} new columns: {new_columns}")

            # Final output columns: original + new columns
            final_columns = list(existing_columns) + new_columns
            
            processed_rows = 0
            first_chunk = True

            # Process details file in chunks
            batched_reader = pl.read_csv_batched(
                details_file_path, 
                batch_size=chunk_size, 
                null_values=['∞'], 
                schema_overrides=details_schema_overrides, 
                infer_schema_length=0
            )
            
            for chunk in batched_reader.next_batches(chunk_size):
                if chunk.is_empty():
                    break

                # Add timestamp column for joining
                chunk = chunk.with_columns(
                    (pl.col("TestDateUTC").str.strptime(pl.Datetime, format="%d/%m/%Y %H:%M:%S%.f", strict=False)
                    .cast(pl.Int64) / 1_000_000).alias("_timestamp")
                )

                # Preserve original row order
                chunk = chunk.with_row_index("_row_order")

                # Perform asof join with LMD lookup on timestamp
                joined_chunk = chunk.join_asof(
                    lmd_lookup,
                    on="_timestamp",
                    tolerance=1.0,
                    suffix="_lmd"
                )

                # Update columns from LMD
                # For each selected column, merge LMD value if available
                for col in selected_columns:
                    if f"{col}_lmd" in joined_chunk.columns:
                        # If column exists in original, update it; otherwise create new
                        joined_chunk = joined_chunk.with_columns(
                            pl.when(pl.col(f"{col}_lmd").is_not_null())
                            .then(pl.col(f"{col}_lmd"))
                            .otherwise(pl.col(col) if col in existing_columns else pl.lit(None))
                            .alias(col)
                        )
                        # Drop the _lmd column
                        joined_chunk = joined_chunk.drop(f"{col}_lmd")
                
                # Select columns in correct order: original columns + new columns
                final_output_cols = list(existing_columns) + [col for col in selected_columns if col not in existing_columns]
                
                # Only include columns that actually exist in joined_chunk
                final_output_cols = [col for col in final_output_cols if col in joined_chunk.columns]
                
                joined_chunk = joined_chunk.select(final_output_cols)
                
                # Drop temp columns if they exist
                temp_cols = ["_row_order", "_timestamp"]
                cols_to_drop = [col for col in temp_cols if col in joined_chunk.columns]
                if cols_to_drop:
                    joined_chunk = joined_chunk.drop(cols_to_drop)

                # Write to file
                if first_chunk:
                    joined_chunk.write_csv(output_file_path, include_bom=True)
                    first_chunk = False
                else:
                    with open(output_file_path, 'a', encoding='utf-8') as f:
                        f.write(joined_chunk.write_csv(include_header=False))

                processed_rows += len(chunk)
                self._emit_progress(f"  Processed {processed_rows:,}/{total_rows:,} rows")

            self._emit_progress(f"✓ Output written: {processed_rows:,} rows")
            return True

        except Exception as e:
            logger.error(f"Failed to process details file: {e}", exc_info=True)
            self._emit_progress(f"✗ Error processing details: {e}")
            return False