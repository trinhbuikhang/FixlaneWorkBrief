"""
Add Columns Processor for Data Processing Tool
Handles adding columns from LMD data to Details data using Polars.
"""

import gc
import logging
import os
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Callable, List, Optional, Tuple

# ⚡ Lazy import for Polars (heavy library)
from utils.lazy_imports import polars as pl

from utils.file_lock import FileLock, FileLockTimeout
from utils.path_utils import is_network_path

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

    def _remove_duplicate_testdateutc(self, df: pl.DataFrame) -> Tuple[pl.DataFrame, int]:
        """
        Remove duplicate TestDateUTC rows, keeping the first occurrence.
        
        Args:
            df: DataFrame to process
            
        Returns:
            Tuple of (processed DataFrame, number of duplicates removed)
        """
        if 'TestDateUTC' not in df.columns:
            return df, 0
        
        before_count = len(df)
        df_unique = df.unique(subset=['TestDateUTC'], keep='first', maintain_order=True)
        removed_count = before_count - len(df_unique)
        
        return df_unique, removed_count

    def _validate_file_exists(self, file_path: str) -> bool:
        """Validate that file exists."""
        if not os.path.exists(file_path):
            error_msg = f"ERROR: File not found: {file_path}"
            logger.error(error_msg)
            self._emit_progress(error_msg)
            return False
        return True

    def process_files(self, lmd_file_path: str, details_file_path: str,
                     selected_columns: List[str], chunk_size: int = 10000,
                     tolerance_seconds: int = 60) -> Optional[str]:
        """
        Process files to add columns from LMD to Details data.

        Args:
            lmd_file_path: Path to Combined_LMD CSV file
            details_file_path: Path to Combined Details CSV file
            selected_columns: List of columns to add/update
            chunk_size: Number of rows to process per chunk
            tolerance_seconds: Max time difference (seconds) for timestamp-based join; default 60

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
            self._emit_progress(f"📋 Available for matching: {len(lmd_lookup):,} unique timestamps in LMD data")
            self._emit_progress("")

            # STEP 2: Prepare output file
            self._emit_progress("STEP 2: PREPARING OUTPUT FILE")
            self._emit_progress("-" * 80)
            base_path = os.path.dirname(details_file_path)
            current_date = datetime.now().strftime("%Y-%m-%d")
            base_name = os.path.splitext(os.path.basename(details_file_path))[0]
            output_file_name = f"{base_name}_updated_{current_date}.csv"
            output_file_path = os.path.join(base_path, output_file_name)

            # When output is on network, write to system temp first then copy (avoids Permission denied)
            write_path = output_file_path
            if is_network_path(output_file_path):
                fd, write_path = tempfile.mkstemp(suffix=".csv", prefix="addcols_", dir=tempfile.gettempdir())
                os.close(fd)
                logger.info("Output path is on network; writing to temp then copying")
                self._emit_progress("Output on network drive – writing to temporary location, then copying result")

            # Check write permissions (for final path when not using temp)
            if write_path == output_file_path:
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
                details_file_path, write_path, lmd_lookup,
                selected_columns, chunk_size, row_count, tolerance_seconds
            )

            if result:
                if write_path != output_file_path:
                    try:
                        shutil.copy2(write_path, output_file_path)
                        try:
                            os.remove(write_path)
                        except OSError:
                            pass
                        self._emit_progress("✓ Result copied to output location")
                    except OSError as e:
                        logger.warning("Could not copy result to network path: %s", e)
                        self._emit_progress(f"⚠ Result saved to: {write_path}")
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

            # Remove duplicate TestDateUTC rows
            lmd_df, removed_duplicates = self._remove_duplicate_testdateutc(lmd_df)
            if removed_duplicates > 0:
                self._emit_progress(f"✓ Removed {removed_duplicates} duplicate TestDateUTC rows from LMD")
            else:
                self._emit_progress(f"✓ No duplicate TestDateUTC rows found in LMD")

            # Create timestamp for asof join - handle multiple LMD formats with fallbacks
            try:
                lmd_df = lmd_df.with_columns(
                    pl.when(pl.col("TestDateUTC").str.contains("T"))
                    .then(
                        pl.when(pl.col("TestDateUTC").str.ends_with("Z"))
                        .then(
                            # ISO format with Z: 2024-08-09T23:40:29.308Z
                            pl.col("TestDateUTC").str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%.fZ", strict=False)
                            .cast(pl.Int64) / 1_000_000
                        )
                        .otherwise(
                            # ISO format without Z: 2024-08-09T23:40:29.308
                            pl.col("TestDateUTC").str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%.f", strict=False)
                            .cast(pl.Int64) / 1_000_000
                        )
                    )
                    .otherwise(
                        # DD/MM/YYYY (05/12/2025 ...) or DD/MM/YY (18/02/26 ...)
                        # Use 2-digit year when date part ends with 2-digit year (avoid "18/02/26" parsed as year 26 AD)
                        pl.when(
                            pl.col("TestDateUTC").str.split(" ").list.first().str.split("/").list.last().str.len_chars() == 2
                        )
                        .then(pl.col("TestDateUTC").str.strptime(pl.Datetime, format="%d/%m/%y %H:%M:%S%.f", strict=False))
                        .otherwise(pl.col("TestDateUTC").str.strptime(pl.Datetime, format="%d/%m/%Y %H:%M:%S%.f", strict=False))
                        .cast(pl.Int64) / 1_000_000
                    )
                    .alias("_timestamp")
                )
                
                # Check for any null timestamps (parsing failures)
                null_timestamps = lmd_df.filter(pl.col("_timestamp").is_null()).height
                if null_timestamps > 0:
                    self._emit_progress(f"⚠️ Warning: {null_timestamps:,} LMD records had unparseable timestamps")
                
            except Exception as e:
                logger.error(f"Error parsing LMD timestamps: {e}")
                self._emit_progress(f"✗ Error parsing LMD timestamps: {e}")
                return None

            # Analyze timestamp formats in LMD data
            iso_with_z_count = lmd_df.filter(pl.col("TestDateUTC").str.contains("T") & pl.col("TestDateUTC").str.ends_with("Z")).height
            iso_without_z_count = lmd_df.filter(pl.col("TestDateUTC").str.contains("T") & ~pl.col("TestDateUTC").str.ends_with("Z")).height
            ddmm_count = lmd_df.filter(~pl.col("TestDateUTC").str.contains("T")).height
            
            self._emit_progress(f"📅 LMD Timestamp formats detected:")
            if iso_with_z_count > 0:
                self._emit_progress(f"   • ISO with Z (YYYY-MM-DDTHH:MM:SS.fffZ): {iso_with_z_count:,} records")
            if iso_without_z_count > 0:
                self._emit_progress(f"   • ISO without Z (YYYY-MM-DDTHH:MM:SS.fff): {iso_without_z_count:,} records")
            if ddmm_count > 0:
                self._emit_progress(f"   • DD/MM/YYYY or DD/MM/YY (DD/MM/YYYY or DD/MM/YY HH:MM:SS.fff): {ddmm_count:,} records")

            # Create lookup DataFrame with unique keys - keep first occurrence for each key
            lmd_lookup = lmd_df.unique(subset=['Filename', 'lmd_sequence_num'], keep='first').sort(['_timestamp'])
            
            self._emit_progress(f"✓ Created lookup with unique keys: {len(lmd_lookup):,} rows")

            return lmd_lookup

        except Exception as e:
            logger.error(f"Failed to load LMD data: {e}")
            self._emit_progress(f"✗ Error loading LMD data: {e}")
            return None

    def _process_details_file(self, details_file_path: str, output_file_path: str,
                            lmd_lookup: pl.DataFrame, selected_columns: List[str],
                            chunk_size: int, total_rows: int, tolerance_seconds: int = 60) -> bool:
        """Process details file in chunks and write output using join_asof."""
        try:
            # Read sample to get existing columns
            details_sample = pl.read_csv(details_file_path, n_rows=0, null_values=['∞'], infer_schema_length=0)
            existing_columns = details_sample.columns
            
            # Create schema overrides to preserve original formats
            details_schema_overrides = {col: pl.Utf8 for col in existing_columns}

            # Pre-check for duplicates in details file
            if 'TestDateUTC' in existing_columns:
                self._emit_progress("Checking for duplicate TestDateUTC in Details file...")
                details_full = pl.read_csv(details_file_path, null_values=['∞'], schema_overrides=details_schema_overrides, infer_schema_length=0)
                before_count = len(details_full)
                details_full, removed_duplicates = self._remove_duplicate_testdateutc(details_full)
                if removed_duplicates > 0:
                    self._emit_progress(f"✓ Removed {removed_duplicates} duplicate TestDateUTC rows from Details")
                    # Write cleaned details file to a temp location, then use it for processing
                    if is_network_path(details_file_path):
                        fd, temp_details_path = tempfile.mkstemp(suffix=".temp_dedup.csv", prefix="details_", dir=tempfile.gettempdir())
                        os.close(fd)
                    else:
                        temp_details_path = details_file_path + ".temp_dedup"
                    details_full.write_csv(temp_details_path, null_value='', line_terminator='\r\n')
                    details_file_path = temp_details_path
                    total_rows = len(details_full)
                else:
                    self._emit_progress(f"✓ No duplicate TestDateUTC rows found in Details")
                del details_full  # Free memory

            common_columns = [col for col in selected_columns if col in existing_columns]
            new_columns = [col for col in selected_columns if col not in existing_columns]

            self._emit_progress(f"🔄 Updating {len(common_columns)} existing columns")
            self._emit_progress(f"➕ Adding {len(new_columns)} new columns: {new_columns}")
            self._emit_progress(f"🔗 Starting timestamp-based matching with {tolerance_seconds}-second tolerance...")

            # Final output columns: original + new columns
            final_columns = list(existing_columns) + new_columns
            
            processed_rows = 0
            first_chunk = True
            total_matches = 0
            total_records_updated = 0
            
            # Track timestamp formats in details file
            details_iso_with_z = 0
            details_iso_without_z = 0
            details_ddmm = 0

            # Process details file in chunks
            batched_reader = pl.read_csv_batched(
                details_file_path, 
                batch_size=chunk_size, 
                null_values=['∞'], 
                schema_overrides=details_schema_overrides, 
                infer_schema_length=0
            )
            
            for chunk_idx, chunk in enumerate(batched_reader.next_batches(chunk_size)):
                if chunk.is_empty():
                    break

                # Count timestamp formats in this chunk
                chunk_iso_z = chunk.filter(pl.col("TestDateUTC").str.contains("T") & pl.col("TestDateUTC").str.ends_with("Z")).height
                chunk_iso_no_z = chunk.filter(pl.col("TestDateUTC").str.contains("T") & ~pl.col("TestDateUTC").str.ends_with("Z")).height
                chunk_ddmm = chunk.filter(~pl.col("TestDateUTC").str.contains("T")).height
                
                details_iso_with_z += chunk_iso_z
                details_iso_without_z += chunk_iso_no_z
                details_ddmm += chunk_ddmm

                # Add timestamp column for joining - handle multiple details formats with error handling
                try:
                    chunk = chunk.with_columns(
                        pl.when(pl.col("TestDateUTC").str.contains("T"))
                        .then(
                            pl.when(pl.col("TestDateUTC").str.ends_with("Z"))
                            .then(
                                # ISO format with Z: 2024-08-09T23:40:29.308Z
                                pl.col("TestDateUTC").str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%.fZ", strict=False)
                                .cast(pl.Int64) / 1_000_000
                            )
                            .otherwise(
                                # ISO format without Z: 2024-08-09T23:40:29.308
                                pl.col("TestDateUTC").str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%.f", strict=False)
                                .cast(pl.Int64) / 1_000_000
                            )
                        )
                        .otherwise(
                            # DD/MM/YYYY (05/12/2025 ...) or DD/MM/YY (18/02/26 ...)
                            # Use 2-digit year when date part ends with 2-digit year (avoid "18/02/26" parsed as year 26 AD)
                            pl.when(
                                pl.col("TestDateUTC").str.split(" ").list.first().str.split("/").list.last().str.len_chars() == 2
                            )
                            .then(pl.col("TestDateUTC").str.strptime(pl.Datetime, format="%d/%m/%y %H:%M:%S%.f", strict=False))
                            .otherwise(pl.col("TestDateUTC").str.strptime(pl.Datetime, format="%d/%m/%Y %H:%M:%S%.f", strict=False))
                            .cast(pl.Int64) / 1_000_000
                        )
                        .alias("_timestamp")
                    )
                    
                    # Check for parsing failures in this chunk
                    null_timestamps_chunk = chunk.filter(pl.col("_timestamp").is_null()).height
                    if null_timestamps_chunk > 0:
                        self._emit_progress(f"      ⚠️ {null_timestamps_chunk} records in chunk had unparseable timestamps")
                
                except Exception as e:
                    logger.error(f"Error parsing timestamps in details chunk: {e}")
                    self._emit_progress(f"✗ Error parsing timestamps in chunk: {e}")
                    continue  # Skip this chunk and continue with next

                # Preserve original row order
                chunk = chunk.with_row_index("_row_order")

                # Sort chunk by timestamp for asof_join
                chunk = chunk.sort("_timestamp")

                # Perform asof join with LMD lookup on timestamp
                # _timestamp is in seconds (Datetime cast Int64 / 1_000_000), so tolerance is in seconds
                joined_chunk = chunk.join_asof(
                    lmd_lookup,
                    on="_timestamp",
                    tolerance=tolerance_seconds,
                    suffix="_lmd"
                )

                # Count matches in this chunk (records that got LMD data)
                chunk_matches = 0
                chunk_records_updated = 0
                
                # Check for any LMD columns that have non-null values
                lmd_columns_in_chunk = [col for col in joined_chunk.columns if col.endswith('_lmd')]
                if lmd_columns_in_chunk:
                    # Count rows that have at least one non-null LMD column
                    for lmd_col in lmd_columns_in_chunk:
                        non_null_mask = joined_chunk[lmd_col].is_not_null()
                        chunk_matches += non_null_mask.sum()
                    
                    # Each row that has any LMD data is considered "updated"
                    has_any_lmd_data = joined_chunk.select([
                        pl.any_horizontal([pl.col(col).is_not_null() for col in lmd_columns_in_chunk])
                    ]).to_series()
                    chunk_records_updated = has_any_lmd_data.sum()

                total_matches += chunk_matches
                total_records_updated += chunk_records_updated

                # Track field-level updates for detailed logging
                field_updates = {}
                
                # Update columns from LMD
                # For each selected column, merge LMD value if available
                for col in selected_columns:
                    if f"{col}_lmd" in joined_chunk.columns:
                        # Count non-null values for this specific column
                        non_null_count = joined_chunk[f"{col}_lmd"].is_not_null().sum()
                        field_updates[col] = non_null_count
                        
                        # If column exists in original, update it; otherwise create new
                        joined_chunk = joined_chunk.with_columns(
                            pl.when(pl.col(f"{col}_lmd").is_not_null())
                            .then(pl.col(f"{col}_lmd"))
                            .otherwise(pl.col(col) if col in existing_columns else pl.lit(None))
                            .alias(col)
                        )
                        # Drop the _lmd column
                        joined_chunk = joined_chunk.drop(f"{col}_lmd")
                
                # Log field-specific updates if any
                if field_updates and any(count > 0 for count in field_updates.values()):
                    field_summary = ", ".join([f"{field}: {count}" for field, count in field_updates.items() if count > 0])
                    if field_summary:
                        self._emit_progress(f"      └─ Field updates: {field_summary}")
                
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
                    joined_chunk.write_csv(output_file_path, include_bom=True, line_terminator='\r\n')
                    first_chunk = False
                else:
                    with open(output_file_path, 'a', encoding='utf-8', newline='') as f:
                        f.write(joined_chunk.write_csv(include_header=False, line_terminator='\r\n'))

                # Clean up chunk to free memory
                chunk_len = len(chunk)
                del joined_chunk
                del chunk
                if chunk_idx % 10 == 0:  # Every 10 chunks
                    gc.collect()

                processed_rows += chunk_len
                
                # Log progress with match statistics
                if chunk_records_updated > 0:
                    self._emit_progress(f"  Processed {processed_rows:,}/{total_rows:,} rows | Chunk matches: {chunk_records_updated}/{chunk_len} records updated")
                else:
                    self._emit_progress(f"  Processed {processed_rows:,}/{total_rows:,} rows | No matches in this chunk")

            # Final statistics
            match_rate = (total_records_updated / processed_rows * 100) if processed_rows > 0 else 0
            self._emit_progress(f"✓ Output written: {processed_rows:,} rows")
            self._emit_progress(f"")
            
            # Log Details timestamp formats
            self._emit_progress(f"📅 Details Timestamp formats processed:")
            if details_iso_with_z > 0:
                self._emit_progress(f"   • ISO with Z (YYYY-MM-DDTHH:MM:SS.fffZ): {details_iso_with_z:,} records")
            if details_iso_without_z > 0:
                self._emit_progress(f"   • ISO without Z (YYYY-MM-DDTHH:MM:SS.fff): {details_iso_without_z:,} records")
            if details_ddmm > 0:
                self._emit_progress(f"   • DD/MM/YYYY or DD/MM/YY (DD/MM/YYYY or DD/MM/YY HH:MM:SS.fff): {details_ddmm:,} records")
            self._emit_progress(f"")
            
            self._emit_progress(f"📊 MATCHING STATISTICS:")
            self._emit_progress(f"   • Total records processed: {processed_rows:,}")
            self._emit_progress(f"   • Records with matches: {total_records_updated:,}")
            self._emit_progress(f"   • Total field matches: {total_matches:,}")
            self._emit_progress(f"   • Match rate: {match_rate:.2f}%")
            self._emit_progress(f"")
            self._emit_progress(f"🎯 SELECTED COLUMNS ({len(selected_columns)}):")
            for i, col in enumerate(selected_columns, 1):
                column_status = "✓ Added/Updated" if col in new_columns else "🔄 Updated existing"
                self._emit_progress(f"   {i}. {col} - {column_status}")
            
            # Clean up temp dedup file if we created one
            if "temp_dedup" in details_file_path and os.path.exists(details_file_path):
                try:
                    os.remove(details_file_path)
                except OSError:
                    pass
            
            return True

        except Exception as e:
            logger.error(f"Failed to process details file: {e}", exc_info=True)
            self._emit_progress(f"✗ Error processing details: {e}")
            return False