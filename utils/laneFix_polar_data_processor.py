"""
Data processing module for Fixlane WorkBrief application - Polars Optimized Version.
Provides classes and methods for lane fixing and workbrief processing operations using Polars.
"""

import gc
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional, Tuple

# ⚡ Lazy import for Polars (heavy library)
from utils.lazy_imports import polars as pl

from config.laneFix_config import Config, Messages
from utils.file_lock import FileLock, FileLockTimeout
from utils.timestamp_handler import timestamp_handler

logger = logging.getLogger(__name__)


class PolarsDataProcessor:
    """Base class for data processing operations using Polars."""
    
    def __init__(self, progress_callback: Optional[Callable] = None):
        """
        Initialize data processor.
        
        Args:
            progress_callback: Optional callback function for progress updates
        """
        self.progress_callback = progress_callback
        from config.laneFix_config import Config
        self.config = Config()
    
    def _emit_progress(self, message: str, progress: float = None):
        """Emit progress update if callback is available."""
        if self.progress_callback:
            self.progress_callback(message, progress)
        logger.info(message)
    
    def _validate_file_exists(self, file_path: str) -> bool:
        """Validate that file exists."""
        if not os.path.exists(file_path):
            error_msg = Messages.ERROR_FILE_NOT_FOUND.format(file_path)
            logger.error(error_msg)
            self._emit_progress(error_msg)
            return False
        return True
    
    def _validate_columns(self, df: pl.DataFrame, required_columns: list, file_name: str) -> bool:
        """Validate that required columns exist in Polars dataframe."""
        logger.info(f"Validating columns for {file_name}")
        logger.info(f"Available columns: {df.columns}")
        logger.info(f"Required columns: {required_columns}")
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            error_msg = f"Missing columns in {file_name}: {missing_columns}"
            logger.error(error_msg)
            self._emit_progress(error_msg)
            return False
        return True
    
    def _validate_columns_flexible(self, df: pl.DataFrame, required_columns: list, file_name: str) -> bool:
        """Validate that required columns exist using flexible column mapping."""
        logger.info(f"Validating columns for {file_name}")
        logger.info(f"Available columns: {df.columns}")
        logger.info(f"Required columns: {required_columns}")
        
        missing_columns = []
        for required_col in required_columns:
            # Get variants for this column from config
            variants = self.config.COLUMN_MAPPINGS.get(required_col, [required_col])
            
            # Check if any variant exists in the dataframe
            found = any(variant in df.columns for variant in variants)
            
            if not found:
                missing_columns.append(required_col)
        
        if missing_columns:
            error_msg = f"Missing columns in {file_name}: {missing_columns}"
            logger.error(error_msg)
            self._emit_progress(error_msg)
            return False
        return True

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

    def _preserve_input_row_count(self, df: pl.DataFrame, original_input_count: int, description: str = "") -> pl.DataFrame:
        """
        Ensure output has exactly same number of rows as input by removing only processing-generated duplicates.
        
        Args:
            df: DataFrame to check
            original_input_count: Expected number of rows (from original input)
            description: Description for logging
            
        Returns:
            DataFrame with exact input row count preserved
        """
        current_count = len(df)
        
        if current_count == original_input_count:
            logger.info(f"Row count validation {description}: Perfect match ({current_count:,} rows)")
            return df
        elif current_count > original_input_count:
            excess_rows = current_count - original_input_count
            logger.warning(f"Row count validation {description}: {excess_rows:,} excess rows detected, removing processing duplicates")
            
            # Only remove the excess rows that were created during processing
            # Keep the first N rows where N = original_input_count
            df_corrected = df.head(original_input_count)
            
            self._emit_progress(f"Row count correction: Removed {excess_rows:,} processing-generated duplicates")
            return df_corrected
        else:
            missing_rows = original_input_count - current_count
            logger.error(f"Row count validation {description}: {missing_rows:,} rows missing - this should not happen!")
            self._emit_progress(f"ERROR: {missing_rows:,} input rows missing from output")
            return df

    def _detect_file_type(self, df: pl.DataFrame) -> str:
        """Detect the type of data file based on column names."""
        columns = set(df.columns)
        
        # Check for lane fixes file
        lane_fixes_indicators = {'From', 'To', 'Lane', 'Ignore', 'Plate'}
        if lane_fixes_indicators.issubset(columns):
            return "lane_fixes"
        
        # Check for combined LMD file
        lmd_indicators = {'TestDateUTC', 'BinViewerVersion', 'tsdSlope2000', 'compositeModulus200'}
        if any(indicator in columns for indicator in lmd_indicators):
            return "combined_lmd"
        
        # Check for workbrief file
        workbrief_indicators = {'RoadName', 'Lane'}
        if workbrief_indicators.issubset(columns) and len(columns) < 20:
            return "workbrief"
        
        return "unknown"
    
    def _standardize_boolean_columns(self, df: pl.DataFrame, boolean_columns: list) -> pl.DataFrame:
        """Standardize boolean columns to True/False values."""
        for col in boolean_columns:
            if col in df.columns:
                df = df.with_columns([
                    pl.when(pl.col(col).cast(pl.Utf8).str.strip_chars().is_in(['1', 'True', 'true', 'TRUE', 'T', 't']))
                    .then(True)
                    .when(pl.col(col).cast(pl.Utf8).str.strip_chars().is_in(['0', 'False', 'false', 'FALSE', 'F', 'f', '']))
                    .then(False)
                    .otherwise(None)
                    .alias(col)
                ])
        return df
    
    def _prepare_for_csv_output(self, df: pl.DataFrame) -> pl.DataFrame:
        """Prepare dataframe for CSV output with proper boolean and null handling."""
        # Convert all columns to string to preserve original formatting
        expressions = []

        for col in df.columns:
            if df[col].dtype == pl.Boolean:
                # Convert boolean columns to True/False strings (with capital T/F)
                expr = (
                    pl.when(pl.col(col).is_null())
                    .then(pl.lit(""))  # Empty string for null values
                    .when(pl.col(col))
                    .then(pl.lit("True"))  # Capital T
                    .otherwise(pl.lit("False"))  # Capital F
                    .alias(col)
                )
                expressions.append(expr)
            else:
                # Convert all other columns to string to preserve formatting
                expr = pl.col(col).cast(pl.Utf8).alias(col)
                expressions.append(expr)

        df = df.with_columns(expressions)

        return df
    
    def _save_to_csv_with_proper_formatting(self, df: pl.DataFrame, output_path: str) -> str:
        """Save DataFrame to CSV with proper boolean and null formatting."""
        # Prepare data for output
        df_output = self._prepare_for_csv_output(df)
        
        # Write to CSV with file locking
        try:
            with FileLock(output_path, timeout=60):
                df_output.write_csv(
                    output_path,
                    null_value="",  # Empty string for null values
                    quote_char='"',
                    separator=','
                )
        except FileLockTimeout:
            logger.error(f"Output file {output_path} is locked by another process")
            raise RuntimeError(f"Cannot write to {output_path} - file is locked")
        
        # Clean up output dataframe
        del df_output
        gc.collect()
        
        logger.info(f"Results saved to: {output_path}")
        return output_path


class PolarsLaneFixProcessor(PolarsDataProcessor):
    """Handles lane fixing operations using Polars."""
    
    def process(self, lane_fixes_path: str, combined_lmd_path: str) -> Optional[str]:
        """
        Process lane fixes and update combined LMD data using pure Polars.
        
        Args:
            lane_fixes_path: Path to lane fixes CSV file
            combined_lmd_path: Path to combined LMD CSV file
            
        Returns:
            Output file path if successful, None otherwise
        """
        try:
            self._emit_progress("Starting lane fix processing...")
            
            # Validate input files
            if not self._validate_file_exists(lane_fixes_path):
                return None
            if not self._validate_file_exists(combined_lmd_path):
                return None
            
            # Load data files using Polars
            self._emit_progress("Loading lane fixes file...")
            lane_fixes = self._load_lane_fixes_polars(lane_fixes_path)
            if lane_fixes is None:
                return None
            
            self._emit_progress("Loading combined LMD file...")
            combined_lmd = self._load_combined_lmd_polars(combined_lmd_path)
            if combined_lmd is None:
                return None
            
            # Process timestamps using Polars
            lane_fixes, combined_lmd = self._process_timestamps_polars(lane_fixes, combined_lmd)
            if lane_fixes is None or combined_lmd is None:
                return None
            
            # Update lanes using Polars
            self._emit_progress("Updating lane information...")
            updated_lmd = self._update_lanes_polars(lane_fixes, combined_lmd)
            
            # Save results
            output_path = self.config.get_output_filename(combined_lmd_path, 'fixlane')
            self._save_to_csv_with_proper_formatting(updated_lmd, output_path)
            
            self._emit_progress(Messages.SUCCESS_LANE_UPDATE)
            return output_path
            
        except Exception as e:
            error_msg = Messages.ERROR_PROCESSING.format(str(e))
            # Clean up unicode characters that might cause logging issues
            clean_error = str(e).encode('ascii', 'ignore').decode('ascii')
            logger.error(f"{error_msg}: {clean_error}")
            self._emit_progress(error_msg)
            return None
    
    def process_in_memory(self, lane_fixes_path: str, combined_lmd_path: str) -> Optional[pl.DataFrame]:
        """
        Process lane fixes and return updated LMD data in memory using Polars.
        
        Args:
            lane_fixes_path: Path to lane fixes CSV file
            combined_lmd_path: Path to combined LMD CSV file
            
        Returns:
            Updated Polars DataFrame if successful, None otherwise
        """
        try:
            self._emit_progress("Starting lane fix processing...")
            
            # Validate input files
            if not self._validate_file_exists(lane_fixes_path):
                return None
            if not self._validate_file_exists(combined_lmd_path):
                return None
            
            # Load data files
            self._emit_progress("Loading lane fixes file...")
            lane_fixes = self._load_lane_fixes_polars(lane_fixes_path)
            if lane_fixes is None:
                return None
            
            self._emit_progress("Loading combined LMD file...")
            combined_lmd = self._load_combined_lmd_polars(combined_lmd_path)
            if combined_lmd is None:
                return None
            
            # Process timestamps
            lane_fixes, combined_lmd = self._process_timestamps_polars(lane_fixes, combined_lmd)
            if lane_fixes is None or combined_lmd is None:
                return None
            
            # Update lanes
            self._emit_progress("Updating lane information...")
            updated_lmd = self._update_lanes_polars(lane_fixes, combined_lmd)
            
            # Note: Lane fixes processing complete - deduplication handled at final stage
            logger.info(f"Lane fixes processing complete: {len(updated_lmd):,} rows")
            
            # Clean up temporary columns and preserve original TestDateUTC format
            if 'TestDateUTC_ts' in updated_lmd.columns:
                updated_lmd = updated_lmd.drop('TestDateUTC_ts')

            # Preserve original TestDateUTC format - do not reformat to DD/MM/YYYY
            # The original ISO format should be maintained for proper sorting
            # Only convert back to string if it's still datetime type
            if 'TestDateUTC' in updated_lmd.columns:
                if updated_lmd['TestDateUTC'].dtype in [pl.Datetime, pl.Datetime('ns'), pl.Datetime('us')]:
                    # Convert back to ISO format to preserve original sorting order
                    updated_lmd = updated_lmd.with_columns([
                        pl.col('TestDateUTC').dt.strftime('%Y-%m-%dT%H:%M:%S%.3fZ').alias('TestDateUTC')
                    ])
            
            self._emit_progress(Messages.SUCCESS_LANE_UPDATE)
            return updated_lmd
            
        except Exception as e:
            error_msg = Messages.ERROR_PROCESSING.format(str(e))
            # Clean up unicode characters that might cause logging issues
            clean_error = str(e).encode('ascii', 'ignore').decode('ascii')
            logger.error(f"{error_msg}: {clean_error}")
            self._emit_progress(error_msg)
            return None
    
    def _load_lane_fixes_polars(self, file_path: str) -> Optional[pl.DataFrame]:
        """Load and validate lane fixes file using pure Polars."""
        try:
            # Read CSV with Polars - don't try to parse dates automatically, read as strings
            df = pl.read_csv(
                file_path, 
                try_parse_dates=False,  # Disable automatic date parsing
                infer_schema_length=0,  # Read all as strings initially
                null_values=["", "NULL", "null", "NA"]
            )
            
            self._emit_progress(Messages.INFO_FILE_LOADED.format(len(df)))
            
            logger.info(f"Loading lane fixes from: {file_path}")
            logger.info(f"File has {len(df)} rows and {len(df.columns)} columns")
            
            # Detect file type
            detected_type = self._detect_file_type(df)
            logger.info(f"Detected file type: {detected_type}")
            
            if detected_type != "lane_fixes":
                error_msg = f"ERROR: Wrong file type detected!"
                if detected_type == "combined_lmd":
                    error_msg += " This appears to be a Combined LMD file."
                elif detected_type == "workbrief":
                    error_msg += " This appears to be a Workbrief file."
                else:
                    error_msg += f" This appears to be an unknown file type."
                error_msg += f" Please select a Lane Fixes CSV file (should contain: From, To, Lane, Ignore columns)."
                
                self._emit_progress(error_msg)
                logger.error(error_msg)
                return None
            
            # Validate required columns
            required_cols = ['From', 'To', 'Lane', 'Ignore']
            if not self._validate_columns(df, required_cols, f"lane fixes file '{file_path}'"):
                return None
            
            # Standardize boolean column
            df = self._standardize_boolean_columns(df, ['Ignore'])
            
            return df
            
        except Exception as e:
            error_msg = f"Failed to load lane fixes file: {str(e)}"
            # Clean up unicode characters that might cause logging issues
            error_msg = error_msg.encode('ascii', 'ignore').decode('ascii')
            logger.error(error_msg)
            self._emit_progress(error_msg)
            return None
    
    def _load_combined_lmd_polars(self, file_path: str) -> Optional[pl.DataFrame]:
        """Load and validate combined LMD file using pure Polars."""
        try:
            # Read CSV with Polars - don't try to parse dates automatically
            df = pl.read_csv(
                file_path, 
                try_parse_dates=False,  # Disable automatic date parsing
                infer_schema_length=0,  # Read all as strings initially
                null_values=["", "NULL", "null", "NA"]
            )
            
            self._emit_progress(Messages.INFO_FILE_LOADED.format(len(df)))
            
            logger.info(f"Loading combined LMD from: {file_path}")
            logger.info(f"File has {len(df)} rows and {len(df.columns)} columns")
            
            # Detect file type
            detected_type = self._detect_file_type(df)
            logger.info(f"Detected file type: {detected_type}")
            
            if detected_type != "combined_lmd":
                error_msg = f"ERROR: Wrong file type detected!"
                if detected_type == "lane_fixes":
                    error_msg += " This appears to be a Lane Fixes file."
                elif detected_type == "workbrief":
                    error_msg += " This appears to be a Workbrief file."
                else:
                    error_msg += f" This appears to be an unknown file type."
                error_msg += f" Please select a Combined LMD CSV file (should contain: TestDateUTC, Lane, RoadName columns)."
                
                self._emit_progress(error_msg)
                logger.error(error_msg)
                return None
            
            # Validate required columns
            required_cols = self.config.REQUIRED_COLUMNS['combined_LMD']
            if not self._validate_columns_flexible(df, required_cols, f"combined LMD file '{file_path}'"):
                return None
            
            # Remove duplicate TestDateUTC rows
            df, removed_duplicates = self._remove_duplicate_testdateutc(df)
            if removed_duplicates > 0:
                self._emit_progress(f"Removed {removed_duplicates} duplicate TestDateUTC rows")
            else:
                self._emit_progress(f"No duplicate TestDateUTC rows found")
            
            return df
            
        except Exception as e:
            error_msg = f"Failed to load combined LMD file: {str(e)}"
            # Clean up unicode characters that might cause logging issues
            error_msg = error_msg.encode('ascii', 'ignore').decode('ascii')
            logger.error(error_msg)
            self._emit_progress(error_msg)
            return None
    
    def _process_timestamps_polars(self, lane_fixes: pl.DataFrame, 
                                 combined_lmd: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Process timestamps using Polars operations for better performance."""
        try:
            self._emit_progress("Processing timestamps...")
            
            # For now, use existing timestamp handler but optimize with Polars where possible
            # Convert to pandas temporarily for timestamp processing (this part needs existing logic)
            lane_fixes_pd = lane_fixes.to_pandas()
            combined_lmd_pd = combined_lmd.to_pandas()
            
            # Process lane fixes timestamps
            lane_fixes_pd['From'], from_format = timestamp_handler.detect_and_parse_timestamps(
                lane_fixes_pd['From'], 'lane_fixes_From'
            )
            lane_fixes_pd['To'], to_format = timestamp_handler.detect_and_parse_timestamps(
                lane_fixes_pd['To'], 'lane_fixes_To'
            )
            
            # Check for parsing failures
            from_failed = lane_fixes_pd['From'].isna().sum()
            to_failed = lane_fixes_pd['To'].isna().sum()
            
            if from_failed > 0 or to_failed > 0:
                logger.warning(f"Lane fixes parsing: {from_failed} 'From', {to_failed} 'To' failed")
            
            if lane_fixes_pd['From'].isna().all() or lane_fixes_pd['To'].isna().all():
                self._emit_progress("Critical: All timestamps failed to parse in lane fixes")
                return None, None
            
            # Process combined LMD timestamps
            original_samples = combined_lmd_pd['TestDateUTC'].dropna().head(5).astype(str).tolist()
            combined_lmd_pd['TestDateUTC'], lmd_format = timestamp_handler.detect_and_parse_timestamps(
                combined_lmd_pd['TestDateUTC'], 'combined_LMD_TestDateUTC'
            )
            
            # Apply RoadName cleanup if ISO format detected
            if timestamp_handler.is_iso_format(original_samples):
                logger.info("Detected ISO format - applying RoadName cleanup")
                roadname_variants = self.config.COLUMN_MAPPINGS.get('RoadName', ['RoadName'])
                roadname_col = None
                for variant in roadname_variants:
                    if variant in combined_lmd_pd.columns:
                        roadname_col = variant
                        break
                
                if roadname_col:
                    combined_lmd_pd[roadname_col] = combined_lmd_pd[roadname_col].apply(self._remove_last_word)
            
            # Check combined LMD parsing
            lmd_failed = combined_lmd_pd['TestDateUTC'].isna().sum()
            if lmd_failed > 0:
                logger.warning(f"Combined LMD parsing: {lmd_failed} timestamps failed")
            
            if combined_lmd_pd['TestDateUTC'].isna().all():
                self._emit_progress("Critical: All timestamps failed to parse in combined LMD")
                return None, None
            
            # Convert to timestamps for comparison
            combined_lmd_pd['TestDateUTC_ts'] = combined_lmd_pd['TestDateUTC'].astype('int64') // 10**9
            lane_fixes_pd['From_ts'] = lane_fixes_pd['From'].astype('int64') // 10**9
            lane_fixes_pd['To_ts'] = lane_fixes_pd['To'].astype('int64') // 10**9
            
            # Convert back to Polars
            lane_fixes_pl = pl.from_pandas(lane_fixes_pd)
            combined_lmd_pl = pl.from_pandas(combined_lmd_pd)
            
            return lane_fixes_pl, combined_lmd_pl
            
        except Exception as e:
            logger.error(f"Timestamp processing failed: {e}")
            self._emit_progress(f"Timestamp processing failed: {e}")
            return None, None
    
    def _remove_last_word(self, value) -> str:
        """Remove last word from string value (for RoadName cleanup)."""
        if isinstance(value, str) and ' ' in value:
            return value.rsplit(' ', 1)[0]
        return value
    
    def _update_lanes_polars(self, lane_fixes: pl.DataFrame, 
                           combined_lmd: pl.DataFrame) -> pl.DataFrame:
        """Update lane information using Polars join operations for better performance."""
        try:
            # Check if we should use optimized method for large datasets
            if len(combined_lmd) > 100000:  # Use optimized method for datasets > 100k rows
                logger.info("Using optimized lane update algorithm for large dataset")
                return self._update_lanes_polars_optimized(lane_fixes, combined_lmd)
            else:
                logger.info("Using standard Polars join for smaller dataset")
                return self._update_lanes_polars_standard(lane_fixes, combined_lmd)
            
        except Exception as e:
            logger.error(f"Lane update failed: {e}")
            # Return original data if Polars fails
            logger.warning("Returning original data without lane updates due to processing error")
            return combined_lmd
    
    def _update_lanes_polars_standard(self, lane_fixes: pl.DataFrame, 
                                    combined_lmd: pl.DataFrame) -> pl.DataFrame:
        """Update lane information using Polars join operations for better performance."""
        try:
            logger.info(f"Starting lane update for {len(combined_lmd)} records using Polars")
            self._emit_progress("Preparing lane fixes data...")
            
            # Find the correct Lane column variant using Polars
            lane_variants = self.config.COLUMN_MAPPINGS.get('Lane', ['Lane'])
            lane_col = None
            for variant in lane_variants:
                if variant in combined_lmd.columns:
                    lane_col = variant
                    break
            
            if not lane_col:
                logger.error("No Lane column found in combined LMD data")
                return combined_lmd
            
            logger.info(f"Using Lane column: '{lane_col}'")
            
            # Prepare lane fixes data with proper boolean handling
            self._emit_progress("Processing lane fixes data...")
            lane_fixes_prepared = lane_fixes.with_columns([
                # Ensure boolean Ignore column is properly handled
                pl.when(pl.col("Ignore").is_null())
                .then(False)
                .otherwise(pl.col("Ignore"))
                .alias("Ignore")
            ])
            
            # Add row index to combined_lmd for tracking
            self._emit_progress("Indexing combined LMD data...")
            combined_lmd_indexed = combined_lmd.with_row_index("row_idx")
            
            # Create Ignore column if it doesn't exist
            if 'Ignore' not in combined_lmd_indexed.columns:
                combined_lmd_indexed = combined_lmd_indexed.with_columns([
                    pl.lit(False).alias('Ignore')
                ])
            
            # Sort data for join_asof operation
            self._emit_progress("Sorting data for efficient processing...")
            lane_fixes_sorted = lane_fixes_prepared.select([
                "From_ts", "To_ts", "Lane", "Ignore"
            ]).rename({
                "From_ts": "ts_start", 
                "To_ts": "ts_end", 
                "Lane": "fix_lane", 
                "Ignore": "fix_ignore"
            }).sort("ts_start")
            
            combined_lmd_sorted = combined_lmd_indexed.sort("TestDateUTC_ts")
            
            # Use join_asof to find matching lane fixes, then left join back to preserve all rows
            self._emit_progress("Applying lane fixes (this may take a while for large files)...")

            # First, find all matching lane fixes
            matched_fixes = combined_lmd_sorted.join_asof(
                lane_fixes_sorted,
                left_on="TestDateUTC_ts",
                right_on="ts_start",
                strategy="backward"
            ).filter(
                # Only keep matches where timestamp is within the range
                pl.col("TestDateUTC_ts").is_between(pl.col("ts_start"), pl.col("ts_end"), closed="both")
            ).select([
                "row_idx", "fix_lane", "fix_ignore"
            ])

            # Left join the matches back to the original indexed data to preserve ALL rows
            updated_lmd = combined_lmd_indexed.join(
                matched_fixes,
                on="row_idx",
                how="left"
            )
            
            self._emit_progress("Finalizing lane updates...")
            # Apply lane updates using Polars expressions - handle null values for non-matches
            if len(updated_lmd) > 0:
                # Create the updated lane values - only update where we have matches
                updated_lmd = updated_lmd.with_columns([
                    # Update lane based on fix_lane length and current lane value
                    pl.when(pl.col("fix_lane").is_not_null())
                    .then(
                        pl.when(pl.col("fix_lane").cast(pl.Utf8).eq("-1"))
                        .then(pl.col(lane_col))  # Keep original if fix is -1
                        .when(pl.col("fix_lane").cast(pl.Utf8).str.len_chars() > 2)
                        .then(pl.col("fix_lane"))  # Use full fix_lane if length > 2
                        .when(pl.col(lane_col).cast(pl.Utf8).str.len_chars() > 1)
                        .then(
                            pl.col(lane_col).cast(pl.Utf8).str.slice(0, 1) +
                            pl.col("fix_lane").cast(pl.Utf8) +
                            pl.col(lane_col).cast(pl.Utf8).str.slice(2)
                        )  # Replace middle character
                        .otherwise(pl.col("fix_lane"))  # Use fix_lane as is
                    )
                    .otherwise(pl.col(lane_col))  # Keep original if no fix
                    .alias(f"{lane_col}_updated"),

                    # Update ignore flag - only update where we have matches
                    pl.when(pl.col("fix_ignore").is_not_null())
                    .then(pl.col("fix_ignore"))
                    .otherwise(pl.col("Ignore"))  # Keep original if no fix
                    .alias("Ignore_updated")
                ])

                # Apply updates to the dataframe
                combined_lmd_final = updated_lmd.with_columns([
                    # Update Lane column
                    pl.col(f"{lane_col}_updated").alias(lane_col),

                    # Update Ignore column
                    pl.col("Ignore_updated").alias("Ignore")
                ])

                logger.info(f"Lane update completed: {len(matched_fixes)} records updated out of {len(combined_lmd)} total")

                # Remove temporary columns and row index
                return combined_lmd_final.drop(["row_idx", "fix_lane", "fix_ignore", f"{lane_col}_updated", "Ignore_updated"])
            else:
                logger.info("No lane fixes applied - no matching records found")
                return combined_lmd_indexed.drop(["row_idx"])
            
        except Exception as e:
            logger.error(f"Lane update failed: {e}")
            # Return original data if Polars fails
            logger.warning("Returning original data without lane updates due to processing error")
            return combined_lmd
    
    def _update_lanes_polars_optimized(self, lane_fixes: pl.DataFrame,
                                     combined_lmd: pl.DataFrame) -> pl.DataFrame:
        """Update lane information using optimized vectorized operations."""
        try:
            logger.info(f"Starting optimized lane update for {len(combined_lmd)} records")
            self._emit_progress("Preparing lane fixes data...")

            # Find the correct Lane column variant
            lane_variants = self.config.COLUMN_MAPPINGS.get('Lane', ['Lane'])
            lane_col = None
            for variant in lane_variants:
                if variant in combined_lmd.columns:
                    lane_col = variant
                    break

            if not lane_col:
                logger.error("No Lane column found in combined LMD data")
                return combined_lmd

            logger.info(f"Using Lane column: '{lane_col}'")

            # Prepare lane fixes data - sort by start time for efficient lookup
            lane_fixes_prepared = lane_fixes.with_columns([
                pl.when(pl.col("Ignore").is_null())
                .then(False)
                .otherwise(pl.col("Ignore"))
                .alias("Ignore")
            ]).sort("From_ts")

            # Convert to lists for faster processing
            fix_starts = lane_fixes_prepared["From_ts"].to_list()
            fix_ends = lane_fixes_prepared["To_ts"].to_list()
            fix_lanes = lane_fixes_prepared["Lane"].to_list()
            fix_ignores = lane_fixes_prepared["Ignore"].to_list()

            logger.info(f"Fix starts sample: {fix_starts[:3]}")
            logger.info(f"Fix ends sample: {fix_ends[:3]}")
            logger.info(f"Fix timestamps range: {min(fix_starts)} to {max(fix_ends)}")

            # Add TestDateUTC_ts column temporarily for processing
            # Check if already processed by _process_timestamps_polars
            if 'TestDateUTC_ts' in combined_lmd.columns:
                # Already processed, use existing timestamps
                temp_df = combined_lmd
                logger.info(f"Using pre-processed TestDateUTC_ts column")
            elif combined_lmd['TestDateUTC'].dtype in [pl.Datetime, pl.Datetime('ns'), pl.Datetime('us')]:
                # Already datetime, just convert to timestamp
                temp_df = combined_lmd.with_columns([
                    pl.col('TestDateUTC').dt.timestamp().alias('TestDateUTC_ts')
                ])
            else:
                # Parse from string format
                temp_df = combined_lmd.with_columns([
                    pl.col('TestDateUTC').str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.3fZ", strict=False)
                    .dt.timestamp()
                    .alias('TestDateUTC_ts')
                ])

            logger.info(f"LMD TestDateUTC_ts sample: {temp_df['TestDateUTC_ts'].head(3).to_list()}")

            # Get LMD timestamps (already in seconds from previous processing)
            lmd_timestamps = temp_df["TestDateUTC_ts"].to_list()
            logger.info(f"LMD timestamps (seconds) sample: {lmd_timestamps[:3]}")
            logger.info(f"LMD timestamps range: {min(lmd_timestamps)} to {max(lmd_timestamps)}")
            lmd_lanes = temp_df[lane_col].to_list()

            # Initialize result arrays
            updated_lanes = lmd_lanes.copy()
            updated_ignores = [False] * len(lmd_timestamps)

            # Use binary search for efficient range matching
            self._emit_progress("Applying lane fixes using optimized algorithm...")

            total_records = len(lmd_timestamps)
            matches_found = 0
            updates_made = 0

            for i, ts in enumerate(lmd_timestamps):
                if i % 10000 == 0:  # Update progress every 10k records
                    progress = (i / total_records) * 100
                    self._emit_progress(f"Processing records: {progress:.1f}% ({i:,}/{total_records:,})", progress)

                # Binary search to find potential matches
                # Find the rightmost fix where From_ts <= ts
                left, right = 0, len(fix_starts) - 1
                match_idx = -1

                while left <= right:
                    mid = (left + right) // 2
                    if fix_starts[mid] <= ts:
                        match_idx = mid
                        left = mid + 1
                    else:
                        right = mid - 1

                # Check if this fix applies (ts is within the range)
                if match_idx >= 0 and ts <= fix_ends[match_idx]:
                    matches_found += 1
                    lane_fix_value = str(fix_lanes[match_idx])

                    # Skip if lane value is -1
                    if lane_fix_value == '-1':
                        continue

                    current_lane = str(lmd_lanes[i])

                    # Update lane value based on length
                    if len(lane_fix_value) > 2:
                        updated_lanes[i] = lane_fix_value
                        updates_made += 1
                    else:
                        if len(current_lane) > 1:
                            new_lane = current_lane[0] + lane_fix_value + current_lane[2:]
                            updated_lanes[i] = new_lane
                            updates_made += 1
                        else:
                            updated_lanes[i] = lane_fix_value
                            updates_made += 1

                    # Update ignore flag
                    updated_ignores[i] = fix_ignores[match_idx]

            # Create updated dataframe while preserving column order
            # Use select() to maintain exact column order from original dataframe
            select_exprs = []
            ignore_column_exists = "Ignore" in combined_lmd.columns
            
            for col in combined_lmd.columns:
                if col == lane_col:
                    select_exprs.append(pl.Series(lane_col, updated_lanes).alias(col))
                elif col == "Ignore":
                    select_exprs.append(pl.Series("Ignore", updated_ignores).alias(col))
                else:
                    select_exprs.append(pl.col(col))
            
            # If Ignore column doesn't exist in original, add it at the end
            if not ignore_column_exists:
                select_exprs.append(pl.Series("Ignore", updated_ignores).alias("Ignore"))

            result_df = combined_lmd.select(select_exprs)

            logger.info(f"Optimized lane update completed: {matches_found:,} matches, {updates_made:,} updates")
            self._emit_progress(f"Lane update completed: {matches_found:,} matches, {updates_made:,} updates")

            return result_df

        except Exception as e:
            logger.error(f"Optimized lane update failed: {e}")
            # Return original data if optimized method fails
            logger.warning("Returning original data without lane updates due to processing error")
            return combined_lmd


class PolarsWorkbriefProcessor(PolarsDataProcessor):
    """Handles workbrief processing operations using Polars."""
    
    def process_in_memory(self, input_df: pl.DataFrame, workbrief_path: str) -> Optional[pl.DataFrame]:
        """
        Process workbrief data in memory using pure Polars.
        
        Args:
            input_df: Input Polars DataFrame
            workbrief_path: Path to workbrief CSV file
            
        Returns:
            Processed Polars DataFrame if successful, None otherwise
        """
        try:
            self._emit_progress("Starting workbrief processing...")
            
            # Validate workbrief file
            if not self._validate_file_exists(workbrief_path):
                return None
            
            # Load workbrief file
            self._emit_progress("Loading workbrief file...")
            workbrief_df = self._load_workbrief_file_polars(workbrief_path)
            if workbrief_df is None:
                return None
            
            # Process workbrief data using Polars
            self._emit_progress("Processing workbrief data...")
            result_df = self._process_workbrief_data_polars(input_df, workbrief_df)
            
            # Note: Deduplication removed - will be handled at final stage to preserve input count
            logger.info(f"Workbrief processing complete: {len(result_df):,} rows")
            
            self._emit_progress(Messages.SUCCESS_WORKBRIEF)
            return result_df
            
        except Exception as e:
            error_msg = Messages.ERROR_PROCESSING.format(str(e))
            logger.error(f"{error_msg}\\n{e}", exc_info=True)
            self._emit_progress(error_msg)
            return None
    
    def _load_workbrief_file_polars(self, file_path: str) -> Optional[pl.DataFrame]:
        """Load and validate workbrief file using pure Polars."""
        try:
            # Read CSV with Polars - don't try to parse dates automatically
            df = pl.read_csv(
                file_path, 
                try_parse_dates=False,  # Disable automatic date parsing
                infer_schema_length=0,  # Read all as strings initially
                null_values=["", "NULL", "null", "NA"]
            )
            
            self._emit_progress(Messages.INFO_FILE_LOADED.format(len(df)))
            return df
            
        except Exception as e:
            error_msg = f"Failed to load workbrief file: {str(e)}"
            # Clean up unicode characters that might cause logging issues
            error_msg = error_msg.encode('ascii', 'ignore').decode('ascii')
            logger.error(error_msg)
            self._emit_progress(error_msg)
            return None
    
    def _process_workbrief_data_polars(self, input_df: pl.DataFrame, 
                                     workbrief_df: pl.DataFrame) -> pl.DataFrame:
        """Process workbrief data using pure Polars operations."""
        logger.info("Processing workbrief data using Polars...")
        
        # Store original column order
        original_columns = input_df.columns
        logger.info(f"Workbrief processing - original columns: {len(original_columns)}, has Ignore: {'Ignore' in original_columns}")
        
        # Ensure InBrief column exists and is in the right position
        result_df = input_df
        if 'InBrief' not in result_df.columns:
            logger.warning("Column 'InBrief' does not exist. Adding 'InBrief' column with False values.")
            # Add InBrief column while preserving order
            select_exprs = []
            for col in original_columns:
                select_exprs.append(pl.col(col))
            select_exprs.append(pl.lit(False).alias('InBrief'))
            result_df = result_df.select(select_exprs)
            original_columns = result_df.columns  # Update with new column order
        
        # Handle different chainage column variants
        start_chainage_variants = ['Start Chainage (km)', 'From Chainage', 'From', 'start_chainage', 'from_chainage']
        end_chainage_variants = ['End Chainage (km)', 'To Chainage', 'To', 'end_chainage', 'to_chainage']
        
        start_col = None
        end_col = None
        
        for variant in start_chainage_variants:
            if variant in workbrief_df.columns:
                start_col = variant
                break
                
        for variant in end_chainage_variants:
            if variant in workbrief_df.columns:
                end_col = variant
                break
        
        if not start_col or not end_col:
            logger.error(f"Required chainage columns not found. Available columns: {workbrief_df.columns}")
            return result_df
        
        # Process workbrief data with Polars  
        workbrief_processed = workbrief_df.with_columns([
            # Convert chainage from km to meters and round to nearest 10
            (((pl.col(start_col).cast(pl.Float64) * 1000) / 10).round(0) * 10).alias('start_chainage_m'),
            (((pl.col(end_col).cast(pl.Float64) * 1000) / 10).round(0) * 10).alias('end_chainage_m')
        ])
        
        # Find Road ID column variants
        road_id_variants = ['Road ID', 'RoadID', 'road_id', 'roadid', 'ROADID', 'Road_ID', 'road ID']
        workbrief_road_col = None
        input_road_col = None
        
        for variant in road_id_variants:
            if variant in workbrief_processed.columns and workbrief_road_col is None:
                workbrief_road_col = variant
            if variant in result_df.columns and input_road_col is None:
                input_road_col = variant
        
        if not workbrief_road_col or not input_road_col:
            logger.error("No Road ID column found in workbrief or input data")
            return result_df
        
        # Find input chainage column
        chainage_variants = ['Chainage', 'chainage', 'CHAINAGE', 'Location', 'location']
        input_chainage_col = None
        
        for variant in chainage_variants:
            if variant in result_df.columns:
                input_chainage_col = variant
                break
        
        if not input_chainage_col:
            logger.error("No Chainage column found in input data")
            return result_df
        
        logger.info(f"Using columns - Workbrief Road ID: '{workbrief_road_col}', Input Road ID: '{input_road_col}', Input Chainage: '{input_chainage_col}'")
        
        # Prepare input data with proper chainage conversion (temporarily for processing)
        result_processed = result_df.with_columns([
            # Convert chainage to meters if it's from 'location' column (PAS files)
            pl.when(pl.lit(input_chainage_col.lower() == 'location'))
            .then(pl.col(input_chainage_col).cast(pl.Float64) * 1000)
            .otherwise(pl.col(input_chainage_col).cast(pl.Float64))
            .alias('chainage_m'),
            
            # Ensure road ID is numeric for comparison
            pl.col(input_road_col).cast(pl.Float64).alias('road_id_numeric')
        ])
        
        # Prepare workbrief with numeric road ID
        workbrief_final = workbrief_processed.with_columns([
            pl.col(workbrief_road_col).cast(pl.Float64).alias('wb_road_id_numeric')
        ])
        
        # More efficient approach: Use vectorized operations instead of loops
        self._emit_progress("Processing workbrief ranges efficiently...")
        
        # Create a list of conditions for all workbrief ranges
        conditions = []
        for wb_row in workbrief_final.iter_rows(named=True):
            road_id = wb_row['wb_road_id_numeric'] 
            start_chainage = wb_row['start_chainage_m']
            end_chainage = wb_row['end_chainage_m']
            
            # Create condition for this range
            condition = (
                (pl.col('road_id_numeric') == road_id) &
                pl.col('chainage_m').is_between(start_chainage, end_chainage, closed='both')
            )
            conditions.append(condition)
        
        # Start with all rows as InBrief = False, then update based on conditions
        inbrief_values = [False] * len(result_processed)
        
        if conditions:
            # Combine all conditions with OR
            combined_condition = conditions[0]
            for condition in conditions[1:]:
                combined_condition = combined_condition | condition
            
            # Get boolean array for InBrief updates
            inbrief_series = result_processed.select([
                pl.when(combined_condition)
                .then(True)
                .otherwise(False)
                .alias('InBrief')
            ])['InBrief']
            
            inbrief_values = inbrief_series.to_list()
        
        # Create final dataframe with preserved column order
        select_exprs = []
        for col in original_columns:
            if col == 'InBrief':
                select_exprs.append(pl.Series('InBrief', inbrief_values).alias(col))
            else:
                select_exprs.append(pl.col(col))
        
        final_df = result_processed.select(select_exprs)
            
        final_df = self._standardize_boolean_columns(final_df, ['InBrief'])
        
        # Format TestDateUTC back to original string format if it exists and is datetime
        if 'TestDateUTC' in final_df.columns:
            if final_df['TestDateUTC'].dtype in [pl.Datetime, pl.Datetime('ns'), pl.Datetime('us')]:
                # Preserve column order when updating TestDateUTC
                select_exprs = []
                for col in final_df.columns:
                    if col == 'TestDateUTC':
                        select_exprs.append(
                            pl.col('TestDateUTC').dt.strftime('%d/%m/%Y %H:%M:%S%.3f').alias('TestDateUTC')
                        )
                    else:
                        select_exprs.append(pl.col(col))
                final_df = final_df.select(select_exprs)
        
        matches_found = final_df.filter(pl.col('InBrief') == True).height
        logger.info(f"Workbrief processing completed using Polars. Matches found: {matches_found}")
        
        return final_df


class PolarsCombinedProcessor(PolarsDataProcessor):
    """Handles combined lane fixing and workbrief processing using Polars."""

    def __init__(self, progress_callback: Optional[Callable] = None, chunk_size: int = 50000):
        """
        Initialize combined processor with chunked processing support.

        Args:
            progress_callback: Optional callback function for progress updates
            chunk_size: Number of rows to process at once for large files
        """
        super().__init__(progress_callback)
        self.chunk_size = chunk_size

    def process(self, combined_lmd_path: str, lane_fixes_path: str, workbrief_path: str) -> Optional[str]:
        """
        Process complete workflow using pure Polars operations with chunked processing for large files.

        Args:
            combined_lmd_path: Path to combined LMD CSV file
            lane_fixes_path: Path to lane fixes CSV file
            workbrief_path: Path to workbrief CSV file

        Returns:
            Output file path if successful, None otherwise
        """
        try:
            self._emit_progress("Starting complete processing workflow...")

            # Check file sizes for memory warnings and processing strategy
            import os
            lmd_size = os.path.getsize(combined_lmd_path) / (1024 * 1024)  # MB
            if lmd_size > 1000:  # Use chunked processing for files over 1GB
                self._emit_progress(f"🔄 Large file detected ({lmd_size:.1f}MB) - using optimized chunked processing")
                return self._process_chunked(combined_lmd_path, lane_fixes_path, workbrief_path)
            elif lmd_size > 500:  # Warn for files over 500MB
                self._emit_progress(f"⚠️ Large file detected ({lmd_size:.1f}MB) - processing may take longer")

            # Use standard processing for smaller files
            return self._process_standard(combined_lmd_path, lane_fixes_path, workbrief_path)

        except Exception as e:
            error_msg = Messages.ERROR_PROCESSING.format(str(e))
            logger.error(f"{error_msg}\n{e}", exc_info=True)
            self._emit_progress(error_msg)
            return None

    def _process_standard(self, combined_lmd_path: str, lane_fixes_path: str, workbrief_path: str) -> Optional[str]:
        """Standard processing for smaller files."""
        # Read original input to track row count
        self._emit_progress("Reading input file to track row count...")
        original_input = pl.read_csv(combined_lmd_path, ignore_errors=True, infer_schema_length=0)
        original_input_count = len(original_input)

        logger.info(f"Original input: {original_input_count:,} rows - MUST preserve this exact count")
        self._emit_progress(f"Input: {original_input_count:,} rows (exact count will be preserved)")

        # Step 1: Apply Lane Fixes to Combined LMD
        self._emit_progress("Step 1/2: Applying Lane Fixes to Combined LMD data...")
        lane_fix_processor = PolarsLaneFixProcessor(self.progress_callback)
        updated_lmd_data = lane_fix_processor.process_in_memory(lane_fixes_path, combined_lmd_path)

        if updated_lmd_data is None:
            self._emit_progress("ERROR: Lane fixes processing failed")
            return None

        self._emit_progress("Lane fixes completed successfully")

        # Step 2: Apply Workbrief Processing
        self._emit_progress("Step 2/2: Processing with Workbrief data...")
        workbrief_processor = PolarsWorkbriefProcessor(self.progress_callback)
        final_data = workbrief_processor.process_in_memory(updated_lmd_data, workbrief_path)

        if final_data is None:
            self._emit_progress("ERROR: Workbrief processing failed")
            return None

        # Ensure output has exactly same row count as original input
        self._emit_progress("Ensuring exact input row count preservation...")
        final_data = self._preserve_input_row_count(final_data, original_input_count, "final validation")

        # Save final result
        output_path = self.config.get_output_filename(combined_lmd_path, 'complete')
        self._save_to_csv_with_proper_formatting(final_data, output_path)

        final_count = len(final_data)
        logger.info(f"Output: {final_count:,} rows (exact 1.00x ratio preserved)")
        self._emit_progress(f"✅ Complete: {final_count:,} rows (exact input count preserved)")
        self._emit_progress(f"Final output: {output_path}")

        return output_path

    def _process_chunked(self, combined_lmd_path: str, lane_fixes_path: str, workbrief_path: str) -> Optional[str]:
        """Chunked processing for very large files."""
        try:
            self._emit_progress("🔄 Starting chunked processing for large file...")

            # Get EXACT row count instead of estimating
            self._emit_progress("Counting exact number of rows in input file...")
            exact_total_lines = self._count_exact_rows(combined_lmd_path)
            # Data rows = total lines - 1 (header)
            exact_data_rows = exact_total_lines - 1
            num_chunks = (exact_data_rows + self.chunk_size - 1) // self.chunk_size  # Ceiling division

            logger.info(f"Exact total lines: {exact_total_lines:,}, data rows: {exact_data_rows:,}, chunks: {num_chunks}")

            # Load lane fixes and workbrief data once (these are usually small)
            self._emit_progress("Loading lane fixes and workbrief data...")
            lane_fix_processor = PolarsLaneFixProcessor(self.progress_callback)
            workbrief_processor = PolarsWorkbriefProcessor(self.progress_callback)

            # Load lane fixes
            lane_fixes_df = pl.read_csv(lane_fixes_path, ignore_errors=True, infer_schema_length=0)
            if lane_fixes_df is None or len(lane_fixes_df) == 0:
                self._emit_progress("ERROR: Failed to load lane fixes data")
                return None

            # Load a small sample of combined LMD data to determine timestamp format
            sample_lmd_df = pl.read_csv(
                combined_lmd_path,
                n_rows=100,  # Just need a small sample for timestamp format detection
                ignore_errors=True,
                infer_schema_length=0
            )
            if sample_lmd_df is None or len(sample_lmd_df) == 0:
                self._emit_progress("ERROR: Failed to load sample LMD data")
                return None

            # Preprocess lane fixes data (add timestamps) using sample data for format detection
            lane_fix_processor_temp = PolarsLaneFixProcessor(self.progress_callback)
            lane_fixes_processed, _ = lane_fix_processor_temp._process_timestamps_polars(lane_fixes_df, sample_lmd_df)
            if lane_fixes_processed is None:
                self._emit_progress("ERROR: Failed to preprocess lane fixes timestamps")
                return None
            lane_fixes_df = lane_fixes_processed

            # Load workbrief
            workbrief_df = pl.read_csv(workbrief_path, ignore_errors=True, infer_schema_length=0)
            if workbrief_df is None:
                workbrief_df = pl.DataFrame()  # Empty dataframe if workbrief fails

            # Process in chunks
            output_chunks = []
            total_processed = 0

            for chunk_idx in range(num_chunks):
                chunk_start = chunk_idx * self.chunk_size
                chunk_end = min((chunk_idx + 1) * self.chunk_size, exact_data_rows)

                self._emit_progress(f"Processing chunk {chunk_idx + 1}/{num_chunks} (rows {chunk_start:,}-{chunk_end:,})...")

                # Read chunk of LMD data with proper column names
                if chunk_idx == 0:
                    # First chunk: read with headers to get column names
                    chunk_df = pl.read_csv(
                        combined_lmd_path,
                        n_rows=self.chunk_size,
                        ignore_errors=True,
                        infer_schema_length=0
                    )
                    # Store original column names and order
                    self._original_columns = chunk_df.columns
                else:
                    # Subsequent chunks: read without headers and assign original column names
                    chunk_df = pl.read_csv(
                        combined_lmd_path,
                        skip_rows=chunk_start + 1,  # +1 for header
                        n_rows=self.chunk_size,
                        has_header=False,
                        ignore_errors=True,
                        new_columns=self._original_columns,  # Use new_columns parameter to set column names
                        infer_schema_length=0
                    )

                if len(chunk_df) == 0:
                    break

                # Apply lane fixes to chunk
                updated_chunk = self._apply_lane_fixes_to_chunk(chunk_df, lane_fixes_df)
                if updated_chunk is None:
                    continue

                # Apply workbrief to chunk
                final_chunk = self._apply_workbrief_to_chunk(updated_chunk, workbrief_df)
                if final_chunk is None:
                    continue

                output_chunks.append(final_chunk)
                total_processed += len(final_chunk)

                # Update progress
                progress = ((chunk_idx + 1) / num_chunks) * 100
                self._emit_progress(f"Chunk {chunk_idx + 1}/{num_chunks} completed - Total processed: {total_processed:,}", progress)

            if not output_chunks:
                self._emit_progress("ERROR: No data chunks were processed successfully")
                return None

            # Combine all chunks
            self._emit_progress("Combining processed chunks...")
            try:
                final_data = pl.concat(output_chunks, how="vertical_relaxed")
                logger.info(f"Concat successful - final_data columns: {len(final_data.columns)}, has Ignore: {'Ignore' in final_data.columns}")
            except Exception as e:
                logger.warning(f"Concat with vertical_relaxed failed: {e}, trying diagonal concat...")
                # Fallback: use diagonal concat which handles schema differences better
                final_data = pl.concat(output_chunks, how="diagonal")
                logger.info(f"Diagonal concat - final_data columns: {len(final_data.columns)}, has Ignore: {'Ignore' in final_data.columns}")

            # Save final result
            output_path = self.config.get_output_filename(combined_lmd_path, 'complete_chunked')
            self._save_to_csv_with_proper_formatting(final_data, output_path)

            logger.info(f"Chunked processing complete: {len(final_data):,} rows")
            self._emit_progress(f"✅ Chunked processing complete: {len(final_data):,} rows")
            self._emit_progress(f"Final output: {output_path}")

            return output_path

        except Exception as e:
            error_msg = f"Chunked processing failed: {str(e)}"
            logger.error(f"{error_msg}\n{e}", exc_info=True)
            self._emit_progress(error_msg)
            return None

    def _count_exact_rows(self, file_path: str) -> int:
        """Count exact number of rows in a CSV file."""
        try:
            with open(file_path, 'rb') as f:
                return sum(1 for _ in f)
        except Exception as e:
            logger.error(f"Failed to count rows in {file_path}: {e}")
            return 0

            logger.info(f"Estimated {estimated_rows:,} rows based on sample of {lines_sampled:,} lines ({avg_row_size:.1f} bytes/row)")
            return max(estimated_rows, 1000)  # Minimum estimate

        except Exception as e:
            logger.warning(f"Could not estimate row count: {e}, using default chunk size")
            return 100000  # Default estimate

    def _apply_lane_fixes_to_chunk(self, chunk_df: pl.DataFrame, lane_fixes_df: pl.DataFrame) -> Optional[pl.DataFrame]:
        """Apply lane fixes to a data chunk."""
        try:
            # Create a temporary lane fix processor to apply fixes to this chunk
            lane_processor = PolarsLaneFixProcessor(self.progress_callback)

            # Apply the optimized lane update algorithm for the chunk
            # Pass the dataframe directly without adding temporary columns
            updated_chunk = lane_processor._update_lanes_polars_optimized(lane_fixes_df, chunk_df)

            return updated_chunk
        except Exception as e:
            logger.error(f"Failed to apply lane fixes to chunk: {e}")
            return None

    def _apply_workbrief_to_chunk(self, chunk_df: pl.DataFrame, workbrief_df: pl.DataFrame) -> Optional[pl.DataFrame]:
        """Apply workbrief processing to a data chunk."""
        try:
            # Create a temporary workbrief processor to apply workbrief logic to this chunk
            workbrief_processor = PolarsWorkbriefProcessor(self.progress_callback)
            
            # Apply workbrief processing to this chunk
            result_df = workbrief_processor._process_workbrief_data_polars(chunk_df, workbrief_df)
            
            return result_df
        except Exception as e:
            logger.error(f"Failed to apply workbrief to chunk: {e}")
            return None