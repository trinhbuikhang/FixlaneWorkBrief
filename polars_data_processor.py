"""
Data processing module for Fixlane WorkBrief application - Polars Optimized Version.
Provides classes and methods for lane fixing and workbrief processing operations using Polars.
"""

import polars as pl
import os
import logging
from pathlib import Path
from typing import Optional, Tuple, Callable
from datetime import datetime

from config import Config, Messages
from timestamp_handler import timestamp_handler


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
        from config import Config
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
        # Convert boolean columns to True/False strings (with capital T/F)
        boolean_cols = [col for col in df.columns if df[col].dtype == pl.Boolean]
        
        if boolean_cols:
            expressions = []
            for col in boolean_cols:
                expr = (
                    pl.when(pl.col(col).is_null())
                    .then(pl.lit(None))  # Keep null as empty in CSV
                    .when(pl.col(col))
                    .then(pl.lit("True"))  # Capital T
                    .otherwise(pl.lit("False"))  # Capital F
                    .alias(col)
                )
                expressions.append(expr)
            
            # Add all non-boolean columns as-is
            for col in df.columns:
                if col not in boolean_cols:
                    expressions.append(pl.col(col))
            
            df = df.with_columns(expressions)
        
        return df
    
    def _save_to_csv_with_proper_formatting(self, df: pl.DataFrame, output_path: str) -> str:
        """Save DataFrame to CSV with proper boolean and null formatting."""
        # Prepare data for output
        df_output = self._prepare_for_csv_output(df)
        
        # Write to CSV with null values as empty strings
        df_output.write_csv(
            output_path,
            null_value="",  # Empty string for null values
            quote_char='"',
            separator=','
        )
        
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
            
            # Clean up temporary columns and format TestDateUTC back to original string format
            if 'TestDateUTC_ts' in updated_lmd.columns:
                updated_lmd = updated_lmd.drop('TestDateUTC_ts')
            
            # Format TestDateUTC back to original string format if it's datetime
            if 'TestDateUTC' in updated_lmd.columns:
                if updated_lmd['TestDateUTC'].dtype in [pl.Datetime, pl.Datetime('ns'), pl.Datetime('us')]:
                    updated_lmd = updated_lmd.with_columns([
                        pl.col('TestDateUTC').dt.strftime('%d/%m/%Y %H:%M:%S%.3f').alias('TestDateUTC')
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
            logger.info(f"Starting lane update for {len(combined_lmd)} records using Polars")
            
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
            lane_fixes_prepared = lane_fixes.with_columns([
                # Ensure boolean Ignore column is properly handled
                pl.when(pl.col("Ignore").is_null())
                .then(False)
                .otherwise(pl.col("Ignore"))
                .alias("Ignore")
            ])
            
            # Add row index to combined_lmd for tracking
            combined_lmd_indexed = combined_lmd.with_row_index("row_idx")
            
            # Create Ignore column if it doesn't exist
            if 'Ignore' not in combined_lmd_indexed.columns:
                combined_lmd_indexed = combined_lmd_indexed.with_columns([
                    pl.lit(False).alias('Ignore')
                ])
            
            # Sort data for join_asof operation
            lane_fixes_sorted = lane_fixes_prepared.select([
                "From_ts", "To_ts", "Lane", "Ignore"
            ]).rename({
                "From_ts": "ts_start", 
                "To_ts": "ts_end", 
                "Lane": "fix_lane", 
                "Ignore": "fix_ignore"
            }).sort("ts_start")
            
            combined_lmd_sorted = combined_lmd_indexed.sort("TestDateUTC_ts")
            
            # Use join_asof for efficient timestamp-based joins
            # This finds the lane fix that applies to each LMD record
            updated_lmd = combined_lmd_sorted.join_asof(
                lane_fixes_sorted,
                left_on="TestDateUTC_ts",
                right_on="ts_start",
                strategy="backward"
            ).filter(
                # Only keep matches where timestamp is within the range
                pl.col("TestDateUTC_ts").is_between(pl.col("ts_start"), pl.col("ts_end"), closed="both")
            )
            
            # Apply lane updates using Polars expressions
            if len(updated_lmd) > 0:
                # Create the updated lane values
                updated_lmd = updated_lmd.with_columns([
                    # Update lane based on fix_lane length and current lane value
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
                    .alias(f"{lane_col}_updated"),
                    
                    # Update ignore flag
                    pl.col("fix_ignore").alias("Ignore_updated")
                ])
                
                # Get the row indices of updated records
                updated_indices = updated_lmd.select("row_idx")["row_idx"].to_list()
                
                # Apply updates to the original dataframe
                combined_lmd_final = combined_lmd_indexed.with_columns([
                    # Update Lane column
                    pl.when(pl.col("row_idx").is_in(updated_indices))
                    .then(
                        updated_lmd.join(
                            combined_lmd_indexed.select(["row_idx"]),
                            on="row_idx",
                            how="inner"
                        )[f"{lane_col}_updated"]
                    )
                    .otherwise(pl.col(lane_col))
                    .alias(lane_col),
                    
                    # Update Ignore column  
                    pl.when(pl.col("row_idx").is_in(updated_indices))
                    .then(
                        updated_lmd.join(
                            combined_lmd_indexed.select(["row_idx"]),
                            on="row_idx", 
                            how="inner"
                        )["Ignore_updated"]
                    )
                    .otherwise(pl.col("Ignore"))
                    .alias("Ignore")
                ])
                
                logger.info(f"Lane update completed: {len(updated_lmd)} records updated")
                
                # Remove temporary columns and row index
                return combined_lmd_final.drop(["row_idx"])
            else:
                logger.info("No lane fixes applied - no matching records found")
                return combined_lmd_indexed.drop(["row_idx"])
            
        except Exception as e:
            logger.error(f"Lane update failed: {e}")
            # Fallback to pandas-based approach if Polars fails
            return self._update_lanes_fallback_pandas(lane_fixes, combined_lmd)
    
    def _update_lanes_fallback_pandas(self, lane_fixes: pl.DataFrame, 
                                    combined_lmd: pl.DataFrame) -> pl.DataFrame:
        """Fallback to pandas-based lane update if Polars approach fails."""
        logger.warning("Using pandas fallback for lane updates")
        
        # Convert to pandas
        lane_fixes_pd = lane_fixes.to_pandas()
        combined_lmd_pd = combined_lmd.to_pandas()
        
        logger.info(f"Starting lane update for {len(combined_lmd_pd)} records")
        
        # Find the correct Lane column variant
        lane_variants = self.config.COLUMN_MAPPINGS.get('Lane', ['Lane'])
        lane_col = None
        for variant in lane_variants:
            if variant in combined_lmd_pd.columns:
                lane_col = variant
                break
        
        if not lane_col:
            logger.error("No Lane column found in combined LMD data")
            return pl.from_pandas(combined_lmd_pd)
        
        # Fill NA values in Ignore column
        lane_fixes_pd['Ignore'] = lane_fixes_pd['Ignore'].fillna(False)
        
        matches_found = 0
        updates_made = 0
        
        for idx, row in combined_lmd_pd.iterrows():
            # Update progress periodically
            if idx % self.config.PROGRESS_UPDATE_INTERVAL == 0:
                progress = (idx / len(combined_lmd_pd)) * 100
                self._emit_progress(f"Processing records: {progress:.1f}%", progress)
            
            test_date_ts = row['TestDateUTC_ts']
            
            # Find matching lane fixes
            matching_rows = lane_fixes_pd[
                (lane_fixes_pd['From_ts'] <= test_date_ts) & 
                (lane_fixes_pd['To_ts'] >= test_date_ts)
            ]
            
            if not matching_rows.empty:
                matches_found += 1
                lane_fix_row = matching_rows.iloc[0]
                lane_value_fixes = str(lane_fix_row['Lane'])
                
                # Skip if lane value is -1
                if lane_value_fixes == '-1':
                    continue
                
                lane_value_current = str(row[lane_col])
                
                # Update lane value based on length
                if len(lane_value_fixes) > 2:
                    combined_lmd_pd.at[idx, lane_col] = lane_value_fixes
                    updates_made += 1
                else:
                    if len(lane_value_current) > 1:
                        new_lane = (lane_value_current[0] + lane_value_fixes + 
                                   lane_value_current[2:])
                        combined_lmd_pd.at[idx, lane_col] = new_lane
                        updates_made += 1
                    else:
                        combined_lmd_pd.at[idx, lane_col] = lane_value_fixes
                        updates_made += 1
                
                # Update ignore flag
                combined_lmd_pd.at[idx, 'Ignore'] = lane_fix_row['Ignore']
        
        logger.info(f"Lane update completed: {matches_found} matches, {updates_made} updates")
        
        # Format TestDateUTC back to original string format if it's datetime
        if 'TestDateUTC' in combined_lmd_pd.columns:
            if hasattr(combined_lmd_pd['TestDateUTC'], 'dt'):
                combined_lmd_pd['TestDateUTC'] = (
                    combined_lmd_pd['TestDateUTC']
                    .dt.strftime('%d/%m/%Y %H:%M:%S.%f')
                    .str[:-3]  # Remove last 3 digits to keep milliseconds
                )
        
        return pl.from_pandas(combined_lmd_pd)


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
        
        # Ensure InBrief column exists
        result_df = input_df
        if 'InBrief' not in result_df.columns:
            logger.warning("Column 'InBrief' does not exist. Adding 'InBrief' column with False values.")
            result_df = result_df.with_columns([pl.lit(False).alias('InBrief')])
        
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
        
        # Prepare input data with proper chainage conversion
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
        
        # Simple approach: Update InBrief column directly without joins
        # This preserves exact 1:1 relationship with input rows
        
        # Start with all rows as InBrief = False
        matched_df = result_processed.with_columns([
            pl.lit(False).alias('InBrief')
        ])
        
        # For each workbrief range, update matching rows to InBrief = True
        for wb_row in workbrief_final.iter_rows(named=True):
            road_id = wb_row['wb_road_id_numeric'] 
            start_chainage = wb_row['start_chainage_m']
            end_chainage = wb_row['end_chainage_m']
            
            # Update InBrief for matching rows
            matched_df = matched_df.with_columns([
                pl.when(
                    (pl.col('road_id_numeric') == road_id) &
                    pl.col('chainage_m').is_between(start_chainage, end_chainage, closed='both')
                ).then(True)
                .otherwise(pl.col('InBrief'))
                .alias('InBrief')
            ])
            
        logger.info(f"Updated InBrief flags for {len(workbrief_final)} workbrief ranges")
        
        # Clean up temporary columns
        cols_to_drop = ['chainage_m', 'road_id_numeric']
        final_df = matched_df.drop([col for col in cols_to_drop if col in matched_df.columns])
            
        final_df = self._standardize_boolean_columns(final_df, ['InBrief'])
        
        # Format TestDateUTC back to original string format if it exists and is datetime
        if 'TestDateUTC' in final_df.columns:
            if final_df['TestDateUTC'].dtype in [pl.Datetime, pl.Datetime('ns'), pl.Datetime('us')]:
                final_df = final_df.with_columns([
                    pl.col('TestDateUTC').dt.strftime('%d/%m/%Y %H:%M:%S%.3f').alias('TestDateUTC')
                ])
        
        matches_found = final_df.filter(pl.col('InBrief') == True).height
        logger.info(f"Workbrief processing completed using Polars. Matches found: {matches_found}")
        
        return final_df


class PolarsCombinedProcessor(PolarsDataProcessor):
    """Handles combined lane fixing and workbrief processing using Polars."""
    
    def process(self, combined_lmd_path: str, lane_fixes_path: str, workbrief_path: str) -> Optional[str]:
        """
        Process complete workflow using pure Polars operations.
        
        Args:
            combined_lmd_path: Path to combined LMD CSV file
            lane_fixes_path: Path to lane fixes CSV file
            workbrief_path: Path to workbrief CSV file
            
        Returns:
            Output file path if successful, None otherwise
        """
        try:
            self._emit_progress("Starting complete processing workflow...")
            
            # Read original input to track row count
            self._emit_progress("Reading input file to track row count...")
            original_input = pl.read_csv(combined_lmd_path, ignore_errors=True)
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
            
        except Exception as e:
            error_msg = Messages.ERROR_PROCESSING.format(str(e))
            logger.error(f"{error_msg}\n{e}", exc_info=True)
            self._emit_progress(error_msg)
            return None