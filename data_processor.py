"""
Data processing module for Fixlane WorkBrief application.
Provides classes and methods for lane fixing and workbrief processing operations.
"""

import polars as pl
import pandas as pd
import os
import logging
from pathlib import Path
from typing import Optional, Tuple, Callable
from datetime import datetime

from config import Config, Messages
from timestamp_handler import timestamp_handler

# Import Polars-optimized processors
from polars_data_processor import (
    PolarsLaneFixProcessor,
    PolarsWorkbriefProcessor, 
    PolarsCombinedProcessor
)


logger = logging.getLogger(__name__)


class DataProcessor:
    """Base class for data processing operations."""
    
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
    
    def _validate_columns(self, df: pd.DataFrame, required_columns: list, file_name: str) -> bool:
        """Validate that required columns exist in dataframe."""
        logger.info(f"Validating columns for {file_name}")
        logger.info(f"Available columns: {list(df.columns)}")
        logger.info(f"Required columns: {required_columns}")
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            error_msg = f"Missing columns in {file_name}: {missing_columns}"
            logger.error(error_msg)
            self._emit_progress(error_msg)
            return False
        return True
    
    def _validate_columns_flexible(self, df: pd.DataFrame, required_columns: list, file_name: str) -> bool:
        """Validate that required columns exist using flexible column mapping."""
        logger.info(f"Validating columns for {file_name}")
        logger.info(f"Available columns: {list(df.columns)}")
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

    def _validate_columns_polars(self, df: pl.DataFrame, required_columns: list, file_name: str) -> bool:
        """Validate that required columns exist in Polars DataFrame."""
        logger.info(f"Validating columns for {file_name}")
        logger.info(f"Available columns: {list(df.columns)}")
        logger.info(f"Required columns: {required_columns}")
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            error_msg = f"Missing columns in {file_name}: {missing_columns}"
            logger.error(error_msg)
            self._emit_progress(error_msg)
            return False
        return True
    
    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names using config mappings."""
        logger.info("Standardizing column names")
        logger.info(f"Original columns: {list(df.columns)}")
        
        reverse_mapping = {
            col: std_col 
            for std_col, variants in self.config.COLUMN_MAPPINGS.items() 
            for col in variants
        }
        
        logger.info(f"Column mapping: {reverse_mapping}")
        old_columns = list(df.columns)
        df.columns = [reverse_mapping.get(col, col) for col in df.columns]
        
        logger.info(f"Standardized columns: {list(df.columns)}")
        
        # Log changes
        for old, new in zip(old_columns, df.columns):
            if old != new:
                logger.info(f"Column renamed: '{old}' -> '{new}'")
        
        return df
    
    def _find_column_variants(self, df: pd.DataFrame, target_variants: list) -> str:
        """Find which variant of a column exists in dataframe without modifying columns."""
        for variant in target_variants:
            if variant in df.columns:
                return variant
        return None
    
    def _detect_file_type(self, df: pd.DataFrame) -> str:
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
    
    def _remove_last_word(self, value) -> str:
        """Remove last word from string value (for RoadName cleanup)."""
        if isinstance(value, str) and ' ' in value:
            return value.rsplit(' ', 1)[0]
        return value


class LaneFixProcessor(DataProcessor):
    """Handles lane fixing operations."""
    
    def process(self, lane_fixes_path: str, combined_lmd_path: str) -> Optional[str]:
        """
        Process lane fixes and update combined LMD data using optimized Polars processor.
        
        Args:
            lane_fixes_path: Path to lane fixes CSV file
            combined_lmd_path: Path to combined LMD CSV file
            
        Returns:
            Output file path if successful, None otherwise
        """
        # Use Polars-optimized processor for better performance
        polars_processor = PolarsLaneFixProcessor(self.progress_callback)
        return polars_processor.process(lane_fixes_path, combined_lmd_path)
    
    def process_in_memory(self, lane_fixes_path: str, combined_lmd_path: str) -> Optional[pl.DataFrame]:
        """
        Process lane fixes and return updated LMD data in memory using Polars.
        
        Args:
            lane_fixes_path: Path to lane fixes CSV file
            combined_lmd_path: Path to combined LMD CSV file
            
        Returns:
            Updated Polars DataFrame if successful, None otherwise
        """
        # Use Polars-optimized processor for better performance
        polars_processor = PolarsLaneFixProcessor(self.progress_callback)
        return polars_processor.process_in_memory(lane_fixes_path, combined_lmd_path)
    
    def _load_lane_fixes(self, file_path: str) -> Optional[pl.DataFrame]:
        """Load and validate lane fixes file using Polars for faster loading."""
        try:
            # Use Polars for faster CSV loading
            df_pl = pl.read_csv(file_path, infer_schema_length=0)  # Read as strings for compatibility
            
            self._emit_progress(Messages.INFO_FILE_LOADED.format(len(df_pl)))
            
            logger.info(f"Loading lane fixes from: {file_path}")
            logger.info(f"File has {len(df_pl)} rows and {len(df_pl.columns)} columns")
            
            # Convert to pandas temporarily for file type detection (will optimize this later)
            df_pandas = df_pl.to_pandas()
            detected_type = self._detect_file_type(df_pandas)
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
            
            # Validate required columns using original column names
            required_cols = ['From', 'To', 'Lane', 'Ignore']  # Use original names
            if not self._validate_columns_polars(df_pl, required_cols, f"lane fixes file '{file_path}'"):
                return None
            
            return df_pl
            
        except Exception as e:
            logger.error(f"Failed to load lane fixes file: {e}")
            self._emit_progress(f"Failed to load lane fixes file: {e}")
            return None
    
    def _load_combined_lmd(self, file_path: str) -> Optional[pl.DataFrame]:
        """Load and validate combined LMD file using Polars for faster loading."""
        try:
            # Use Polars for faster CSV loading
            df_pl = pl.read_csv(file_path, infer_schema_length=0)  # Read as strings for compatibility
            
            self._emit_progress(Messages.INFO_FILE_LOADED.format(len(df_pl)))
            
            logger.info(f"Loading combined LMD from: {file_path}")
            logger.info(f"File has {len(df_pl)} rows and {len(df_pl.columns)} columns")
            
            # Convert to pandas temporarily for file type detection
            df_pandas = df_pl.to_pandas()
            detected_type = self._detect_file_type(df_pandas)
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
            
            # Validate required columns using column variants (will create polars version later)
            required_cols = self.config.REQUIRED_COLUMNS['combined_LMD']
            if not self._validate_columns_flexible(df_pandas, required_cols, f"combined LMD file '{file_path}'"):
                return None
            
            return df_pl
            
        except Exception as e:
            logger.error(f"Failed to load combined LMD file: {e}")
            self._emit_progress(f"Failed to load combined LMD file: {e}")
            return None
    
    def _process_timestamps(self, lane_fixes: pl.DataFrame, 
                          combined_lmd: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Process timestamps in both dataframes using Polars for better performance."""
        try:
            self._emit_progress("Processing timestamps...")
            
            # Convert to pandas temporarily for timestamp processing (will optimize later)
            # This is a gradual conversion - keeping existing timestamp logic for now
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
                # Find the correct RoadName column variant
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
    
    def _update_lanes(self, lane_fixes: pl.DataFrame, 
                     combined_lmd: pl.DataFrame) -> pl.DataFrame:
        """Update lane information in combined LMD data."""
        # Convert to pandas temporarily for existing logic (will vectorize later)
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
        
        logger.info(f"Using Lane column: '{lane_col}'")
        
        # Fill NA values in Ignore column
        lane_fixes_pd['Ignore'] = lane_fixes_pd['Ignore'].fillna('').apply(
            lambda x: True if str(x) == '1' else False
        )
        
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
        
        # Convert back to Polars
        return pl.from_pandas(combined_lmd_pd)
    
    def _save_results(self, combined_lmd: pd.DataFrame, original_path: str) -> str:
        """Save updated data to output file using Polars for better performance."""
        # Clean up data before saving
        combined_lmd.drop(columns=['TestDateUTC_ts'], inplace=True, errors='ignore')
        
        # Format timestamps back to standard format
        combined_lmd['TestDateUTC'] = (
            combined_lmd['TestDateUTC']
            .dt.strftime('%d/%m/%Y %H:%M:%S.%f')
            .str[:-3]
        )
        
        # Generate output path
        output_path = self.config.get_output_filename(original_path, 'fixlane')
        
        # Convert to Polars and save to CSV (faster than pandas)
        df_pl = pl.from_pandas(combined_lmd)
        df_pl.write_csv(output_path)
        logger.info(f"Results saved to: {output_path}")
        
        return output_path


class CombinedProcessor(DataProcessor):
    """Handles combined lane fixing and workbrief processing."""
    
    def process(self, combined_lmd_path: str, lane_fixes_path: str, workbrief_path: str) -> Optional[str]:
        """
        Process complete workflow using optimized Polars processor.
        
        Args:
            combined_lmd_path: Path to combined LMD CSV file
            lane_fixes_path: Path to lane fixes CSV file
            workbrief_path: Path to workbrief CSV file
            
        Returns:
            Output file path if successful, None otherwise
        """
        # Use Polars-optimized processor for better performance
        polars_processor = PolarsCombinedProcessor(self.progress_callback)
        return polars_processor.process(combined_lmd_path, lane_fixes_path, workbrief_path)


class WorkbriefProcessor(DataProcessor):
    """Handles workbrief processing operations."""
    
    def process(self, input_file_path: str, workbrief_path: str) -> Optional[str]:
        """
        Process workbrief data using Polars processor.
        
        Args:
            input_file_path: Path to input CSV file
            workbrief_path: Path to workbrief CSV file
            
        Returns:
            Output file path if successful, None otherwise
        """
        try:
            # Load input file using Polars
            input_df = pl.read_csv(input_file_path, try_parse_dates=True, null_values=["", "NULL", "null", "NA"])
            
            # Use Polars-optimized processor
            polars_processor = PolarsWorkbriefProcessor(self.progress_callback)
            result_df = polars_processor.process_in_memory(input_df, workbrief_path)
            
            if result_df is None:
                return None
                
            # Save results
            output_path = self.config.get_output_filename(input_file_path, 'workbrief')
            polars_processor._save_to_csv_with_proper_formatting(result_df, output_path)
            
            self._emit_progress(Messages.SUCCESS_WORKBRIEF)
            return output_path
            
        except Exception as e:
            error_msg = Messages.ERROR_PROCESSING.format(str(e))
            logger.error(f"{error_msg}\\n{e}", exc_info=True)
            self._emit_progress(error_msg)
            return None
    
    def process_in_memory(self, input_df: pl.DataFrame, workbrief_path: str) -> Optional[pl.DataFrame]:
        """
        Process workbrief data in memory using Polars.
        
        Args:
            input_df: Input Polars DataFrame (from previous processing step)
            workbrief_path: Path to workbrief CSV file
            
        Returns:
            Processed Polars DataFrame if successful, None otherwise
        """
        # Use Polars-optimized processor for better performance
        polars_processor = PolarsWorkbriefProcessor(self.progress_callback)
        return polars_processor.process_in_memory(input_df, workbrief_path)
    
    def _load_input_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """Load and validate input file using Polars for faster loading."""
        try:
            # Use Polars for faster CSV loading
            df_pl = pl.read_csv(file_path, infer_schema_length=0)  # Read as strings for compatibility
            
            # Convert to pandas for compatibility with existing code
            df = df_pl.to_pandas()
            
            self._emit_progress(Messages.INFO_FILE_LOADED.format(len(df)))
            
            # Don't standardize columns - preserve original names
            # df = self._standardize_columns(df)
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to load input file: {e}")
            self._emit_progress(f"Failed to load input file: {e}")
            return None
    
    def _load_workbrief_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """Load and validate workbrief file using Polars for faster loading."""
        try:
            # Use Polars for faster CSV loading
            df_pl = pl.read_csv(file_path, infer_schema_length=0)  # Read as strings for compatibility
            
            # Convert to pandas for compatibility with existing code
            df = df_pl.to_pandas()
            
            self._emit_progress(Messages.INFO_FILE_LOADED.format(len(df)))
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to load workbrief file: {e}")
            self._emit_progress(f"Failed to load workbrief file: {e}")
            return None
    
    def _process_input_timestamps(self, input_df: pd.DataFrame) -> bool:
        """Process timestamps in input dataframe."""
        try:
            self._emit_progress("Processing input timestamps...")
            
            # Store original samples for format detection
            original_samples = input_df['TestDateUTC'].dropna().head(5).astype(str).tolist()
            
            # Parse timestamps
            input_df['TestDateUTC'], format_name = timestamp_handler.detect_and_parse_timestamps(
                input_df['TestDateUTC'], 'workbrief_input_TestDateUTC'
            )
            
            # Apply RoadName cleanup if ISO format detected
            if timestamp_handler.is_iso_format(original_samples):
                logger.info("Detected ISO format - applying RoadName cleanup")
                input_df['RoadName'] = input_df['RoadName'].apply(self._remove_last_word)
            
            # Check parsing results
            failed_count = input_df['TestDateUTC'].isna().sum()
            if failed_count > 0:
                logger.warning(f"{failed_count} timestamps failed to parse")
            
            if input_df['TestDateUTC'].isna().all():
                self._emit_progress("Critical: All timestamps failed to parse")
                return False
            
            # Format timestamps back to standard format
            input_df['TestDateUTC'] = (
                input_df['TestDateUTC']
                .dt.strftime('%d/%m/%Y %H:%M:%S.%f')
                .str[:-3]
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Input timestamp processing failed: {e}")
            self._emit_progress(f"Input timestamp processing failed: {e}")
            return False
    
    def _process_input_timestamps_in_memory(self, input_df: pd.DataFrame) -> bool:
        """Process timestamps in input dataframe for in-memory processing."""
        try:
            self._emit_progress("Processing input timestamps...")
            
            # If TestDateUTC is already a datetime, no need to parse again
            if pd.api.types.is_datetime64_any_dtype(input_df['TestDateUTC']):
                logger.info("TestDateUTC is already parsed as datetime, skipping timestamp processing")
                return True
            
            # Store original samples for format detection
            original_samples = input_df['TestDateUTC'].dropna().head(5).astype(str).tolist()
            
            # Parse timestamps
            input_df['TestDateUTC'], format_name = timestamp_handler.detect_and_parse_timestamps(
                input_df['TestDateUTC'], 'workbrief_input_TestDateUTC'
            )
            
            # Apply RoadName cleanup if ISO format detected
            if timestamp_handler.is_iso_format(original_samples):
                logger.info("Detected ISO format - applying RoadName cleanup")
                input_df['RoadName'] = input_df['RoadName'].apply(self._remove_last_word)
            
            # Check parsing results
            failed_count = input_df['TestDateUTC'].isna().sum()
            if failed_count > 0:
                logger.warning(f"{failed_count} timestamps failed to parse")
            
            if input_df['TestDateUTC'].isna().all():
                self._emit_progress("Critical: All timestamps failed to parse")
                return False
            
            # Keep timestamps as datetime objects for in-memory processing
            # Don't format back to strings
            
            return True
            
        except Exception as e:
            logger.error(f"Input timestamp processing failed: {e}")
            self._emit_progress(f"Input timestamp processing failed: {e}")
            return False
    
    def _process_workbrief_data(self, input_df: pd.DataFrame, 
                               workbrief_df: pd.DataFrame) -> pd.DataFrame:
        """Process workbrief data operations - sets InBrief column based on chainage ranges."""
        logger.info("Processing workbrief data...")
        
        # Make a copy to avoid modifying original data
        result_df = input_df.copy()
        
        # Ensure InBrief column exists
        logger.info("Checking for InBrief column")
        if 'InBrief' not in result_df.columns:
            logger.warning("Column 'InBrief' does not exist in the Combined file. Adding 'InBrief' column with empty values.")
            result_df['InBrief'] = False
        
        # Handle different chainage column variants
        logger.info("Converting chainage values in workbrief")
        
        # Define chainage column variants
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
            logger.error(f"Required chainage columns not found. Available columns: {list(workbrief_df.columns)}")
            return result_df
            
        # Convert and standardize chainage columns
        if start_col != 'Start Chainage (km)':
            workbrief_df['Start Chainage (km)'] = workbrief_df[start_col]
        if end_col != 'End Chainage (km)':
            workbrief_df['End Chainage (km)'] = workbrief_df[end_col]
            
        # Convert from km to meters and round
        workbrief_df['Start Chainage (km)'] = pd.to_numeric(workbrief_df['Start Chainage (km)'], errors='coerce') * 1000
        workbrief_df['End Chainage (km)'] = pd.to_numeric(workbrief_df['End Chainage (km)'], errors='coerce') * 1000
        workbrief_df['Start Chainage (km)'] = workbrief_df['Start Chainage (km)'].round(-1)
        workbrief_df['End Chainage (km)'] = workbrief_df['End Chainage (km)'].round(-1)
        
        logger.info(f"Using chainage columns: '{start_col}' -> 'Start Chainage (km)', '{end_col}' -> 'End Chainage (km)'")
        
        # Find column mappings without creating new columns
        road_id_variants = ['Road ID', 'RoadID', 'road_id', 'roadid', 'ROADID', 'Road_ID', 'road ID']
        workbrief_road_col = None
        
        for variant in road_id_variants:
            if variant in workbrief_df.columns:
                workbrief_road_col = variant
                break
                
        if not workbrief_road_col:
            logger.error("No Road ID column found in workbrief data")
            return result_df
        logger.info(f"Using workbrief Road ID column: '{workbrief_road_col}'")
            
        # Find input data Road ID column
        input_road_col = None
        for variant in road_id_variants:
            if variant in result_df.columns:
                input_road_col = variant
                break
                
        if not input_road_col:
            logger.error("No Road ID column found in input data")
            return result_df
        logger.info(f"Using input Road ID column: '{input_road_col}'")
            
        # Find input data Chainage column
        chainage_variants = ['Chainage', 'chainage', 'CHAINAGE', 'Location', 'location']
        input_chainage_col = None
        
        for variant in chainage_variants:
            if variant in result_df.columns:
                input_chainage_col = variant
                break
                
        if not input_chainage_col:
            logger.error("No Chainage column found in input data")
            return result_df
        logger.info(f"Using input Chainage column: '{input_chainage_col}'")
        
        logger.info(f"Starting workbrief matching process for {len(result_df)} records")
        matches_found = 0
        
        # Process each record to determine if it's InBrief
        for idx, row in result_df.iterrows():
            chainage = pd.to_numeric(row[input_chainage_col], errors='coerce')
            road_id = row[input_road_col]
            
            # Skip if chainage or road_id is invalid
            if pd.isna(chainage) or pd.isna(road_id):
                result_df.at[idx, 'InBrief'] = False
                continue
                
            # Convert chainage from km to meters if column is 'location' (PAS files)
            if input_chainage_col.lower() == 'location':
                chainage_meters = chainage * 1000
            else:
                chainage_meters = chainage
                
            # Convert both road IDs to same type for comparison
            wb_road_id = pd.to_numeric(workbrief_df[workbrief_road_col], errors='coerce')
            input_road_id = pd.to_numeric(road_id, errors='coerce')
            
            matching_rows = workbrief_df[
                (wb_road_id == input_road_id) &
                (workbrief_df['Start Chainage (km)'] <= chainage_meters) &
                (workbrief_df['End Chainage (km)'] >= chainage_meters)
            ]
            
            if not matching_rows.empty:
                result_df.at[idx, 'InBrief'] = True
                matches_found += 1
                # Log first few matches for debugging
                if matches_found <= 3:
                    logger.info(f"Match {matches_found}: Road {road_id}, Chainage {chainage} -> {chainage_meters}m")
            else:
                result_df.at[idx, 'InBrief'] = False
                # Log first few non-matches for debugging  
                if matches_found == 0 and idx <= 5:
                    wb_roads = workbrief_df[workbrief_road_col].unique()[:5]
                    logger.info(f"No match: Road {road_id}, Chainage {chainage} -> {chainage_meters}m. Workbrief has roads: {wb_roads}")
            
            # Progress logging every 10000 records
            if idx > 0 and idx % 10000 == 0:
                logger.info(f"Processed {idx} records ({idx/len(result_df):.1%})")
        
        logger.info(f"Workbrief processing completed. Matches found: {matches_found}")
        
        # Format timestamps back to standard string format for final output
        if 'TestDateUTC' in result_df.columns and pd.api.types.is_datetime64_any_dtype(result_df['TestDateUTC']):
            result_df['TestDateUTC'] = (
                result_df['TestDateUTC']
                .dt.strftime('%d/%m/%Y %H:%M:%S.%f')
                .str[:-3]
            )
        
        return result_df
    
    def _save_workbrief_results(self, result_df: pd.DataFrame, 
                               original_path: str) -> str:
        """Save workbrief processing results using Polars for better performance."""
        # Generate output path
        output_path = self.config.get_output_filename(original_path, 'workbrief')
        
        # Convert to Polars and save to CSV (faster than pandas)
        df_pl = pl.from_pandas(result_df)
        df_pl.write_csv(output_path)
        logger.info(f"Workbrief results saved to: {output_path}")
        
        return output_path