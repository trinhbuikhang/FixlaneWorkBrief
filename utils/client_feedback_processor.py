"""
Optimized Client Feedback Processor for Data Processing Tool
"""

import gc
import logging
from pathlib import Path
from typing import Callable, Optional, Tuple

# ⚡ Lazy import for Polars (heavy library)
from utils.lazy_imports import polars as pl

from utils.file_lock import FileLock, FileLockTimeout

logger = logging.getLogger(__name__)


class ClientFeedbackProcessor:
    """Handles client feedback processing using Polars."""

    def __init__(self, progress_callback: Optional[Callable] = None):
        """
        Initialize client feedback processor.

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
        import os
        if not os.path.exists(file_path):
            error_msg = f"ERROR: File not found: {file_path}"
            logger.error(error_msg)
            self._emit_progress(error_msg)
            return False
        return True

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
        expressions = []

        for col in df.columns:
            if df[col].dtype == pl.Boolean:
                # Convert boolean to True/False strings, keep null as null
                expr = (
                    pl.when(pl.col(col).is_null())
                    .then(None)
                    .when(pl.col(col))
                    .then(pl.lit("True"))
                    .otherwise(pl.lit("False"))
                    .alias(col)
                )
                expressions.append(expr)
            elif df[col].dtype == pl.Utf8:
                # For string columns, convert empty strings to null for proper CSV output
                expr = (
                    pl.when(pl.col(col) == "")
                    .then(None)
                    .otherwise(pl.col(col))
                    .alias(col)
                )
                expressions.append(expr)

        if expressions:
            df = df.with_columns(expressions)
        return df

    def process_in_memory(self, lmd_df: pl.DataFrame, feedback_path: str) -> Optional[pl.DataFrame]:
        """
        Process client feedback and update LMD data in memory using Polars.

        Args:
            lmd_df: Polars DataFrame containing LMD data
            feedback_path: Path to client feedback CSV file

        Returns:
            Updated Polars DataFrame if successful, None otherwise
        """
        try:
            self._emit_progress("Starting client feedback processing...")

            # Load feedback data
            feedback_df = pl.read_csv(feedback_path, ignore_errors=True, infer_schema_length=0)
            if feedback_df is None or len(feedback_df) == 0:
                self._emit_progress("ERROR: Failed to load client feedback data")
                return None

            # Process feedback data using optimized method
            result_df = self._process_client_feedback_data_polars_optimized(lmd_df, feedback_df)

            if result_df is None:
                self._emit_progress("ERROR: Client feedback processing failed")
                # Clean up on failure
                del feedback_df
                gc.collect()
                return None

            # Clean up intermediate dataframes
            del feedback_df
            gc.collect()
            self._emit_progress("Memory cleanup completed")

            self._emit_progress("Client feedback processing completed")
            return result_df

        except Exception as e:
            error_msg = f"Client feedback processing failed: {str(e)}"
            logger.error(f"{error_msg}\n{e}", exc_info=True)
            self._emit_progress(error_msg)
            return None

    def process_with_chunking(self, lmd_path: str, feedback_path: str, chunk_size: int = 10000, extra_columns: list = None) -> Optional[pl.DataFrame]:
        """
        Process client feedback with chunking for large files.
        
        Args:
            lmd_path: Path to LMD CSV file
            feedback_path: Path to client feedback CSV file
            chunk_size: Size of chunks for processing
            extra_columns: Additional columns to add from feedback file
        """
        try:
            self._emit_progress("STARTING CLIENT FEEDBACK PROCESSING")

            # Validate files exist
            if not self._validate_file_exists(lmd_path) or not self._validate_file_exists(feedback_path):
                return None

            # Load feedback data
            self._emit_progress("STEP 1: LOADING FEEDBACK DATA")
            feedback_df = pl.read_csv(feedback_path, ignore_errors=True, infer_schema_length=0)
            if feedback_df is None or len(feedback_df) == 0:
                self._emit_progress("❌ ERROR: Failed to load client feedback data")
                return None

            self._emit_progress(f"✓ Loaded feedback data: {len(feedback_df):,} rows, {len(feedback_df.columns)} columns")
            
            # Analyze feedback data structure
            self._emit_progress(f"📋 Feedback file columns: {', '.join(feedback_df.columns[:10])}{'...' if len(feedback_df.columns) > 10 else ''}")

            # Process using lazy evaluation for better memory efficiency
            self._emit_progress("STEP 2: LOADING LMD DATA")
            lazy_df = pl.scan_csv(lmd_path, ignore_errors=True, infer_schema_length=0)
            
            # Process the entire file using optimized method
            result_df = lazy_df.collect()
            self._emit_progress(f"✓ Loaded LMD data: {len(result_df):,} rows, {len(result_df.columns)} columns")
            
            # Remove duplicate TestDateUTC rows
            result_df, removed_duplicates = self._remove_duplicate_testdateutc(result_df)
            if removed_duplicates > 0:
                self._emit_progress(f"✓ Removed {removed_duplicates} duplicate TestDateUTC rows")
            else:
                self._emit_progress(f"✓ No duplicate TestDateUTC rows found")
            
            result_df = self._process_client_feedback_data_polars_optimized(result_df, feedback_df, extra_columns)
            
            if result_df is None:
                self._emit_progress("❌ ERROR: Processing failed")
                # Clean up on failure
                del feedback_df
                del lazy_df
                gc.collect()
                return None

            # Clean up intermediate dataframes
            del feedback_df
            del lazy_df
            gc.collect()
            self._emit_progress("✓ Memory cleanup completed")

            return result_df

        except Exception as e:
            error_msg = f"Client feedback processing failed: {str(e)}"
            logger.error(f"{error_msg}\n{e}", exc_info=True)
            self._emit_progress(error_msg)
            return None

    def _process_client_feedback_data_polars_optimized(self, result_df: pl.DataFrame, feedback_df: pl.DataFrame, extra_columns: list = None) -> Optional[pl.DataFrame]:
        """
        Optimized version using Polars join operations instead of nested loops.
        Maintains exact row count and order from input file.
        """
        try:
            # Store original columns and row count
            original_columns = result_df.columns.copy()
            original_row_count = len(result_df)
            
            self._emit_progress("STEP 1: ANALYZING INPUT DATA")
            self._emit_progress(f"📋 LMD input: {original_row_count:,} records with {len(original_columns)} columns")
            self._emit_progress(f"📋 Feedback input: {len(feedback_df):,} records")

            # Find Road ID column variants with case-insensitive matching
            # Support: Road ID, RoadID, sectionID, roadid, roadname, sectionName
            road_id_variants = ['Road ID', 'RoadID', 'road_id', 'roadid', 'ROADID', 'Road_ID', 'road ID', 
                               'Road Name', 'road_name', 'ROAD_NAME', 'roadname', 'RoadName',
                               'sectionID', 'SectionID', 'section_id', 'SECTIONID',
                               'sectionName', 'SectionName', 'section_name', 'SECTIONNAME']
            input_road_col = None
            feedback_road_col = None

            # Case-sensitive exact match first
            for variant in road_id_variants:
                if variant in result_df.columns and input_road_col is None:
                    input_road_col = variant
                if variant in feedback_df.columns and feedback_road_col is None:
                    feedback_road_col = variant
            
            # If not found, try case-insensitive matching
            if not input_road_col:
                for col in result_df.columns:
                    for variant in road_id_variants:
                        if col.lower() == variant.lower():
                            input_road_col = col
                            break
                    if input_road_col:
                        break
                        
            if not feedback_road_col:
                for col in feedback_df.columns:
                    for variant in road_id_variants:
                        if col.lower() == variant.lower():
                            feedback_road_col = col
                            break
                    if feedback_road_col:
                        break

            if not input_road_col or not feedback_road_col:
                self._emit_progress("⚠️ WARNING: Road ID columns not found - adding empty feedback columns")
                self._emit_progress(f"   • LMD Road ID column: {'✓ ' + input_road_col if input_road_col else '❌ Not found'}")
                self._emit_progress(f"   • Feedback Road ID column: {'✓ ' + feedback_road_col if feedback_road_col else '❌ Not found'}")
                logger.error("Road ID columns not found")
                # Add empty feedback columns to maintain structure
                feedback_columns_to_add = ['Site Description', 'Treatment 2024', 'Treatment 2025', 
                                          'Treatment 2026', 'Terminal', 'Foamed Bitumen %', 
                                          'Cement %', 'Lime %']
                if extra_columns:
                    feedback_columns_to_add.extend(extra_columns)
                for col in feedback_columns_to_add:
                    result_df = result_df.with_columns([pl.lit(None).alias(col)])
                self._emit_progress(f"✓ Added {len(feedback_columns_to_add)} empty feedback columns")
                return result_df

            # Find chainage columns with case-insensitive matching
            # Support: Chainage, Location, lane (for lane-based matching)
            chainage_variants = ['Chainage', 'chainage', 'CHAINAGE', 'Location', 'location', 'LOCATION', 
                                'Chainage (km)', 'chainage_km', 'chainage_m',
                                'loc', 'Loc', 'LOC', 'distance', 'Distance', 'DISTANCE']
            input_chainage_col = None

            # Case-sensitive exact match first
            for variant in chainage_variants:
                if variant in result_df.columns:
                    input_chainage_col = variant
                    break
            
            # If not found, try case-insensitive matching  
            if not input_chainage_col:
                for col in result_df.columns:
                    for variant in chainage_variants:
                        if col.lower() == variant.lower():
                            input_chainage_col = col
                            break
                    if input_chainage_col:
                        break

            if not input_chainage_col:
                self._emit_progress("❌ ERROR: No Chainage column found in input data")
                self._emit_progress(f"   • Searched for: {', '.join(chainage_variants)}")
                logger.error("No Chainage column found in input data")
                return result_df
            
            # Find Lane column for optional lane-based matching
            lane_variants = ['Lane', 'lane', 'LANE', 'lane_id', 'LaneID', 'lane_name', 'LaneName']
            input_lane_col = None
            feedback_lane_col = None
            
            for variant in lane_variants:
                if variant in result_df.columns and input_lane_col is None:
                    input_lane_col = variant
                if variant in feedback_df.columns and feedback_lane_col is None:
                    feedback_lane_col = variant
            
            if input_lane_col and feedback_lane_col:
                self._emit_progress(f"✓ Lane column detected - will use lane-based matching")
                self._emit_progress(f"   • LMD Lane column: '{input_lane_col}'")
                self._emit_progress(f"   • Feedback Lane column: '{feedback_lane_col}'")
            else:
                self._emit_progress(f"ℹ️ Lane column not found in both files - using road+chainage matching only")

            # Find start/end chainage in feedback with case-insensitive matching
            # Support: Start Chainage (km), locFrom (meters), start chainage
            start_chainage_variants = ['Start Chainage', 'start_chainage', 'StartChainage', 'start chainage', 
                                       'Start Chainage (km)', 'START_CHAINAGE', 'start_chainage_km',
                                       'locFrom', 'LocFrom', 'loc_from', 'LOC_FROM', 'locfrom',
                                       'start_m', 'Start_M', 'startM', 'StartM']
            end_chainage_variants = ['End Chainage', 'end_chainage', 'EndChainage', 'end chainage', 
                                     'End Chainage (km)', 'END_CHAINAGE', 'end_chainage_km',
                                     'locTo', 'LocTo', 'loc_to', 'LOC_TO', 'locto',
                                     'end_m', 'End_M', 'endM', 'EndM']

            start_col = None
            end_col = None

            # Case-sensitive exact match first
            for variant in start_chainage_variants:
                if variant in feedback_df.columns:
                    start_col = variant
                    break
            
            # If not found, try case-insensitive
            if not start_col:
                for col in feedback_df.columns:
                    for variant in start_chainage_variants:
                        if col.lower() == variant.lower():
                            start_col = col
                            break
                    if start_col:
                        break

            # Case-sensitive exact match first
            for variant in end_chainage_variants:
                if variant in feedback_df.columns:
                    end_col = variant
                    break
            
            # If not found, try case-insensitive
            if not end_col:
                for col in feedback_df.columns:
                    for variant in end_chainage_variants:
                        if col.lower() == variant.lower():
                            end_col = col
                            break
                    if end_col:
                        break

            self._emit_progress("STEP 2: COLUMN MAPPING DETECTED")
            self._emit_progress(f"✓ LMD Road ID column: '{input_road_col}'")
            self._emit_progress(f"✓ Feedback Road ID column: '{feedback_road_col}'")  
            self._emit_progress(f"✓ LMD Chainage column: '{input_chainage_col}'")
            
            # Debug: Show available columns
            self._emit_progress(f"🔍 Available LMD columns: {', '.join(result_df.columns[:10])}{'...' if len(result_df.columns) > 10 else ''}")
            self._emit_progress(f"🔍 Available Feedback columns: {', '.join(feedback_df.columns[:10])}{'...' if len(feedback_df.columns) > 10 else ''}")
            
            if not start_col or not end_col:
                self._emit_progress(f"❌ ERROR: Required chainage columns not found in feedback data")
                self._emit_progress(f"   • Start chainage: {'✓ ' + start_col if start_col else '❌ Not found'}")
                self._emit_progress(f"   • End chainage: {'✓ ' + end_col if end_col else '❌ Not found'}")
                logger.error(f"Required chainage columns not found in feedback data")
                return result_df
            
            self._emit_progress(f"✓ Feedback Start chainage: '{start_col}'")
            self._emit_progress(f"✓ Feedback End chainage: '{end_col}'")

            logger.info(f"Using columns - Road: {input_road_col}/{feedback_road_col}, Chainage: {input_chainage_col}")

            # Use only the columns specified by the user (extra_columns parameter)
            # If no columns specified, use standard default columns
            if extra_columns:
                feedback_columns_to_add = extra_columns.copy()
                self._emit_progress(f"📋 Using user-selected columns: {', '.join(extra_columns)}")
            else:
                # Fallback to standard columns if none selected
                feedback_columns_to_add = ['Site Description', 'Treatment 2024', 'Treatment 2025', 
                                          'Treatment 2026', 'Terminal', 'Foamed Bitumen %', 
                                          'Cement %', 'Lime %']
                self._emit_progress(f"⚠️ No columns selected - using default columns: {', '.join(feedback_columns_to_add)}")
            
            # System columns that should not be included in data columns
            system_columns = {feedback_road_col, start_col, end_col, 'road_id', 'region_id', 'project_id', 'Road Name'}
            
            # Log what columns are available for reference
            available_data_columns = [col for col in feedback_df.columns if col not in system_columns]
            self._emit_progress(f"� Available data columns in feedback file: {', '.join(available_data_columns) if available_data_columns else 'None'}")
            
            # Remove duplicates while preserving order
            feedback_columns_to_add = list(dict.fromkeys(feedback_columns_to_add))

            # Validate required columns exist
            if feedback_road_col not in feedback_df.columns:
                self._emit_progress(f"❌ ERROR: Feedback Road ID column '{feedback_road_col}' not found in feedback file")
                self._emit_progress(f"   Available columns: {list(feedback_df.columns)}")
                logger.error(f"Feedback Road ID column '{feedback_road_col}' not found")
                return result_df
                
            if start_col not in feedback_df.columns:
                self._emit_progress(f"❌ ERROR: Start chainage column '{start_col}' not found in feedback file")
                logger.error(f"Start chainage column '{start_col}' not found")
                return result_df
                
            if end_col not in feedback_df.columns:
                self._emit_progress(f"❌ ERROR: End chainage column '{end_col}' not found in feedback file")
                logger.error(f"End chainage column '{end_col}' not found")
                return result_df

            # Detect if chainage is in meters or kilometers
            # locFrom/locTo are typically in meters, Start Chainage/End Chainage are typically in km
            start_col_lower = start_col.lower()
            end_col_lower = end_col.lower()
            
            # Check if column names indicate meters (locFrom, locTo, _m suffix)
            is_meters = any(x in start_col_lower for x in ['locfrom', 'loc_from', '_m', 'start_m']) or \
                       any(x in end_col_lower for x in ['locto', 'loc_to', '_m', 'end_m'])
            
            if is_meters:
                self._emit_progress(f"📏 Detected chainage unit: METERS (columns: {start_col}, {end_col})")
            else:
                self._emit_progress(f"📏 Detected chainage unit: KILOMETERS (columns: {start_col}, {end_col})")
            
            # Process feedback data with appropriate unit conversion
            try:
                if is_meters:
                    # Data is already in meters - round to nearest 10m
                    feedback_processed = feedback_df.with_columns([
                        pl.col(feedback_road_col).cast(pl.Utf8).alias('fb_road_id'),
                        ((pl.col(start_col).cast(pl.Float64) / 10).round(0) * 10).alias('start_chainage_m'),
                        ((pl.col(end_col).cast(pl.Float64) / 10).round(0) * 10).alias('end_chainage_m')
                    ])
                else:
                    # Data is in kilometers - convert to meters and round to nearest 10m
                    feedback_processed = feedback_df.with_columns([
                        pl.col(feedback_road_col).cast(pl.Utf8).alias('fb_road_id'),
                        (((pl.col(start_col).cast(pl.Float64) * 1000) / 10).round(0) * 10).alias('start_chainage_m'),
                        (((pl.col(end_col).cast(pl.Float64) * 1000) / 10).round(0) * 10).alias('end_chainage_m')
                    ])
                
            except Exception as e:
                self._emit_progress(f"❌ ERROR: Failed to process feedback columns: {e}")
                logger.error(f"Failed to process feedback columns: {e}")
                return result_df

            # Add feedback columns if they don't exist, with detailed logging
            columns_found = []
            columns_added_empty = []
            for col in feedback_columns_to_add:
                if col not in feedback_processed.columns:
                    feedback_processed = feedback_processed.with_columns([
                        pl.lit("").alias(col)
                    ])
                    columns_added_empty.append(col)
                else:
                    columns_found.append(col)
            
            # Log which columns were found vs added empty
            if columns_found:
                self._emit_progress(f"✓ Selected columns found with data: {', '.join(columns_found)}")
            if columns_added_empty:
                self._emit_progress(f"⚠️ Selected columns not found (will be empty): {', '.join(columns_added_empty)}")
                self._emit_progress(f"💡 Tip: Only select columns that exist in your feedback file to get actual data")

            # Process input data - also add lane column if available
            process_exprs = [
                pl.col(input_road_col).cast(pl.Utf8).alias('road_id_join'),
                pl.when(pl.lit(input_chainage_col.lower() == 'location'))
                .then(pl.col(input_chainage_col).cast(pl.Float64) * 1000)
                .otherwise(pl.col(input_chainage_col).cast(pl.Float64))
                .alias('chainage_m')
            ]
            
            # Add lane column for matching if available
            use_lane_matching = input_lane_col and feedback_lane_col
            if use_lane_matching:
                process_exprs.append(pl.col(input_lane_col).cast(pl.Utf8).alias('lane_join'))
                # Also add lane to feedback for joining
                feedback_processed = feedback_processed.with_columns([
                    pl.col(feedback_lane_col).cast(pl.Utf8).alias('fb_lane')
                ])
            
            result_processed = result_df.with_columns(process_exprs)

            # Add row index for tracking
            result_processed = result_processed.with_row_count('_row_idx')

            self._emit_progress("STEP 3: ROAD SECTION MATCHING")
            if use_lane_matching:
                self._emit_progress(f"🔗 Starting chainage+lane-based matching process...")
            else:
                self._emit_progress(f"🔗 Starting chainage-based matching process...")
            
            # Get unique road IDs for statistics
            unique_lmd_roads = result_processed['road_id_join'].n_unique()
            unique_feedback_roads = feedback_processed['fb_road_id'].n_unique()
            
            self._emit_progress(f"📊 Unique road IDs - LMD: {unique_lmd_roads}, Feedback: {unique_feedback_roads}")
            
            # OPTIMIZED: Use cross join with filtering instead of nested loops
            # This is much faster with Polars' columnar operations
            
            # First, join on road_id (and optionally lane) with explicit suffix to avoid naming conflicts
            if use_lane_matching:
                joined = result_processed.join(
                    feedback_processed,
                    left_on=['road_id_join', 'lane_join'],
                    right_on=['fb_road_id', 'fb_lane'],
                    how='left',
                    suffix='_feedback'
                )
            else:
                joined = result_processed.join(
                    feedback_processed,
                    left_on='road_id_join',
                    right_on='fb_road_id',
                    how='left',
                    suffix='_feedback'
                )
            
            # Debug: Show columns after join to understand what's available
            self._emit_progress(f"🔍 Columns after road join: {', '.join(joined.columns[:20])}{'...' if len(joined.columns) > 20 else ''}")
            
            # After join, we can identify matches by checking if any feedback column has values
            # Use start_chainage_m as indicator of successful match (it will be null if no match)
            start_chainage_col = 'start_chainage_m' if 'start_chainage_m' in joined.columns else 'start_chainage_m_feedback'
            road_matches = joined.filter(pl.col(start_chainage_col).is_not_null()).height
            self._emit_progress(f"   • Road ID matches: {road_matches:,}/{original_row_count:,} ({road_matches/original_row_count*100:.1f}%)")

            # Filter for chainage range - check for suffix in column names
            start_chainage_col = 'start_chainage_m' if 'start_chainage_m' in joined.columns else 'start_chainage_m_feedback'
            end_chainage_col = 'end_chainage_m' if 'end_chainage_m' in joined.columns else 'end_chainage_m_feedback'
            
            matched = joined.filter(
                (pl.col('chainage_m') >= pl.col(start_chainage_col)) &
                (pl.col('chainage_m') <= pl.col(end_chainage_col))
            )
            
            chainage_matches = matched.height
            self._emit_progress(f"   • Chainage range matches: {chainage_matches:,}/{road_matches:,} records")

            # Group by row index and take first match (in case of overlaps)
            # Need to check for suffix in column names after join
            agg_columns = []
            for col in feedback_columns_to_add:
                if col in matched.columns:
                    agg_columns.append(pl.first(col).alias(col))
                elif f"{col}_feedback" in matched.columns:
                    agg_columns.append(pl.first(f"{col}_feedback").alias(col))
                else:
                    # Column not found, will add as null later
                    pass
            
            if agg_columns:
                matched_agg = matched.group_by('_row_idx').agg(agg_columns)
            else:
                # No columns to aggregate, create empty DataFrame with just _row_idx
                matched_agg = matched.select('_row_idx').unique()

            # Join back to original data - CRITICAL: use 'left' join to keep ALL input rows
            final_df = result_processed.join(
                matched_agg,
                on='_row_idx',
                how='left'
            )

            # Sort by original row index to maintain exact input order
            final_df = final_df.sort('_row_idx')

            # Update existing columns with feedback values where available
            for col in feedback_columns_to_add:
                if col in original_columns:
                    final_df = final_df.with_columns([
                        pl.coalesce([pl.col(col + '_right'), pl.col(col)]).alias(col)
                    ])

            # Add feedback columns with null values for rows without matches (only for new columns)
            for col in feedback_columns_to_add:
                if col not in original_columns and col not in final_df.columns:
                    final_df = final_df.with_columns([
                        pl.lit(None).alias(col)
                    ])

            # Select final columns in original order plus new feedback columns
            select_cols = original_columns + [col for col in feedback_columns_to_add if col not in original_columns]
            final_df = final_df.select(select_cols)

            # Format TestDateUTC if needed
            if 'TestDateUTC' in final_df.columns:
                if final_df['TestDateUTC'].dtype in [pl.Datetime, pl.Datetime('ns'), pl.Datetime('us')]:
                    final_df = final_df.with_columns([
                        pl.col('TestDateUTC').dt.strftime('%d/%m/%Y %H:%M:%S%.3f').alias('TestDateUTC')
                    ])

            # Count final matches and calculate statistics
            records_with_feedback = final_df.filter(
                pl.any_horizontal([pl.col(col).is_not_null() for col in feedback_columns_to_add])
            ).height
            
            match_rate = (records_with_feedback / original_row_count * 100) if original_row_count > 0 else 0
            
            # Verify row count is preserved
            if len(final_df) != original_row_count:
                logger.warning(f"Row count mismatch! Input: {original_row_count}, Output: {len(final_df)}")
                self._emit_progress(f"⚠️ WARNING: Row count mismatch! Input: {original_row_count}, Output: {len(final_df)}")
                # This should never happen with left join, but log if it does

            self._emit_progress(f"")
            self._emit_progress(f"📊 PROCESSING STATISTICS:")
            self._emit_progress(f"   • Total records processed: {original_row_count:,}")
            self._emit_progress(f"   • Records with feedback data: {records_with_feedback:,}")
            self._emit_progress(f"   • Match rate: {match_rate:.2f}%")
            self._emit_progress(f"")
            self._emit_progress(f"🎯 FEEDBACK COLUMNS ADDED ({len(feedback_columns_to_add)}):")
            for i, col in enumerate(feedback_columns_to_add, 1):
                # Count non-null values for this column
                non_null_count = final_df[col].drop_nulls().len() if col in final_df.columns else 0
                fill_rate = (non_null_count / original_row_count * 100) if original_row_count > 0 else 0
                status = "✓ Added" if col not in original_columns else "🔄 Updated"
                self._emit_progress(f"   {i}. {col} - {status} ({non_null_count:,} values, {fill_rate:.1f}%)")
            
            self._emit_progress(f"✓ Client feedback processing completed successfully!")
            return final_df

        except Exception as e:
            logger.error(f"Client feedback processing failed: {e}")
            return result_df

    def write_output_csv(self, df: pl.DataFrame, output_path: str) -> bool:
        """
        Write DataFrame to CSV with proper formatting.
        Ensures empty cells remain empty (no quotes, no text).
        
        Args:
            df: Polars DataFrame to write
            output_path: Path for output CSV file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Prepare for CSV output (handle booleans and empty strings)
            df_output = self._prepare_for_csv_output(df)
            
            # Write CSV with file locking to prevent concurrent writes
            try:
                with FileLock(output_path, timeout=60):
                    df_output.write_csv(
                        output_path,
                        quote_style='necessary',  # Only quote when needed
                        null_value='',  # Null values become empty cells (no quotes)
                        datetime_format='%d/%m/%Y %H:%M:%S%.3f'
                    )
            except FileLockTimeout:
                logger.error(f"Output file {output_path} is locked by another process")
                self._emit_progress(f"ERROR: Cannot write - file is locked by another process")
                return False
            
            # Clean up output dataframe
            del df_output
            gc.collect()
            
            self._emit_progress(f"Output written to: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to write output: {e}")
            self._emit_progress(f"ERROR: Failed to write output - {str(e)}")
            return False
    def _process_client_feedback_data_polars(self, result_df: pl.DataFrame, feedback_df: pl.DataFrame) -> Optional[pl.DataFrame]:
        """
        DEPRECATED: Use _process_client_feedback_data_polars_optimized instead.
        This method uses nested loops and is very slow for large datasets.
        """
        logger.warning("Using deprecated slow method. Consider using optimized version.")
        return self._process_client_feedback_data_polars_optimized(result_df, feedback_df)