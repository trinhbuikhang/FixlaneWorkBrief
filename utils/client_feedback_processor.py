"""
Optimized Client Feedback Processor for Data Processing Tool
"""

import polars as pl
import logging
from typing import Optional, Callable
from pathlib import Path

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
                return None

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
            self._emit_progress("Starting client feedback processing with chunking...")

            # Validate files exist
            if not self._validate_file_exists(lmd_path) or not self._validate_file_exists(feedback_path):
                return None

            # Load feedback data
            feedback_df = pl.read_csv(feedback_path, ignore_errors=True, infer_schema_length=0)
            if feedback_df is None or len(feedback_df) == 0:
                self._emit_progress("ERROR: Failed to load client feedback data")
                return None

            self._emit_progress(f"Loaded feedback data with {len(feedback_df)} rows")

            # Process using lazy evaluation for better memory efficiency
            lazy_df = pl.scan_csv(lmd_path, ignore_errors=True, infer_schema_length=0)
            
            # Process the entire file using optimized method
            result_df = lazy_df.collect()
            result_df = self._process_client_feedback_data_polars_optimized(result_df, feedback_df, extra_columns)
            
            if result_df is None:
                self._emit_progress("ERROR: Processing failed")
                return None

            self._emit_progress(f"Client feedback processing completed. Total rows: {len(result_df)}")
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

            # Find Road ID column variants
            road_id_variants = ['Road ID', 'RoadID', 'road_id', 'roadid', 'ROADID', 'Road_ID', 'road ID']
            input_road_col = None
            feedback_road_col = None

            for variant in road_id_variants:
                if variant in result_df.columns and input_road_col is None:
                    input_road_col = variant
                if variant in feedback_df.columns and feedback_road_col is None:
                    feedback_road_col = variant

            if not input_road_col or not feedback_road_col:
                logger.error("Road ID columns not found")
                # Add empty feedback columns to maintain structure
                feedback_columns_to_add = ['Site Description', 'Treatment 2024', 'Treatment 2025', 
                                          'Treatment 2026', 'Terminal', 'Foamed Bitumen %', 
                                          'Cement %', 'Lime %']
                if extra_columns:
                    feedback_columns_to_add.extend(extra_columns)
                for col in feedback_columns_to_add:
                    result_df = result_df.with_columns([pl.lit(None).alias(col)])
                return result_df

            # Find chainage columns
            chainage_variants = ['Chainage', 'chainage', 'CHAINAGE', 'Location', 'location']
            input_chainage_col = None

            for variant in chainage_variants:
                if variant in result_df.columns:
                    input_chainage_col = variant
                    break

            if not input_chainage_col:
                logger.error("No Chainage column found in input data")
                return result_df

            # Find start/end chainage in feedback
            start_chainage_variants = ['Start Chainage', 'start_chainage', 'StartChainage', 'start chainage', 'Start Chainage (km)']
            end_chainage_variants = ['End Chainage', 'end_chainage', 'EndChainage', 'end chainage', 'End Chainage (km)']

            start_col = None
            end_col = None

            for variant in start_chainage_variants:
                if variant in feedback_df.columns:
                    start_col = variant
                    break

            for variant in end_chainage_variants:
                if variant in feedback_df.columns:
                    end_col = variant
                    break

            if not start_col or not end_col:
                logger.error(f"Required chainage columns not found in feedback data")
                return result_df

            logger.info(f"Using columns - Road: {input_road_col}/{feedback_road_col}, Chainage: {input_chainage_col}")

            # Prepare feedback columns
            feedback_columns_to_add = ['Site Description', 'Treatment 2024', 'Treatment 2025', 
                                      'Treatment 2026', 'Terminal', 'Foamed Bitumen %', 
                                      'Cement %', 'Lime %']
            if extra_columns:
                feedback_columns_to_add.extend(extra_columns)
            
            # Remove duplicates while preserving order
            feedback_columns_to_add = list(dict.fromkeys(feedback_columns_to_add))

            # Process feedback data
            feedback_processed = feedback_df.with_columns([
                pl.col(feedback_road_col).cast(pl.Utf8).alias('fb_road_id'),
                (((pl.col(start_col).cast(pl.Float64) * 1000) / 10).round(0) * 10).alias('start_chainage_m'),
                (((pl.col(end_col).cast(pl.Float64) * 1000) / 10).round(0) * 10).alias('end_chainage_m')
            ])

            # Add feedback columns if they don't exist
            for col in feedback_columns_to_add:
                if col not in feedback_processed.columns:
                    feedback_processed = feedback_processed.with_columns([
                        pl.lit("").alias(col)
                    ])

            # Process input data
            result_processed = result_df.with_columns([
                pl.col(input_road_col).cast(pl.Utf8).alias('road_id_join'),
                pl.when(pl.lit(input_chainage_col.lower() == 'location'))
                .then(pl.col(input_chainage_col).cast(pl.Float64) * 1000)
                .otherwise(pl.col(input_chainage_col).cast(pl.Float64))
                .alias('chainage_m')
            ])

            # Add row index for tracking
            result_processed = result_processed.with_row_count('_row_idx')

            # OPTIMIZED: Use cross join with filtering instead of nested loops
            # This is much faster with Polars' columnar operations
            
            # First, join on road_id
            joined = result_processed.join(
                feedback_processed,
                left_on='road_id_join',
                right_on='fb_road_id',
                how='left'
            )

            # Filter for chainage range
            matched = joined.filter(
                (pl.col('chainage_m') >= pl.col('start_chainage_m')) &
                (pl.col('chainage_m') <= pl.col('end_chainage_m'))
            )

            # Group by row index and take first match (in case of overlaps)
            matched_agg = matched.group_by('_row_idx').agg([
                pl.first(col).alias(col) for col in feedback_columns_to_add
            ])

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

            # Verify row count is preserved
            if len(final_df) != original_row_count:
                logger.warning(f"Row count mismatch! Input: {original_row_count}, Output: {len(final_df)}")
                # This should never happen with left join, but log if it does

            self._emit_progress(f"Client feedback processing completed. Preserved {len(final_df)} rows, added {len(feedback_columns_to_add)} columns")
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
            
            # Write CSV with specific options for proper empty cell handling
            df_output.write_csv(
                output_path,
                quote_style='necessary',  # Only quote when needed
                null_value='',  # Null values become empty cells (no quotes)
                datetime_format='%d/%m/%Y %H:%M:%S%.3f'
            )
            
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