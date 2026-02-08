"""
Data Validation Module
Provides comprehensive validation for CSV data structures
"""

import logging
from typing import Dict, List, Optional, Tuple

# ⚡ Lazy import for Polars (heavy library)
from utils.lazy_imports import polars as pl

logger = logging.getLogger(__name__)


class DataValidator:
    """Validates CSV data structure and content before processing"""
    
    # Maximum limits to prevent DoS
    MAX_ROWS = 100_000_000  # 100 million rows
    MAX_COLUMNS = 10_000    # 10K columns
    MAX_COLUMN_NAME_LENGTH = 500
    
    @staticmethod
    def validate_csv_structure(df: pl.DataFrame, file_name: str = "input file") -> Tuple[bool, str]:
        """
        Validate basic CSV structure.
        
        Args:
            df: Polars DataFrame to validate
            file_name: Name of file for error messages
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            # Check if DataFrame is empty
            if len(df) == 0:
                return False, f"{file_name} is empty (0 rows)"
            
            # Check for reasonable row count (prevent DoS)
            if len(df) > DataValidator.MAX_ROWS:
                return False, f"{file_name} is too large: {len(df):,} rows exceeds limit of {DataValidator.MAX_ROWS:,}"
            
            # Check for reasonable column count
            if len(df.columns) > DataValidator.MAX_COLUMNS:
                return False, f"{file_name} has too many columns: {len(df.columns):,} exceeds limit of {DataValidator.MAX_COLUMNS:,}"
            
            # Check for duplicate column names
            if len(df.columns) != len(set(df.columns)):
                duplicates = [col for col in df.columns if df.columns.count(col) > 1]
                unique_duplicates = list(set(duplicates))
                return False, f"{file_name} has duplicate column names: {', '.join(unique_duplicates[:5])}"
            
            # Check for empty column names
            empty_cols = [i for i, col in enumerate(df.columns) if not col or col.strip() == ""]
            if empty_cols:
                return False, f"{file_name} has empty column names at positions: {empty_cols[:5]}"
            
            # Check for excessively long column names
            long_cols = [col for col in df.columns if len(col) > DataValidator.MAX_COLUMN_NAME_LENGTH]
            if long_cols:
                return False, f"{file_name} has column names that are too long (>{DataValidator.MAX_COLUMN_NAME_LENGTH} chars): {long_cols[0][:50]}..."
            
            logger.info(f"CSV structure validation passed for {file_name}: {len(df):,} rows, {len(df.columns)} columns")
            return True, "Validation passed"
            
        except Exception as e:
            logger.error(f"CSV structure validation error: {e}")
            return False, f"Validation error: {str(e)}"
    
    @staticmethod
    def validate_required_columns(df: pl.DataFrame, required_columns: List[str], 
                                  file_name: str = "input file") -> Tuple[bool, str]:
        """
        Validate that required columns exist in DataFrame.
        
        Args:
            df: Polars DataFrame to validate
            required_columns: List of required column names
            file_name: Name of file for error messages
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            # Check for missing columns
            missing = set(required_columns) - set(df.columns)
            if missing:
                return False, f"{file_name} is missing required columns: {', '.join(sorted(missing))}"
            
            logger.info(f"Required columns validation passed for {file_name}")
            return True, "All required columns present"
            
        except Exception as e:
            logger.error(f"Required columns validation error: {e}")
            return False, f"Validation error: {str(e)}"
    
    @staticmethod
    def validate_data_integrity(df: pl.DataFrame, file_name: str = "input file",
                                check_all_null_rows: bool = True,
                                check_all_null_columns: bool = True) -> Tuple[bool, str]:
        """
        Validate data integrity (check for all-null rows/columns).
        
        Args:
            df: Polars DataFrame to validate
            file_name: Name of file for error messages
            check_all_null_rows: Whether to check for rows with all null values
            check_all_null_columns: Whether to check for columns with all null values
            
        Returns:
            Tuple of (is_valid, error_message) with warnings
        """
        warnings = []
        
        try:
            # Check for all-null rows
            if check_all_null_rows:
                all_null_mask = pl.all_horizontal([pl.col(col).is_null() for col in df.columns])
                all_null_count = df.select(all_null_mask).to_series().sum()
                
                if all_null_count > 0:
                    warnings.append(f"{all_null_count:,} rows have all null values")
            
            # Check for all-null columns
            if check_all_null_columns:
                null_columns = []
                for col in df.columns:
                    if df[col].is_null().sum() == len(df):
                        null_columns.append(col)
                
                if null_columns:
                    warnings.append(f"{len(null_columns)} columns are completely null: {', '.join(null_columns[:5])}")
            
            if warnings:
                warning_msg = f"{file_name} - Warnings: " + "; ".join(warnings)
                logger.warning(warning_msg)
                return True, warning_msg  # Still valid, but with warnings
            
            logger.info(f"Data integrity validation passed for {file_name}")
            return True, "Data integrity check passed"
            
        except Exception as e:
            logger.error(f"Data integrity validation error: {e}")
            return False, f"Validation error: {str(e)}"
    
    @staticmethod
    def validate_column_data_types(df: pl.DataFrame, column_type_expectations: Dict[str, type],
                                   file_name: str = "input file") -> Tuple[bool, str]:
        """
        Validate that columns can be cast to expected data types.
        
        Args:
            df: Polars DataFrame to validate
            column_type_expectations: Dict mapping column name to expected Python type
            file_name: Name of file for error messages
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        errors = []
        
        try:
            for col_name, expected_type in column_type_expectations.items():
                if col_name not in df.columns:
                    continue
                
                # Try to identify if column can be cast to expected type
                try:
                    sample = df[col_name].head(100).drop_nulls()
                    
                    if expected_type in [int, float]:
                        # Check if numeric
                        try:
                            sample.cast(pl.Float64, strict=False)
                        except:
                            errors.append(f"Column '{col_name}' cannot be converted to numeric type")
                    
                    elif expected_type == str:
                        # Everything can be string
                        pass
                    
                    elif expected_type == bool:
                        # Check if boolean-like
                        unique_values = set(sample.cast(pl.Utf8).str.to_lowercase().to_list())
                        valid_bool_values = {'true', 'false', '1', '0', 't', 'f', 'yes', 'no', 'y', 'n'}
                        if not unique_values.issubset(valid_bool_values):
                            errors.append(f"Column '{col_name}' contains non-boolean values")
                
                except Exception as e:
                    errors.append(f"Column '{col_name}' type validation failed: {str(e)}")
            
            if errors:
                error_msg = f"{file_name} - Type validation errors: " + "; ".join(errors)
                logger.error(error_msg)
                return False, error_msg
            
            logger.info(f"Column data type validation passed for {file_name}")
            return True, "Type validation passed"
            
        except Exception as e:
            logger.error(f"Column type validation error: {e}")
            return False, f"Validation error: {str(e)}"
    
    @staticmethod
    def get_data_summary(df: pl.DataFrame, file_name: str = "data") -> str:
        """
        Get a summary of DataFrame for logging/debugging.
        
        Args:
            df: Polars DataFrame
            file_name: Name for the summary
            
        Returns:
            Summary string
        """
        try:
            summary_lines = [
                f"\n{'='*60}",
                f"Data Summary: {file_name}",
                f"{'='*60}",
                f"Rows: {len(df):,}",
                f"Columns: {len(df.columns)}",
                f"Column names: {', '.join(df.columns[:10])}{'...' if len(df.columns) > 10 else ''}",
                f"Memory usage: ~{df.estimated_size('mb'):.2f} MB",
            ]
            
            # Null counts
            null_summary = []
            for col in df.columns[:10]:  # First 10 columns
                null_count = df[col].is_null().sum()
                if null_count > 0:
                    null_pct = (null_count / len(df)) * 100
                    null_summary.append(f"{col}: {null_pct:.1f}%")
            
            if null_summary:
                summary_lines.append(f"Null percentages: {', '.join(null_summary)}")
            
            summary_lines.append(f"{'='*60}\n")
            
            return "\n".join(summary_lines)
            
        except Exception as e:
            return f"Could not generate summary: {e}"
