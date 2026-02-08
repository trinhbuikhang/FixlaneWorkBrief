"""
Safe File Writing with Automatic Backup
Provides utilities for safely writing files with automatic backups
"""

import logging
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

# ⚡ Lazy import for Polars (heavy library)
from utils.lazy_imports import polars as pl

from utils.file_lock import FileLock, FileLockTimeout

logger = logging.getLogger(__name__)


class BackupManager:
    """Manages automatic file backups before overwriting"""
    
    def __init__(self, max_backups: int = 5):
        """
        Initialize backup manager.
        
        Args:
            max_backups: Maximum number of backup files to keep
        """
        self.max_backups = max_backups
    
    def create_backup(self, file_path: str) -> Optional[str]:
        """
        Create a backup of an existing file.
        
        Args:
            file_path: Path to file to backup
            
        Returns:
            Path to backup file, or None if failed
        """
        file_path = Path(file_path).resolve()
        
        # Only backup if file exists
        if not file_path.exists():
            logger.debug(f"No backup needed - file does not exist: {file_path}")
            return None
        
        try:
            # Create backup filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"{file_path.stem}_backup_{timestamp}{file_path.suffix}"
            backup_path = file_path.parent / backup_name
            
            # Copy file to backup
            shutil.copy2(file_path, backup_path)
            file_size_mb = backup_path.stat().st_size / (1024 * 1024)
            logger.info(f"Created backup: {backup_path} ({file_size_mb:.2f} MB)")
            
            # Clean up old backups
            self._cleanup_old_backups(file_path)
            
            return str(backup_path)
            
        except Exception as e:
            logger.warning(f"Failed to create backup for {file_path}: {e}")
            return None
    
    def _cleanup_old_backups(self, original_file: Path):
        """Remove old backup files, keeping only the most recent ones"""
        try:
            # Find all backup files for this original file
            pattern = f"{original_file.stem}_backup_*{original_file.suffix}"
            backup_files = list(original_file.parent.glob(pattern))
            
            # Sort by modification time (newest first)
            backup_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
            
            # Remove old backups
            for old_backup in backup_files[self.max_backups:]:
                try:
                    old_backup.unlink()
                    logger.debug(f"Removed old backup: {old_backup}")
                except Exception as e:
                    logger.warning(f"Failed to remove old backup {old_backup}: {e}")
            
        except Exception as e:
            logger.warning(f"Failed to cleanup old backups: {e}")
    
    def restore_from_backup(self, original_file: str, backup_file: str) -> bool:
        """
        Restore a file from backup.
        
        Args:
            original_file: Path to restore to
            backup_file: Path to backup file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            shutil.copy2(backup_file, original_file)
            logger.info(f"Restored {original_file} from {backup_file}")
            return True
        except Exception as e:
            logger.error(f"Failed to restore from backup: {e}")
            return False


def safe_write_dataframe(
    df: pl.DataFrame,
    output_file: str,
    create_backup: bool = True,
    verify_write: bool = True,
    max_backups: int = 5,
    progress_callback: Optional[Callable] = None,
    **write_options
) -> bool:
    """
    Safely write DataFrame to CSV with automatic backup and verification.
    
    Features:
    - Creates backup of existing file
    - Writes to temporary file first
    - Verifies the written file
    - Atomic rename to final destination
    - File locking to prevent concurrent writes
    - Automatic cleanup on error
    
    Args:
        df: Polars DataFrame to write
        output_file: Path to output file
        create_backup: Whether to create backup of existing file
        verify_write: Whether to verify the written file
        max_backups: Maximum number of backups to keep
        progress_callback: Optional callback for progress updates
        **write_options: Additional options passed to write_csv()
    
    Returns:
        True if successful, False otherwise
    
    Example:
        safe_write_dataframe(
            df, 
            "output.csv", 
            include_header=True, 
            null_value=""
        )
    """
    output_path = Path(output_file).resolve()
    backup_path = None
    temp_path = None
    
    def log(message: str):
        """Helper for logging and progress callback"""
        logger.info(message)
        if progress_callback:
            progress_callback(message)
    
    try:
        # Step 1: Create backup if file exists
        if create_backup and output_path.exists():
            backup_manager = BackupManager(max_backups=max_backups)
            backup_path = backup_manager.create_backup(str(output_path))
            if backup_path:
                log(f"✓ Created backup: {Path(backup_path).name}")
        
        # Step 2: Write to temporary file
        temp_path = output_path.parent / f"{output_path.stem}.tmp{output_path.suffix}"
        
        log(f"Writing to temporary file...")
        try:
            with FileLock(str(temp_path), timeout=60):
                df.write_csv(str(temp_path), **write_options)
        except FileLockTimeout:
            log("ERROR: Temporary file is locked by another process")
            return False
        
        # Step 3: Verify the written file
        if verify_write:
            log("Verifying written data...")
            try:
                verify_df = pl.read_csv(str(temp_path), n_rows=10)
                
                # Check column count matches
                if len(verify_df.columns) != len(df.columns):
                    raise ValueError(
                        f"Column count mismatch: expected {len(df.columns)}, "
                        f"got {len(verify_df.columns)}"
                    )
                
                # Check file size is reasonable
                file_size = temp_path.stat().st_size
                if file_size < 100:  # Less than 100 bytes is suspicious
                    raise ValueError(f"Output file is suspiciously small: {file_size} bytes")
                
                log(f"✓ Verification passed ({file_size / (1024*1024):.2f} MB)")
                
            except Exception as e:
                logger.error(f"Verification failed: {e}")
                log(f"ERROR: Verification failed - {str(e)}")
                
                # Restore from backup if available
                if backup_path and Path(backup_path).exists():
                    log("Attempting to restore from backup...")
                    BackupManager().restore_from_backup(str(output_path), backup_path)
                
                return False
        
        # Step 4: Atomic rename to final destination
        try:
            # Use replace for atomic operation (overwrites if exists)
            temp_path.replace(output_path)
            log(f"✓ Successfully wrote {len(df):,} rows to {output_path.name}")
            
        except Exception as e:
            logger.error(f"Failed to rename temp file: {e}")
            log(f"ERROR: Failed to finalize write - {str(e)}")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to write file: {e}", exc_info=True)
        log(f"ERROR: Write failed - {str(e)}")
        
        # Cleanup temp file if it exists
        if temp_path and temp_path.exists():
            try:
                temp_path.unlink()
            except:
                pass
        
        # Restore from backup if available
        if backup_path and Path(backup_path).exists():
            log("Attempting to restore from backup...")
            if BackupManager().restore_from_backup(str(output_path), backup_path):
                log("✓ Restored from backup")
        
        return False
