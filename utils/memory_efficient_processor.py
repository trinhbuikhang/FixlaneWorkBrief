"""
Memory-efficient Add Columns Processor for very large datasets (>20GB)
Uses streaming processing and memory mapping to handle files that don't fit in RAM
"""

import polars as pl
import os
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Callable, Dict, Any
import psutil
import gc

logger = logging.getLogger(__name__)

class MemoryEfficientAddColumnsProcessor:
    """
    Memory-efficient processor for very large datasets.
    Uses streaming processing and temporary index files for optimal memory usage.
    """
    
    def __init__(self, progress_callback: Optional[Callable[[str, Optional[float]], None]] = None):
        self.progress_callback = progress_callback
        self._cancelled = False
        
    def _emit_progress(self, message: str, progress: Optional[float] = None):
        """Emit progress message and optionally update progress bar."""
        if self.progress_callback:
            self.progress_callback(message, progress)
        logger.info(message)
    
    def _get_memory_info(self) -> Dict[str, float]:
        """Get current memory usage information."""
        process = psutil.Process()
        memory_info = process.memory_info()
        virtual_memory = psutil.virtual_memory()
        
        return {
            'process_memory_mb': memory_info.rss / 1024 / 1024,
            'available_memory_mb': virtual_memory.available / 1024 / 1024,
            'memory_percent': virtual_memory.percent
        }
    
    def _estimate_file_size_gb(self, file_path: str) -> float:
        """Estimate file size in GB."""
        try:
            size_bytes = os.path.getsize(file_path)
            return size_bytes / (1024 ** 3)  # Convert to GB
        except:
            return 0.0
    
    def _choose_processing_strategy(self, lmd_file: str, details_file: str) -> str:
        """Choose processing strategy based on file sizes and available memory."""
        lmd_size = self._estimate_file_size_gb(lmd_file)
        details_size = self._estimate_file_size_gb(details_file)
        total_size = lmd_size + details_size
        
        memory_info = self._get_memory_info()
        available_gb = memory_info['available_memory_mb'] / 1024
        
        self._emit_progress(f"📊 MEMORY ANALYSIS:")
        self._emit_progress(f"   • LMD file size: {lmd_size:.2f} GB")
        self._emit_progress(f"   • Details file size: {details_size:.2f} GB")
        self._emit_progress(f"   • Total data size: {total_size:.2f} GB")
        self._emit_progress(f"   • Available memory: {available_gb:.2f} GB")
        self._emit_progress(f"   • Memory usage: {memory_info['memory_percent']:.1f}%")
        
        if total_size > available_gb * 0.8:  # If files > 80% of available memory
            strategy = "streaming"
            self._emit_progress(f"🔄 Strategy: STREAMING (files too large for memory)")
        elif lmd_size > 10:  # If LMD > 10GB
            strategy = "hybrid"
            self._emit_progress(f"🔄 Strategy: HYBRID (large LMD, efficient processing)")
        else:
            strategy = "memory"
            self._emit_progress(f"🔄 Strategy: IN-MEMORY (files fit comfortably)")
            
        return strategy
    
    def _create_lmd_index(self, lmd_file: str, chunk_size: int = 50000) -> str:
        """Create an indexed temporary file from LMD data for fast lookups."""
        try:
            self._emit_progress("📑 Creating LMD index for fast lookups...")
            
            # Create temporary index file
            index_file = lmd_file.replace('.csv', '_temp_index.parquet')
            
            # Check if index already exists and is newer than source
            if os.path.exists(index_file):
                index_mtime = os.path.getmtime(index_file)
                source_mtime = os.path.getmtime(lmd_file)
                if index_mtime > source_mtime:
                    self._emit_progress("✓ Using existing LMD index file")
                    return index_file
            
            # Columns needed for indexing
            index_columns = ['Filename', 'lmd_sequence_num', 'TestDateUTC']
            
            processed_rows = 0
            index_chunks = []
            
            # Process LMD in chunks to create index
            batched_reader = pl.read_csv_batched(
                lmd_file,
                batch_size=chunk_size,
                null_values=['∞'],
                infer_schema_length=0
            )
            
            # Process all batches
            batches = batched_reader.next_batches(1)
            
            while batches:
                for chunk in batches:
                    if chunk is None or chunk.is_empty():
                        continue
                        
                    if self._cancelled:
                        return None
                    
                    # Check if all required columns exist
                    missing_cols = [col for col in index_columns if col not in chunk.columns]
                    if missing_cols:
                        self._emit_progress(f"⚠️ Missing columns in LMD: {missing_cols}")
                        continue
                    
                    # Select only needed columns and add timestamp
                    chunk_indexed = chunk.select(index_columns).with_columns(
                        pl.when(pl.col("TestDateUTC").is_not_null() & pl.col("TestDateUTC").str.contains("T"))
                        .then(
                            pl.when(pl.col("TestDateUTC").str.ends_with("Z"))
                            .then(pl.col("TestDateUTC").str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%.fZ", strict=False))
                            .otherwise(pl.col("TestDateUTC").str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%.f", strict=False))
                        )
                        .otherwise(
                            pl.when(pl.col("TestDateUTC").is_not_null())
                            .then(pl.col("TestDateUTC").str.strptime(pl.Datetime, format="%d/%m/%Y %H:%M:%S%.f", strict=False))
                            .otherwise(None)
                        )
                        .alias("_timestamp")
                    )
                    
                    # Filter out invalid timestamps
                    chunk_indexed = chunk_indexed.filter(pl.col("_timestamp").is_not_null())
                    
                    if not chunk_indexed.is_empty():
                        index_chunks.append(chunk_indexed)
                    
                    processed_rows += len(chunk)
                    
                    if processed_rows % 100000 == 0:
                        self._emit_progress(f"   Indexed {processed_rows:,} LMD rows...")
                
                # Get next batch
                batches = batched_reader.next_batches(1)
            
            if not index_chunks:
                self._emit_progress("❌ No valid LMD data found for indexing")
                return None
                
            # Combine and sort index
            self._emit_progress("🔄 Combining and sorting index...")
            full_index = pl.concat(index_chunks, how='vertical')
            
            # Remove duplicates and sort by timestamp
            full_index = full_index.unique(subset=['Filename', 'lmd_sequence_num'], keep='first').sort('_timestamp')
            
            # Save as Parquet for fast loading
            full_index.write_parquet(index_file, compression='snappy')
            
            self._emit_progress(f"✓ Created LMD index: {len(full_index):,} unique records")
            
            # Clean up memory
            del index_chunks, full_index
            gc.collect()
            
            return index_file
            
        except Exception as e:
            self._emit_progress(f"❌ Failed to create LMD index: {e}")
            logger.error(f"Failed to create LMD index: {e}", exc_info=True)
            return None
    
    def _process_with_streaming(self, lmd_file: str, details_file: str, 
                               selected_columns: List[str], chunk_size: int = 10000) -> Optional[str]:
        """Process using streaming method for very large files."""
        lmd_index_file = None
        try:
            self._emit_progress("🌊 STREAMING PROCESSING MODE")
            self._emit_progress("=" * 60)
            
            # Step 1: Create LMD index
            lmd_index_file = self._create_lmd_index(lmd_file, chunk_size)
            if not lmd_index_file:
                return None
                
            # Step 2: Load index into memory (much smaller than full LMD)
            self._emit_progress("📥 Loading LMD index into memory...")
            lmd_index = pl.read_parquet(lmd_index_file)
            
            memory_info = self._get_memory_info()
            self._emit_progress(f"✓ Index loaded: {len(lmd_index):,} records ({memory_info['process_memory_mb']:.1f} MB used)")
            
            # Step 3: Prepare output file
            base_path = os.path.dirname(details_file)
            current_date = datetime.now().strftime("%Y-%m-%d")
            base_name = os.path.splitext(os.path.basename(details_file))[0]
            output_file = os.path.join(base_path, f"{base_name}_updated_{current_date}.csv")
            
            # Step 4: Process details file in streaming chunks
            result = self._stream_process_details(details_file, output_file, lmd_index, 
                                              selected_columns, chunk_size)
            
            # Cleanup temporary index file
            if lmd_index_file and os.path.exists(lmd_index_file):
                try:
                    os.remove(lmd_index_file)
                    self._emit_progress("🧹 Cleaned up temporary index file")
                except Exception as e:
                    logger.warning(f"Could not remove temporary index file: {e}")
            
            return result
                                              
        except Exception as e:
            self._emit_progress(f"❌ Streaming processing failed: {e}")
            logger.error(f"Streaming processing failed: {e}", exc_info=True)
            
            # Cleanup on error
            if lmd_index_file and os.path.exists(lmd_index_file):
                try:
                    os.remove(lmd_index_file)
                except:
                    pass
            
            return None
    
    def _stream_process_details(self, details_file: str, output_file: str,
                               lmd_index: pl.DataFrame, selected_columns: List[str],
                               chunk_size: int) -> Optional[str]:
        """Stream process details file with LMD index lookups."""
        try:
            # Get details file structure
            details_sample = pl.read_csv(details_file, n_rows=0, null_values=['∞'], infer_schema_length=0)
            existing_columns = details_sample.columns
            
            new_columns = [col for col in selected_columns if col not in existing_columns]
            final_columns = list(existing_columns) + new_columns
            
            self._emit_progress(f"📝 Output columns: {len(final_columns)} total ({len(new_columns)} new)")
            
            processed_rows = 0
            total_matches = 0
            first_chunk = True
            
            # Process in chunks
            batched_reader = pl.read_csv_batched(
                details_file,
                batch_size=chunk_size,
                null_values=['∞'],
                infer_schema_length=0
            )
            
            with open(output_file, 'w', encoding='utf-8', newline='') as f:
                # Process all batches
                batches = batched_reader.next_batches(1)
                
                while batches:
                    for chunk in batches:
                        if chunk is None or chunk.is_empty():
                            continue
                            
                        if self._cancelled:
                            return None
                        
                        # Check if TestDateUTC exists
                        if 'TestDateUTC' not in chunk.columns:
                            self._emit_progress("⚠️ TestDateUTC column not found in details file")
                            continue
                        
                        # Add timestamp for matching
                        chunk = chunk.with_columns(
                            pl.when(pl.col("TestDateUTC").is_not_null() & pl.col("TestDateUTC").str.contains("T"))
                            .then(
                                pl.when(pl.col("TestDateUTC").str.ends_with("Z"))
                                .then(pl.col("TestDateUTC").str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%.fZ", strict=False))
                                .otherwise(pl.col("TestDateUTC").str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%.f", strict=False))
                            )
                            .otherwise(
                                pl.when(pl.col("TestDateUTC").is_not_null())
                                .then(pl.col("TestDateUTC").str.strptime(pl.Datetime, format="%d/%m/%Y %H:%M:%S%.f", strict=False))
                                .otherwise(None)
                            )
                            .alias("_timestamp")
                        )
                        
                        # Filter out rows with invalid timestamps
                        valid_chunk = chunk.filter(pl.col("_timestamp").is_not_null())
                        
                        if valid_chunk.is_empty():
                            self._emit_progress(f"⚠️ No valid timestamps in chunk, skipping...")
                            continue
                        
                        # Prepare LMD columns for join - only select columns that exist
                        lmd_join_cols = ['_timestamp'] + [col for col in selected_columns if col in lmd_index.columns]
                        
                        # Join with LMD index using asof join
                        try:
                            matched_chunk = valid_chunk.join_asof(
                                lmd_index.select(lmd_join_cols),
                                on='_timestamp',
                                tolerance='60s'
                            )
                        except Exception as join_error:
                            self._emit_progress(f"⚠️ Join failed: {join_error}")
                            matched_chunk = valid_chunk
                        
                        # Count matches in this chunk
                        available_selected_cols = [col for col in selected_columns if col in matched_chunk.columns]
                        if available_selected_cols:
                            chunk_matches = matched_chunk.filter(
                                pl.any_horizontal([pl.col(col).is_not_null() for col in available_selected_cols])
                            ).height
                        else:
                            chunk_matches = 0
                        
                        total_matches += chunk_matches
                        
                        # Add missing columns as null
                        for col in new_columns:
                            if col not in matched_chunk.columns:
                                matched_chunk = matched_chunk.with_columns(pl.lit(None).alias(col))
                        
                        # Select final columns and remove timestamp
                        final_chunk = matched_chunk.select(final_columns)
                        
                        # Write to output
                        if first_chunk:
                            final_chunk.write_csv(f, include_header=True)
                            first_chunk = False
                        else:
                            final_chunk.write_csv(f, include_header=False)
                        
                        processed_rows += len(chunk)
                        
                        # Progress update every 50k rows
                        if processed_rows % 50000 == 0:
                            memory_info = self._get_memory_info()
                            match_rate = (total_matches / processed_rows * 100) if processed_rows > 0 else 0
                            self._emit_progress(f"   📊 Processed: {processed_rows:,} | Matches: {total_matches:,} ({match_rate:.1f}%) | Memory: {memory_info['process_memory_mb']:.1f}MB")
                    
                    # Get next batch
                    batches = batched_reader.next_batches(1)
            
            # Final statistics
            match_rate = (total_matches / processed_rows * 100) if processed_rows > 0 else 0
            self._emit_progress("")
            self._emit_progress("🎯 FINAL STATISTICS:")
            self._emit_progress(f"   • Total records processed: {processed_rows:,}")
            self._emit_progress(f"   • Records with matches: {total_matches:,}")
            self._emit_progress(f"   • Match rate: {match_rate:.2f}%")
            self._emit_progress(f"   • Output file: {output_file}")
            
            return output_file
            
        except Exception as e:
            self._emit_progress(f"❌ Stream processing failed: {e}")
            logger.error(f"Stream processing failed: {e}", exc_info=True)
            return None
    
    def process_add_columns(self, lmd_file_path: str, details_file_path: str,
                           selected_columns: List[str], chunk_size: int = None) -> Optional[str]:
        """
        Main entry point for memory-efficient add columns processing.
        
        Args:
            lmd_file_path: Path to LMD CSV file
            details_file_path: Path to Combined Details CSV file
            selected_columns: List of columns to add/update
            chunk_size: Number of rows to process per chunk (auto-determined if None)
        
        Returns:
            Path to output file if successful, None otherwise
        """
        try:
            self._emit_progress("🚀 MEMORY-EFFICIENT ADD COLUMNS PROCESSOR")
            self._emit_progress("=" * 80)
            
            # Auto-determine chunk size based on available memory
            if chunk_size is None:
                memory_info = self._get_memory_info()
                available_gb = memory_info['available_memory_mb'] / 1024
                
                if available_gb > 16:
                    chunk_size = 50000  # Large chunks for high-memory systems
                elif available_gb > 8:
                    chunk_size = 25000  # Medium chunks
                elif available_gb > 4:
                    chunk_size = 10000  # Small chunks
                else:
                    chunk_size = 5000   # Very small chunks for low-memory systems
                    
                self._emit_progress(f"🎯 Auto-selected chunk size: {chunk_size:,} (based on {available_gb:.1f}GB available)")
            
            # Validate files
            if not os.path.exists(lmd_file_path):
                self._emit_progress(f"❌ LMD file not found: {lmd_file_path}")
                return None
                
            if not os.path.exists(details_file_path):
                self._emit_progress(f"❌ Details file not found: {details_file_path}")
                return None
            
            # Choose processing strategy
            strategy = self._choose_processing_strategy(lmd_file_path, details_file_path)
            
            # Process based on strategy
            if strategy == "streaming":
                return self._process_with_streaming(lmd_file_path, details_file_path, selected_columns, chunk_size)
            else:
                # Fallback to original processor for smaller files
                self._emit_progress("📝 File sizes manageable - using standard processor")
                try:
                    from .add_columns_processor import AddColumnsProcessor
                    standard_processor = AddColumnsProcessor(self.progress_callback)
                    return standard_processor.process_add_columns(lmd_file_path, details_file_path, selected_columns, chunk_size)
                except ImportError:
                    self._emit_progress("⚠️ Standard processor not available, using streaming mode")
                    return self._process_with_streaming(lmd_file_path, details_file_path, selected_columns, chunk_size)
                
        except Exception as e:
            self._emit_progress(f"❌ Processing failed: {e}")
            logger.error(f"Processing failed: {e}", exc_info=True)
            return None
    
    def cancel(self):
        """Cancel the current processing operation."""
        self._cancelled = True
        self._emit_progress("⏹️ Processing cancelled by user")