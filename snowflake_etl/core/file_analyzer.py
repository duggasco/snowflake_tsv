"""
File analysis utilities for ETL operations
Fast row counting and processing time estimation
"""

import logging
import os
from pathlib import Path
from typing import Dict, Optional, Tuple

from snowflake_etl.core.progress import ProgressTracker


class FileAnalyzer:
    """
    Fast file analysis for row counting and time estimation
    Refactored to remove progress bar dependencies
    """
    
    # Realistic benchmark rates based on empirical testing
    BENCHMARKS = {
        'row_count': 500_000,         # Can count 500K rows/second (simple line counting)
        'quality_check': 50_000,      # Can QC 50K rows/second WITH date parsing/validation
        'compression': 25_000_000,    # Can compress 25MB/second (gzip level 1)
        'upload': 5_000_000,          # Can upload 5MB/second (typical network)
        'snowflake_copy': 100_000     # Snowflake processes 100K rows/second
    }
    
    def __init__(self, progress_tracker: Optional[ProgressTracker] = None):
        """
        Initialize file analyzer
        
        Args:
            progress_tracker: Optional progress tracker for reporting progress
        """
        self.logger = logging.getLogger(__name__)
        self.progress_tracker = progress_tracker
    
    def count_rows_fast(self, filepath: str, sample_size: int = 10000) -> Tuple[int, float]:
        """
        Quickly count rows using sampling and estimation
        
        Args:
            filepath: Path to the file
            sample_size: Number of lines to sample for estimation
            
        Returns:
            Tuple of (row_count, file_size_gb)
        """
        self.logger.debug(f"Counting rows in {filepath}")
        
        try:
            if not os.path.exists(filepath):
                self.logger.error(f"File not found: {filepath}")
                return 0, 0
            
            file_size = os.path.getsize(filepath)
            file_size_gb = file_size / (1024**3)
            
            # For small files, do exact count
            if file_size < 100_000_000:  # < 100MB
                with open(filepath, 'rb') as f:
                    row_count = sum(1 for _ in f)
                self.logger.debug(
                    f"File {filepath}: {row_count} rows, {file_size_gb:.2f} GB (exact count)"
                )
                return row_count, file_size_gb
            
            # For large files, estimate based on sample
            with open(filepath, 'rb') as f:
                sample = f.read(1_000_000)  # 1MB sample
                sample_lines = sample.count(b'\n')
                
                if sample_lines > 0:
                    bytes_per_line = len(sample) / sample_lines
                    estimated_rows = int(file_size / bytes_per_line)
                else:
                    estimated_rows = 0
            
            self.logger.debug(
                f"File {filepath} (estimated): {estimated_rows} rows, {file_size_gb:.2f} GB"
            )
            return estimated_rows, file_size_gb
            
        except Exception as e:
            self.logger.error(f"Error counting rows in {filepath}: {e}")
            return 0, 0
    
    def count_rows_accurate(self, filepath: str, chunk_size: int = 8 * 1024 * 1024) -> int:
        """
        Accurate row count by reading entire file
        
        Args:
            filepath: Path to the file
            chunk_size: Size of chunks to read (default 8MB)
            
        Returns:
            Exact row count
        """
        self.logger.debug(f"Starting accurate row count for {filepath}")
        
        file_size = os.path.getsize(filepath)
        rows = 0
        bytes_read = 0
        
        # Report to progress tracker if available
        if self.progress_tracker:
            self.progress_tracker.start_file(filepath, file_size)
        
        with open(filepath, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                
                rows += chunk.count(b'\n')
                bytes_read += len(chunk)
                
                # Update progress if tracker available
                if self.progress_tracker:
                    self.progress_tracker.update_progress(bytes_processed=len(chunk))
        
        # Complete file in progress tracker
        if self.progress_tracker:
            self.progress_tracker.complete_file(success=True)
        
        self.logger.debug(f"Accurate count complete: {rows} rows")
        return rows
    
    def estimate_processing_time(
        self, 
        row_count: int, 
        file_size_gb: float,
        num_workers: int = 1,
        skip_qc: bool = False
    ) -> Dict[str, float]:
        """
        Estimate time for each processing step
        
        Args:
            row_count: Number of rows in the file
            file_size_gb: File size in GB
            num_workers: Number of parallel workers
            skip_qc: Whether quality checks will be skipped
            
        Returns:
            Dictionary of step names to estimated seconds
        """
        estimates = {}
        
        # Calculate parallel efficiency factor
        parallel_factor = self._calculate_parallel_factor(num_workers)
        
        self.logger.debug(f"Workers: {num_workers}, Effective parallel factor: {parallel_factor:.1f}")
        
        # Row counting (already done in analysis phase)
        estimates['row_counting'] = 0
        
        # Quality checks (if not skipped)
        if not skip_qc:
            qc_rate = self.BENCHMARKS['quality_check'] * parallel_factor
            estimates['quality_checks'] = row_count / qc_rate if row_count > 0 else 0
        else:
            estimates['quality_checks'] = 0
        
        # Compression
        compression_rate_mb = (self.BENCHMARKS['compression'] / (1024 * 1024)) * min(parallel_factor, 4)
        file_size_mb = file_size_gb * 1024
        estimates['compression'] = file_size_mb / compression_rate_mb if file_size_mb > 0 else 0
        
        # Upload to Snowflake
        compressed_size_mb = file_size_gb * 1024 * 0.15  # Assume 15% compression ratio
        upload_rate_mb = (self.BENCHMARKS['upload'] / (1024 * 1024)) * min(parallel_factor, 8)
        estimates['upload'] = compressed_size_mb / upload_rate_mb if compressed_size_mb > 0 else 0
        
        # Snowflake COPY operation
        copy_rate = self.BENCHMARKS['snowflake_copy'] * min(parallel_factor, 4)
        estimates['snowflake_copy'] = row_count / copy_rate if row_count > 0 else 0
        
        # Overhead for process creation, coordination, etc.
        estimates['overhead'] = 5 + (num_workers * 0.5)
        
        # Total
        estimates['total'] = sum(estimates.values())
        
        self.logger.debug(
            f"Time estimates: QC={estimates['quality_checks']:.1f}s, "
            f"Compression={estimates['compression']:.1f}s, "
            f"Upload={estimates['upload']:.1f}s, "
            f"Copy={estimates['snowflake_copy']:.1f}s, "
            f"Overhead={estimates['overhead']:.1f}s, "
            f"Total={estimates['total']:.1f}s"
        )
        
        return estimates
    
    def _calculate_parallel_factor(self, num_workers: int) -> float:
        """
        Calculate effective parallelism factor based on diminishing returns
        
        Args:
            num_workers: Number of parallel workers
            
        Returns:
            Effective parallel factor
        """
        if num_workers <= 1:
            return 1.0
        elif num_workers <= 4:
            return num_workers * 0.9  # 90% efficiency
        elif num_workers <= 8:
            return 4 + (num_workers - 4) * 0.7  # 70% efficiency
        elif num_workers <= 16:
            return 6.8 + (num_workers - 8) * 0.5  # 50% efficiency
        elif num_workers <= 32:
            return 10.8 + (num_workers - 16) * 0.3  # 30% efficiency
        else:
            return 15.6 + (num_workers - 32) * 0.1  # 10% efficiency beyond 32
    
    def analyze_file(self, filepath: str, quick: bool = True) -> Dict[str, any]:
        """
        Comprehensive file analysis
        
        Args:
            filepath: Path to the file
            quick: Use quick estimation vs accurate counting
            
        Returns:
            Dictionary with analysis results
        """
        path = Path(filepath)
        
        if not path.exists():
            return {
                'exists': False,
                'error': f"File not found: {filepath}"
            }
        
        file_size = path.stat().st_size
        
        if quick:
            row_count, size_gb = self.count_rows_fast(filepath)
        else:
            row_count = self.count_rows_accurate(filepath)
            size_gb = file_size / (1024**3)
        
        return {
            'exists': True,
            'filepath': filepath,
            'filename': path.name,
            'file_size_bytes': file_size,
            'file_size_gb': size_gb,
            'row_count': row_count,
            'avg_row_size': file_size / row_count if row_count > 0 else 0
        }