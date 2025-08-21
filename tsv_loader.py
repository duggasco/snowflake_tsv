#!/usr/bin/env python3
"""
Enhanced TSV to Snowflake Loader - Complete Version with Debug Logging
All functionality restored with extensive debugging capabilities
"""

import argparse
import json
import os
import sys
import time
import threading
import multiprocessing
import gzip
import csv
import re
import snowflake.connector
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed, TimeoutError
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from dataclasses import dataclass
from collections import defaultdict
from pathlib import Path
import logging
import pandas as pd
import traceback
import signal

# CREATE LOGS DIRECTORY FIRST - Before any logging setup
os.makedirs('logs', exist_ok=True)

# Defer logging setup until we parse arguments to check for --quiet flag
def setup_logging(quiet_mode=False):
    """Setup logging configuration based on quiet mode"""
    handlers = [logging.FileHandler('logs/tsv_loader_debug.log')]
    
    # Only add console handler if NOT in quiet mode
    if not quiet_mode:
        handlers.append(logging.StreamHandler(sys.stdout))
    
    logging.basicConfig(
        level=logging.DEBUG,  # Keep DEBUG level for detailed troubleshooting
        format='%(asctime)s - %(processName)s - %(threadName)s - %(levelname)s - %(message)s',
        handlers=handlers,
        force=True  # Force reconfiguration if already configured
    )
    
    logger = logging.getLogger(__name__)
    logger.info("="*60)
    logger.info("TSV LOADER STARTING")
    logger.info("Python version: {}".format(sys.version))
    logger.info("Process ID: {}".format(os.getpid()))
    logger.info("Quiet mode: {}".format(quiet_mode))
    logger.info("="*60)
    return logger

# Initial minimal logger for early errors
logger = logging.getLogger(__name__)

# For progress bar
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
    logger.debug("tqdm is available for progress bars")
except ImportError:
    TQDM_AVAILABLE = False
    logger.warning("tqdm not available - install for progress bars: pip install tqdm")

# Try to import psutil for memory checking
try:
    import psutil
    PSUTIL_AVAILABLE = True
    logger.debug("psutil is available for system monitoring")
except ImportError:
    PSUTIL_AVAILABLE = False
    logger.debug("psutil not available - install for memory monitoring: pip install psutil")

@dataclass
class FileConfig:
    file_path: str
    table_name: str
    expected_columns: List[str]
    date_column: str
    expected_date_range: tuple

class ProgressTracker:
    """Track and display progress across multiple files"""

    def __init__(self, total_files: int, total_rows: int, total_size_gb: float, month: str = None, show_qc_progress: bool = True):
        self.total_files = total_files
        self.total_rows = total_rows
        self.total_size_gb = total_size_gb
        self.total_size_mb = total_size_gb * 1024  # Convert GB to MB for compression tracking
        self.processed_files = 0
        self.processed_rows = 0
        self.compressed_mb = 0
        self.start_time = time.time()
        self.lock = threading.Lock()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.month = month
        self.show_qc_progress = show_qc_progress  # Whether to show row-by-row QC progress
        self.current_file = None  # Track which file is being processed
        self.file_sizes = {}  # Store individual file sizes

        # Calculate position offset based on month/job ID for parallel processing
        # Use environment variable set by wrapper script for position
        self.position_offset = 0
        try:
            import os
            # Try to get job position from environment variable
            job_position = os.environ.get('TSV_JOB_POSITION', '')
            if job_position:
                # 5 lines per job if showing QC progress, 4 lines if not
                # Files, QC (optional), Compression, Upload, COPY
                lines_per_job = 5 if self.show_qc_progress else 4
                self.position_offset = int(job_position) * lines_per_job
            elif month:
                # Fallback: extract numeric part from month (e.g., "2024-01" -> 1)
                month_num = int(month.split('-')[-1])
                lines_per_job = 5 if self.show_qc_progress else 4
                self.position_offset = (month_num - 1) * lines_per_job
        except:
            # Default: no offset
            self.position_offset = 0

        # Progress bars if tqdm available
        if TQDM_AVAILABLE:
            # Add month prefix to descriptions if provided
            desc_prefix = "[{}] ".format(month) if month else ""
            
            # Use position parameter to stack progress bars
            # Note: tqdm handles positioning automatically, no need for manual spacing
            
            self.file_pbar = tqdm(total=total_files, 
                                 desc="{}Files".format(desc_prefix), 
                                 unit="file",
                                 position=self.position_offset,
                                 leave=False,  # Clean up after completion
                                 file=sys.stderr)
            
            # Only show rows progress bar if doing file-based QC
            if self.show_qc_progress:
                self.row_pbar = tqdm(total=total_rows, 
                                    desc="{}QC Progress".format(desc_prefix),  # Clarify this is for QC
                                    unit="rows", 
                                    unit_scale=True,
                                    position=self.position_offset + 1,
                                    leave=False,  # Clean up after completion
                                    file=sys.stderr)
            else:
                self.row_pbar = None  # No row progress when skipping QC
                
            # Create a placeholder for compression bar - will update per file
            self.compress_pbar = None
            self.compress_position = self.position_offset + 2 if self.show_qc_progress else self.position_offset + 1
            
            # Placeholders for upload and copy bars
            self.upload_pbar = None
            self.upload_position = self.compress_position + 1
            self.copy_pbar = None
            self.copy_position = self.upload_position + 1
            
            self.desc_prefix = desc_prefix
            self.logger.debug("Progress bars initialized with position offset {}".format(self.position_offset))

    def start_file_compression(self, filename: str, file_size_mb: float):
        """Start compression progress for a specific file"""
        import os
        with self.lock:
            self.current_file = filename
            if TQDM_AVAILABLE:
                if self.compress_pbar is None:
                    # First time - create the bar
                    self.compress_pbar = tqdm(total=file_size_mb,
                                            desc="{}Compressing {}".format(self.desc_prefix, os.path.basename(filename)),
                                            unit="MB",
                                            unit_scale=True,
                                            position=self.compress_position,
                                            leave=True,  # Keep bar for reuse
                                            file=sys.stderr)
                else:
                    # Reuse existing bar - reset it for the new file
                    self.compress_pbar.reset(total=file_size_mb)
                    self.compress_pbar.set_description("{}Compressing {}".format(self.desc_prefix, os.path.basename(filename)))
                    self.compress_pbar.refresh()
    
    def start_file_upload(self, filename: str, file_size_mb: float):
        """Start upload progress for a specific file"""
        import os
        with self.lock:
            if TQDM_AVAILABLE:
                if self.upload_pbar is None:
                    # First time - create the bar
                    self.upload_pbar = tqdm(total=file_size_mb,
                                          desc="{}Uploading {}".format(self.desc_prefix, os.path.basename(filename)),
                                          unit="MB",
                                          unit_scale=True,
                                          position=self.upload_position,
                                          leave=True,  # Keep bar for reuse
                                          file=sys.stderr)
                else:
                    # Reuse existing bar - reset it for the new file
                    self.upload_pbar.reset(total=file_size_mb)
                    self.upload_pbar.set_description("{}Uploading {}".format(self.desc_prefix, os.path.basename(filename)))
                    self.upload_pbar.refresh()
    
    def start_copy_operation(self, table_name: str, row_count: int):
        """Start COPY progress for Snowflake operation"""
        with self.lock:
            if TQDM_AVAILABLE:
                if self.copy_pbar is None:
                    # First time - create the bar
                    self.copy_pbar = tqdm(total=row_count,
                                        desc="{}Loading into {}".format(self.desc_prefix, table_name),
                                        unit="rows",
                                        unit_scale=True,
                                        position=self.copy_position,
                                        leave=True,  # Keep bar for reuse
                                        file=sys.stderr)
                else:
                    # Reuse existing bar - reset it for the new table
                    self.copy_pbar.reset(total=row_count)
                    self.copy_pbar.set_description("{}Loading into {}".format(self.desc_prefix, table_name))
                    self.copy_pbar.refresh()
    
    def update(self, files: int = 0, rows: int = 0, compressed_mb: float = 0, 
                uploaded_mb: float = 0, copied_rows: int = 0):
        """Update progress"""
        with self.lock:
            self.processed_files += files
            self.processed_rows += rows
            self.compressed_mb += compressed_mb

            if TQDM_AVAILABLE:
                if files > 0:
                    self.file_pbar.update(files)
                if rows > 0 and self.row_pbar:
                    self.row_pbar.update(rows)
                if compressed_mb > 0 and self.compress_pbar:
                    self.compress_pbar.update(compressed_mb)
                if uploaded_mb > 0 and self.upload_pbar:
                    self.upload_pbar.update(uploaded_mb)
                if copied_rows > 0 and self.copy_pbar:
                    self.copy_pbar.update(copied_rows)

            self.logger.debug("Progress: {}/{} files, {}/{} rows, {:.1f}/{:.1f} MB compressed".format(
                self.processed_files, self.total_files,
                self.processed_rows, self.total_rows,
                self.compressed_mb, self.total_size_mb))

    def get_eta(self) -> str:
        """Calculate estimated time remaining"""
        elapsed = time.time() - self.start_time
        if self.processed_rows > 0:
            rate = self.processed_rows / elapsed
            remaining_rows = self.total_rows - self.processed_rows
            eta_seconds = remaining_rows / rate if rate > 0 else 0
            return str(timedelta(seconds=int(eta_seconds)))
        return "Unknown"

    def clear_file_bars(self):
        """Clear file-specific progress bars between files"""
        with self.lock:
            if TQDM_AVAILABLE:
                # Clear the bars from display but keep them for reuse
                if self.compress_pbar:
                    self.compress_pbar.clear()
                if self.upload_pbar:
                    self.upload_pbar.clear()
                if self.copy_pbar:
                    self.copy_pbar.clear()
    
    def close(self):
        """Close progress bars"""
        if TQDM_AVAILABLE:
            # Clear all bars first to remove from display
            if self.compress_pbar:
                self.compress_pbar.clear()
                self.compress_pbar.close()
            if self.upload_pbar:
                self.upload_pbar.clear()
                self.upload_pbar.close()
            if self.copy_pbar:
                self.copy_pbar.clear()
                self.copy_pbar.close()
            # Close the main bars
            self.file_pbar.close()
            if self.row_pbar:
                self.row_pbar.close()
        self.logger.debug("Progress tracker closed")

class FileAnalyzer:
    """Fast file analysis for row counting and time estimation"""

    # REALISTIC benchmark rates based on actual performance
    BENCHMARKS = {
        'row_count': 500_000,         # Can count 500K rows/second (simple line counting)
        'quality_check': 50_000,      # Can QC 50K rows/second WITH date parsing/validation
        'compression': 25_000_000,    # Can compress 25MB/second = 25,000,000 bytes/second (gzip level 1)
        'upload': 5_000_000,          # Can upload 5MB/second = 5,000,000 bytes/second (typical network)
        'snowflake_copy': 100_000     # Snowflake processes 100K rows/second (includes parsing)
    }

    @staticmethod
    def count_rows_fast(filepath: str, sample_size: int = 10000) -> Tuple[int, float]:
        """Quickly count rows using sampling and estimation"""
        logger.debug("Counting rows in {}".format(filepath))
        try:
            if not os.path.exists(filepath):
                logger.error("File not found: {}".format(filepath))
                return 0, 0

            file_size = os.path.getsize(filepath)
            file_size_gb = file_size / (1024**3)

            if file_size < 100_000_000:  # < 100MB
                with open(filepath, 'rb') as f:
                    row_count = sum(1 for _ in f)
                logger.debug("File {}: {} rows, {:.2f} GB (exact count)".format(
                    filepath, row_count, file_size_gb))
                return row_count, file_size_gb

            # For large files, estimate
            with open(filepath, 'rb') as f:
                sample = f.read(1_000_000)  # 1MB sample
                sample_lines = sample.count(b'\n')

                if sample_lines > 0:
                    bytes_per_line = len(sample) / sample_lines
                    estimated_rows = int(file_size / bytes_per_line)
                else:
                    estimated_rows = 0

            logger.debug("File {} (estimated): {} rows, {:.2f} GB".format(
                filepath, estimated_rows, file_size_gb))
            return estimated_rows, file_size_gb
        except Exception as e:
            logger.error("Error counting rows in {}: {}".format(filepath, e))
            return 0, 0

    @staticmethod
    def count_rows_accurate(filepath: str, show_progress: bool = True) -> int:
        """Accurate row count with progress bar"""
        logger.debug("Starting accurate row count for {}".format(filepath))
        file_size = os.path.getsize(filepath)
        rows = 0
        bytes_read = 0

        if show_progress and TQDM_AVAILABLE:
            # Get position offset from environment for parallel jobs
            position_offset = 0
            try:
                import os
                # Try to get job position from environment variable set by wrapper script
                job_position = os.environ.get('TSV_JOB_POSITION', '0')
                position_offset = int(job_position) * 4  # 4 lines per job
            except:
                position_offset = 0
            
            pbar = tqdm(total=file_size, unit='B', unit_scale=True, desc="Counting rows", 
                       position=position_offset + 3, leave=False, file=sys.stderr)

        with open(filepath, 'rb') as f:
            while True:
                chunk = f.read(8192 * 1024)  # 8MB chunks
                if not chunk:
                    break
                rows += chunk.count(b'\n')
                bytes_read += len(chunk)

                if show_progress and TQDM_AVAILABLE:
                    pbar.update(len(chunk))

        if show_progress and TQDM_AVAILABLE:
            pbar.close()

        logger.debug("Accurate count complete: {} rows".format(rows))
        return rows

    @staticmethod
    def estimate_processing_time(row_count: int, file_size_gb: float,
                                num_workers: int = 1) -> Dict[str, float]:
        """Estimate time for each processing step with REALISTIC benchmarks"""
        estimates = {}

        # Parallel processing has diminishing returns due to:
        # - Python GIL for threads
        # - Inter-process communication overhead
        # - File I/O bottlenecks
        # - Memory bandwidth limits

        # More realistic parallel efficiency curve
        if num_workers <= 1:
            parallel_factor = 1.0
        elif num_workers <= 4:
            parallel_factor = num_workers * 0.9  # 90% efficiency
        elif num_workers <= 8:
            parallel_factor = 4 + (num_workers - 4) * 0.7  # 70% efficiency
        elif num_workers <= 16:
            parallel_factor = 6.8 + (num_workers - 8) * 0.5  # 50% efficiency
        elif num_workers <= 32:
            parallel_factor = 10.8 + (num_workers - 16) * 0.3  # 30% efficiency
        else:
            parallel_factor = 15.6 + (num_workers - 32) * 0.1  # 10% efficiency beyond 32

        logger.debug("Workers: {}, Effective parallel factor: {:.1f}".format(
            num_workers, parallel_factor))

        # Row counting (already done in analysis phase)
        estimates['row_counting'] = 0

        # Quality checks - MUCH slower than expected due to:
        # - CSV parsing overhead
        # - Date string parsing for EVERY row
        # - Missing date calculation
        # - Python interpreter overhead
        qc_rate = FileAnalyzer.BENCHMARKS['quality_check'] * parallel_factor
        estimates['quality_checks'] = row_count / qc_rate if row_count > 0 else 0

        # Compression - somewhat parallel but disk I/O bound
        # Convert compression rate from bytes/sec to MB/sec
        compression_rate_mb = (FileAnalyzer.BENCHMARKS['compression'] / (1024 * 1024)) * min(parallel_factor, 4)
        file_size_mb = file_size_gb * 1024
        estimates['compression'] = file_size_mb / compression_rate_mb if file_size_mb > 0 else 0

        # Upload to Snowflake - limited by network, not very parallel
        compressed_size_mb = file_size_gb * 1024 * 0.15  # Assume 15% compression ratio
        # Convert upload rate from bytes/sec to MB/sec
        upload_rate_mb = (FileAnalyzer.BENCHMARKS['upload'] / (1024 * 1024)) * min(parallel_factor, 8)
        estimates['upload'] = compressed_size_mb / upload_rate_mb if compressed_size_mb > 0 else 0

        # Snowflake COPY operation - limited parallelism on Snowflake side
        copy_rate = FileAnalyzer.BENCHMARKS['snowflake_copy'] * min(parallel_factor, 4)
        estimates['snowflake_copy'] = row_count / copy_rate if row_count > 0 else 0

        # Add overhead for process creation, coordination, etc.
        estimates['overhead'] = 5 + (num_workers * 0.5)  # More workers = more overhead

        # Total
        estimates['total'] = sum(estimates.values())

        logger.debug("Realistic time estimates: QC={:.1f}s, Compression={:.1f}s, Upload={:.1f}s, Copy={:.1f}s, Overhead={:.1f}s, Total={:.1f}s".format(
            estimates['quality_checks'], estimates['compression'],
            estimates['upload'], estimates['snowflake_copy'],
            estimates['overhead'], estimates['total']))

        return estimates

class StreamingDataQualityChecker:
    """Memory-efficient streaming quality checks"""
    
    def __init__(self, chunk_size: int = 100000, buffer_size: int = 8192):
        self.chunk_size = chunk_size
        self.buffer_size = buffer_size * 1024  # Convert KB to bytes (8MB default)
        self.logger = logging.getLogger(self.__class__.__name__)

    def check_date_completeness(self, file_path: str, date_column_index: int,
                               expected_start: datetime, expected_end: datetime,
                               delimiter: str = '\t') -> Dict:
        """Stream through file checking dates - memory efficient"""
        self.logger.debug("Streaming date check for {} (column {})".format(file_path, date_column_index))
        self.logger.debug("Expected range: {} to {}".format(expected_start.date(), expected_end.date()))

        date_counts = defaultdict(int)
        invalid_dates = []
        total_rows = 0
        
        # Pre-calculate expected dates for O(1) lookup
        expected_dates_set = set()
        current = expected_start
        while current <= expected_end:
            expected_dates_set.add(current.strftime('%Y-%m-%d'))
            current += pd.Timedelta(days=1)

        try:
            if not os.path.exists(file_path):
                self.logger.error("File not found: {}".format(file_path))
                return {'error': 'File not found', 'total_rows': 0, 'unique_dates': 0,
                       'missing_dates': [], 'invalid_dates': []}
            
            start_time = time.time()

            # Stream with larger buffer for better performance
            with open(file_path, 'r', encoding='utf-8', errors='ignore', buffering=self.buffer_size) as file:
                reader = csv.reader(file, delimiter=delimiter)
                
                # Process dates in batches for better performance
                batch_dates = []
                batch_size = 10000
                
                for row_num, row in enumerate(reader, start=1):
                    total_rows += 1

                    if total_rows % 100000 == 0:
                        elapsed = time.time() - start_time
                        rate = total_rows / elapsed if elapsed > 0 else 0
                        self.logger.debug("  Processed {:,} rows at {:,.0f} rows/sec".format(total_rows, rate))

                    try:
                        if len(row) > date_column_index:
                            date_str = row[date_column_index].strip()
                            batch_dates.append((row_num, date_str))
                            
                            # Process batch when full
                            if len(batch_dates) >= batch_size:
                                self._process_date_batch(batch_dates, expected_start, expected_end, 
                                                        date_counts, invalid_dates)
                                batch_dates = []
                    except Exception as e:
                        invalid_dates.append((row_num, 'ERROR'))
                
                # Process remaining dates
                if batch_dates:
                    self._process_date_batch(batch_dates, expected_start, expected_end, 
                                            date_counts, invalid_dates)

            # Find missing dates using set difference (O(n) instead of O(n*m))
            found_dates = set(date_counts.keys())
            missing_dates = sorted(expected_dates_set - found_dates)
            
            elapsed = time.time() - start_time
            self.logger.debug("Date check complete: {} rows in {:.1f}s ({:,.0f} rows/sec)".format(
                total_rows, elapsed, total_rows/elapsed if elapsed > 0 else 0))

            return {
                'total_rows': total_rows,
                'unique_dates': len(date_counts),
                'missing_dates': missing_dates,
                'invalid_dates': invalid_dates[:100],  # First 100 only
                'date_distribution': dict(date_counts),
                'processing_rate': total_rows / elapsed if elapsed > 0 else 0
            }

        except Exception as e:
            self.logger.error("Error in date check: {}".format(e))
            self.logger.error(traceback.format_exc())
            return {'error': str(e), 'total_rows': 0, 'unique_dates': 0,
                   'missing_dates': [], 'invalid_dates': []}
    
    def _process_date_batch(self, batch_dates, expected_start, expected_end, 
                           date_counts, invalid_dates):
        """Process a batch of dates efficiently"""
        date_formats = ['%Y-%m-%d', '%Y%m%d', '%m/%d/%Y']
        
        for row_num, date_str in batch_dates:
            parsed = False
            for fmt in date_formats:
                try:
                    date_obj = datetime.strptime(date_str, fmt)
                    if expected_start <= date_obj <= expected_end:
                        date_counts[date_obj.strftime('%Y-%m-%d')] += 1
                    parsed = True
                    break
                except:
                    continue
            
            if not parsed and len(invalid_dates) < 100:  # Limit invalid dates stored
                invalid_dates.append((row_num, date_str))

    def check_schema_sample(self, file_path: str, expected_columns: List[str],
                          sample_size: int = 10000, delimiter: str = '\t') -> Dict:
        """Check schema by sampling file - memory efficient"""
        self.logger.debug("Checking schema in {}".format(file_path))
        self.logger.debug("Expected {} columns: {}".format(len(expected_columns), expected_columns[:5]))

        try:
            if not os.path.exists(file_path):
                self.logger.error("File not found: {}".format(file_path))
                return {'schema_match': False, 'error': 'File not found'}

            with open(file_path, 'r', encoding='utf-8', errors='ignore', buffering=self.buffer_size) as file:
                reader = csv.reader(file, delimiter=delimiter)

                try:
                    first_row = next(reader)
                except StopIteration:
                    return {'schema_match': False, 'error': 'File is empty'}

                actual_col_count = len(first_row)
                expected_col_count = len(expected_columns)
                schema_match = actual_col_count == expected_col_count

                self.logger.debug("Schema: {} columns found, {} expected - Match: {}".format(
                    actual_col_count, expected_col_count, schema_match))

                # Sample rows for type inference
                sample_rows = [first_row]
                for i, row in enumerate(reader):
                    if i >= sample_size - 1:
                        break
                    sample_rows.append(row)

                # Basic type inference and null counts
                column_types = {}
                null_counts = defaultdict(int)

                for col_idx, col_name in enumerate(expected_columns):
                    if col_idx < actual_col_count:
                        values = [row[col_idx] if col_idx < len(row) else None
                                 for row in sample_rows]

                        # Count nulls
                        null_counts[col_name] = sum(1 for v in values
                                                   if v in ('', 'NULL', 'null', '\\N', None))

                        # Infer type from non-null values
                        non_null_values = [v for v in values
                                          if v not in ('', 'NULL', 'null', '\\N', None)]

                        if non_null_values:
                            sample_val = non_null_values[0]
                            if sample_val.isdigit():
                                column_types[col_name] = 'INTEGER'
                            elif sample_val.replace('.', '').replace('-', '').isdigit():
                                column_types[col_name] = 'FLOAT'
                            elif self._is_date(sample_val):
                                column_types[col_name] = 'DATE'
                            else:
                                column_types[col_name] = 'VARCHAR'
                        else:
                            column_types[col_name] = 'UNKNOWN'

                self.logger.debug("Column types inferred: {}".format(column_types))

                return {
                    'schema_match': schema_match,
                    'actual_columns': actual_col_count,
                    'expected_columns': expected_col_count,
                    'column_types': column_types,
                    'null_counts': dict(null_counts),
                    'sample_row_count': len(sample_rows)
                }

        except Exception as e:
            self.logger.error("Error in schema check: {}".format(e))
            self.logger.error(traceback.format_exc())
            return {'schema_match': False, 'error': str(e)}

    @staticmethod
    def _is_date(value: str) -> bool:
        date_formats = ['%Y-%m-%d', '%Y%m%d', '%m/%d/%Y']
        for fmt in date_formats:
            try:
                datetime.strptime(value, fmt)
                return True
            except:
                continue
        return False

class SnowflakeDataValidator:
    """Validates data completeness directly in Snowflake tables"""
    
    def __init__(self, connection_params: Dict):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug("Initializing Snowflake validator")
        try:
            self.conn = snowflake.connector.connect(**connection_params)
            self.cursor = self.conn.cursor()
            self.logger.info("Snowflake validator connection established")
        except Exception as e:
            self.logger.error("Failed to connect to Snowflake: {}".format(e))
            raise
    
    def validate_date_completeness(self, table_name: str, date_column: str, 
                                  start_date: str, end_date: str, 
                                  expected_daily_rows: int = None) -> Dict:
        """
        Efficiently validate date completeness in Snowflake table.
        Uses aggregated queries to minimize data scanning.
        """
        self.logger.info("Validating date completeness for {} from {} to {}".format(
            table_name, start_date, end_date))
        
        try:
            # Query 1: Get date range summary
            # Convert date strings to YYYYMMDD format for comparison
            start_yyyymmdd = start_date.replace('-', '')
            end_yyyymmdd = end_date.replace('-', '')
            
            range_query = """
            SELECT 
                MIN({date_col}) as min_date,
                MAX({date_col}) as max_date,
                COUNT(DISTINCT {date_col}) as unique_dates,
                COUNT(*) as total_rows
            FROM {table}
            WHERE {date_col} BETWEEN '{start}' AND '{end}'
            """.format(
                date_col=date_column,
                table=table_name,
                start=start_yyyymmdd,
                end=end_yyyymmdd
            )
            
            self.logger.debug("Executing range query: {}".format(range_query))
            self.cursor.execute(range_query)
            range_result = self.cursor.fetchone()
            
            if not range_result or range_result[3] == 0 or range_result[3] is None:
                return {
                    'valid': False,
                    'error': 'No data found in specified date range',
                    'statistics': {
                        'total_rows': 0,
                        'unique_dates': 0,
                        'expected_dates': 0,
                        'missing_dates': 0,
                        'avg_rows_per_day': 0
                    }
                }
            
            min_date, max_date, unique_dates, total_rows = range_result
            
            # Query 2: Get daily distribution with statistics for anomaly detection
            distribution_query = """
            WITH daily_counts AS (
                SELECT 
                    {date_col} as date_value,
                    COUNT(*) as row_count
                FROM {table}
                WHERE {date_col} BETWEEN '{start}' AND '{end}'
                GROUP BY {date_col}
            ),
            stats AS (
                SELECT 
                    AVG(row_count) as avg_count,
                    STDDEV(row_count) as std_dev,
                    MIN(row_count) as min_count,
                    MAX(row_count) as max_count,
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY row_count) as q1,
                    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY row_count) as median,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY row_count) as q3
                FROM daily_counts
            )
            SELECT 
                dc.date_value,
                dc.row_count,
                s.avg_count,
                s.std_dev,
                s.min_count,
                s.max_count,
                s.q1,
                s.median,
                s.q3,
                CASE 
                    WHEN dc.row_count < (s.avg_count * 0.1) THEN 'SEVERELY_LOW'
                    WHEN dc.row_count < (s.q1 - 1.5 * (s.q3 - s.q1)) THEN 'OUTLIER_LOW'
                    WHEN dc.row_count < (s.avg_count * 0.5) THEN 'LOW'
                    WHEN dc.row_count > (s.q3 + 1.5 * (s.q3 - s.q1)) THEN 'OUTLIER_HIGH'
                    ELSE 'NORMAL'
                END as anomaly_flag
            FROM daily_counts dc
            CROSS JOIN stats s
            ORDER BY dc.date_value
            LIMIT 1000
            """.format(
                date_col=date_column,
                table=table_name,
                start=start_yyyymmdd,
                end=end_yyyymmdd
            )
            
            self.logger.debug("Executing distribution query with anomaly detection")
            self.cursor.execute(distribution_query)
            daily_counts = self.cursor.fetchall()
            
            # Query 3: Find gaps in date sequence
            # For YYYYMMDD format, we need to convert to dates for DATEDIFF
            gap_query = """
            WITH date_sequence AS (
                SELECT 
                    {date_col} as date_value,
                    LAG({date_col}) OVER (ORDER BY {date_col}) as prev_date
                FROM (
                    SELECT DISTINCT {date_col}
                    FROM {table}
                    WHERE {date_col} BETWEEN '{start}' AND '{end}'
                )
            )
            SELECT 
                prev_date,
                date_value,
                DATEDIFF(day, 
                    TO_DATE(prev_date::VARCHAR, 'YYYYMMDD'), 
                    TO_DATE(date_value::VARCHAR, 'YYYYMMDD')
                ) as gap_days
            FROM date_sequence
            WHERE DATEDIFF(day, 
                TO_DATE(prev_date::VARCHAR, 'YYYYMMDD'), 
                TO_DATE(date_value::VARCHAR, 'YYYYMMDD')
            ) > 1
            ORDER BY prev_date
            LIMIT 100
            """.format(
                date_col=date_column,
                table=table_name,
                start=start_yyyymmdd,
                end=end_yyyymmdd
            )
            
            self.logger.debug("Executing gap detection query")
            self.cursor.execute(gap_query)
            gaps = self.cursor.fetchall()
            
            # Calculate expected dates
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            expected_days = (end_dt - start_dt).days + 1
            
            # Helper function to format YYYYMMDD to YYYY-MM-DD
            def format_yyyymmdd(date_val):
                if date_val and len(str(date_val)) == 8:
                    date_str = str(date_val)
                    return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                return str(date_val)
            
            # Process daily counts with anomaly detection
            anomalous_dates = []
            row_count_stats = {}
            daily_sample = []
            
            if daily_counts and len(daily_counts) > 0:
                # Extract statistics from first row (they're the same for all rows)
                first_row = daily_counts[0]
                if len(first_row) >= 9:
                    row_count_stats = {
                        'mean': float(first_row[2]) if first_row[2] else 0,
                        'std_dev': float(first_row[3]) if first_row[3] else 0,
                        'min': int(first_row[4]) if first_row[4] else 0,
                        'max': int(first_row[5]) if first_row[5] else 0,
                        'q1': float(first_row[6]) if first_row[6] else 0,
                        'median': float(first_row[7]) if first_row[7] else 0,
                        'q3': float(first_row[8]) if first_row[8] else 0
                    }
                
                # Process each day's data
                for row in daily_counts[:100]:  # Process up to 100 days
                    if len(row) >= 10:
                        date_val = format_yyyymmdd(row[0])
                        count = int(row[1]) if row[1] else 0
                        anomaly = row[9] if len(row) > 9 else 'UNKNOWN'
                        
                        # Add to daily sample
                        daily_sample.append({
                            'date': date_val,
                            'count': count,
                            'anomaly': anomaly
                        })
                        
                        # Track anomalous dates
                        if anomaly in ['SEVERELY_LOW', 'OUTLIER_LOW', 'LOW']:
                            anomalous_dates.append({
                                'date': date_val,
                                'count': count,
                                'expected_range': [int(row_count_stats.get('q1', 0)), 
                                                 int(row_count_stats.get('q3', 0))],
                                'severity': anomaly,
                                'percent_of_avg': (count / row_count_stats['mean'] * 100) if row_count_stats.get('mean', 0) > 0 else 0
                            })
            
            # Determine validation status and failure reasons
            has_gaps = len(gaps) > 0
            has_missing_dates = unique_dates != expected_days
            has_anomalies = len(anomalous_dates) > 0
            is_valid = not (has_gaps or has_missing_dates or has_anomalies)
            
            # Build list of failure reasons
            failure_reasons = []
            if has_missing_dates:
                missing_count = expected_days - unique_dates
                failure_reasons.append(f"Missing {missing_count} dates (found {unique_dates} of {expected_days} expected)")
            if has_gaps:
                failure_reasons.append(f"Found {len(gaps)} gap(s) in date sequence")
            if has_anomalies:
                severely_low = len([a for a in anomalous_dates if a.get('severity') == 'SEVERELY_LOW'])
                if severely_low > 0:
                    failure_reasons.append(f"{severely_low} date(s) with critically low row counts (<10% of average)")
                else:
                    failure_reasons.append(f"{len(anomalous_dates)} date(s) with anomalous row counts")
            
            # Compile validation results with enhanced statistics
            validation_result = {
                'valid': is_valid,
                'failure_reasons': failure_reasons,
                'table_name': table_name,
                'date_column': date_column,
                'date_range': {
                    'requested_start': start_date,
                    'requested_end': end_date,
                    'actual_min': format_yyyymmdd(min_date),
                    'actual_max': format_yyyymmdd(max_date)
                },
                'statistics': {
                    'total_rows': total_rows,
                    'unique_dates': unique_dates,
                    'expected_dates': expected_days,
                    'missing_dates': expected_days - unique_dates,
                    'avg_rows_per_day': total_rows / unique_dates if unique_dates > 0 else 0
                },
                'row_count_analysis': {
                    **row_count_stats,
                    'anomalous_dates_count': len(anomalous_dates),
                    'threshold_10_percent': row_count_stats.get('mean', 0) * 0.1 if row_count_stats else 0,
                    'threshold_50_percent': row_count_stats.get('mean', 0) * 0.5 if row_count_stats else 0
                },
                'anomalous_dates': anomalous_dates[:20],  # Limit to first 20 anomalies
                'gaps': [
                    {
                        'from': format_yyyymmdd(gap[0]) if gap and len(gap) > 0 else '',
                        'to': format_yyyymmdd(gap[1]) if gap and len(gap) > 1 else '',
                        'missing_days': gap[2] - 1 if gap and len(gap) > 2 else 0
                    } for gap in gaps[:10] if gap and len(gap) >= 3  # Limit to first 10 gaps
                ],
                'daily_sample': daily_sample[:30]  # First 30 days sample
            }
            
            # Add warnings if issues detected
            validation_result['warnings'] = []
            
            if validation_result['statistics']['missing_dates'] > 0:
                validation_result['warnings'].append(
                    "Missing {} dates out of {} expected".format(
                        validation_result['statistics']['missing_dates'],
                        expected_days
                    )
                )
            
            if len(anomalous_dates) > 0:
                # Count by severity
                severity_counts = {}
                for anomaly in anomalous_dates:
                    severity = anomaly.get('severity', 'UNKNOWN')
                    severity_counts[severity] = severity_counts.get(severity, 0) + 1
                
                validation_result['warnings'].append(
                    "Found {} dates with anomalous row counts: {}".format(
                        len(anomalous_dates),
                        ', '.join(["{} {}".format(count, sev) for sev, count in severity_counts.items()])
                    )
                )
                
                # Add specific warnings for severe cases
                severely_low = [a for a in anomalous_dates if a.get('severity') == 'SEVERELY_LOW']
                if severely_low:
                    validation_result['warnings'].append(
                        "CRITICAL: {} dates have less than 10% of average row count - possible data loss".format(
                            len(severely_low)
                        )
                    )
            
            self.logger.info("Validation complete: {} dates found, {} expected".format(
                unique_dates, expected_days))
            
            return validation_result
            
        except Exception as e:
            self.logger.error("Error validating date completeness: {}".format(e))
            self.logger.error(traceback.format_exc())
            return {
                'valid': False,
                'error': str(e)
            }
    
    def get_table_stats(self, table_name: str) -> Dict:
        """Get basic table statistics for monitoring"""
        try:
            stats_query = """
            SELECT 
                COUNT(*) as row_count,
                COUNT(DISTINCT {cols}) as distinct_count
            FROM {table}
            LIMIT 1
            """.format(
                cols="*",  # This will be refined based on actual needs
                table=table_name
            )
            
            # Simpler query for large tables
            count_query = "SELECT COUNT(*) FROM {} LIMIT 1".format(table_name)
            
            self.logger.debug("Getting table stats for {}".format(table_name))
            self.cursor.execute(count_query)
            row_count = self.cursor.fetchone()[0]
            
            return {
                'table_name': table_name,
                'row_count': row_count,
                'checked_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error("Error getting table stats: {}".format(e))
            return {'error': str(e)}
    
    def close(self):
        """Close the connection"""
        if hasattr(self, 'cursor'):
            self.cursor.close()
        if hasattr(self, 'conn'):
            self.conn.close()
        self.logger.debug("Validator connection closed")

class SnowflakeLoader:
    """Snowflake loading with streaming compression"""
    
    def __init__(self, connection_params: Dict, progress_tracker=None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug("Initializing Snowflake connection")
        self.progress_tracker = progress_tracker
        try:
            self.conn = snowflake.connector.connect(**connection_params)
            self.cursor = self.conn.cursor()
            self.logger.info("Snowflake connection established")
        except Exception as e:
            self.logger.error("Failed to connect to Snowflake: {}".format(e))
            raise

    def load_file_to_stage_and_table(self, config: FileConfig):
        """Load TSV file to Snowflake with streaming compression"""
        import time
        import os
        
        self.logger.info("Loading {} to {}".format(config.file_path, config.table_name))
        
        compressed_file = None
        stage_name = None

        try:
            # Validate file exists
            if not os.path.exists(config.file_path):
                raise FileNotFoundError("File not found: {}".format(config.file_path))

            print("Loading {} to {}...".format(config.file_path, config.table_name))

            # Use user stage with subdirectory including unique identifier to avoid conflicts
            # This is critical for parallel processing to prevent file corruption
            timestamp = int(time.time() * 1000)  # millisecond timestamp
            file_basename = os.path.basename(config.file_path).replace('.tsv', '')
            stage_name = "@~/tsv_stage/{}/{}_{}/".format(config.table_name, file_basename, timestamp)
            self.logger.debug("Using stage: {}".format(stage_name))

            # Stream compress file
            compressed_file = "{}.gz".format(config.file_path)
            if not os.path.exists(compressed_file):
                print("Compressing {} (streaming)...".format(config.file_path))
                self.logger.debug("Streaming compression to {}".format(compressed_file))
                start_time = time.time()
                
                # Stream compression with progress
                file_size = os.path.getsize(config.file_path)
                file_size_mb = file_size / (1024 * 1024)
                bytes_processed = 0
                
                # Start compression progress for this specific file
                if self.progress_tracker:
                    self.progress_tracker.start_file_compression(config.file_path, file_size_mb)

                with open(config.file_path, 'rb') as f_in:
                    with gzip.open(compressed_file, 'wb', compresslevel=1) as f_out:  # Level 1 for speed
                        # Stream in chunks
                        chunk_size = 1024 * 1024 * 10  # 10MB chunks
                        last_update_mb = 0
                        while True:
                            chunk = f_in.read(chunk_size)
                            if not chunk:
                                break
                            f_out.write(chunk)
                            bytes_processed += len(chunk)
                            
                            # Update progress tracker if available
                            if self.progress_tracker:
                                mb_processed = bytes_processed / (1024 * 1024)
                                mb_to_update = mb_processed - last_update_mb
                                if mb_to_update >= 10:  # Update every 10MB
                                    self.progress_tracker.update(compressed_mb=mb_to_update)
                                    last_update_mb = mb_processed
                            
                            if bytes_processed % (100 * 1024 * 1024) == 0:  # Every 100MB
                                pct = (bytes_processed / file_size) * 100
                                self.logger.debug("Compression progress: {:.1f}%".format(pct))
                        
                        # Final update for remaining bytes
                        if self.progress_tracker:
                            mb_processed = bytes_processed / (1024 * 1024)
                            mb_to_update = mb_processed - last_update_mb
                            if mb_to_update > 0:
                                self.progress_tracker.update(compressed_mb=mb_to_update)

                compression_time = time.time() - start_time
                self.logger.debug("Compression completed in {:.1f} seconds".format(compression_time))

            # PUT file to stage
            # Reduce PARALLEL setting to 4 to avoid overwhelming the system during concurrent uploads
            # This helps prevent corruption when multiple months are processed simultaneously
            print("Uploading to Snowflake stage...")
            
            # Start upload progress tracking
            compressed_size = os.path.getsize(compressed_file)
            compressed_size_mb = compressed_size / (1024 * 1024)
            if self.progress_tracker:
                self.progress_tracker.start_file_upload(compressed_file, compressed_size_mb)
            
            put_command = "PUT file://{} {} AUTO_COMPRESS=FALSE OVERWRITE=TRUE PARALLEL=4".format(
                compressed_file, stage_name)
            self.logger.debug("Executing PUT command with PARALLEL=4")
            upload_start = time.time()
            self.cursor.execute(put_command)
            upload_time = time.time() - upload_start
            
            # Complete upload progress
            if self.progress_tracker:
                self.progress_tracker.update(uploaded_mb=compressed_size_mb)
            
            self.logger.debug("Upload completed in {:.1f} seconds ({:.1f} MB/s)".format(
                upload_time, compressed_size_mb / upload_time if upload_time > 0 else 0))

            # COPY INTO table
            copy_query = """
            COPY INTO {}
            FROM {}
            FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_DELIMITER = '\t'
                SKIP_HEADER = 0
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                ESCAPE_UNENCLOSED_FIELD = NONE
                DATE_FORMAT = 'YYYY-MM-DD'
                TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
                NULL_IF = ('', 'NULL', 'null', '\\N')
            )
            ON_ERROR = 'CONTINUE'
            VALIDATION_MODE = 'RETURN_ERRORS'
            """.format(config.table_name, stage_name)

            # First validate
            print("Validating data...")
            self.logger.debug("Running validation")
            validation_result = self.cursor.execute(
                copy_query.replace("ON_ERROR = 'CONTINUE'", "")
            ).fetchall()

            if validation_result:
                self.logger.warning("Validation errors found: {}".format(validation_result))
                print("Validation errors found: {}".format(validation_result))

            # If validation passes, do actual copy
            print("Copying data to {}...".format(config.table_name))
            
            # Get row count for progress tracking (quick estimate from file analysis if available)
            # We'll use the file size as a proxy for now since exact row count requires full scan
            estimated_rows = int(file_size_mb * 50000)  # Rough estimate: 50K rows per MB
            if self.progress_tracker:
                self.progress_tracker.start_copy_operation(config.table_name, estimated_rows)
            
            self.logger.debug("Executing COPY command")
            copy_start = time.time()
            copy_result = self.cursor.execute(copy_query.replace("VALIDATION_MODE = 'RETURN_ERRORS'", ""))
            copy_time = time.time() - copy_start
            
            # Get actual rows loaded from result
            rows_loaded = 0
            for row in copy_result:
                if row[0] and 'rows_loaded' in str(row[0]).lower():
                    # Extract number from result
                    import re
                    match = re.search(r'(\d+)', str(row[0]))
                    if match:
                        rows_loaded = int(match.group(1))
                        break
            
            # Complete COPY progress
            if self.progress_tracker:
                self.progress_tracker.update(copied_rows=estimated_rows)
            
            self.logger.debug("COPY completed in {:.1f} seconds ({:,.0f} rows/sec)".format(
                copy_time, rows_loaded / copy_time if copy_time > 0 and rows_loaded > 0 else 0))

            # Clean up stage
            self.logger.debug("Removing stage")
            self.cursor.execute("REMOVE {}".format(stage_name))

            print("Successfully loaded {}".format(config.table_name))
            self.logger.info("Successfully loaded {} to Snowflake".format(config.table_name))

        except Exception as e:
            self.logger.error("Failed to load {}: {}".format(config.table_name, e))
            raise
        finally:
            # Clean up compressed file
            if 'compressed_file' in locals() and os.path.exists(compressed_file):
                self.logger.debug("Removing compressed file")
                os.remove(compressed_file)

def run_quality_checks_worker(args):
    """Worker function for parallel quality checks using streaming"""
    config, worker_id = args

    # Setup logging for worker process
    worker_logger = logging.getLogger("Worker_{}".format(worker_id))
    worker_logger.setLevel(logging.DEBUG)

    worker_logger.info("Starting streaming QC for {} (file: {})".format(
        config.table_name, config.file_path))

    try:
        # Use streaming quality checker
        quality_checker = StreamingDataQualityChecker()
        results = {}

        # Schema check (samples only)
        worker_logger.debug("Checking schema...")
        results['schema'] = quality_checker.check_schema_sample(
            config.file_path, config.expected_columns)

        # Date check (streaming)
        worker_logger.debug("Checking dates...")
        date_col_idx = config.expected_columns.index(config.date_column)
        results['dates'] = quality_checker.check_date_completeness(
            config.file_path, date_col_idx,
            config.expected_date_range[0], config.expected_date_range[1])

        # Evaluate
        results['passed'] = True
        if not results.get('schema', {}).get('schema_match', False):
            results['passed'] = False
            worker_logger.warning("Schema check failed")
        if len(results.get('dates', {}).get('missing_dates', [])) > 0:
            results['passed'] = False
            worker_logger.warning("Date completeness check failed - {} missing dates".format(
                len(results['dates']['missing_dates'])))

        worker_logger.info("Completed QC for {}: Passed={}, Rate={:,.0f} rows/sec".format(
            config.table_name, results['passed'], 
            results.get('dates', {}).get('processing_rate', 0)))

        return config.table_name, results

    except Exception as e:
        worker_logger.error("Worker {} failed: {}".format(worker_id, e))
        worker_logger.error(traceback.format_exc())
        return config.table_name, {
            'error': str(e),
            'passed': False,
            'schema': {'schema_match': False},
            'dates': {'total_rows': 0, 'unique_dates': 0, 'missing_dates': []}
        }

def check_system_capabilities() -> Dict:
    """Check system capabilities and recommend settings"""
    print("\n" + "="*60)
    print("SYSTEM CAPABILITIES CHECK")
    print("="*60)

    cpu_count = os.cpu_count() or 1

    # Better calculation for optimal workers based on CPU count
    if cpu_count <= 4:
        optimal_workers = cpu_count  # Use all cores for small systems
    elif cpu_count <= 8:
        optimal_workers = cpu_count - 1  # Leave 1 core for system
    elif cpu_count <= 16:
        optimal_workers = int(cpu_count * 0.75)  # Use 75% of cores
    elif cpu_count <= 32:
        optimal_workers = int(cpu_count * 0.6)  # Use 60% of cores
    else:
        # For very large servers, cap at a reasonable number
        optimal_workers = min(int(cpu_count * 0.5), 32)

    # Different recommendations for different operations
    qc_workers = optimal_workers  # CPU-bound quality checks
    upload_workers = min(optimal_workers * 2, 32)  # I/O-bound uploads can use more

    capabilities = {
        'cpu_count': cpu_count,
        'python_version': sys.version.split()[0],
        'multiprocessing_available': True,
        'threading_available': True,
        'optimal_workers': optimal_workers,
        'qc_workers': qc_workers,
        'upload_workers': upload_workers
    }

    # Check memory if psutil available
    memory_gb = "Unknown (install psutil for memory detection)"
    if PSUTIL_AVAILABLE:
        memory = psutil.virtual_memory()
        memory_gb = memory.total / (1024**3)

        # Each worker needs minimal memory with streaming
        memory_limited_workers = int(memory_gb * 2)  # Can handle more with streaming

        if memory_limited_workers < optimal_workers:
            print("\nMemory check: {:.1f} GB available".format(memory_gb))
            print("With streaming, memory is less of a constraint")

    # Check if we can use multiprocessing
    try:
        multiprocessing.cpu_count()
        capabilities['multiprocessing_available'] = True
    except:
        capabilities['multiprocessing_available'] = False

    # Check threading
    try:
        threading.active_count()
        capabilities['threading_available'] = True
    except:
        capabilities['threading_available'] = False

    print("CPU Cores Available: {}".format(cpu_count))
    print("Memory Available: {}".format(
        "{:.1f} GB".format(memory_gb) if isinstance(memory_gb, float) else memory_gb))
    print("Python Version: {}".format(capabilities['python_version']))
    print("Streaming Mode: ENABLED (constant memory usage)")
    print("Multiprocessing: {}".format('Yes' if capabilities['multiprocessing_available'] else 'No'))
    print("Threading: {}".format('Yes' if capabilities['threading_available'] else 'No'))

    print("\n" + "-"*60)
    print("WORKER RECOMMENDATIONS BY CPU COUNT:")
    print("-"*60)
    print("Your {} cores suggest:".format(cpu_count))
    print("  - Quality Checks: {} workers (CPU-bound)".format(qc_workers))
    print("  - File Uploads: {} workers (I/O-bound)".format(upload_workers))
    print("  - Optimal Balance: {} workers".format(optimal_workers))

    print("\n" + "-"*60)
    print("SCALING GUIDELINES:")
    print("-"*60)
    print("  1-4 cores:    Use all cores")
    print("  5-8 cores:    Use cores - 1")
    print("  9-16 cores:   Use 75% of cores")
    print("  17-32 cores:  Use 60% of cores")
    print("  33-64 cores:  Use 50% of cores (max 32)")
    print("  64+ cores:    Test to find optimal (start with 32)")

    print("\n" + "-"*60)
    print("RECOMMENDED COMMANDS FOR YOUR SYSTEM:")
    print("-"*60)
    print("  # Conservative (safer):")
    print("  python3 tsv_loader.py --config config.json --max-workers {}".format(
        max(optimal_workers // 2, 1)))
    print("\n  # Balanced (recommended):")
    print("  python3 tsv_loader.py --config config.json --max-workers {}".format(
        optimal_workers))
    print("\n  # Aggressive (maximum performance):")
    print("  python3 tsv_loader.py --config config.json --max-workers {}".format(
        min(cpu_count, 48)))
    print("\n  # Auto-detect (uses balanced):")
    print("  python3 tsv_loader.py --config config.json")

    logger.info("System check complete: {} CPUs, optimal workers: {}".format(
        cpu_count, optimal_workers))

    return capabilities

def extract_date_range_from_filename(filename: str) -> Tuple[datetime, datetime]:
    """Extract date range from filename"""
    # Try YYYYMMDD-YYYYMMDD format
    pattern = r'(\d{8})-(\d{8})'
    match = re.search(pattern, filename)

    if match:
        start_str = match.group(1)
        end_str = match.group(2)
        start_date = datetime.strptime(start_str, '%Y%m%d')
        end_date = datetime.strptime(end_str, '%Y%m%d')
        logger.debug("Extracted date range from {}: {} to {}".format(
            filename, start_date, end_date))
        return (start_date, end_date)

    # Try YYYY-MM format
    pattern2 = r'(\d{4})-(\d{2})'
    match2 = re.search(pattern2, filename)

    if match2:
        year = int(match2.group(1))
        month = int(match2.group(2))
        start_date = datetime(year, month, 1)

        if month == 12:
            end_date = datetime(year + 1, 1, 1) - pd.Timedelta(days=1)
        else:
            end_date = datetime(year, month + 1, 1) - pd.Timedelta(days=1)

        logger.debug("Extracted month from {}: {} to {}".format(
            filename, start_date, end_date))
        return (start_date, end_date)

    raise ValueError("Could not extract date range from filename: {}".format(filename))

def load_config(config_path: str) -> Dict:
    """Load configuration from JSON file"""
    logger.debug("Loading config from {}".format(config_path))
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
            logger.debug("Config loaded successfully with {} file definitions".format(
                len(config.get('files', []))))
            return config
    except Exception as e:
        logger.error("Error loading config: {}".format(e))
        logger.error(traceback.format_exc())
        sys.exit(1)

def create_file_configs(config: Dict, base_path: str, month: str = None) -> List[FileConfig]:
    """Create FileConfig objects from configuration"""
    logger.info("Creating file configs from base path: {}".format(base_path))
    file_configs = []

    # Verify base path exists
    if not os.path.exists(base_path):
        logger.error("Base path does not exist: {}".format(base_path))
        return file_configs

    for file_def in config['files']:
        file_pattern = file_def['file_pattern']
        logger.debug("Processing pattern: {}".format(file_pattern))

        if '{date_range}' in file_pattern:
            # Pattern with date range
            pattern_regex = file_pattern.replace('{date_range}', r'(\d{8}-\d{8})')
            pattern_regex = pattern_regex.replace('.', r'\.')

            logger.debug("Looking for files matching regex: {}".format(pattern_regex))
            found_file = False

            try:
                files_in_dir = os.listdir(base_path)
                logger.debug("Found {} files in directory".format(len(files_in_dir)))

                for file in files_in_dir:
                    if re.match(pattern_regex, file):
                        file_path = os.path.join(base_path, file)
                        file_path = os.path.abspath(file_path)
                        logger.info("Found matching file: {}".format(file))

                        try:
                            start_date, end_date = extract_date_range_from_filename(file)
                        except ValueError as e:
                            logger.warning("Could not extract date range: {}".format(e))
                            continue

                        config_obj = FileConfig(
                            file_path=file_path,
                            table_name=file_def['table_name'],
                            expected_columns=file_def['expected_columns'],
                            date_column=file_def['date_column'],
                            expected_date_range=(start_date, end_date)
                        )
                        file_configs.append(config_obj)
                        found_file = True
                        break
            except Exception as e:
                logger.error("Error searching for files: {}".format(e))
                logger.error(traceback.format_exc())

            if not found_file:
                logger.warning("No file found matching pattern {} in {}".format(
                    file_pattern, base_path))

        elif '{month}' in file_pattern:
            # Original month-based pattern
            if not month:
                logger.warning("Month parameter required for {} pattern files".format(file_pattern))
                continue

            try:
                # Parse month to get date range
                month_date = datetime.strptime(month, '%Y-%m')
                month_start = month_date.replace(day=1)

                # Get last day of month
                if month_date.month == 12:
                    month_end = month_date.replace(year=month_date.year + 1, month=1, day=1) - pd.Timedelta(days=1)
                else:
                    month_end = month_date.replace(month=month_date.month + 1, day=1) - pd.Timedelta(days=1)

                # Build file path
                file_name = file_pattern.format(month=month)
                file_path = os.path.join(base_path, file_name)
                file_path = os.path.abspath(file_path)

                logger.info("Looking for month-based file: {}".format(file_path))

                config_obj = FileConfig(
                    file_path=file_path,
                    table_name=file_def['table_name'],
                    expected_columns=file_def['expected_columns'],
                    date_column=file_def['date_column'],
                    expected_date_range=(month_start, month_end)
                )
                file_configs.append(config_obj)

            except Exception as e:
                logger.error("Error processing month pattern: {}".format(e))

    logger.info("Created {} file configurations".format(len(file_configs)))
    for fc in file_configs:
        logger.debug("  - {}: {}".format(fc.table_name, fc.file_path))

    return file_configs

def analyze_files(file_configs: List[FileConfig], max_workers: int = 4) -> Dict:
    """Analyze files with streaming row counting"""
    print("\n" + "="*60)
    print("FILE ANALYSIS & TIME ESTIMATION")
    print("="*60)

    logger.info("Analyzing {} files with {} workers".format(len(file_configs), max_workers))

    analyzer = FileAnalyzer()
    total_rows = 0
    total_size_gb = 0
    file_details = []

    # Count rows in each file
    for config in file_configs:
        logger.debug("Analyzing {}".format(config.file_path))
        rows, size_gb = analyzer.count_rows_fast(config.file_path)
        total_rows += rows
        total_size_gb += size_gb

        file_details.append({
            'file': os.path.basename(config.file_path),
            'table': config.table_name,
            'rows': rows,
            'size_gb': size_gb
        })

        print("\n{}:".format(config.table_name))
        print("  File: {}".format(os.path.basename(config.file_path)))
        print("  Rows: {:,}".format(rows))
        print("  Size: {:.2f} GB".format(size_gb))

    # Calculate REALISTIC time estimates
    estimates = analyzer.estimate_processing_time(total_rows, total_size_gb, max_workers)

    print("\n" + "-"*60)
    print("SUMMARY:")
    print("  Total Files: {}".format(len(file_configs)))
    print("  Total Rows: {:,}".format(total_rows))
    print("  Total Size: {:.2f} GB".format(total_size_gb))
    print("  Workers: {}".format(max_workers))
    print("  Mode: STREAMING (constant memory usage)")

    print("\n" + "-"*60)
    print("REALISTIC TIME ESTIMATES:")
    print("  Quality Checks: {:.1f} seconds ({:.1f} minutes)".format(
        estimates['quality_checks'], estimates['quality_checks']/60))
    print("  Compression: {:.1f} seconds ({:.1f} minutes)".format(
        estimates['compression'], estimates['compression']/60))
    print("  Upload to Snowflake: {:.1f} seconds ({:.1f} minutes)".format(
        estimates['upload'], estimates['upload']/60))
    print("  Snowflake Processing: {:.1f} seconds ({:.1f} minutes)".format(
        estimates['snowflake_copy'], estimates['snowflake_copy']/60))
    print("  Process Overhead: {:.1f} seconds".format(estimates['overhead']))
    print("  ----------------------------------------")
    print("  TOTAL ESTIMATED TIME: {:.1f} seconds".format(estimates['total']))
    print("                        ({:.1f} minutes)".format(estimates['total']/60))
    print("                        ({:.1f} hours)".format(estimates['total']/3600))

    # Performance expectations
    print("\n" + "-"*60)
    print("PERFORMANCE EXPECTATIONS:")
    print("  Effective rate with {} workers:".format(max_workers))
    if total_rows > 0 and estimates['total'] > 0:
        effective_rate = total_rows / estimates['total']
        print("    ~{:,.0f} rows/second total".format(effective_rate))
        print("    ~{:,.0f} rows/second per worker".format(effective_rate / max_workers))

    # Warnings for large jobs
    if estimates['total'] > 3600:
        print("\n  WARNING: This is a large job (>{:.1f} hours)".format(estimates['total']/3600))
        print("  Consider:")
        print("    - Running overnight or on weekends")
        print("    - Splitting into smaller batches")
        print("    - Using a more powerful server")

    return {
        'total_rows': total_rows,
        'total_size_gb': total_size_gb,
        'estimates': estimates,
        'file_details': file_details
    }


def process_files(file_configs: List[FileConfig], snowflake_params: Dict,
                 max_workers: int, skip_qc: bool, analysis_results: Dict,
                 validate_in_snowflake: bool = False, validate_only: bool = False, month: str = None) -> Dict:
    """Process files with streaming quality checks and loading"""
    logger.info("="*60)
    logger.info("Starting file processing (STREAMING MODE)")
    logger.info("  Files: {}".format(len(file_configs)))
    logger.info("  Workers: {}".format(max_workers))
    logger.info("  Skip QC: {}".format(skip_qc))
    logger.info("  Validate in Snowflake: {}".format(validate_in_snowflake))
    logger.info("  Validate Only: {}".format(validate_only))
    logger.info("="*60)

    start_time = time.time()
    results = {}
    failed_files = []

    # Initialize progress tracker if available
    tracker = None
    if analysis_results and TQDM_AVAILABLE:
        # Determine if we're showing QC progress (only if doing file-based QC)
        show_qc_progress = not skip_qc and not validate_in_snowflake
        tracker = ProgressTracker(
            len(file_configs),
            analysis_results['total_rows'],
            analysis_results['total_size_gb'],
            month=month,  # Pass month for job identification
            show_qc_progress=show_qc_progress  # Show QC progress only if doing file-based QC
        )

    try:
        # If validate_only mode, skip to validation
        if validate_only:
            logger.info("Starting validation-only mode")
            validator = SnowflakeDataValidator(snowflake_params)
            validation_results = []
            
            try:
                # Create progress bar for validation
                if TQDM_AVAILABLE:
                    pbar = tqdm(
                        total=len(file_configs),
                        desc="Validating tables",
                        unit="table",
                        file=sys.stderr,  # Output to stderr so it shows in quiet mode
                        leave=True
                    )
                else:
                    pbar = None
                
                for config in file_configs:
                    # Update progress bar description
                    if pbar:
                        pbar.set_description(f"Validating {config.table_name}")
                    
                    # Convert date range to strings
                    start_date = config.expected_date_range[0].strftime('%Y-%m-%d')
                    end_date = config.expected_date_range[1].strftime('%Y-%m-%d')
                    
                    validation_result = validator.validate_date_completeness(
                        table_name=config.table_name,
                        date_column=config.date_column,
                        start_date=start_date,
                        end_date=end_date
                    )
                    
                    # Store result for later display
                    validation_results.append(validation_result)
                    
                    # Update progress bar with status
                    if pbar:
                        status = "✓" if validation_result.get('valid') else "✗"
                        anomaly_count = validation_result.get('row_count_analysis', {}).get('anomalous_dates_count', 0)
                        if anomaly_count > 0:
                            status += f" ({anomaly_count} anomalies)"
                        pbar.set_postfix_str(f"{config.table_name}: {status}")
                        pbar.update(1)
                
                if pbar:
                    pbar.close()
                    
            finally:
                validator.close()
            
            return {'validation_results': validation_results}
        
        # Skip file-based QC if validating in Snowflake
        if validate_in_snowflake:
            skip_qc = True
            print("\n=== Skipping file-based QC (will validate in Snowflake) ===")
        
        # Run quality checks if not skipped
        if not skip_qc:
            print("\n=== Running Streaming Data Quality Checks ===")
            logger.info("Starting streaming quality checks with {} workers".format(max_workers))

            # Prepare arguments
            worker_args = [(config, i) for i, config in enumerate(file_configs)]
            logger.debug("Prepared {} worker arguments".format(len(worker_args)))

            # Use Pool for multiprocessing
            try:
                logger.debug("Creating multiprocessing pool...")
                with multiprocessing.Pool(processes=max_workers) as pool:
                    logger.info("Pool created, starting streaming quality checks...")

                    # Process files with streaming
                    for i, (table_name, result) in enumerate(pool.imap(run_quality_checks_worker, worker_args)):
                        logger.info("Completed QC {}/{}: {} at {:,.0f} rows/sec".format(
                            i+1, len(file_configs), table_name,
                            result.get('dates', {}).get('processing_rate', 0)))
                        results[table_name] = result

                        # Update progress
                        if tracker:
                            tracker.update(files=1, rows=result.get('dates', {}).get('total_rows', 0))

                        # Print results
                        print("\n--- QC Results for {} ---".format(table_name))
                        print("  Method: STREAMING")
                        print("  Schema Match: {}".format(
                            result.get('schema', {}).get('schema_match', False)))
                        print("  Total Rows: {:,}".format(
                            result.get('dates', {}).get('total_rows', 0)))
                        print("  Unique Dates: {}".format(
                            result.get('dates', {}).get('unique_dates', 0)))
                        print("  Processing Rate: {:,.0f} rows/sec".format(
                            result.get('dates', {}).get('processing_rate', 0)))

                        missing_dates = result.get('dates', {}).get('missing_dates', [])
                        if missing_dates:
                            print("  Missing Dates: {} X".format(len(missing_dates)))
                            print("    First missing: {}".format(missing_dates[0]))
                        else:
                            print("  Missing Dates: 0 OK")

                        print("  QC Passed: {}".format('OK' if result.get('passed', False) else 'FAILED'))

                        if not result.get('passed', False):
                            failed_files.append(table_name)
                            if missing_dates:
                                logger.error("{} has {} missing dates. First 5: {}".format(
                                    table_name, len(missing_dates), missing_dates[:5]))

                    if failed_files:
                        logger.error("Quality checks failed for: {}".format(failed_files))
                        print("\n" + "="*60)
                        print("ERROR: Quality checks failed for {} files".format(len(failed_files)))
                        print("Process halted - No files loaded to Snowflake")
                        print("="*60)
                        return
                    else:
                        logger.info("All quality checks passed!")
                        print("\n=== All Quality Checks Passed ===")

            except Exception as e:
                logger.error("Error in quality check processing: {}".format(e))
                logger.error(traceback.format_exc())
                return

        # Load to Snowflake
        print("\n=== Loading Files to Snowflake (Streaming) ===")
        logger.info("Starting Snowflake uploads with {} workers".format(max_workers))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            loader_futures = {}
            for config in file_configs:
                try:
                    loader = SnowflakeLoader(snowflake_params, progress_tracker=tracker)
                    future = executor.submit(loader.load_file_to_stage_and_table, config)
                    loader_futures[future] = config
                except Exception as e:
                    logger.error("Failed to create loader for {}: {}".format(config.table_name, e))

            for future in as_completed(loader_futures):
                config = loader_futures[future]
                try:
                    future.result()
                    logger.info("Successfully loaded {}".format(config.table_name))
                    print("Successfully loaded {}".format(config.table_name))
                    if tracker:
                        tracker.update(files=1)
                except Exception as e:
                    logger.error("Failed to load {}: {}".format(config.table_name, e))
                    print("Failed to load {}: {}".format(config.table_name, e))
        
        # Validate in Snowflake if requested
        if validate_in_snowflake:
            print("\n=== Validating Data Completeness in Snowflake ===")
            validator = SnowflakeDataValidator(snowflake_params)
            
            try:
                validation_failed = False
                validation_results = []  # Store for detailed display
                
                # Create progress bar for validation
                if TQDM_AVAILABLE:
                    val_pbar = tqdm(
                        total=len(file_configs),
                        desc="Validating tables",
                        unit="table",
                        file=sys.stderr,  # Output to stderr so it shows in quiet mode
                        leave=True
                    )
                else:
                    val_pbar = None
                
                for config in file_configs:
                    # Update progress bar description
                    if val_pbar:
                        val_pbar.set_description(f"Validating {config.table_name}")
                    
                    # Convert date range to strings
                    start_date = config.expected_date_range[0].strftime('%Y-%m-%d')
                    end_date = config.expected_date_range[1].strftime('%Y-%m-%d')
                    
                    validation_result = validator.validate_date_completeness(
                        table_name=config.table_name,
                        date_column=config.date_column,
                        start_date=start_date,
                        end_date=end_date
                    )
                    
                    # Store result for later display
                    validation_results.append(validation_result)
                    
                    # Update progress bar with status
                    if val_pbar:
                        status = "✓" if validation_result.get('valid') else "✗"
                        anomaly_count = validation_result.get('row_count_analysis', {}).get('anomalous_dates_count', 0)
                        if anomaly_count > 0:
                            status += f" ({anomaly_count} anomalies)"
                        val_pbar.set_postfix_str(f"{config.table_name}: {status}")
                        val_pbar.update(1)
                    
                    # Quick status in log
                    if validation_result.get('valid'):
                        logger.info("{} passed Snowflake validation".format(config.table_name))
                    else:
                        validation_failed = True
                        # Use failure reasons if available, otherwise fall back to warnings
                        failure_msg = validation_result.get('failure_reasons', [])
                        if failure_msg:
                            logger.warning("{} failed validation: {}".format(
                                config.table_name, '; '.join(failure_msg)))
                        else:
                            logger.warning("{} failed validation: {}".format(
                                config.table_name,
                                validation_result.get('warnings', ['Unknown issue'])[0] if validation_result.get('warnings') else 'Unknown issue'))
                
                if val_pbar:
                    val_pbar.close()
                
                # Store validation results in main results
                results['validation_results'] = validation_results
                
                if validation_failed:
                    print("\n⚠ WARNING: Some tables have incomplete date ranges")
            finally:
                validator.close()

    finally:
        if tracker:
            tracker.close()

    elapsed = time.time() - start_time
    print("\n" + "="*60)
    print("PROCESSING COMPLETE")
    print("="*60)
    print("Total Time: {:.1f} seconds ({:.1f} minutes)".format(elapsed, elapsed/60))

    if analysis_results and analysis_results['total_rows'] > 0:
        print("Average Rate: {:.0f} rows/second".format(
            analysis_results['total_rows'] / elapsed))

    logger.info("Processing complete in {:.1f} seconds".format(elapsed))
    
    # Return empty dict for non-validate modes
    return {}

def main():
    global logger
    
    parser = argparse.ArgumentParser(description='Load TSV files to Snowflake with progress tracking')
    parser.add_argument('--config', type=str, help='Path to configuration JSON file')
    parser.add_argument('--base-path', type=str, default='.', help='Base path for TSV files')
    parser.add_argument('--month', type=str, help='Month to process (format: YYYY-MM)')
    parser.add_argument('--skip-qc', action='store_true', help='Skip quality checks')
    parser.add_argument('--max-workers', type=int, default=None, help='Maximum parallel workers')
    parser.add_argument('--analyze-only', action='store_true', help='Only analyze files and show estimates')
    parser.add_argument('--check-system', action='store_true', help='Check system capabilities')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging (already on by default)')
    parser.add_argument('--yes', '-y', action='store_true', help='Skip confirmation prompt and proceed automatically')
    parser.add_argument('--quiet', action='store_true', help='Suppress console output (keep progress bars and file logging)')
    parser.add_argument('--validate-in-snowflake', action='store_true', 
                       help='Validate date completeness in Snowflake after loading (skip file-based QC)')
    parser.add_argument('--validate-only', action='store_true',
                       help='Only validate existing data in Snowflake tables (no loading)')
    parser.add_argument('--validation-output', type=str,
                       help='Path to save validation results JSON (for batch aggregation)')

    args = parser.parse_args()

    # Setup logging based on quiet mode
    logger = setup_logging(quiet_mode=args.quiet)

    # Debug logging is already on by default
    if args.debug:
        logger.info("Debug logging already enabled")

    logger.info("Arguments: {}".format(args))

    # Check system if requested - NO CONFIG NEEDED
    if args.check_system:
        logger.info("Running system capabilities check")
        check_system_capabilities()
        return 0

    # For all other operations, config is required
    if not args.config:
        logger.error("Config file required for processing")
        print("ERROR: --config is required for processing files")
        print("Use --check-system to check capabilities without a config file")
        parser.print_help()
        return 1

    # Check config file exists
    if not os.path.exists(args.config):
        logger.error("Config file not found: {}".format(args.config))
        print("ERROR: Config file not found: {}".format(args.config))
        return 1

    # Auto-detect optimal workers if not specified
    if args.max_workers is None:
        cpu_count = os.cpu_count() or 1

        # Better auto-detection based on CPU count
        if cpu_count <= 4:
            args.max_workers = cpu_count
        elif cpu_count <= 8:
            args.max_workers = cpu_count - 1
        elif cpu_count <= 16:
            args.max_workers = int(cpu_count * 0.75)
        elif cpu_count <= 32:
            args.max_workers = int(cpu_count * 0.6)
        else:
            args.max_workers = min(int(cpu_count * 0.5), 32)

        logger.info("Auto-detected {} workers (from {} CPUs)".format(
            args.max_workers, cpu_count))
        print("Auto-detected optimal workers: {} (for {} cores)".format(
            args.max_workers, cpu_count))

    # Load config
    config = load_config(args.config)
    
    # For validate-only mode, we don't need file configs
    if args.validate_only:
        logger.info("Validate-only mode - skipping file discovery")
        # Create minimal file configs just for table information
        file_configs = []
        for file_config in config.get('files', []):
            # Create a FileConfig with just the table and date info, no actual file
            # Calculate expected date range if month is provided
            if args.month:
                try:
                    year, month = args.month.split('-')
                    start_date = datetime.strptime(f"{year}-{month}-01", "%Y-%m-%d")
                    # Get last day of month
                    import calendar
                    last_day = calendar.monthrange(int(year), int(month))[1]
                    end_date = datetime.strptime(f"{year}-{month}-{last_day:02d}", "%Y-%m-%d")
                    expected_date_range = (start_date, end_date)
                except Exception as e:
                    logger.warning(f"Could not parse month {args.month}: {e}")
                    expected_date_range = (None, None)
            else:
                expected_date_range = (None, None)
                
            fc = FileConfig(
                file_path="",  # No file needed for validate-only
                table_name=file_config.get('table_name'),
                expected_columns=file_config.get('expected_columns', []),
                date_column=file_config.get('date_column'),
                expected_date_range=expected_date_range
            )
            file_configs.append(fc)
    else:
        # Normal mode - find actual files
        file_configs = create_file_configs(config, args.base_path, args.month)
        
        if not file_configs:
            logger.error("No files found matching patterns")
            print("ERROR: No files found matching the patterns in config")
            return 1

    # Print files/tables to be processed
    if args.validate_only:
        print("\n=== Validating {} tables ===".format(len(file_configs)))
        for fc in file_configs:
            if fc.expected_date_range and fc.expected_date_range[0] and fc.expected_date_range[1]:
                print("  - Table: {} (Date range: {} to {})".format(
                    fc.table_name, 
                    fc.expected_date_range[0].date(), 
                    fc.expected_date_range[1].date()))
            else:
                print("  - Table: {}".format(fc.table_name))
    else:
        print("\n=== Processing {} files ===".format(len(file_configs)))
        for fc in file_configs:
            date_range = "{} to {}".format(
                fc.expected_date_range[0].date(),
                fc.expected_date_range[1].date())
            print("  - {}: {} ({})".format(fc.table_name,
                                           os.path.basename(fc.file_path),
                                           date_range))

    # Verify files exist (skip for validate-only mode)
    if not args.validate_only:
        missing_files = [fc.file_path for fc in file_configs if not os.path.exists(fc.file_path)]
        if missing_files:
            logger.error("Missing files: {}".format(missing_files))
            print("\nERROR: The following files are missing:")
            for f in missing_files:
                print("  - {}".format(f))
            return 1

    # Analyze files (skip for validate-only mode)
    if args.validate_only:
        analysis_results = {}  # Empty results for validate-only
    else:
        analysis_results = analyze_files(file_configs, args.max_workers)

    if args.analyze_only:
        logger.info("Analysis complete (--analyze-only mode)")
        print("\n[Analysis only mode - not processing files]")
        return 0

    # Ask for confirmation (unless --yes flag is provided or validate-only mode)
    if not args.validate_only:
        print("\n" + "="*60)
        estimated_minutes = analysis_results['estimates']['total'] / 60
        
        if not args.yes:
            response = input("Proceed with processing? (estimated {:.1f} minutes) [y/N]: ".format(estimated_minutes))
            
            if response.lower() != 'y':
                logger.info("Processing cancelled by user")
                print("Processing cancelled")
                return 0
        else:
            logger.info("Skipping confirmation prompt (--yes flag provided)")
            print("Proceeding automatically (--yes flag provided)")
        print("Estimated time: {:.1f} minutes".format(estimated_minutes))
    elif not args.quiet:
        print("\n" + "="*60)
        print("Validation-only mode - checking existing Snowflake tables")

    # Process files
    results = process_files(
        file_configs=file_configs,
        snowflake_params=config.get('snowflake', {}),
        max_workers=args.max_workers,
        skip_qc=args.skip_qc or args.validate_in_snowflake,  # Skip file QC if validating in Snowflake
        analysis_results=analysis_results,
        validate_in_snowflake=args.validate_in_snowflake,
        validate_only=args.validate_only,
        month=args.month  # Pass month for progress bar identification
    )
    
    # Save validation results to file if requested
    if args.validate_only and results.get('validation_results') and args.validation_output:
        import json
        validation_data = {
            'month': args.month,
            'timestamp': datetime.now().isoformat(),
            'results': results['validation_results']
        }
        try:
            with open(args.validation_output, 'w') as f:
                json.dump(validation_data, f, indent=2, default=str)
            logger.info(f"Validation results saved to {args.validation_output}")
        except Exception as e:
            logger.error(f"Failed to save validation results: {e}")
    
    # Display detailed validation results if available (ALWAYS show - even in quiet mode)
    # Validation data is critical and should always be visible
    if (args.validate_only or args.validate_in_snowflake) and results.get('validation_results'):
        print("\n" + "="*60)
        print("VALIDATION DETAILS")
        print("="*60)
        
        for result in results['validation_results']:
            table_name = result.get('table_name', 'Unknown')
            print("\n{}:".format(table_name))
            
            if result.get('error'):
                print("  Status: ✗ ERROR")
                print("  Error: {}".format(result['error']))
            elif result.get('valid'):
                print("  Status: ✓ VALID")
                if 'statistics' in result:
                    stats = result['statistics']
                    print("  Date Range: {} to {}".format(
                        result.get('date_range', {}).get('actual_min', 'N/A'),
                        result.get('date_range', {}).get('actual_max', 'N/A')))
                    print("  Total Rows: {:,}".format(stats.get('total_rows', 0)))
                    print("  Unique Dates: {}".format(stats.get('unique_dates', 0)))
                    print("  Expected Dates: {}".format(stats.get('expected_dates', 0)))
                    print("  Avg Rows/Day: {:,.0f}".format(stats.get('avg_rows_per_day', 0)))
                    
                    # Show anomalous dates even for valid results as a warning
                    if result.get('anomalous_dates'):
                        print("\n  ⚠️  WARNING: Anomalous dates detected (but within tolerance):")
                        for i, anomaly in enumerate(result['anomalous_dates'][:5], 1):
                            print("    {}) {} - {} rows ({:.1f}% of avg) - {}".format(
                                i, anomaly['date'], anomaly['count'],
                                anomaly['percent_of_avg'], anomaly['severity']))
                        if len(result['anomalous_dates']) > 5:
                            print("    ... and {} more anomalous dates".format(
                                len(result['anomalous_dates']) - 5))
            else:
                print("  Status: ✗ INVALID")
                
                # Show clear failure reasons first
                if result.get('failure_reasons'):
                    print("\n  ❌ VALIDATION FAILED BECAUSE:")
                    for reason in result['failure_reasons']:
                        print(f"    • {reason}")
                
                if 'statistics' in result:
                    stats = result['statistics']
                    print("\n  Date Range Requested: {} to {}".format(
                        result.get('date_range', {}).get('requested_start', 'N/A'),
                        result.get('date_range', {}).get('requested_end', 'N/A')))
                    print("  Date Range Found: {} to {}".format(
                        result.get('date_range', {}).get('actual_min', 'N/A'),
                        result.get('date_range', {}).get('actual_max', 'N/A')))
                    print("  Total Rows: {:,}".format(stats.get('total_rows', 0)))
                    print("  Unique Dates: {} of {} expected".format(
                        stats.get('unique_dates', 0),
                        stats.get('expected_dates', 0)))
                    if stats.get('missing_dates', 0) > 0:
                        print("  Missing Dates: {} completely absent".format(stats.get('missing_dates', 0)))
                    print("  Avg Rows/Day: {:,.0f}".format(stats.get('avg_rows_per_day', 0)))
                    
                    # Show row count analysis if available
                    if 'row_count_analysis' in result:
                        analysis = result['row_count_analysis']
                        if analysis.get('anomalous_dates_count', 0) > 0:
                            print("\n  Row Count Analysis:")
                            print("    Mean: {:,.0f} rows/day".format(analysis.get('mean', 0)))
                            print("    Median: {:,.0f} rows/day".format(analysis.get('median', 0)))
                            print("    Range: {:,} - {:,} rows".format(
                                analysis.get('min', 0), analysis.get('max', 0)))
                            print("    Anomalies Detected: {} dates".format(
                                analysis.get('anomalous_dates_count', 0)))
                    
                    # Show anomalous dates if any
                    if result.get('anomalous_dates'):
                        print("\n  ⚠️  SPECIFIC DATES WITH ANOMALIES:")
                        
                        # Group by severity for clarity
                        severely_low = [a for a in result['anomalous_dates'] if a.get('severity') == 'SEVERELY_LOW']
                        low = [a for a in result['anomalous_dates'] if a.get('severity') == 'LOW']
                        outlier_low = [a for a in result['anomalous_dates'] if a.get('severity') == 'OUTLIER_LOW']
                        
                        if severely_low:
                            print("    CRITICALLY LOW (<10% of average):")
                            for anomaly in severely_low[:5]:
                                expected_min = anomaly.get('expected_range', [0, 0])[0]
                                print("      • {} → {} rows (expected ~{:,}, got {:.1f}% of avg)".format(
                                    anomaly['date'], anomaly['count'], expected_min,
                                    anomaly['percent_of_avg']))
                        
                        if low:
                            print("    LOW (<50% of average):")
                            for anomaly in low[:3]:
                                expected_min = anomaly.get('expected_range', [0, 0])[0]
                                print("      • {} → {} rows (expected ~{:,}, got {:.1f}% of avg)".format(
                                    anomaly['date'], anomaly['count'], expected_min,
                                    anomaly['percent_of_avg']))
                        
                        if outlier_low:
                            print("    STATISTICAL OUTLIERS:")
                            for anomaly in outlier_low[:3]:
                                expected_min = anomaly.get('expected_range', [0, 0])[0]
                                print("      • {} → {} rows (expected ~{:,})".format(
                                    anomaly['date'], anomaly['count'], expected_min))
                        
                        total_shown = len(severely_low[:5]) + len(low[:3]) + len(outlier_low[:3])
                        if len(result['anomalous_dates']) > total_shown:
                            print("    ... and {} more anomalous dates".format(
                                len(result['anomalous_dates']) - total_shown))
                    
                    # Show gaps if any
                    if result.get('gaps'):
                        print("\n  Date Gaps Found:")
                        for i, gap in enumerate(result['gaps'][:5], 1):  # Show first 5 gaps
                            print("    {}) {} to {} ({} days missing)".format(
                                i, gap['from'], gap['to'], gap['missing_days']))
                        if len(result['gaps']) > 5:
                            print("    ... and {} more gaps".format(len(result['gaps']) - 5))
                    
                    # Show warnings if any
                    if result.get('warnings'):
                        print("\n  ⚠️  Warnings:")
                        for warning in result['warnings']:
                            print("    • {}".format(warning))

    logger.info("Main function complete")
    return 0

if __name__ == "__main__":
    try:
        # Set up signal handling for better debugging
        def signal_handler(sig, frame):
            logger.warning("Received signal {}".format(sig))
            logger.info("Cleaning up...")
            sys.exit(1)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        exit_code = main()
        logger.info("Exiting with code {}".format(exit_code))
        sys.exit(exit_code)

    except KeyboardInterrupt:
        logger.warning("Process interrupted by user")
        print("\nProcess interrupted")
        sys.exit(1)
    except Exception as e:
        logger.error("Unhandled exception: {}".format(e))
        logger.error(traceback.format_exc())
        print("ERROR: {}".format(e))
        sys.exit(1)
