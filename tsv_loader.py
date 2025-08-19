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

# NOW setup logging
logging.basicConfig(
    level=logging.DEBUG,  # Keep DEBUG level for detailed troubleshooting
    format='%(asctime)s - %(processName)s - %(threadName)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/tsv_loader_debug.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

logger.info("="*60)
logger.info("TSV LOADER STARTING")
logger.info("Python version: {}".format(sys.version))
logger.info("Process ID: {}".format(os.getpid()))
logger.info("="*60)

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

    def __init__(self, total_files: int, total_rows: int, total_size_gb: float):
        self.total_files = total_files
        self.total_rows = total_rows
        self.total_size_gb = total_size_gb
        self.processed_files = 0
        self.processed_rows = 0
        self.start_time = time.time()
        self.lock = threading.Lock()
        self.logger = logging.getLogger(self.__class__.__name__)

        # Progress bars if tqdm available
        if TQDM_AVAILABLE:
            self.file_pbar = tqdm(total=total_files, desc="Files", unit="file")
            self.row_pbar = tqdm(total=total_rows, desc="Rows", unit="rows", unit_scale=True)
            self.logger.debug("Progress bars initialized")

    def update(self, files: int = 0, rows: int = 0):
        """Update progress"""
        with self.lock:
            self.processed_files += files
            self.processed_rows += rows

            if TQDM_AVAILABLE:
                if files > 0:
                    self.file_pbar.update(files)
                if rows > 0:
                    self.row_pbar.update(rows)

            self.logger.debug("Progress: {}/{} files, {}/{} rows".format(
                self.processed_files, self.total_files,
                self.processed_rows, self.total_rows))

    def get_eta(self) -> str:
        """Calculate estimated time remaining"""
        elapsed = time.time() - self.start_time
        if self.processed_rows > 0:
            rate = self.processed_rows / elapsed
            remaining_rows = self.total_rows - self.processed_rows
            eta_seconds = remaining_rows / rate if rate > 0 else 0
            return str(timedelta(seconds=int(eta_seconds)))
        return "Unknown"

    def close(self):
        """Close progress bars"""
        if TQDM_AVAILABLE:
            self.file_pbar.close()
            self.row_pbar.close()
        self.logger.debug("Progress tracker closed")

class FileAnalyzer:
    """Fast file analysis for row counting and time estimation"""

    # REALISTIC benchmark rates based on actual performance
    BENCHMARKS = {
        'row_count': 500_000,         # Can count 500K rows/second (simple line counting)
        'quality_check': 50_000,      # Can QC 50K rows/second WITH date parsing/validation
        'compression': 25_000_000,    # Can compress 25MB/second (gzip level 6)
        'upload': 5_000_000,          # Can upload 5MB/second (typical network)
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
            pbar = tqdm(total=file_size, unit='B', unit_scale=True, desc="Counting rows")

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
        compression_rate = FileAnalyzer.BENCHMARKS['compression'] * min(parallel_factor, 4)
        estimates['compression'] = (file_size_gb * 1024) / compression_rate if file_size_gb > 0 else 0

        # Upload to Snowflake - limited by network, not very parallel
        compressed_size_mb = file_size_gb * 1024 * 0.15  # Assume 15% compression ratio
        upload_rate = FileAnalyzer.BENCHMARKS['upload'] * min(parallel_factor, 8)
        estimates['upload'] = compressed_size_mb / upload_rate if compressed_size_mb > 0 else 0

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

class SnowflakeLoader:
    """Snowflake loading with streaming compression"""
    
    def __init__(self, connection_params: Dict):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug("Initializing Snowflake connection")
        try:
            self.conn = snowflake.connector.connect(**connection_params)
            self.cursor = self.conn.cursor()
            self.logger.info("Snowflake connection established")
        except Exception as e:
            self.logger.error("Failed to connect to Snowflake: {}".format(e))
            raise

    def load_file_to_stage_and_table(self, config: FileConfig):
        """Load TSV file to Snowflake with streaming compression"""
        self.logger.info("Loading {} to {}".format(config.file_path, config.table_name))

        try:
            # Validate file exists
            if not os.path.exists(config.file_path):
                raise FileNotFoundError("File not found: {}".format(config.file_path))

            print("Loading {} to {}...".format(config.file_path, config.table_name))

            # Use user stage with subdirectory (no need to create, @~ always exists)
            stage_name = "@~/tsv_stage/{}/".format(config.table_name)
            self.logger.debug("Using stage: {}".format(stage_name))

            # Stream compress file
            compressed_file = "{}.gz".format(config.file_path)
            if not os.path.exists(compressed_file):
                print("Compressing {} (streaming)...".format(config.file_path))
                self.logger.debug("Streaming compression to {}".format(compressed_file))
                start_time = time.time()
                
                # Stream compression with progress
                file_size = os.path.getsize(config.file_path)
                bytes_processed = 0

                with open(config.file_path, 'rb') as f_in:
                    with gzip.open(compressed_file, 'wb', compresslevel=1) as f_out:  # Level 1 for speed
                        # Stream in chunks
                        chunk_size = 1024 * 1024 * 10  # 10MB chunks
                        while True:
                            chunk = f_in.read(chunk_size)
                            if not chunk:
                                break
                            f_out.write(chunk)
                            bytes_processed += len(chunk)
                            
                            if bytes_processed % (100 * 1024 * 1024) == 0:  # Every 100MB
                                pct = (bytes_processed / file_size) * 100
                                self.logger.debug("Compression progress: {:.1f}%".format(pct))

                compression_time = time.time() - start_time
                self.logger.debug("Compression completed in {:.1f} seconds".format(compression_time))

            # PUT file to stage
            print("Uploading to Snowflake stage...")
            put_command = "PUT file://{} {} AUTO_COMPRESS=FALSE OVERWRITE=TRUE PARALLEL=8".format(
                compressed_file, stage_name)
            self.logger.debug("Executing PUT command with PARALLEL=8")
            self.cursor.execute(put_command)

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
            self.logger.debug("Executing COPY command")
            self.cursor.execute(copy_query.replace("VALIDATION_MODE = 'RETURN_ERRORS'", ""))

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
                 max_workers: int, skip_qc: bool, analysis_results: Dict) -> None:
    """Process files with streaming quality checks and loading"""
    logger.info("="*60)
    logger.info("Starting file processing (STREAMING MODE)")
    logger.info("  Files: {}".format(len(file_configs)))
    logger.info("  Workers: {}".format(max_workers))
    logger.info("  Skip QC: {}".format(skip_qc))
    logger.info("="*60)

    start_time = time.time()
    results = {}
    failed_files = []

    # Initialize progress tracker if available
    tracker = None
    if analysis_results and TQDM_AVAILABLE:
        tracker = ProgressTracker(
            len(file_configs),
            analysis_results['total_rows'],
            analysis_results['total_size_gb']
        )

    try:
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
                    loader = SnowflakeLoader(snowflake_params)
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

def main():
    logger.info("Main function starting")

    parser = argparse.ArgumentParser(description='Load TSV files to Snowflake with progress tracking')
    parser.add_argument('--config', type=str, help='Path to configuration JSON file')
    parser.add_argument('--base-path', type=str, default='.', help='Base path for TSV files')
    parser.add_argument('--month', type=str, help='Month to process (format: YYYY-MM)')
    parser.add_argument('--skip-qc', action='store_true', help='Skip quality checks')
    parser.add_argument('--max-workers', type=int, default=None, help='Maximum parallel workers')
    parser.add_argument('--analyze-only', action='store_true', help='Only analyze files and show estimates')
    parser.add_argument('--check-system', action='store_true', help='Check system capabilities')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging (already on by default)')

    args = parser.parse_args()

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

    # Load config and create file configs
    config = load_config(args.config)
    file_configs = create_file_configs(config, args.base_path, args.month)

    if not file_configs:
        logger.error("No files found matching patterns")
        print("ERROR: No files found matching the patterns in config")
        return 1

    # Print files to be processed
    print("\n=== Processing {} files ===".format(len(file_configs)))
    for fc in file_configs:
        date_range = "{} to {}".format(
            fc.expected_date_range[0].date(),
            fc.expected_date_range[1].date())
        print("  - {}: {} ({})".format(fc.table_name,
                                       os.path.basename(fc.file_path),
                                       date_range))

    # Verify files exist
    missing_files = [fc.file_path for fc in file_configs if not os.path.exists(fc.file_path)]
    if missing_files:
        logger.error("Missing files: {}".format(missing_files))
        print("\nERROR: The following files are missing:")
        for f in missing_files:
            print("  - {}".format(f))
        return 1

    # Analyze files
    analysis_results = analyze_files(file_configs, args.max_workers)

    if args.analyze_only:
        logger.info("Analysis complete (--analyze-only mode)")
        print("\n[Analysis only mode - not processing files]")
        return 0

    # Ask for confirmation
    print("\n" + "="*60)
    estimated_minutes = analysis_results['estimates']['total'] / 60
    response = input("Proceed with processing? (estimated {:.1f} minutes) [y/N]: ".format(estimated_minutes))

    if response.lower() != 'y':
        logger.info("Processing cancelled by user")
        print("Processing cancelled")
        return 0

    # Process files
    process_files(
        file_configs=file_configs,
        snowflake_params=config.get('snowflake', {}),
        max_workers=args.max_workers,
        skip_qc=args.skip_qc,
        analysis_results=analysis_results
    )

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
