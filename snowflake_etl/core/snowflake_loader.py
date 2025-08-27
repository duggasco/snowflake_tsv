"""
Snowflake loader with dependency injection for ETL operations.
Refactored to use ApplicationContext for shared resources.
"""

import os
import re
import gzip
import time
import logging
from typing import Optional, Dict, Any
from pathlib import Path

from ..models.file_config import FileConfig
from ..core.progress import ProgressTracker, ProgressPhase
from ..utils.snowflake_connection_v3 import SnowflakeConnectionManager


class SnowflakeLoader:
    """
    Manages Snowflake loading operations with streaming compression and async support.
    Uses dependency injection for connection and progress tracking.
    """
    
    # Configuration constants
    CHUNK_SIZE_MB = 10
    ASYNC_THRESHOLD_MB = 100
    COMPRESSION_LEVEL = 1  # Fast compression
    PARALLEL_UPLOADS = 4
    KEEPALIVE_INTERVAL_SEC = 240  # 4 minutes
    POLL_INTERVAL_SEC = 30
    MAX_WAIT_TIME_SEC = 7200  # 2 hours
    
    def __init__(self, 
                 connection_manager: SnowflakeConnectionManager,
                 progress_tracker: Optional[ProgressTracker] = None,
                 logger: Optional[logging.Logger] = None):
        """
        Initialize loader with injected dependencies.
        
        Args:
            connection_manager: Manages Snowflake connections
            progress_tracker: Optional progress tracking implementation
            logger: Optional logger instance
        """
        self.connection_manager = connection_manager
        self.progress_tracker = progress_tracker
        self.logger = logger or logging.getLogger(__name__)
        
        # Check warehouse size on initialization
        self._check_warehouse_size()
    
    def _check_warehouse_size(self):
        """Check and warn about warehouse size for performance."""
        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get current warehouse
                cursor.execute("SELECT CURRENT_WAREHOUSE()")
                current_wh = cursor.fetchone()[0]
                
                # Get warehouse details
                cursor.execute("SHOW WAREHOUSES")
                warehouses = cursor.fetchall()
                
                for wh in warehouses:
                    if wh[0] == current_wh:
                        wh_size = wh[2]
                        self.logger.info(f"Using warehouse: {current_wh} (Size: {wh_size})")
                        
                        if wh_size in ['X-Small', 'Small']:
                            warning_msg = (
                                f"WARNING: Warehouse '{current_wh}' is {wh_size}. "
                                f"For files >100MB, consider using: "
                                f"ALTER WAREHOUSE {current_wh} SET WAREHOUSE_SIZE = 'MEDIUM';"
                            )
                            self.logger.warning(warning_msg)
                            print(warning_msg)
                        break
                        
        except Exception as e:
            self.logger.warning(f"Could not check warehouse size: {e}")
    
    def load_file(self, config: FileConfig) -> int:
        """
        Load a TSV file to Snowflake with streaming compression.
        
        Args:
            config: File configuration with path and table details
            
        Returns:
            Number of rows loaded
            
        Raises:
            FileNotFoundError: If input file doesn't exist
            Exception: For Snowflake or processing errors
        """
        self.logger.info(f"Loading {config.file_path} to {config.table_name}")
        
        # Validate file exists
        if not os.path.exists(config.file_path):
            raise FileNotFoundError(f"File not found: {config.file_path}")
        
        compressed_file = None
        stage_name = None
        
        try:
            # Report progress phase
            if self.progress_tracker:
                self.progress_tracker.update_phase(ProgressPhase.COMPRESSION)
            
            # Compress file
            compressed_file = self._compress_file(config.file_path)
            
            # Upload to stage
            if self.progress_tracker:
                self.progress_tracker.update_phase(ProgressPhase.UPLOAD)
            stage_name = self._upload_to_stage(compressed_file, config.table_name)
            
            # Validate and copy data
            if self.progress_tracker:
                self.progress_tracker.update_phase(ProgressPhase.COPY)
            rows_loaded = self._copy_to_table(
                stage_name, 
                config.table_name,
                compressed_file
            )
            
            # Cleanup stage
            self._cleanup_stage(stage_name)
            
            self.logger.info(f"Successfully loaded {rows_loaded:,} rows to {config.table_name}")
            return rows_loaded
            
        except Exception as e:
            self.logger.error(f"Failed to load {config.table_name}: {e}")
            raise
            
        finally:
            # Cleanup compressed file
            if compressed_file and os.path.exists(compressed_file):
                self.logger.debug(f"Removing compressed file: {compressed_file}")
                os.remove(compressed_file)
    
    def _compress_file(self, file_path: str) -> str:
        """
        Compress file using streaming gzip compression.
        
        Args:
            file_path: Path to input file
            
        Returns:
            Path to compressed file
        """
        compressed_path = f"{file_path}.gz"
        
        # Check if already compressed
        if os.path.exists(compressed_path):
            if self._is_compression_valid(file_path, compressed_path):
                self.logger.info(f"Using existing compressed file: {compressed_path}")
                return compressed_path
            else:
                self.logger.warning("Invalid compressed file detected, recompressing")
                os.remove(compressed_path)
        
        # Stream compress with progress tracking
        file_size = os.path.getsize(file_path)
        file_size_mb = file_size / (1024 * 1024)
        
        self.logger.info(f"Compressing {file_size_mb:.1f} MB file")
        print(f"Compressing {file_path} ({file_size_mb:.1f} MB)...")
        
        start_time = time.time()
        bytes_processed = 0
        chunk_size = self.CHUNK_SIZE_MB * 1024 * 1024
        
        with open(file_path, 'rb') as f_in:
            with gzip.open(compressed_path, 'wb', compresslevel=self.COMPRESSION_LEVEL) as f_out:
                while True:
                    chunk = f_in.read(chunk_size)
                    if not chunk:
                        break
                    
                    f_out.write(chunk)
                    bytes_processed += len(chunk)
                    
                    # Update progress
                    if self.progress_tracker:
                        progress_pct = (bytes_processed / file_size) * 100
                        self.progress_tracker.update_progress(
                            compression_progress=progress_pct
                        )
        
        compression_time = time.time() - start_time
        compressed_size_mb = os.path.getsize(compressed_path) / (1024 * 1024)
        compression_ratio = (compressed_size_mb / file_size_mb) * 100
        
        self.logger.info(
            f"Compression complete in {compression_time:.1f}s: "
            f"{file_size_mb:.1f} MB -> {compressed_size_mb:.1f} MB "
            f"({compression_ratio:.1f}% of original)"
        )
        
        return compressed_path
    
    def _is_compression_valid(self, original_path: str, compressed_path: str) -> bool:
        """
        Check if existing compressed file has valid compression ratio.
        
        Args:
            original_path: Path to original file
            compressed_path: Path to compressed file
            
        Returns:
            True if compression ratio is reasonable (8-40%)
        """
        original_size = os.path.getsize(original_path)
        compressed_size = os.path.getsize(compressed_path)
        
        if original_size == 0:
            return False
        
        ratio = (compressed_size / original_size) * 100
        return 8 <= ratio <= 40
    
    def _upload_to_stage(self, file_path: str, table_name: str) -> str:
        """
        Upload compressed file to Snowflake stage.
        
        Args:
            file_path: Path to compressed file
            table_name: Target table name for stage directory
            
        Returns:
            Stage name/path
        """
        # Generate unique stage name to avoid conflicts
        timestamp = int(time.time() * 1000)
        file_basename = Path(file_path).stem.replace('.tsv', '')
        stage_name = f"@~/tsv_stage/{table_name}/{file_basename}_{timestamp}/"
        
        self.logger.info(f"Uploading to stage: {stage_name}")
        print(f"Uploading to Snowflake stage...")
        
        # Clean up old stages for this table
        self._cleanup_old_stages(table_name, file_basename)
        
        # Execute PUT command
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        put_command = (
            f"PUT file://{file_path} {stage_name} "
            f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE PARALLEL={self.PARALLEL_UPLOADS}"
        )
        
        start_time = time.time()
        
        with self.connection_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(put_command)
        
        upload_time = time.time() - start_time
        upload_rate = file_size_mb / upload_time if upload_time > 0 else 0
        
        self.logger.info(
            f"Upload complete in {upload_time:.1f}s ({upload_rate:.1f} MB/s)"
        )
        
        # Update progress
        if self.progress_tracker:
            self.progress_tracker.update_progress(upload_complete=True)
        
        return stage_name
    
    def _cleanup_old_stages(self, table_name: str, file_basename: str):
        """Remove old stage files for this table/file combination."""
        old_stage_pattern = f"@~/tsv_stage/{table_name}/{file_basename}_*/"
        
        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"REMOVE {old_stage_pattern}")
                self.logger.debug(f"Cleaned up old stages: {old_stage_pattern}")
        except Exception as e:
            self.logger.debug(f"No old stages to clean or cleanup failed: {e}")
    
    def _copy_to_table(self, stage_name: str, table_name: str, compressed_file: str) -> int:
        """
        Copy data from stage to table.
        Relies on ON_ERROR='ABORT_STATEMENT' to catch any data errors.
        
        Args:
            stage_name: Snowflake stage path
            table_name: Target table name
            compressed_file: Path to compressed file for size check
            
        Returns:
            Number of rows loaded
        """
        # Build COPY query
        copy_query = self._build_copy_query(table_name, stage_name)
        
        # Determine sync vs async based on file size
        compressed_size_mb = os.path.getsize(compressed_file) / (1024 * 1024)
        use_async = compressed_size_mb > self.ASYNC_THRESHOLD_MB
        
        # No validation - rely on ON_ERROR='ABORT_STATEMENT' during COPY
        
        # Estimate rows for progress tracking
        estimated_rows = int(compressed_size_mb * 50000)  # ~50K rows per MB
        
        # Execute COPY
        if use_async:
            self.logger.info(
                f"Using async COPY for large file ({compressed_size_mb:.1f} MB)"
            )
            return self._execute_copy_async(copy_query, table_name, estimated_rows)
        else:
            return self._execute_copy_sync(copy_query, table_name, estimated_rows)
    
    def _build_copy_query(self, table_name: str, stage_name: str) -> str:
        """Build COPY INTO query with appropriate settings."""
        return f"""
        COPY INTO {table_name}
        FROM {stage_name}
        FILE_FORMAT = (
            TYPE = 'CSV'
            FIELD_DELIMITER = '\\t'
            SKIP_HEADER = 0
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            ESCAPE_UNENCLOSED_FIELD = NONE
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
            REPLACE_INVALID_CHARACTERS = TRUE
            DATE_FORMAT = 'YYYY-MM-DD'
            TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
            NULL_IF = ('', 'NULL', 'null', '\\\\N')
        )
        ON_ERROR = 'ABORT_STATEMENT'
        PURGE = TRUE
        SIZE_LIMIT = 5368709120
        """
    
    def _execute_copy_sync(self, query: str, table_name: str, estimated_rows: int) -> int:
        """Execute COPY synchronously for smaller files."""
        print(f"Copying data to {table_name} (sync mode)...")
        self.logger.debug("Executing synchronous COPY")
        
        start_time = time.time()
        
        with self.connection_manager.get_connection() as conn:
            cursor = conn.cursor()
            copy_result = cursor.execute(query)
            
            # Extract rows loaded from result
            rows_loaded = self._extract_rows_loaded(copy_result)
        
        copy_time = time.time() - start_time
        rows_per_sec = rows_loaded / copy_time if copy_time > 0 else 0
        
        self.logger.info(
            f"COPY completed in {copy_time:.1f}s "
            f"({rows_loaded:,} rows at {rows_per_sec:,.0f} rows/sec)"
        )
        
        if self.progress_tracker:
            self.progress_tracker.update_progress(copy_complete=True)
        
        return rows_loaded
    
    def _execute_copy_async(self, query: str, table_name: str, estimated_rows: int) -> int:
        """
        Execute COPY asynchronously with keepalive for large files.
        
        Args:
            query: COPY query to execute
            table_name: Table name for logging
            estimated_rows: Estimated row count for progress
            
        Returns:
            Number of rows loaded
        """
        self.logger.info(
            f"Starting async COPY for {table_name} "
            f"(estimated {estimated_rows:,} rows)"
        )
        print(f"Executing async COPY for {table_name} (may take several minutes)...")
        
        with self.connection_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # Enable async queries
            cursor.execute("ALTER SESSION SET ABORT_DETACHED_QUERY = FALSE")
            
            # Start async query
            query_id = cursor.execute_async(query).get('queryId')
            self.logger.info(f"Async COPY started with query ID: {query_id}")
            
            # Poll for completion with keepalive
            rows_loaded = self._poll_async_query(conn, cursor, query_id, table_name)
            
            if self.progress_tracker:
                self.progress_tracker.update_progress(copy_complete=True)
            
            return rows_loaded
    
    def _poll_async_query(self, conn, cursor, query_id: str, table_name: str) -> int:
        """
        Poll async query status with keepalive.
        
        Returns:
            Number of rows loaded
        """
        start_time = time.time()
        last_keepalive = start_time
        last_status_update = start_time
        
        while True:
            elapsed = time.time() - start_time
            
            # Check timeout
            if elapsed > self.MAX_WAIT_TIME_SEC:
                raise Exception(
                    f"COPY operation timed out after {self.MAX_WAIT_TIME_SEC/60:.0f} minutes"
                )
            
            # Check query status
            status = conn.get_query_status(query_id)
            
            if not conn.is_still_running(status):
                break
            
            # Send keepalive to prevent timeout
            if time.time() - last_keepalive > self.KEEPALIVE_INTERVAL_SEC:
                self.logger.debug(f"Sending keepalive for query {query_id}")
                try:
                    cursor.get_results_from_sfqid(query_id)
                except:
                    pass  # Expected to fail while running
                last_keepalive = time.time()
            
            # Status update
            if time.time() - last_status_update > self.POLL_INTERVAL_SEC:
                elapsed_mins = elapsed / 60
                self.logger.info(f"COPY still running after {elapsed_mins:.1f} minutes")
                print(f"Still copying... ({elapsed_mins:.1f} minutes elapsed)")
                last_status_update = time.time()
            
            time.sleep(5)
        
        # Check for errors and get results
        conn.get_query_status_throw_if_error(query_id)
        copy_result = cursor.get_results_from_sfqid(query_id)
        
        rows_loaded = self._extract_rows_loaded(copy_result)
        
        total_time = time.time() - start_time
        self.logger.info(
            f"Async COPY completed in {total_time:.1f}s ({rows_loaded:,} rows)"
        )
        print(f"COPY completed: {rows_loaded:,} rows in {total_time/60:.1f} minutes")
        
        return rows_loaded
    
    def _extract_rows_loaded(self, result) -> int:
        """Extract row count from COPY result."""
        for row in result:
            if row[0] and 'rows_loaded' in str(row[0]).lower():
                match = re.search(r'(\d+)', str(row[0]))
                if match:
                    return int(match.group(1))
        return 0
    
    def _cleanup_stage(self, stage_name: str):
        """Remove stage after successful load."""
        try:
            with self.connection_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"REMOVE {stage_name}")
                self.logger.debug(f"Removed stage: {stage_name}")
        except Exception as e:
            self.logger.warning(f"Could not remove stage {stage_name}: {e}")